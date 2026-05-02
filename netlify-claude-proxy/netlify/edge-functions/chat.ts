// OpenAI Chat Completions ⇄ Anthropic Messages translator.
// Runs as a Netlify Edge Function (Deno runtime, no timeout limit).
// Credentials are auto-injected by Netlify AI Gateway via env vars:
//   - ANTHROPIC_API_KEY
//   - ANTHROPIC_BASE_URL (e.g. https://ai-gateway.netlify.app/anthropic)

interface OpenAIMessage {
  role: "system" | "user" | "assistant" | "tool";
  content: string | Array<{ type: string; text?: string; image_url?: { url: string } }>;
  name?: string;
  tool_call_id?: string;
}

interface OpenAIRequest {
  model: string;
  messages: OpenAIMessage[];
  max_tokens?: number;
  max_completion_tokens?: number;
  temperature?: number;
  top_p?: number;
  stream?: boolean;
  stop?: string | string[];
}

interface AnthropicContentBlock {
  type: "text" | "image";
  text?: string;
  source?: { type: "base64" | "url"; media_type?: string; data?: string; url?: string };
}

interface AnthropicMessage {
  role: "user" | "assistant";
  content: string | AnthropicContentBlock[];
}

interface AnthropicRequest {
  model: string;
  system?: string;
  messages: AnthropicMessage[];
  max_tokens: number;
  temperature?: number;
  top_p?: number;
  stream?: boolean;
  stop_sequences?: string[];
}

// ---------- Model alias mapping ----------
// Allow short aliases; pass through full IDs unchanged.
const MODEL_ALIASES: Record<string, string> = {
  "claude-opus-4-6": "claude-opus-4-6",
  "claude-opus": "claude-opus-4-6",
  "claude-opus-latest": "claude-opus-4-6",
  "claude-sonnet-4-6": "claude-sonnet-4-6",
  "claude-sonnet": "claude-sonnet-4-6",
  "claude-sonnet-latest": "claude-sonnet-4-6",
  "claude-haiku": "claude-haiku-4-5",
  "claude-haiku-latest": "claude-haiku-4-5",
};

function resolveModel(input: string): string {
  return MODEL_ALIASES[input] ?? input;
}

// ---------- OpenAI → Anthropic request ----------
function convertContent(content: OpenAIMessage["content"]): string | AnthropicContentBlock[] {
  if (typeof content === "string") return content;
  return content.map((part): AnthropicContentBlock => {
    if (part.type === "text") return { type: "text", text: part.text ?? "" };
    if (part.type === "image_url" && part.image_url?.url) {
      const url = part.image_url.url;
      if (url.startsWith("data:")) {
        const m = url.match(/^data:(.+?);base64,(.+)$/);
        if (m) {
          return {
            type: "image",
            source: { type: "base64", media_type: m[1], data: m[2] },
          };
        }
      }
      return { type: "image", source: { type: "url", url } };
    }
    return { type: "text", text: "" };
  });
}

function openaiToAnthropic(req: OpenAIRequest): AnthropicRequest {
  const systemParts: string[] = [];
  const messages: AnthropicMessage[] = [];

  for (const m of req.messages) {
    if (m.role === "system") {
      const txt = typeof m.content === "string"
        ? m.content
        : m.content.map((p) => p.text ?? "").join("");
      systemParts.push(txt);
    } else if (m.role === "user" || m.role === "assistant") {
      messages.push({ role: m.role, content: convertContent(m.content) });
    } else if (m.role === "tool") {
      // Demote tool results to user text for v0.1 (no tool-call support yet).
      const txt = typeof m.content === "string" ? m.content : JSON.stringify(m.content);
      messages.push({ role: "user", content: `[tool result] ${txt}` });
    }
  }

  // Merge consecutive same-role messages (Anthropic forbids them).
  // Preserve structured content blocks (images etc) — never flatten to text.
  const toBlocks = (c: string | AnthropicContentBlock[]): AnthropicContentBlock[] =>
    typeof c === "string" ? [{ type: "text", text: c }] : c;

  const merged: AnthropicMessage[] = [];
  for (const m of messages) {
    const last = merged[merged.length - 1];
    if (last && last.role === m.role) {
      const lhs = toBlocks(last.content);
      const rhs = toBlocks(m.content);
      // Insert a blank text block between merged turns for clarity.
      const sep: AnthropicContentBlock = { type: "text", text: "\n\n" };
      last.content = [...lhs, sep, ...rhs];
    } else {
      merged.push({ ...m });
    }
  }

  return {
    model: resolveModel(req.model),
    system: systemParts.length > 0 ? systemParts.join("\n\n") : undefined,
    messages: merged,
    max_tokens: req.max_tokens ?? req.max_completion_tokens ?? 4096,
    // Claude 4.x rejects requests with BOTH temperature and top_p set.
    // Prefer temperature (more commonly used); drop top_p when both are present.
    temperature: req.temperature,
    top_p: req.temperature !== undefined ? undefined : req.top_p,
    stream: req.stream,
    stop_sequences: req.stop
      ? Array.isArray(req.stop) ? req.stop : [req.stop]
      : undefined,
  };
}

// ---------- Anthropic non-stream → OpenAI ----------
interface AnthropicResponse {
  id: string;
  type: "message";
  role: "assistant";
  model: string;
  content: Array<{ type: string; text?: string }>;
  stop_reason: string;
  usage: { input_tokens: number; output_tokens: number };
}

function anthropicToOpenAI(resp: AnthropicResponse, requestedModel: string) {
  const text = resp.content
    .filter((b) => b.type === "text")
    .map((b) => b.text ?? "")
    .join("");

  const stopMap: Record<string, string> = {
    end_turn: "stop",
    max_tokens: "length",
    stop_sequence: "stop",
    tool_use: "tool_calls",
  };

  return {
    id: resp.id,
    object: "chat.completion",
    created: Math.floor(Date.now() / 1000),
    model: requestedModel,
    choices: [
      {
        index: 0,
        message: { role: "assistant", content: text },
        finish_reason: stopMap[resp.stop_reason] ?? "stop",
      },
    ],
    usage: {
      prompt_tokens: resp.usage.input_tokens,
      completion_tokens: resp.usage.output_tokens,
      total_tokens: resp.usage.input_tokens + resp.usage.output_tokens,
    },
  };
}

// ---------- Anthropic SSE → OpenAI SSE ----------
async function* parseAnthropicSSE(body: ReadableStream<Uint8Array>): AsyncGenerator<any> {
  const reader = body.getReader();
  const decoder = new TextDecoder();
  let buffer = "";

  while (true) {
    const { value, done } = await reader.read();
    if (done) break;
    // Normalize CRLF → LF so the \n\n splitter works on either style.
    buffer += decoder.decode(value, { stream: true }).replace(/\r\n/g, "\n");

    let nlIdx;
    while ((nlIdx = buffer.indexOf("\n\n")) !== -1) {
      const block = buffer.slice(0, nlIdx);
      buffer = buffer.slice(nlIdx + 2);

      let dataLine = "";
      for (const line of block.split("\n")) {
        if (line.startsWith("data:")) dataLine += line.slice(5).trim();
      }
      if (!dataLine) continue;
      try {
        yield JSON.parse(dataLine);
      } catch {
        // Ignore malformed JSON
      }
    }
  }
}

function makeOpenAIChunk(
  id: string,
  model: string,
  delta: Record<string, unknown>,
  finishReason: string | null = null,
) {
  return {
    id,
    object: "chat.completion.chunk",
    created: Math.floor(Date.now() / 1000),
    model,
    choices: [{ index: 0, delta, finish_reason: finishReason }],
  };
}

async function streamAnthropicToOpenAI(
  upstream: Response,
  requestedModel: string,
): Promise<Response> {
  if (!upstream.body) {
    return new Response("Upstream returned no body", { status: 502 });
  }

  const stopMap: Record<string, string> = {
    end_turn: "stop",
    max_tokens: "length",
    stop_sequence: "stop",
    tool_use: "tool_calls",
  };

  const encoder = new TextEncoder();
  const id = `chatcmpl-${crypto.randomUUID()}`;
  let openedRole = false;
  let sawError = false;

  const stream = new ReadableStream({
    async start(controller) {
      try {
        for await (const evt of parseAnthropicSSE(upstream.body!)) {
          if (evt.type === "message_start") {
            controller.enqueue(
              encoder.encode(
                `data: ${JSON.stringify(
                  makeOpenAIChunk(id, requestedModel, { role: "assistant", content: "" }),
                )}\n\n`,
              ),
            );
            openedRole = true;
          } else if (evt.type === "content_block_delta") {
            const text = evt.delta?.text ?? "";
            if (text) {
              controller.enqueue(
                encoder.encode(
                  `data: ${JSON.stringify(
                    makeOpenAIChunk(id, requestedModel, { content: text }),
                  )}\n\n`,
                ),
              );
            }
          } else if (evt.type === "message_delta") {
            const reason = evt.delta?.stop_reason;
            if (reason) {
              controller.enqueue(
                encoder.encode(
                  `data: ${JSON.stringify(
                    makeOpenAIChunk(id, requestedModel, {}, stopMap[reason] ?? "stop"),
                  )}\n\n`,
                ),
              );
            }
          } else if (evt.type === "message_stop") {
            // emitted below as [DONE]
          } else if (evt.type === "error") {
            sawError = true;
            controller.enqueue(
              encoder.encode(
                `data: ${JSON.stringify({ error: evt.error ?? evt })}\n\n`,
              ),
            );
            // Do NOT emit [DONE] — surface failure clearly to the client.
            break;
          }
        }
        if (!sawError && !openedRole) {
          controller.enqueue(
            encoder.encode(
              `data: ${JSON.stringify(
                makeOpenAIChunk(id, requestedModel, { role: "assistant", content: "" }),
              )}\n\n`,
            ),
          );
        }
        if (!sawError) {
          controller.enqueue(encoder.encode(`data: [DONE]\n\n`));
        }
        controller.close();
      } catch (err) {
        controller.error(err);
      }
    },
  });

  return new Response(stream, {
    headers: {
      "content-type": "text/event-stream; charset=utf-8",
      "cache-control": "no-cache, no-transform",
      "x-accel-buffering": "no",
      "access-control-allow-origin": "*",
    },
  });
}

// ---------- Main handler ----------
export default async (req: Request): Promise<Response> => {
  if (req.method === "OPTIONS") {
    return new Response(null, {
      status: 204,
      headers: {
        "access-control-allow-origin": "*",
        "access-control-allow-methods": "POST, OPTIONS",
        "access-control-allow-headers": "authorization, content-type",
      },
    });
  }

  if (req.method !== "POST") {
    return new Response(
      JSON.stringify({ error: { message: "method not allowed" } }),
      { status: 405, headers: { "content-type": "application/json" } },
    );
  }

  // ---- Auth: Bearer <PROXY_SECRET> (FAIL-CLOSED) ----
  const expected = Deno.env.get("PROXY_SECRET");
  if (!expected) {
    return new Response(
      JSON.stringify({ error: { message: "PROXY_SECRET not configured on this site; refusing to serve" } }),
      { status: 503, headers: { "content-type": "application/json" } },
    );
  }
  {
    const auth = req.headers.get("authorization") ?? "";
    if (auth !== `Bearer ${expected}`) {
      return new Response(
        JSON.stringify({ error: { message: "unauthorized" } }),
        { status: 401, headers: { "content-type": "application/json" } },
      );
    }
  }

  // ---- Read AI Gateway env ----
  const apiKey = Deno.env.get("ANTHROPIC_API_KEY");
  const baseUrl = Deno.env.get("ANTHROPIC_BASE_URL") ?? "https://api.anthropic.com";
  if (!apiKey) {
    return new Response(
      JSON.stringify({
        error: {
          message:
            "ANTHROPIC_API_KEY not present — AI Gateway not enabled or function not running on Netlify",
        },
      }),
      { status: 500, headers: { "content-type": "application/json" } },
    );
  }

  // ---- Parse + translate ----
  let openaiReq: OpenAIRequest;
  try {
    openaiReq = await req.json();
  } catch {
    return new Response(
      JSON.stringify({ error: { message: "invalid JSON body" } }),
      { status: 400, headers: { "content-type": "application/json" } },
    );
  }

  if (!openaiReq.model || !Array.isArray(openaiReq.messages)) {
    return new Response(
      JSON.stringify({ error: { message: "model and messages are required" } }),
      { status: 400, headers: { "content-type": "application/json" } },
    );
  }

  const requestedModel = openaiReq.model;
  const anthropicReq = openaiToAnthropic(openaiReq);

  // ---- Forward to Anthropic via AI Gateway ----
  const upstream = await fetch(`${baseUrl.replace(/\/$/, "")}/v1/messages`, {
    method: "POST",
    headers: {
      "content-type": "application/json",
      "x-api-key": apiKey,
      "anthropic-version": "2023-06-01",
    },
    body: JSON.stringify(anthropicReq),
  });

  if (!upstream.ok) {
    const errText = await upstream.text();
    return new Response(
      JSON.stringify({
        error: {
          message: `upstream ${upstream.status}: ${errText.slice(0, 500)}`,
          type: "upstream_error",
        },
      }),
      { status: upstream.status, headers: { "content-type": "application/json" } },
    );
  }

  if (anthropicReq.stream) {
    return streamAnthropicToOpenAI(upstream, requestedModel);
  }

  const data = (await upstream.json()) as AnthropicResponse;
  return Response.json(anthropicToOpenAI(data, requestedModel));
};

export const config = { path: "/v1/chat/completions" };
