// OpenAI-compatible /v1/models endpoint.
// Lists Claude models available through Netlify AI Gateway.

const MODELS = [
  "claude-opus-4-6",
  "claude-opus-4-5",
  "claude-sonnet-4-6",
  "claude-sonnet-4-5",
  "claude-haiku-4-5",
];

export default async (req: Request) => {
  const expected = Deno.env.get("PROXY_SECRET");
  if (!expected) {
    return new Response(
      JSON.stringify({ error: { message: "PROXY_SECRET not configured on this site; refusing to serve" } }),
      { status: 503, headers: { "content-type": "application/json" } },
    );
  }
  const auth = req.headers.get("authorization") ?? "";
  if (auth !== `Bearer ${expected}`) {
    return new Response(JSON.stringify({ error: { message: "unauthorized" } }), {
      status: 401,
      headers: { "content-type": "application/json" },
    });
  }

  return Response.json({
    object: "list",
    data: MODELS.map((id) => ({
      id,
      object: "model",
      created: Math.floor(Date.now() / 1000),
      owned_by: "anthropic",
    })),
  });
};

export const config = { path: "/v1/models" };
