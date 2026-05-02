# Netlify Claude Proxy

OpenAI-compatible reverse proxy that exposes Anthropic Claude (Sonnet 4.6 + Opus 4.6) through Netlify's AI Gateway. Each Netlify free account grants 300 credits/month with no credit card required, and the AI Gateway auto-injects `ANTHROPIC_API_KEY` and `ANTHROPIC_BASE_URL` into Edge Function runtimes — no Anthropic API key of your own is needed.

## Endpoints

| Path | Description |
|---|---|
| `POST /v1/chat/completions` | OpenAI Chat Completions API (streaming + non-streaming) |
| `GET  /v1/models` | List available Claude models |
| `GET  /healthz` | Diagnostic — confirms AI Gateway env vars are injected |

## Architecture

```
Client (OpenAI SDK / Cline / Cursor)
    │ Authorization: Bearer <PROXY_SECRET>
    ▼
Netlify Edge Function (Deno)
    │ ANTHROPIC_API_KEY + ANTHROPIC_BASE_URL auto-injected
    ▼
Netlify AI Gateway → Anthropic API
```

Edge Functions are used (not regular Functions) because they have **no wall-clock timeout** — required for long Claude responses. Auth is a single shared secret (`PROXY_SECRET`) sent as a Bearer token, which is compatible with the OpenAI SDK's `apiKey` parameter.

## Manual deploy (per Netlify account)

```bash
# 1. Sign up at https://app.netlify.com (email only, no credit card)
# 2. Install the CLI
npm install -g netlify-cli

# 3. From this directory:
netlify login                              # opens browser, authenticate
netlify init                               # creates a new site under your account
netlify env:set PROXY_SECRET "$(openssl rand -hex 32)"
netlify deploy --prod                      # deploy to production

# 4. Note your site URL (e.g. https://abc-xyz.netlify.app) and the PROXY_SECRET.
```

## Bulk deploy via API (for multi-account pool)

See `../scripts/src/deploy-netlify.ts`. Generate a Personal Access Token in each
Netlify account (Applications → Personal access tokens), then run:

```bash
pnpm --filter @workspace/scripts exec tsx src/deploy-netlify.ts deploy <PAT> [siteName]
```

## Verify a deployed site

```bash
SITE=https://abc-xyz.netlify.app
SECRET=...your PROXY_SECRET...

# 1. Health check (confirms AI Gateway is wired)
curl -s "$SITE/healthz" | jq

# Expected:
# {
#   "status": "ok",
#   "aiGateway": {
#     "anthropicKeyInjected": true,
#     "anthropicBaseUrl": "https://ai-gateway.netlify.app/anthropic"
#   },
#   ...
# }

# 2. Non-streaming chat
curl -s "$SITE/v1/chat/completions" \
  -H "Authorization: Bearer $SECRET" \
  -H "Content-Type: application/json" \
  -d '{
    "model": "claude-sonnet-4-6",
    "messages": [{"role":"user","content":"Say hi in 5 words."}],
    "max_tokens": 64
  }' | jq

# 3. Streaming chat
curl -N -s "$SITE/v1/chat/completions" \
  -H "Authorization: Bearer $SECRET" \
  -H "Content-Type: application/json" \
  -d '{
    "model": "claude-sonnet-4-6",
    "messages": [{"role":"user","content":"Count 1 to 5."}],
    "max_tokens": 64,
    "stream": true
  }'
```

## OpenAI SDK example

```python
from openai import OpenAI

client = OpenAI(
    base_url="https://abc-xyz.netlify.app/v1",
    api_key="<PROXY_SECRET>",
)

resp = client.chat.completions.create(
    model="claude-sonnet-4-6",
    messages=[{"role": "user", "content": "Hello"}],
)
print(resp.choices[0].message.content)
```

## Notes

- **Credits are per Netlify team, not per site.** All sites under one account share the 300/month pool. To scale, register more accounts (each with its own PAT).
- The `PROXY_SECRET` prevents random people from burning your free credits if they discover the URL.
- Tool calling is not supported in v0.1 — text chat only.
