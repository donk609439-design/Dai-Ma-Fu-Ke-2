// Health check + auth-required diagnostic endpoint.
// Returns whether AI Gateway environment variables are present.

export default async () => {
  const hasKey = !!Deno.env.get("ANTHROPIC_API_KEY");
  const baseUrl = Deno.env.get("ANTHROPIC_BASE_URL") ?? null;
  const hasProxySecret = !!Deno.env.get("PROXY_SECRET");

  return Response.json({
    status: "ok",
    aiGateway: {
      anthropicKeyInjected: hasKey,
      anthropicBaseUrl: baseUrl,
    },
    proxySecretConfigured: hasProxySecret,
    timestamp: new Date().toISOString(),
  });
};

export const config = { path: "/healthz" };
