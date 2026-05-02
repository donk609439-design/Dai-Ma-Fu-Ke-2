/**
 * Bulk-deploy the Netlify Claude proxy to a Netlify account using its
 * Personal Access Token (PAT) and the Netlify REST API.
 *
 * Usage:
 *   tsx scripts/src/deploy-netlify.ts deploy <PAT> [siteName]
 *   tsx scripts/src/deploy-netlify.ts list   <PAT>
 *   tsx scripts/src/deploy-netlify.ts status <PAT> <siteId>
 *
 * Generates a PROXY_SECRET, creates a new site, sets the env var, and
 * uploads a ZIP build of `netlify-claude-proxy/`. Prints the site URL
 * and secret so you can record them in your account-pool database.
 */

import { join, resolve } from "node:path";
import { randomBytes } from "node:crypto";
import { execSync } from "node:child_process";

const API = "https://api.netlify.com/api/v1";
const PROJECT_ROOT = resolve(import.meta.dirname, "..", "..");
const TEMPLATE_DIR = join(PROJECT_ROOT, "netlify-claude-proxy");
const NETLIFY_CLI = resolve(import.meta.dirname, "..", "node_modules", ".bin", "netlify");

interface NetlifySite {
  id: string;
  name: string;
  ssl_url: string;
  url: string;
}

async function api<T>(token: string, path: string, init: RequestInit = {}): Promise<T> {
  const res = await fetch(`${API}${path}`, {
    ...init,
    headers: {
      Authorization: `Bearer ${token}`,
      ...(init.headers ?? {}),
    },
  });
  if (!res.ok) {
    const text = await res.text();
    throw new Error(`Netlify API ${res.status} ${path}: ${text}`);
  }
  return (await res.json()) as T;
}

async function listSites(token: string): Promise<NetlifySite[]> {
  return api<NetlifySite[]>(token, "/sites?per_page=100");
}

async function getSite(token: string, siteId: string): Promise<NetlifySite> {
  return api<NetlifySite>(token, `/sites/${siteId}`);
}

async function createSite(token: string, name?: string): Promise<NetlifySite> {
  return api<NetlifySite>(token, "/sites", {
    method: "POST",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify(name ? { name } : {}),
  });
}

async function setEnvVar(
  token: string,
  accountSlug: string,
  siteId: string,
  key: string,
  value: string,
) {
  // Netlify env-vars API is account-scoped with a site_id query param.
  return api(token, `/accounts/${accountSlug}/env?site_id=${siteId}`, {
    method: "POST",
    headers: { "Content-Type": "application/json" },
    // Free tier rejects explicit scopes ("Upgrade your Netlify account...").
    // Omitting scopes => Netlify defaults to all scopes, which is what we want.
    body: JSON.stringify([
      {
        key,
        values: [{ context: "all", value }],
      },
    ]),
  });
}

async function getAccountSlug(token: string, preferred?: string): Promise<string> {
  const accounts = await api<Array<{ slug: string; name: string }>>(token, "/accounts");
  if (!accounts.length) throw new Error("no accounts found for this PAT");
  if (preferred) {
    const hit = accounts.find((a) => a.slug === preferred || a.name === preferred);
    if (!hit) {
      const list = accounts.map((a) => `${a.slug} (${a.name})`).join(", ");
      throw new Error(`account "${preferred}" not found; available: ${list}`);
    }
    return hit.slug;
  }
  if (accounts.length > 1) {
    const list = accounts.map((a) => `${a.slug} (${a.name})`).join(", ");
    throw new Error(
      `PAT has access to multiple accounts; pass NETLIFY_ACCOUNT=<slug> env var. Available: ${list}`,
    );
  }
  return accounts[0].slug;
}

// Deploy via Netlify CLI — required for edge functions, since the raw zip
// REST API treats uploads as static publish content and ignores netlify.toml
// + edge_functions/. CLI invokes @netlify/edge-bundler properly.
function deployViaCLI(token: string, siteId: string, srcDir: string) {
  execSync(
    `${JSON.stringify(NETLIFY_CLI)} deploy --prod --site=${JSON.stringify(siteId)} --auth=${JSON.stringify(token)} --message="auto-deploy"`,
    {
      stdio: "inherit",
      cwd: srcDir,
      env: { ...process.env, NETLIFY_AUTH_TOKEN: token },
    },
  );
}

async function cmdDeploy(token: string, siteName?: string) {
  const reuseSiteId = process.env.NETLIFY_SITE_ID;
  const reuseSecret = process.env.NETLIFY_REUSE_SECRET;

  console.error("Step 1/4: getting account slug...");
  const accountSlug = await getAccountSlug(token, process.env.NETLIFY_ACCOUNT);
  console.error(`  account: ${accountSlug}`);

  let site: NetlifySite;
  let secret: string;

  if (reuseSiteId) {
    console.error(`Step 2/4: reusing existing site ${reuseSiteId}...`);
    site = await getSite(token, reuseSiteId);
    console.error(`  url:     ${site.ssl_url}`);
    if (reuseSecret) {
      secret = reuseSecret;
      console.error("  reusing existing PROXY_SECRET (provided via env)");
    } else {
      secret = randomBytes(32).toString("hex");
      console.error("Step 3/4: rotating PROXY_SECRET...");
      await setEnvVar(token, accountSlug, site.id, "PROXY_SECRET", secret);
      console.error("  PROXY_SECRET rotated");
    }
  } else {
    console.error("Step 2/4: creating site...");
    site = await createSite(token, siteName);
    console.error(`  site_id: ${site.id}`);
    console.error(`  url:     ${site.ssl_url}`);

    console.error("Step 3/4: setting PROXY_SECRET...");
    secret = randomBytes(32).toString("hex");
    await setEnvVar(token, accountSlug, site.id, "PROXY_SECRET", secret);
    console.error("  PROXY_SECRET set");
  }

  console.error("Step 4/4: deploying via Netlify CLI (bundles edge functions)...");
  deployViaCLI(token, site.id, TEMPLATE_DIR);
  console.error("  deploy ready");

  // Final summary on stdout (machine-readable JSON) so you can pipe to a DB.
  const result = {
    account_slug: accountSlug,
    site_id: site.id,
    site_url: site.ssl_url,
    proxy_secret: secret,
    healthz: `${site.ssl_url}/healthz`,
    chat_endpoint: `${site.ssl_url}/v1/chat/completions`,
  };
  console.log(JSON.stringify(result, null, 2));
}

async function cmdList(token: string) {
  const sites = await listSites(token);
  console.log(JSON.stringify(sites.map((s) => ({ id: s.id, name: s.name, url: s.ssl_url })), null, 2));
}

async function cmdStatus(token: string, siteId: string) {
  const site = await getSite(token, siteId);
  console.log(JSON.stringify(site, null, 2));
}

async function cmdVerify(siteUrl: string, secret: string) {
  const base = siteUrl.replace(/\/$/, "");
  console.error(`[1/3] GET ${base}/healthz`);
  const health = (await fetch(`${base}/healthz`).then((r) => r.json())) as {
    aiGateway?: { anthropicKeyInjected?: boolean; anthropicBaseUrl?: string | null };
  };
  console.error(JSON.stringify(health, null, 2));
  if (!health?.aiGateway?.anthropicKeyInjected) {
    console.error("\n❌ AI Gateway env vars not injected — abort.");
    process.exit(2);
  }

  console.error(`\n[2/3] POST ${base}/v1/chat/completions  (non-streaming)`);
  const r1 = await fetch(`${base}/v1/chat/completions`, {
    method: "POST",
    headers: {
      "content-type": "application/json",
      authorization: `Bearer ${secret}`,
    },
    body: JSON.stringify({
      model: "claude-sonnet-4-6",
      messages: [{ role: "user", content: "Reply with the single word: pong" }],
      max_tokens: 16,
    }),
  });
  console.error(`status: ${r1.status}`);
  const j1 = await r1.json();
  console.error(JSON.stringify(j1, null, 2));

  console.error(`\n[3/3] POST ${base}/v1/chat/completions  (streaming)`);
  const r2 = await fetch(`${base}/v1/chat/completions`, {
    method: "POST",
    headers: {
      "content-type": "application/json",
      authorization: `Bearer ${secret}`,
    },
    body: JSON.stringify({
      model: "claude-sonnet-4-6",
      messages: [{ role: "user", content: "Count 1 to 3, comma-separated, no extra words." }],
      max_tokens: 32,
      stream: true,
    }),
  });
  console.error(`status: ${r2.status}`);
  if (!r2.body) {
    console.error("no body");
    process.exit(2);
  }
  const reader = r2.body.getReader();
  const decoder = new TextDecoder();
  let chunks = 0;
  while (true) {
    const { value, done } = await reader.read();
    if (done) break;
    process.stderr.write(decoder.decode(value));
    chunks++;
  }
  console.error(`\n\n✅ verify complete (${chunks} stream reads)`);
}

async function main() {
  const [cmd, ...rest] = process.argv.slice(2);
  if (!cmd) {
    console.error("Usage:");
    console.error("  tsx src/deploy-netlify.ts deploy <PAT> [siteName]");
    console.error("  tsx src/deploy-netlify.ts list   <PAT>");
    console.error("  tsx src/deploy-netlify.ts status <PAT> <siteId>");
    console.error("  tsx src/deploy-netlify.ts verify <siteUrl> <PROXY_SECRET>");
    process.exit(1);
  }
  if (cmd === "deploy") {
    const [token, name] = rest;
    if (!token) throw new Error("deploy requires <PAT>");
    return cmdDeploy(token, name);
  }
  if (cmd === "list") {
    const [token] = rest;
    if (!token) throw new Error("list requires <PAT>");
    return cmdList(token);
  }
  if (cmd === "status") {
    const [token, siteId] = rest;
    if (!token || !siteId) throw new Error("status requires <PAT> <siteId>");
    return cmdStatus(token, siteId);
  }
  if (cmd === "verify") {
    const [url, secret] = rest;
    if (!url || !secret) throw new Error("verify requires <siteUrl> <PROXY_SECRET>");
    return cmdVerify(url, secret);
  }
  console.error(`unknown command: ${cmd}`);
  process.exit(1);
}

main().catch((err) => {
  console.error(err);
  process.exit(1);
});
