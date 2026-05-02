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

import { mkdtempSync, readFileSync, statSync } from "node:fs";
import { tmpdir } from "node:os";
import { join, resolve } from "node:path";
import { randomBytes } from "node:crypto";
import { execSync } from "node:child_process";

const API = "https://api.netlify.com/api/v1";
const PROJECT_ROOT = resolve(import.meta.dirname, "..", "..");
const TEMPLATE_DIR = join(PROJECT_ROOT, "netlify-claude-proxy");

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
    body: JSON.stringify([
      {
        key,
        scopes: ["builds", "functions", "runtime", "post-processing"],
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

function buildZip(srcDir: string): string {
  const tmp = mkdtempSync(join(tmpdir(), "netlify-deploy-"));
  const zipPath = join(tmp, "site.zip");
  // Use Python's stdlib zipfile (the system `zip` binary segfaults on this NixOS).
  const py = `
import zipfile, os, sys
src = sys.argv[1]
out = sys.argv[2]
exclude = ('.git', 'node_modules', '.netlify')
with zipfile.ZipFile(out, 'w', zipfile.ZIP_DEFLATED) as zf:
    for root, dirs, files in os.walk(src):
        dirs[:] = [d for d in dirs if d not in exclude]
        for f in files:
            full = os.path.join(root, f)
            arc = os.path.relpath(full, src)
            zf.write(full, arc)
`;
  execSync(`python3 -c ${JSON.stringify(py)} ${JSON.stringify(srcDir)} ${JSON.stringify(zipPath)}`, {
    stdio: "inherit",
  });
  console.error(`zip built: ${zipPath} (${statSync(zipPath).size} bytes)`);
  return zipPath;
}

async function deployZip(token: string, siteId: string, zipPath: string): Promise<{ id: string; ssl_url: string; state: string }> {
  const body = readFileSync(zipPath);
  const res = await fetch(`${API}/sites/${siteId}/deploys`, {
    method: "POST",
    headers: {
      Authorization: `Bearer ${token}`,
      "Content-Type": "application/zip",
    },
    body,
  });
  if (!res.ok) {
    throw new Error(`deploy upload failed ${res.status}: ${await res.text()}`);
  }
  return (await res.json()) as { id: string; ssl_url: string; state: string };
}

async function waitForDeploy(token: string, siteId: string, deployId: string) {
  for (let i = 0; i < 60; i++) {
    const d = await api<{ state: string; error_message?: string; ssl_url?: string }>(
      token,
      `/sites/${siteId}/deploys/${deployId}`,
    );
    if (d.state === "ready") return d;
    if (d.state === "error") throw new Error(`deploy failed: ${d.error_message}`);
    await new Promise((r) => setTimeout(r, 2000));
  }
  throw new Error("deploy timed out after 120s");
}

async function cmdDeploy(token: string, siteName?: string) {
  console.error("Step 1/5: getting account slug...");
  const accountSlug = await getAccountSlug(token, process.env.NETLIFY_ACCOUNT);
  console.error(`  account: ${accountSlug}`);

  console.error("Step 2/5: creating site...");
  const site = await createSite(token, siteName);
  console.error(`  site_id: ${site.id}`);
  console.error(`  url:     ${site.ssl_url}`);

  console.error("Step 3/5: setting PROXY_SECRET...");
  const secret = randomBytes(32).toString("hex");
  await setEnvVar(token, accountSlug, site.id, "PROXY_SECRET", secret);
  console.error("  PROXY_SECRET set");

  console.error("Step 4/5: building deploy zip...");
  const zipPath = buildZip(TEMPLATE_DIR);

  console.error("Step 5/5: uploading + waiting for ready...");
  const deploy = await deployZip(token, site.id, zipPath);
  await waitForDeploy(token, site.id, deploy.id);
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
