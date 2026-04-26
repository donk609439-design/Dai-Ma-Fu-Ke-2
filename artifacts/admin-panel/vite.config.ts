import { defineConfig, type Plugin } from "vite";
import react from "@vitejs/plugin-react";
import tailwindcss from "@tailwindcss/vite";
import path from "path";
import runtimeErrorOverlay from "@replit/vite-plugin-runtime-error-modal";

const rawPort = process.env.PORT;
const isBuild = process.argv.includes("build");

if (!rawPort && !isBuild) {
  throw new Error(
    "PORT environment variable is required but was not provided.",
  );
}

const port = rawPort ? Number(rawPort) : 20130;

if (!isBuild && (Number.isNaN(port) || port <= 0)) {
  throw new Error(`Invalid PORT value: "${rawPort}"`);
}

const basePath = process.env.BASE_PATH ?? "/admin-panel/";

/**
 * SPA fallback 插件：把所有不是静态资源、不是 Vite 内部路径的请求
 * 重写到 basePath，让 Vite 返回 index.html。
 *
 * 同时处理两种场景：
 *   1. 直连 Vite：请求带完整前缀  /admin-panel/accounts
 *   2. 经 Express 代理：前缀已被 app.use("/admin-panel",...) 剥离 → /accounts
 */
function spaFallbackPlugin(): Plugin {
  return {
    name: "spa-fallback",
    configureServer(server) {
      server.middlewares.use((req, _res, next) => {
        const url = (req.url ?? "/").split("?")[0];
        const isStaticAsset = /\.[a-zA-Z0-9]{1,10}$/.test(url);
        // 包含 /@（含 /admin-panel/@react-refresh 等带 base 的内部路径）
        const isViteInternal =
          url.includes("/@") ||
          url.startsWith("/__vite") ||
          url.startsWith("/node_modules/");

        if (
          !isStaticAsset &&
          !isViteInternal &&
          url !== basePath &&
          url !== `${basePath}index.html`
        ) {
          req.url = basePath;
        }
        next();
      });
    },
  };
}

export default defineConfig({
  base: basePath,
  plugins: [
    spaFallbackPlugin(),
    react(),
    tailwindcss(),
    runtimeErrorOverlay(),
    ...(process.env.NODE_ENV !== "production" &&
    process.env.REPL_ID !== undefined
      ? [
          await import("@replit/vite-plugin-cartographer").then((m) =>
            m.cartographer({
              root: path.resolve(import.meta.dirname, ".."),
            }),
          ),
          await import("@replit/vite-plugin-dev-banner").then((m) =>
            m.devBanner(),
          ),
        ]
      : []),
  ],
  resolve: {
    alias: {
      "@": path.resolve(import.meta.dirname, "src"),
      "@assets": path.resolve(import.meta.dirname, "..", "..", "attached_assets"),
    },
    dedupe: ["react", "react-dom"],
  },
  root: path.resolve(import.meta.dirname),
  build: {
    outDir: path.resolve(import.meta.dirname, "dist/public"),
    emptyOutDir: true,
  },
  server: {
    port,
    host: "0.0.0.0",
    allowedHosts: true,
    proxy: {
      "/v1": { target: "http://localhost:8080", changeOrigin: true },
      "/admin/": { target: "http://localhost:8080", changeOrigin: true },
      "/key/": { target: "http://localhost:8080", changeOrigin: true },
      "/anthropic/": { target: "http://localhost:8080", changeOrigin: true },
    },
    fs: {
      strict: true,
      deny: ["**/.*"],
    },
  },
  preview: {
    port,
    host: "0.0.0.0",
    allowedHosts: true,
  },
});
