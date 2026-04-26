import express, { type Express } from "express";
import cors from "cors";
import pinoHttp from "pino-http";
import path from "path";
import fs from "fs";
import { createProxyMiddleware } from "http-proxy-middleware";
import router from "./routes";
import { proxyMiddleware } from "./routes/proxy";
import { logger } from "./lib/logger";

const app: Express = express();

app.use(
  pinoHttp({
    logger,
    serializers: {
      req(req) {
        return {
          id: req.id,
          method: req.method,
          url: req.url?.split("?")[0],
        };
      },
      res(res) {
        return {
          statusCode: res.statusCode,
        };
      },
    },
  }),
);
app.use(cors());

// ── Admin-panel 路由 ────────────────────────────────────────────────────────
// 生产模式：dist 存在时直接服务静态文件 + SPA fallback
// 开发模式：反向代理到 Vite dev server（固定端口 20130）
const adminPanelDist = path.resolve(
  process.cwd(),
  "..",
  "..",
  "artifacts",
  "admin-panel",
  "dist",
  "public",
);

if (fs.existsSync(adminPanelDist)) {
  // 静态资源（assets/、favicon 等）
  app.use("/admin-panel", express.static(adminPanelDist, { index: false }));
  // SPA fallback：所有 /admin-panel/* 未命中静态文件的请求都返回 index.html
  app.get("/admin-panel", (_req, res) => {
    res.sendFile(path.join(adminPanelDist, "index.html"));
  });
  app.get("/admin-panel/*splat", (_req, res) => {
    res.sendFile(path.join(adminPanelDist, "index.html"));
  });
} else {
  // 开发模式：转发到 Vite dev server
  const VITE_PORT = process.env.ADMIN_PANEL_DEV_PORT ?? "20130";
  const viteTarget = `http://localhost:${VITE_PORT}`;

  // /admin-panel/* → Vite
  // Express 会剥去 /admin-panel 前缀，pathRewrite 把它补回去，
  // 这样 Vite 收到的仍然是 /admin-panel/@react-refresh 等完整路径
  app.use(
    "/admin-panel",
    createProxyMiddleware({
      target: viteTarget,
      changeOrigin: true,
      ws: true,
      pathRewrite: (path: string) => `/admin-panel${path}`,
      logger: console,
    }),
  );

  // /@vite/、/@react-refresh、/@replit/ 等 Vite 内部路径
  // 不挂在子路径下，pathFilter 保留完整路径转发
  app.use(
    createProxyMiddleware({
      target: viteTarget,
      changeOrigin: true,
      pathFilter: (path: string) => path.startsWith("/@"),
      logger: console,
    }),
  );
}

app.use(proxyMiddleware);

app.use(express.json());
app.use(express.urlencoded({ extended: true }));
app.use("/api", router);

export default app;
