import { type Request, type Response, type NextFunction } from "express";
import { createProxyMiddleware } from "http-proxy-middleware";
import http from "http";

// ── Python 服务就绪状态 ──────────────────────────────────────────
let pythonReady = false;
let pythonReadyPromise: Promise<void> | null = null;

/**
 * 探测 Python /health（无需鉴权），任意 HTTP 响应代表服务已就绪。
 * 连接被拒绝（ECONNREFUSED）或超时 → false。
 */
function probePython(): Promise<boolean> {
  return new Promise((resolve) => {
    const req = http.get("http://localhost:8000/health", { timeout: 800 }, (res) => {
      res.resume();
      resolve(true);
    });
    req.on("timeout", () => { req.destroy(); resolve(false); });
    req.on("error", () => resolve(false));
  });
}

/**
 * 等待 Python 就绪（最多 timeoutMs 毫秒，每 500ms 探测一次）。
 * 多个并发请求共享同一个 Promise，不会重复轮询。
 */
function waitForPython(timeoutMs = 60000): Promise<void> {
  if (pythonReady) return Promise.resolve();
  if (pythonReadyPromise) return pythonReadyPromise;

  pythonReadyPromise = new Promise<void>((resolve) => {
    const start = Date.now();
    const check = async () => {
      if (await probePython()) {
        pythonReady = true;
        resolve();
        return;
      }
      if (Date.now() - start >= timeoutMs) {
        pythonReadyPromise = null;
        resolve();
        return;
      }
      setTimeout(check, 500);
    };
    check();
  });

  return pythonReadyPromise;
}

// ── 后台心跳（每 30 秒）：主动检测 Python 是否已崩溃 ─────────────
setInterval(async () => {
  if (!pythonReady) return; // 未就绪时 waitForPython 已在轮询，无需重复
  const alive = await probePython();
  if (!alive) {
    console.warn("[proxy] Python 心跳失败，重置就绪状态");
    pythonReady = false;
    pythonReadyPromise = null;
  }
}, 30_000);

// ── 代理错误处理器（共享逻辑）────────────────────────────────────
function onProxyError(_err: Error, _req: unknown, res: any) {
  // Python 连接失败 → 重置就绪状态，下次请求会重新探测
  pythonReady = false;
  pythonReadyPromise = null;
  if (!res.headersSent) {
    res.status(503).json({
      error: {
        message: "AI 服务正在启动中，请稍后重试",
        type: "server_error",
        code: "service_starting",
      },
    });
  }
}

// ── 代理实例 ──────────────────────────────────────────────────────
const pythonProxy = createProxyMiddleware({
  target: "http://localhost:8000",
  changeOrigin: true,
  proxyTimeout: 120_000,  // 2 分钟超时（兼容长流式请求）
  timeout: 120_000,
  on: { error: onProxyError },
});

const anthropicProxy = createProxyMiddleware({
  target: "http://localhost:8000",
  changeOrigin: true,
  pathRewrite: { "^/anthropic": "" },
  proxyTimeout: 120_000,
  timeout: 120_000,
  on: { error: onProxyError },
});

// ── 路由分发 ──────────────────────────────────────────────────────
export async function proxyMiddleware(req: Request, res: Response, next: NextFunction) {
  const path = req.path;

  if (path.startsWith("/anthropic")) {
    await waitForPython();
    return anthropicProxy(req, res, next);
  }

  if (
    path.startsWith("/v1") ||
    path.startsWith("/admin/") ||
    path === "/admin" ||
    path.startsWith("/key/") ||
    path === "/key" ||
    path.startsWith("/prizes") ||
    path.startsWith("/api/partner") ||
    path === "/health"
  ) {
    await waitForPython();
    return pythonProxy(req, res, next);
  }

  next();
}
