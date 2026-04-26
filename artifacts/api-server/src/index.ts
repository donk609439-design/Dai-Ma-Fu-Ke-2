import { spawn } from "child_process";
import path from "path";
import app from "./app";
import { logger } from "./lib/logger";

const rawPort = process.env["PORT"];

if (!rawPort) {
  throw new Error(
    "PORT environment variable is required but was not provided.",
  );
}

const port = Number(rawPort);

if (Number.isNaN(port) || port <= 0) {
  throw new Error(`Invalid PORT value: "${rawPort}"`);
}

function startPythonService() {
  const { existsSync } = require("fs");

  // 在生产（从 monorepo 根目录启动）和开发（从 artifacts/api-server/ 启动）两种情况下
  // 正确找到 monorepo 根目录：检查哪个路径包含 jetbrainsai2api 目录
  const repoRoot = (() => {
    const candidates = [
      process.cwd(),
      path.resolve(process.cwd(), "..", ".."),
      path.resolve(process.cwd(), ".."),
      "/home/runner/workspace",
    ];
    for (const c of candidates) {
      if (existsSync(path.join(c, "jetbrainsai2api"))) return c;
    }
    return process.cwd();
  })();

  const pythonDir = path.join(repoRoot, "jetbrainsai2api");

  // Python 二进制：优先环境变量，其次查 .pythonlibs，最后回退系统 python3
  const pythonBin =
    process.env.PYTHON_BIN ||
    (() => {
      const binCandidates = [
        path.join(repoRoot, ".pythonlibs", "bin", "python3"),
        "/home/runner/workspace/.pythonlibs/bin/python3",
        "/usr/bin/python3",
        "/usr/local/bin/python3",
      ];
      for (const c of binCandidates) {
        if (existsSync(c)) return c;
      }
      return "python3"; // 最后回退
    })();

  logger.info({ pythonDir, pythonBin, repoRoot }, "Starting Python AI service");

  // 将 .pythonlibs/bin 加入 PATH，只有该目录存在时才加入 PYTHONPATH
  const pythonLibsBin = path.join(repoRoot, ".pythonlibs", "bin");
  const pythonSitePkgs = path.join(repoRoot, ".pythonlibs", "lib", "python3.11", "site-packages");
  const childEnv: Record<string, string> = {
    ...process.env,
    PATH: `${pythonLibsBin}:${process.env.PATH ?? ""}`,
  };
  if (existsSync(pythonSitePkgs)) {
    childEnv["PYTHONPATH"] = pythonSitePkgs;
  }

  const py = spawn(
    pythonBin,
    ["-m", "uvicorn", "main:app", "--host", "0.0.0.0", "--port", "8000"],
    {
      cwd: pythonDir,
      stdio: "inherit",
      env: childEnv,
    },
  );

  py.on("error", (err) => {
    logger.error({ err }, "Failed to start Python AI service");
  });

  py.on("exit", (code, signal) => {
    if (code !== 0) {
      logger.warn({ code, signal }, "Python AI service exited, restarting in 3s...");
      setTimeout(startPythonService, 3000);
    }
  });

  process.on("exit", () => py.kill());
  process.on("SIGTERM", () => { py.kill(); process.exit(0); });
  process.on("SIGINT",  () => { py.kill(); process.exit(0); });
}

startPythonService();

app.listen(port, (err) => {
  if (err) {
    logger.error({ err }, "Error listening on port");
    process.exit(1);
  }

  logger.info({ port }, "Server listening");
});
