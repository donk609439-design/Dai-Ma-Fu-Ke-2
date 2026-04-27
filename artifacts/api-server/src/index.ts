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

let pyProcess: ReturnType<typeof spawn> | null = null;
let httpServer: any = null;
let shuttingDown = false;

async function gracefulShutdown(signal: string) {
  if (shuttingDown) return;
  shuttingDown = true;
  logger.info({ signal }, "Graceful shutdown initiated");

  if (httpServer) httpServer.close(() => logger.info("HTTP server closed"));

  try { if (pyProcess) pyProcess.kill("SIGTERM"); } catch {}
  await new Promise<void>((resolve) => {
    const timer = setTimeout(() => {
      try { if (pyProcess) pyProcess.kill("SIGKILL"); } catch {}
      resolve();
    }, 30_000);
    if (pyProcess) {
      pyProcess.once("exit", () => { clearTimeout(timer); resolve(); });
    } else {
      clearTimeout(timer);
      resolve();
    }
  });

  process.exit(0);
}

process.on("SIGTERM", () => gracefulShutdown("SIGTERM"));
process.on("SIGINT",  () => gracefulShutdown("SIGINT"));
process.on("exit",    () => { if (pyProcess) pyProcess.kill(); });

function startPythonService() {
  const { existsSync } = require("fs");

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
      return "python3";
    })();

  logger.info({ pythonDir, pythonBin, repoRoot }, "Starting Python AI service");

  const pythonLibsBin = path.join(repoRoot, ".pythonlibs", "bin");
  const pythonSitePkgs = path.join(repoRoot, ".pythonlibs", "lib", "python3.11", "site-packages");
  const childEnv: Record<string, string> = {
    ...process.env,
    PATH: `${pythonLibsBin}:${process.env.PATH ?? ""}`,
  };
  if (existsSync(pythonSitePkgs)) {
    childEnv["PYTHONPATH"] = pythonSitePkgs;
  }

  pyProcess = spawn(
    pythonBin,
    ["-m", "uvicorn", "main:app", "--host", "0.0.0.0", "--port", "8000"],
    {
      cwd: pythonDir,
      stdio: "inherit",
      env: childEnv,
    },
  );

  pyProcess.on("error", (err) => {
    logger.error({ err }, "Failed to start Python AI service");
  });

  pyProcess.on("exit", (code, signal) => {
    if (code !== 0) {
      logger.warn({ code, signal }, "Python AI service exited, restarting in 3s...");
      setTimeout(startPythonService, 3000);
    }
  });
}

startPythonService();

httpServer = app.listen(port, (err) => {
  if (err) {
    logger.error({ err }, "Error listening on port");
    process.exit(1);
  }

  logger.info({ port }, "Server listening");
});
