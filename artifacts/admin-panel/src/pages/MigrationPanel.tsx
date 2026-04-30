import { useState, useRef } from "react";
import { Button } from "@/components/ui/button";
import { Input } from "@/components/ui/input";
import { Label } from "@/components/ui/label";
import { Card, CardContent, CardHeader, CardTitle } from "@/components/ui/card";
import { Progress } from "@/components/ui/progress";
import { useToast } from "@/hooks/use-toast";
import { getAdminKey } from "@/lib/admin-auth";

interface JobStatus {
  status: "running" | "completed" | "failed";
  finished: boolean;
  counts: Record<string, number>;
  bytes_read: number;
  total_rows: number;
  elapsed_sec: number;
  errors: string[];
  error?: string;
  total_file_bytes?: number;
}

export function MigrationPanel() {
  const { toast } = useToast();
  const [sourceUrl, setSourceUrl] = useState("");
  const [sourceKey, setSourceKey] = useState("");

  const [downloading,     setDownloading]     = useState(false);
  const [uploading,       setUploading]       = useState(false);
  const [uploadProgress,  setUploadProgress]  = useState(0);
  const [uploadPhase,     setUploadPhase]     = useState<"compress"|"upload"|"import"|null>(null);
  const [importStatus,    setImportStatus]    = useState<JobStatus | null>(null);

  const [pulling,         setPulling]         = useState(false);
  const [jobStatus,       setJobStatus]       = useState<JobStatus | null>(null);
  const [probeResult,     setProbeResult]     = useState("");

  const fileInputRef      = useRef<HTMLInputElement>(null);
  const uploadPollTimer   = useRef<ReturnType<typeof setInterval> | null>(null);
  const pullPollTimer     = useRef<ReturnType<typeof setInterval> | null>(null);

  // ── 下载导出 ──────────────────────────────────────────────────────────────
  async function handleStreamExport() {
    setDownloading(true);
    try {
      const r = await fetch("/admin/db-export-stream", {
        headers: { "X-Admin-Key": getAdminKey() ?? "" },
      });
      if (!r.ok) throw new Error(`HTTP ${r.status}: ${await r.text()}`);
      const blob = await r.blob();
      const url = URL.createObjectURL(blob);
      const a = document.createElement("a");
      a.href = url;
      a.download = `db-export-${Date.now()}.ndjson`;
      a.click();
      URL.revokeObjectURL(url);
      toast({ title: "下载完成", description: `${(blob.size / 1024 / 1024).toFixed(1)} MB` });
    } catch (e: unknown) {
      toast({ title: "下载失败", description: String(e), variant: "destructive" });
    } finally {
      setDownloading(false);
    }
  }

  // ── 上传文件导入 ──────────────────────────────────────────────────────────
  const CHUNK_SIZE  = 16 * 1024 * 1024; // 16 MB（压缩后通常 <2 MB，远低于 Cloud Run 32 MB 限制）
  const CONCURRENCY = 4;                // 4 路并发上传

  function stopUploadPoll() {
    if (uploadPollTimer.current) { clearInterval(uploadPollTimer.current); uploadPollTimer.current = null; }
  }

  async function pollImportJob(jobId: string) {
    try {
      const r = await fetch(`/admin/migration-job/${jobId}`, {
        headers: { "X-Admin-Key": getAdminKey() ?? "" },
      });
      const data: JobStatus = await r.json();
      setImportStatus(data);
      if (data.finished) {
        stopUploadPoll();
        setUploading(false);
        setUploadPhase(null);
        if (data.status === "completed") {
          toast({ title: "导入完成", description: `共 ${data.total_rows} 行 / 耗时 ${data.elapsed_sec}s` });
        } else {
          toast({ title: "导入失败", description: data.error || "未知错误", variant: "destructive" });
        }
      }
    } catch { /* 网络抖动静默重试 */ }
  }

  /** gzip 压缩 Blob（使用浏览器原生 CompressionStream） */
  async function gzipBlob(blob: Blob): Promise<Blob> {
    const cs = new CompressionStream("gzip");
    const compressed = blob.stream().pipeThrough(cs);
    return new Response(compressed).blob();
  }

  /** 将 Blob 按字节偏移切成 ≤ CHUNK_SIZE 的 Blob 数组（压缩后数据无行边界要求） */
  function splitIntoChunks(blob: Blob): Blob[] {
    const chunks: Blob[] = [];
    for (let start = 0; start < blob.size; start += CHUNK_SIZE) {
      chunks.push(blob.slice(start, start + CHUNK_SIZE));
    }
    return chunks;
  }

  /** fetch POST 单个 Blob，返回解析后的 JSON（简单封装，利于 Promise.all） */
  async function postChunk(url: string, body: Blob | null): Promise<unknown> {
    const headers: Record<string, string> = { "X-Admin-Key": getAdminKey() ?? "" };
    if (body) headers["Content-Type"] = "application/octet-stream";
    const r = await fetch(url, { method: "POST", headers, body });
    if (!r.ok) {
      const text = (await r.text()).slice(0, 300).replace(/<[^>]+>/g, " ").trim();
      throw new Error(`HTTP ${r.status}: ${text}`);
    }
    return r.json();
  }

  async function handleUpload(file: File) {
    if (!confirm(`将上传 ${(file.size / 1024 / 1024).toFixed(1)} MB 数据并 upsert 到当前实例。继续？`)) return;

    setUploading(true);
    setUploadProgress(0);
    setUploadPhase("compress");
    setImportStatus(null);

    try {
      // Step 0: gzip 压缩（NDJSON 文本压缩率约 10:1，大幅减少传输量）
      const compressed = await gzipBlob(file);
      setUploadPhase("upload");
      const compMB = (compressed.size / 1024 / 1024).toFixed(1);
      const origMB = (file.size / 1024 / 1024).toFixed(1);
      console.log(`[upload] gzip ${origMB}MB → ${compMB}MB`);

      // Step 1: 创建会话（告知服务端数据已 gzip 压缩）
      const startData = await postChunk("/admin/db-import-start?compressed=gzip", null) as { session_id: string };
      const sessionId = startData.session_id;

      // Step 2: 切块并 4 路并发上传
      const chunks = splitIntoChunks(compressed);
      const total = chunks.length;
      let done = 0;
      for (let i = 0; i < total; i += CONCURRENCY) {
        const batch = chunks.slice(i, i + CONCURRENCY);
        await Promise.all(
          batch.map((chunk, j) => postChunk(`/admin/db-import-chunk/${sessionId}/${i + j}`, chunk))
        );
        done = Math.min(i + CONCURRENCY, total);
        setUploadProgress(Math.round(done / total * 100));
      }

      // Step 3: 通知服务端组装 + 启动后台导入
      setUploadPhase("import");
      const finishData = await postChunk(`/admin/db-import-finish/${sessionId}`, null) as { job_id: string };

      // Step 4: 轮询导入进度
      await pollImportJob(finishData.job_id);
      uploadPollTimer.current = setInterval(() => pollImportJob(finishData.job_id), 2000);
    } catch (e: unknown) {
      setUploading(false);
      setUploadPhase(null);
      toast({ title: "上传失败", description: String(e), variant: "destructive" });
    } finally {
      if (fileInputRef.current) fileInputRef.current.value = "";
    }
  }

  // ── 流式拉取 ─────────────────────────────────────────────────────────────
  function stopPullPoll() {
    if (pullPollTimer.current) { clearInterval(pullPollTimer.current); pullPollTimer.current = null; }
  }

  async function pollPullJob(jobId: string) {
    try {
      const r = await fetch(`/admin/migration-job/${jobId}`, {
        headers: { "X-Admin-Key": getAdminKey() ?? "" },
      });
      const data: JobStatus = await r.json();
      setJobStatus(data);
      if (data.finished) {
        stopPullPoll();
        setPulling(false);
        if (data.status === "completed") {
          toast({
            title: "拉取完成",
            description: `共 ${data.total_rows} 行 / 耗时 ${data.elapsed_sec}s / ${(data.bytes_read / 1024 / 1024).toFixed(1)} MB`,
          });
        } else {
          toast({ title: "拉取失败", description: data.error || "未知错误", variant: "destructive" });
        }
      }
    } catch { /* 静默重试 */ }
  }

  async function handleStreamPull() {
    if (!sourceUrl || !sourceKey) {
      toast({ title: "请填写源端 URL 和 Admin Key", variant: "destructive" });
      return;
    }
    if (!confirm("将从源端拉取所有数据并 upsert 到当前实例。继续？")) return;
    setJobStatus(null);
    setPulling(true);
    try {
      const r = await fetch("/admin/start-migration-bg", {
        method: "POST",
        headers: { "Content-Type": "application/json", "X-Admin-Key": getAdminKey() ?? "" },
        body: JSON.stringify({ source_url: sourceUrl, source_admin_key: sourceKey }),
      });
      const data = await r.json();
      if (!r.ok || !data.job_id) throw new Error(data.error || `HTTP ${r.status}`);
      const jobId: string = data.job_id;
      await pollPullJob(jobId);
      pullPollTimer.current = setInterval(() => pollPullJob(jobId), 2000);
    } catch (e: unknown) {
      setPulling(false);
      toast({ title: "启动迁移失败", description: String(e), variant: "destructive" });
    }
  }

  async function handleProbe() {
    setProbeResult("正在探测...");
    try {
      const r = await fetch("/admin/migration-probe-stream", {
        method: "POST",
        headers: { "Content-Type": "application/json", "X-Admin-Key": getAdminKey() ?? "" },
        body: JSON.stringify({ source_url: sourceUrl, source_admin_key: sourceKey }),
      });
      setProbeResult(JSON.stringify(await r.json(), null, 2));
    } catch (e: unknown) { setProbeResult(`错误：${String(e)}`); }
  }

  // ── 通用进度框组件 ────────────────────────────────────────────────────────
  function JobStatusBox({ status }: { status: JobStatus }) {
    const totalRows = Object.values(status.counts).reduce((a, b) => a + b, 0);
    return (
      <div className="mt-3 p-3 bg-muted rounded text-sm space-y-1">
        <div className="flex items-center gap-2">
          <span className={
            status.status === "completed" ? "text-green-600 font-semibold" :
            status.status === "failed"    ? "text-red-600 font-semibold" :
            "text-blue-600 font-semibold"
          }>
            {status.status === "running"   ? "⏳ 导入中..." :
             status.status === "completed" ? "✅ 完成" : "❌ 失败"}
          </span>
          <span className="text-muted-foreground">
            {totalRows} 行 · {(status.bytes_read / 1024 / 1024).toFixed(1)} MB · {status.elapsed_sec}s
          </span>
        </div>
        {status.status === "running" && status.total_file_bytes && status.total_file_bytes > 0 && (
          <Progress value={Math.min(100, status.bytes_read / status.total_file_bytes * 100)} className="h-2" />
        )}
        {Object.keys(status.counts).length > 0 && (
          <div className="text-xs text-muted-foreground">
            {Object.entries(status.counts).map(([t, n]) => `${t}: ${n}`).join(" · ")}
          </div>
        )}
        {status.error && <div className="text-red-600 text-xs">{status.error}</div>}
        {status.errors.length > 0 && (
          <details className="text-xs">
            <summary className="cursor-pointer text-yellow-600">
              {status.errors.length} 个行级错误（点击展开）
            </summary>
            <pre className="mt-1 overflow-auto max-h-32">{status.errors.slice(0, 20).join("\n")}</pre>
          </details>
        )}
      </div>
    );
  }

  return (
    <Card>
      <CardHeader>
        <CardTitle>数据迁移（流式 NDJSON）</CardTitle>
      </CardHeader>
      <CardContent className="space-y-6">

        {/* 块 1：下载本实例完整数据 */}
        <div>
          <h3 className="font-semibold mb-2">📥 下载本实例完整数据</h3>
          <Button onClick={handleStreamExport} disabled={downloading}>
            {downloading ? "下载中..." : "下载 NDJSON 文件"}
          </Button>
          <p className="text-xs text-muted-foreground mt-2">
            包含 19 张表全部字段（账号 / Key / LOW 审计 / CF 池 / 抽奖 / 积分 / 合作伙伴等）
          </p>
        </div>

        {/* 块 2：上传 NDJSON 文件导入 */}
        <div>
          <h3 className="font-semibold mb-2">📤 上传 NDJSON 文件导入</h3>
          <input
            ref={fileInputRef}
            type="file"
            accept=".ndjson,.json"
            onChange={(e) => e.target.files?.[0] && handleUpload(e.target.files[0])}
            disabled={uploading}
            className="block w-full text-sm"
          />

          {/* 三阶段进度显示 */}
          {uploading && uploadPhase === "compress" && (
            <div className="mt-3 text-xs text-muted-foreground animate-pulse">
              ⚙️ 正在 gzip 压缩（通常压缩到原大小 1/10）...
            </div>
          )}
          {uploading && uploadPhase === "upload" && (
            <div className="mt-3 space-y-1">
              <div className="flex justify-between text-xs text-muted-foreground">
                <span>🚀 正在上传（4 路并发，压缩后传输）</span>
                <span>{uploadProgress}%</span>
              </div>
              <Progress value={uploadProgress} className="h-2" />
            </div>
          )}
          {uploading && uploadPhase === "import" && !importStatus && (
            <div className="mt-3 text-xs text-muted-foreground">⏳ 上传完成，等待后台导入启动...</div>
          )}

          {/* 导入进度 */}
          {importStatus && <JobStatusBox status={importStatus} />}

          {!uploading && !importStatus && (
            <p className="text-xs text-muted-foreground mt-2">
              支持任意大小文件，上传后在后台批量导入，实时显示进度
            </p>
          )}
        </div>

        {/* 块 3：从源端流式拉取 */}
        <div>
          <h3 className="font-semibold mb-2">🔄 从源端流式拉取</h3>
          <div className="space-y-2">
            <div>
              <Label>源端 URL</Label>
              <Input
                value={sourceUrl}
                onChange={(e) => setSourceUrl(e.target.value)}
                placeholder="https://xxx.replit.dev"
              />
            </div>
            <div>
              <Label>源端 Admin Key</Label>
              <Input
                type="password"
                value={sourceKey}
                onChange={(e) => setSourceKey(e.target.value)}
              />
            </div>
            <div className="flex gap-2">
              <Button onClick={handleStreamPull} disabled={pulling}>
                {pulling ? "迁移中..." : "🚀 流式拉取"}
              </Button>
              <Button onClick={handleProbe} variant="outline" disabled={pulling}>探测源端</Button>
            </div>
          </div>

          {jobStatus && <JobStatusBox status={jobStatus} />}

          {probeResult && (
            <pre className="mt-3 p-3 bg-muted text-xs overflow-auto max-h-64 rounded">
              {probeResult}
            </pre>
          )}
        </div>

      </CardContent>
    </Card>
  );
}
