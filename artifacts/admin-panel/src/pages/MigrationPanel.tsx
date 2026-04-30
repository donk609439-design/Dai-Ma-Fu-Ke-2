import { useState, useRef } from "react";
import { Button } from "@/components/ui/button";
import { Input } from "@/components/ui/input";
import { Label } from "@/components/ui/label";
import { Card, CardContent, CardHeader, CardTitle } from "@/components/ui/card";
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
}

export function MigrationPanel() {
  const { toast } = useToast();
  const [sourceUrl, setSourceUrl] = useState("");
  const [sourceKey, setSourceKey] = useState("");
  const [downloading, setDownloading] = useState(false);
  const [uploading,   setUploading]   = useState(false);
  const [pulling,     setPulling]     = useState(false);
  const [probeResult, setProbeResult] = useState("");
  const [jobStatus,   setJobStatus]   = useState<JobStatus | null>(null);
  const fileInputRef = useRef<HTMLInputElement>(null);
  const pollTimerRef = useRef<ReturnType<typeof setInterval> | null>(null);

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

  async function handleUpload(file: File) {
    if (!confirm(`将上传 ${(file.size / 1024 / 1024).toFixed(1)} MB 数据并 upsert 到当前实例。继续？`)) return;
    setUploading(true);
    try {
      const r = await fetch("/admin/db-import-stream", {
        method: "POST",
        headers: {
          "Content-Type": "application/x-ndjson",
          "X-Admin-Key":  getAdminKey() ?? "",
        },
        body: file,
        // @ts-expect-error: duplex 是 fetch 流式上传必须
        duplex: "half",
      });
      const data = await r.json();
      if (!r.ok || !data.success) throw new Error(data.error || `HTTP ${r.status}`);
      const total = Object.values(data.imported as Record<string, number>).reduce((a, b) => a + b, 0);
      toast({
        title: "导入成功",
        description: `共 ${total} 行 / 耗时 ${data.stats.elapsed_sec}s / ${(data.stats.bytes_read / 1024 / 1024).toFixed(1)} MB`,
      });
    } catch (e: unknown) {
      toast({ title: "导入失败", description: String(e), variant: "destructive" });
    } finally {
      setUploading(false);
      if (fileInputRef.current) fileInputRef.current.value = "";
    }
  }

  function stopPolling() {
    if (pollTimerRef.current) {
      clearInterval(pollTimerRef.current);
      pollTimerRef.current = null;
    }
  }

  async function pollJobStatus(jobId: string) {
    try {
      const r = await fetch(`/admin/migration-job/${jobId}`, {
        headers: { "X-Admin-Key": getAdminKey() ?? "" },
      });
      const data: JobStatus = await r.json();
      setJobStatus(data);

      if (data.finished) {
        stopPolling();
        setPulling(false);
        if (data.status === "completed") {
          toast({
            title: "拉取完成",
            description: `共 ${data.total_rows} 行 / 耗时 ${data.elapsed_sec}s / ${(data.bytes_read / 1024 / 1024).toFixed(1)} MB`,
          });
        } else {
          toast({
            title: "拉取失败",
            description: data.error || "未知错误",
            variant: "destructive",
          });
        }
      }
    } catch (e) {
      // 网络抖动时静默重试，不停止轮询
    }
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
      // 启动后台任务，立即返回 job_id（绕过 5 分钟代理超时）
      const r = await fetch("/admin/start-migration-bg", {
        method: "POST",
        headers: { "Content-Type": "application/json", "X-Admin-Key": getAdminKey() ?? "" },
        body: JSON.stringify({ source_url: sourceUrl, source_admin_key: sourceKey }),
      });
      const data = await r.json();
      if (!r.ok || !data.job_id) throw new Error(data.error || `HTTP ${r.status}`);

      const jobId: string = data.job_id;
      // 立刻查一次，然后每 2 秒轮询
      await pollJobStatus(jobId);
      pollTimerRef.current = setInterval(() => pollJobStatus(jobId), 2000);
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
      const data = await r.json();
      setProbeResult(JSON.stringify(data, null, 2));
    } catch (e: unknown) {
      setProbeResult(`错误：${String(e)}`);
    }
  }

  const totalRows = jobStatus ? Object.values(jobStatus.counts).reduce((a, b) => a + b, 0) : 0;

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
          <p className="text-xs text-muted-foreground mt-2">
            {uploading ? "上传中（流式处理，不在浏览器内存累积）..." : "支持任意大小文件，浏览器流式提交"}
          </p>
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

          {/* 实时进度 */}
          {jobStatus && (
            <div className="mt-3 p-3 bg-muted rounded text-sm space-y-1">
              <div className="flex items-center gap-2">
                <span className={
                  jobStatus.status === "completed" ? "text-green-600 font-semibold" :
                  jobStatus.status === "failed"    ? "text-red-600 font-semibold" :
                  "text-blue-600 font-semibold"
                }>
                  {jobStatus.status === "running"   ? "⏳ 迁移中..." :
                   jobStatus.status === "completed" ? "✅ 完成" :
                   "❌ 失败"}
                </span>
                <span className="text-muted-foreground">
                  {totalRows} 行 · {(jobStatus.bytes_read / 1024 / 1024).toFixed(1)} MB · {jobStatus.elapsed_sec}s
                </span>
              </div>
              {Object.keys(jobStatus.counts).length > 0 && (
                <div className="text-xs text-muted-foreground">
                  {Object.entries(jobStatus.counts).map(([t, n]) => `${t}: ${n}`).join(" · ")}
                </div>
              )}
              {jobStatus.error && (
                <div className="text-red-600 text-xs">{jobStatus.error}</div>
              )}
              {jobStatus.errors.length > 0 && (
                <details className="text-xs">
                  <summary className="cursor-pointer text-yellow-600">{jobStatus.errors.length} 个行级错误（点击展开）</summary>
                  <pre className="mt-1 overflow-auto max-h-32">{jobStatus.errors.slice(0, 20).join("\n")}</pre>
                </details>
              )}
            </div>
          )}

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
