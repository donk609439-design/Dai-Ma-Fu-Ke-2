import { useState, useRef } from "react";
import { Button } from "@/components/ui/button";
import { Input } from "@/components/ui/input";
import { Label } from "@/components/ui/label";
import { Card, CardContent, CardHeader, CardTitle } from "@/components/ui/card";
import { useToast } from "@/hooks/use-toast";
import { getAdminKey } from "@/lib/admin-auth";

export function MigrationPanel() {
  const { toast } = useToast();
  const [sourceUrl, setSourceUrl] = useState("");
  const [sourceKey, setSourceKey] = useState("");
  const [downloading, setDownloading] = useState(false);
  const [uploading,   setUploading]   = useState(false);
  const [pulling,     setPulling]     = useState(false);
  const [probeResult, setProbeResult] = useState("");
  const fileInputRef = useRef<HTMLInputElement>(null);

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
      const total = Object.values<number>(data.imported).reduce((a, b) => a + b, 0);
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

  async function handleStreamPull() {
    if (!sourceUrl || !sourceKey) {
      toast({ title: "请填写源端 URL 和 Admin Key", variant: "destructive" });
      return;
    }
    if (!confirm("将从源端拉取所有数据并 upsert 到当前实例。继续？")) return;
    setPulling(true);
    try {
      const r = await fetch("/admin/import-from-source-stream", {
        method: "POST",
        headers: { "Content-Type": "application/json", "X-Admin-Key": getAdminKey() ?? "" },
        body: JSON.stringify({ source_url: sourceUrl, source_admin_key: sourceKey }),
      });
      const data = await r.json();
      if (!r.ok || !data.success) throw new Error(data.error || `HTTP ${r.status}`);
      const total = Object.values<number>(data.imported).reduce((a, b) => a + b, 0);
      toast({
        title: "拉取完成",
        description: `共 ${total} 行 / 耗时 ${data.stats.elapsed_sec}s / ${(data.stats.bytes_read / 1024 / 1024).toFixed(1)} MB`,
      });
    } catch (e: unknown) {
      toast({ title: "拉取失败", description: String(e), variant: "destructive" });
    } finally {
      setPulling(false);
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
                {pulling ? "拉取中..." : "🚀 流式拉取"}
              </Button>
              <Button onClick={handleProbe} variant="outline">探测源端</Button>
            </div>
          </div>
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
