import { useState } from "react";
import { useQuery, useMutation, useQueryClient } from "@tanstack/react-query";
import { adminFetch } from "@/lib/admin-auth";
import { useToast } from "@/hooks/use-toast";
import {
  Plus, Trash2, RefreshCw, Loader2, CheckCircle2, XCircle,
  Globe, Wifi, WifiOff, FlaskConical, Copy, ShieldCheck,
} from "lucide-react";

interface CfProxy {
  id: number;
  url: string;
  label: string;
  is_active: boolean;
  created_at: string | null;
}

interface ProxyListData {
  proxies: CfProxy[];
  loaded_count: number;
  loaded_urls: string[];
}

function StatusBadge({ active }: { active: boolean }) {
  return active ? (
    <span className="inline-flex items-center gap-1 text-xs px-2 py-0.5 rounded-full bg-green-500/15 text-green-400 border border-green-500/30">
      <Wifi className="w-3 h-3" /> 启用
    </span>
  ) : (
    <span className="inline-flex items-center gap-1 text-xs px-2 py-0.5 rounded-full bg-zinc-700 text-zinc-400 border border-zinc-600">
      <WifiOff className="w-3 h-3" /> 禁用
    </span>
  );
}

export default function ProxyPool() {
  const { toast } = useToast();
  const qc = useQueryClient();

  const [newUrl, setNewUrl] = useState("");
  const [newLabel, setNewLabel] = useState("");
  const [testUrl, setTestUrl] = useState("");
  const [testResult, setTestResult] = useState<{ ok: boolean; body?: string; error?: string } | null>(null);
  const [testing, setTesting] = useState(false);

  const { data, isLoading, refetch } = useQuery<ProxyListData>({
    queryKey: ["cf-proxies"],
    queryFn: () => adminFetch("/admin/cf-proxies").then((r) => r.json()),
    refetchInterval: 15_000,
  });

  async function apiFetch(input: Parameters<typeof adminFetch>[0], init?: Parameters<typeof adminFetch>[1]) {
    const r = await adminFetch(input, init);
    if (!r.ok) {
      let detail = `HTTP ${r.status}`;
      try { const j = await r.json(); detail = j.detail || j.error || detail; } catch {}
      throw new Error(detail);
    }
    return r.json();
  }

  const addMut = useMutation({
    mutationFn: (body: { url: string; label: string }) =>
      apiFetch("/admin/cf-proxies", {
        method: "POST",
        body: JSON.stringify(body),
        headers: { "Content-Type": "application/json" },
      }),
    onSuccess: () => {
      toast({ title: "代理已添加", description: newUrl });
      setNewUrl("");
      setNewLabel("");
      qc.invalidateQueries({ queryKey: ["cf-proxies"] });
    },
    onError: (e: any) => toast({ title: "添加失败", description: e.message, variant: "destructive" }),
  });

  const deleteMut = useMutation({
    mutationFn: (id: number) =>
      apiFetch(`/admin/cf-proxies/${id}`, { method: "DELETE" }),
    onSuccess: () => {
      toast({ title: "代理已删除" });
      qc.invalidateQueries({ queryKey: ["cf-proxies"] });
    },
    onError: (e: any) => toast({ title: "删除失败", description: e.message, variant: "destructive" }),
  });

  const toggleMut = useMutation({
    mutationFn: ({ id, is_active }: { id: number; is_active: boolean }) =>
      apiFetch(`/admin/cf-proxies/${id}`, {
        method: "PATCH",
        body: JSON.stringify({ is_active }),
        headers: { "Content-Type": "application/json" },
      }),
    onSuccess: () => qc.invalidateQueries({ queryKey: ["cf-proxies"] }),
    onError: (e: any) => toast({ title: "操作失败", description: e.message, variant: "destructive" }),
  });

  async function handleTest() {
    const url = testUrl.trim().replace(/\/$/, "");
    if (!url) return;
    setTesting(true);
    setTestResult(null);
    try {
      const res = await adminFetch("/admin/cf-proxies/test", {
        method: "POST",
        body: JSON.stringify({ url }),
        headers: { "Content-Type": "application/json" },
      });
      const json = await res.json();
      setTestResult(json);
    } catch (e: any) {
      setTestResult({ ok: false, error: e.message });
    } finally {
      setTesting(false);
    }
  }

  const proxies = data?.proxies ?? [];

  return (
    <div className="max-w-3xl mx-auto space-y-6">
      {/* 标题 */}
      <div className="flex items-center justify-between">
        <div>
          <h1 className="text-xl font-bold text-foreground flex items-center gap-2">
            <Globe className="w-5 h-5 text-primary" /> CF 代理池
          </h1>
          <p className="text-sm text-muted-foreground mt-0.5">
            Cloudflare Worker 反向代理，分散 JetBrains API 请求 IP，规避 429 限流
          </p>
        </div>
        <button
          onClick={() => refetch()}
          className="p-1.5 rounded-md text-muted-foreground hover:bg-accent hover:text-foreground transition-colors"
          title="刷新"
        >
          <RefreshCw className="w-4 h-4" />
        </button>
      </div>

      {/* 当前内存状态 */}
      <div className={`flex items-center gap-3 px-4 py-3 rounded-lg border ${
        (data?.loaded_count ?? 0) > 0
          ? "bg-green-500/5 border-green-500/20 text-green-400"
          : "bg-zinc-800 border-zinc-700 text-zinc-400"
      }`}>
        <ShieldCheck className="w-4 h-4 shrink-0" />
        <p className="text-sm">
          {(data?.loaded_count ?? 0) > 0
            ? <>当前已加载 <strong>{data!.loaded_count}</strong> 个代理，provide-access 请求将自动轮询分流</>
            : "尚未配置代理，所有请求使用直连模式（可能遭遇 429 限流）"}
        </p>
      </div>

      {/* 部署指引 */}
      <div className="rounded-lg border border-dashed border-blue-500/30 bg-blue-500/5 px-4 py-3 text-sm text-blue-300 space-y-1">
        <p className="font-medium text-blue-200">快速部署指引</p>
        <ol className="list-decimal list-inside space-y-0.5 text-blue-300/80">
          <li>登录 <a href="https://workers.cloudflare.com" target="_blank" rel="noreferrer" className="underline hover:text-blue-200">workers.cloudflare.com</a>，新建 Worker</li>
          <li>将项目根目录的 <code className="bg-blue-500/20 px-1 rounded text-blue-100">cf-worker.js</code> 内容粘贴到编辑器中并部署</li>
          <li>将 Worker URL（如 <code className="bg-blue-500/20 px-1 rounded text-blue-100">https://xxx.workers.dev</code>）添加到下方列表</li>
          <li>可部署多个 Worker 组成代理池，请求将自动轮询</li>
        </ol>
      </div>

      {/* 添加代理 */}
      <div className="rounded-lg border border-border bg-card p-4 space-y-3">
        <h2 className="text-sm font-medium text-foreground">添加代理 URL</h2>
        <div className="flex gap-2">
          <input
            className="flex-1 px-3 py-1.5 text-sm bg-background border border-input rounded-md focus:outline-none focus:ring-1 focus:ring-primary"
            placeholder="https://jb-proxy.xxx.workers.dev"
            value={newUrl}
            onChange={(e) => setNewUrl(e.target.value)}
            onKeyDown={(e) => {
              if (e.key === "Enter") {
                const u = newUrl.trim();
                if (!u.startsWith("https://")) { toast({ title: "URL 格式错误", description: "必须以 https:// 开头", variant: "destructive" }); return; }
                addMut.mutate({ url: u, label: newLabel.trim() });
              }
            }}
          />
          <input
            className="w-36 px-3 py-1.5 text-sm bg-background border border-input rounded-md focus:outline-none focus:ring-1 focus:ring-primary"
            placeholder="备注（可选）"
            value={newLabel}
            onChange={(e) => setNewLabel(e.target.value)}
          />
          <button
            onClick={() => {
              const u = newUrl.trim();
              if (!u.startsWith("https://")) { toast({ title: "URL 格式错误", description: "必须以 https:// 开头", variant: "destructive" }); return; }
              addMut.mutate({ url: u, label: newLabel.trim() });
            }}
            disabled={!newUrl.trim() || addMut.isPending}
            className="flex items-center gap-1.5 px-3 py-1.5 text-sm rounded-md bg-primary text-primary-foreground hover:bg-primary/90 disabled:opacity-50 transition-colors"
          >
            {addMut.isPending ? <Loader2 className="w-3.5 h-3.5 animate-spin" /> : <Plus className="w-3.5 h-3.5" />}
            添加
          </button>
        </div>
      </div>

      {/* 代理列表 */}
      <div className="rounded-lg border border-border overflow-hidden">
        <div className="px-4 py-2.5 bg-muted/30 border-b border-border flex items-center justify-between">
          <span className="text-xs font-medium text-muted-foreground uppercase tracking-wide">
            代理列表 ({proxies.length})
          </span>
        </div>
        {isLoading ? (
          <div className="flex items-center justify-center py-8 text-muted-foreground">
            <Loader2 className="w-4 h-4 animate-spin mr-2" /> 加载中…
          </div>
        ) : proxies.length === 0 ? (
          <div className="text-center py-10 text-muted-foreground text-sm">
            <Globe className="w-8 h-8 mx-auto mb-2 opacity-30" />
            暂无代理，添加第一个 CF Worker URL
          </div>
        ) : (
          <ul className="divide-y divide-border max-h-80 overflow-y-auto">
            {proxies.map((p) => (
              <li key={p.id} className="flex items-center gap-3 px-4 py-3 hover:bg-accent/30 transition-colors group">
                <div className="flex-1 min-w-0">
                  <div className="flex items-center gap-2">
                    <span className="text-sm font-mono text-foreground truncate">{p.url}</span>
                    <button
                      onClick={() => { navigator.clipboard.writeText(p.url); toast({ title: "已复制" }); }}
                      className="opacity-0 group-hover:opacity-100 p-0.5 rounded text-muted-foreground hover:text-foreground transition-all"
                    >
                      <Copy className="w-3 h-3" />
                    </button>
                  </div>
                  <div className="flex items-center gap-2 mt-0.5">
                    {p.label && <span className="text-xs text-muted-foreground">{p.label}</span>}
                    <span className="text-xs text-muted-foreground">
                      {p.created_at ? new Date(p.created_at).toLocaleDateString("zh-CN") : ""}
                    </span>
                  </div>
                </div>
                <StatusBadge active={p.is_active} />
                <button
                  onClick={() => toggleMut.mutate({ id: p.id, is_active: !p.is_active })}
                  disabled={toggleMut.isPending}
                  title={p.is_active ? "禁用" : "启用"}
                  className="p-1.5 rounded text-muted-foreground hover:text-foreground hover:bg-accent transition-colors"
                >
                  {p.is_active ? <WifiOff className="w-3.5 h-3.5" /> : <Wifi className="w-3.5 h-3.5" />}
                </button>
                <button
                  onClick={() => deleteMut.mutate(p.id)}
                  disabled={deleteMut.isPending}
                  title="删除"
                  className="p-1.5 rounded text-muted-foreground hover:text-destructive hover:bg-destructive/10 transition-colors"
                >
                  <Trash2 className="w-3.5 h-3.5" />
                </button>
              </li>
            ))}
          </ul>
        )}
      </div>

      {/* 代理测试 */}
      <div className="rounded-lg border border-border bg-card p-4 space-y-3">
        <h2 className="text-sm font-medium text-foreground flex items-center gap-1.5">
          <FlaskConical className="w-4 h-4 text-muted-foreground" /> 连通性测试
        </h2>
        <div className="flex gap-2">
          <input
            className="flex-1 px-3 py-1.5 text-sm bg-background border border-input rounded-md focus:outline-none focus:ring-1 focus:ring-primary"
            placeholder="https://jb-proxy.xxx.workers.dev"
            value={testUrl}
            onChange={(e) => setTestUrl(e.target.value)}
          />
          <button
            onClick={handleTest}
            disabled={!testUrl.trim() || testing}
            className="flex items-center gap-1.5 px-3 py-1.5 text-sm rounded-md bg-secondary text-secondary-foreground hover:bg-secondary/80 disabled:opacity-50 transition-colors"
          >
            {testing ? <Loader2 className="w-3.5 h-3.5 animate-spin" /> : <FlaskConical className="w-3.5 h-3.5" />}
            测试
          </button>
        </div>
        {testResult && (
          <div className={`flex items-start gap-2 px-3 py-2.5 rounded-md text-sm ${
            testResult.ok ? "bg-green-500/10 text-green-400 border border-green-500/20" : "bg-red-500/10 text-red-400 border border-red-500/20"
          }`}>
            {testResult.ok
              ? <CheckCircle2 className="w-4 h-4 mt-0.5 shrink-0" />
              : <XCircle className="w-4 h-4 mt-0.5 shrink-0" />}
            <div>
              <p className="font-medium">{testResult.ok ? "Worker 正常响应" : "Worker 无响应"}</p>
              <p className="text-xs opacity-80 mt-0.5 break-all">{testResult.body || testResult.error}</p>
            </div>
          </div>
        )}
      </div>
    </div>
  );
}
