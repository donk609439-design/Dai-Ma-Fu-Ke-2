import { useState, useMemo } from "react";
import { useQuery, useMutation, useQueryClient } from "@tanstack/react-query";
import { adminFetch } from "@/lib/admin-auth";
import { useToast } from "@/hooks/use-toast";
import {
  Plus, Trash2, RefreshCw, Loader2, Globe, Wifi, WifiOff, Copy, Users, ChevronDown, ChevronRight,
} from "lucide-react";

interface CfProxy {
  id: number;
  url: string;
  label: string;
  is_active: boolean;
  discord_id: string;
  created_at: string | null;
}

interface ProxyListData {
  proxies: CfProxy[];
  loaded_count: number;
  loaded_urls: string[];
  scope?: string;
  scope_discord_id?: string;
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

export default function LowCfPoolAdmin() {
  const { toast } = useToast();
  const qc = useQueryClient();

  const [newDiscordId, setNewDiscordId] = useState("");
  const [newUrl, setNewUrl] = useState("");
  const [newLabel, setNewLabel] = useState("");
  const [collapsed, setCollapsed] = useState<Record<string, boolean>>({});

  const { data, isLoading, refetch, error } = useQuery<ProxyListData>({
    queryKey: ["low-cf-proxies-admin"],
    queryFn: async () => {
      const r = await adminFetch("/admin/low-cf-proxies");
      if (!r.ok) {
        let detail = `HTTP ${r.status}`;
        try { const j = await r.json(); detail = j.detail || j.error || detail; } catch {}
        throw new Error(detail);
      }
      return r.json();
    },
    refetchInterval: 15_000,
    retry: false,
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
    mutationFn: (body: { url: string; label: string; discord_id: string }) =>
      apiFetch("/admin/low-cf-proxies", {
        method: "POST",
        body: JSON.stringify(body),
        headers: { "Content-Type": "application/json" },
      }),
    onSuccess: () => {
      toast({ title: "代理已添加" });
      setNewUrl("");
      setNewLabel("");
      qc.invalidateQueries({ queryKey: ["low-cf-proxies-admin"] });
    },
    onError: (e: any) => toast({ title: "添加失败", description: e.message, variant: "destructive" }),
  });

  const deleteMut = useMutation({
    mutationFn: (id: number) =>
      apiFetch(`/admin/low-cf-proxies/${id}`, { method: "DELETE" }),
    onSuccess: () => {
      toast({ title: "代理已删除" });
      qc.invalidateQueries({ queryKey: ["low-cf-proxies-admin"] });
    },
    onError: (e: any) => toast({ title: "删除失败", description: e.message, variant: "destructive" }),
  });

  const toggleMut = useMutation({
    mutationFn: ({ id, is_active }: { id: number; is_active: boolean }) =>
      apiFetch(`/admin/low-cf-proxies/${id}`, {
        method: "PATCH",
        body: JSON.stringify({ is_active }),
        headers: { "Content-Type": "application/json" },
      }),
    onSuccess: () => qc.invalidateQueries({ queryKey: ["low-cf-proxies-admin"] }),
    onError: (e: any) => toast({ title: "操作失败", description: e.message, variant: "destructive" }),
  });

  // 按 discord_id 分组
  const grouped = useMemo(() => {
    const map = new Map<string, CfProxy[]>();
    for (const p of data?.proxies ?? []) {
      const key = p.discord_id || "";
      if (!map.has(key)) map.set(key, []);
      map.get(key)!.push(p);
    }
    return Array.from(map.entries()).sort((a, b) => {
      // 兜底空 ID 排在最后
      if (!a[0]) return 1;
      if (!b[0]) return -1;
      return a[0].localeCompare(b[0]);
    });
  }, [data?.proxies]);

  return (
    <div className="max-w-4xl mx-auto space-y-6">
      <div className="flex items-center justify-between">
        <div>
          <h1 className="text-xl font-bold text-foreground flex items-center gap-2">
            <Users className="w-5 h-5 text-primary" /> LOW 用户专属 CF 池（按 Discord 分桶）
          </h1>
          <p className="text-sm text-muted-foreground mt-0.5">
            每个 Discord 账号对应一个独立 CF 子池，激活时按 Discord ID 路由。空 Discord ID 视作兜底子池。
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

      {/* 全局统计 */}
      <div className="rounded-lg border border-border bg-card px-4 py-3 flex items-center gap-6 text-sm">
        <div className="flex items-center gap-2">
          <Globe className="w-4 h-4 text-primary" />
          <span className="text-muted-foreground">运行中代理总数：</span>
          <strong className="text-foreground">{data?.loaded_count ?? 0}</strong>
        </div>
        <div className="flex items-center gap-2">
          <Users className="w-4 h-4 text-primary" />
          <span className="text-muted-foreground">已配置 Discord 子池：</span>
          <strong className="text-foreground">{grouped.length}</strong>
        </div>
      </div>

      {/* 添加 */}
      <div className="rounded-lg border border-border bg-card p-4 space-y-3">
        <h2 className="text-sm font-medium text-foreground">为指定 Discord 子池添加代理 URL</h2>
        <div className="grid grid-cols-12 gap-2">
          <input
            className="col-span-3 px-3 py-1.5 text-sm bg-background border border-input rounded-md focus:outline-none focus:ring-1 focus:ring-primary font-mono"
            placeholder="Discord ID（留空=兜底）"
            value={newDiscordId}
            onChange={(e) => setNewDiscordId(e.target.value)}
          />
          <input
            className="col-span-5 px-3 py-1.5 text-sm bg-background border border-input rounded-md focus:outline-none focus:ring-1 focus:ring-primary"
            placeholder="https://jb-proxy.xxx.workers.dev"
            value={newUrl}
            onChange={(e) => setNewUrl(e.target.value)}
          />
          <input
            className="col-span-2 px-3 py-1.5 text-sm bg-background border border-input rounded-md focus:outline-none focus:ring-1 focus:ring-primary"
            placeholder="备注（可选）"
            value={newLabel}
            onChange={(e) => setNewLabel(e.target.value)}
          />
          <button
            onClick={() => {
              const u = newUrl.trim();
              if (!u.startsWith("https://")) {
                toast({ title: "URL 格式错误", description: "必须以 https:// 开头", variant: "destructive" });
                return;
              }
              addMut.mutate({
                url: u,
                label: newLabel.trim(),
                discord_id: newDiscordId.trim(),
              });
            }}
            disabled={!newUrl.trim() || addMut.isPending}
            className="col-span-2 flex items-center justify-center gap-1.5 px-3 py-1.5 text-sm rounded-md bg-primary text-primary-foreground hover:bg-primary/90 disabled:opacity-50 transition-colors"
          >
            {addMut.isPending ? <Loader2 className="w-3.5 h-3.5 animate-spin" /> : <Plus className="w-3.5 h-3.5" />}
            添加
          </button>
        </div>
      </div>

      {error ? (
        <div className="px-4 py-3 rounded-lg border border-red-500/30 bg-red-500/5 text-sm text-red-400">
          加载失败：{(error as Error).message}
        </div>
      ) : null}

      {isLoading ? (
        <div className="flex items-center justify-center py-12 text-muted-foreground">
          <Loader2 className="w-4 h-4 animate-spin mr-2" /> 加载中…
        </div>
      ) : grouped.length === 0 ? (
        <div className="text-center py-12 text-muted-foreground text-sm rounded-lg border border-border bg-card">
          <Globe className="w-8 h-8 mx-auto mb-2 opacity-30" />
          暂无任何 LOW 子池代理
        </div>
      ) : (
        <div className="space-y-3">
          {grouped.map(([dcId, items]) => {
            const collapsedOn = collapsed[dcId] ?? false;
            const activeCount = items.filter((x) => x.is_active).length;
            return (
              <div key={dcId || "__empty__"} className="rounded-lg border border-border overflow-hidden">
                <button
                  onClick={() =>
                    setCollapsed((prev) => ({ ...prev, [dcId]: !collapsedOn }))
                  }
                  className="w-full flex items-center justify-between gap-3 px-4 py-2.5 bg-muted/30 border-b border-border hover:bg-muted/50 transition-colors text-left"
                >
                  <div className="flex items-center gap-2 min-w-0">
                    {collapsedOn ? (
                      <ChevronRight className="w-4 h-4 shrink-0 text-muted-foreground" />
                    ) : (
                      <ChevronDown className="w-4 h-4 shrink-0 text-muted-foreground" />
                    )}
                    <Users className="w-4 h-4 shrink-0 text-primary" />
                    {dcId ? (
                      <span className="text-sm font-mono text-foreground truncate">
                        Discord ID: {dcId}
                      </span>
                    ) : (
                      <span className="text-sm italic text-muted-foreground">
                        （兜底子池 / 无 Discord ID）
                      </span>
                    )}
                  </div>
                  <div className="flex items-center gap-3 text-xs text-muted-foreground shrink-0">
                    <span>共 {items.length} 个 / 启用 {activeCount}</span>
                  </div>
                </button>
                {!collapsedOn && (
                  <ul className="divide-y divide-border">
                    {items.map((p) => (
                      <li
                        key={p.id}
                        className="flex items-center gap-3 px-4 py-3 hover:bg-accent/30 transition-colors group"
                      >
                        <div className="flex-1 min-w-0">
                          <div className="flex items-center gap-2">
                            <span className="text-sm font-mono text-foreground truncate">{p.url}</span>
                            <button
                              onClick={() => {
                                navigator.clipboard.writeText(p.url);
                                toast({ title: "已复制" });
                              }}
                              className="opacity-0 group-hover:opacity-100 p-0.5 rounded text-muted-foreground hover:text-foreground transition-all"
                            >
                              <Copy className="w-3 h-3" />
                            </button>
                          </div>
                          <div className="flex items-center gap-2 mt-0.5">
                            {p.label && (
                              <span className="text-xs text-muted-foreground">{p.label}</span>
                            )}
                            <span className="text-xs text-muted-foreground">
                              {p.created_at
                                ? new Date(p.created_at).toLocaleDateString("zh-CN")
                                : ""}
                            </span>
                          </div>
                        </div>
                        <StatusBadge active={p.is_active} />
                        <button
                          onClick={() =>
                            toggleMut.mutate({ id: p.id, is_active: !p.is_active })
                          }
                          disabled={toggleMut.isPending}
                          title={p.is_active ? "禁用" : "启用"}
                          className="p-1.5 rounded text-muted-foreground hover:text-foreground hover:bg-accent transition-colors"
                        >
                          {p.is_active ? (
                            <WifiOff className="w-3.5 h-3.5" />
                          ) : (
                            <Wifi className="w-3.5 h-3.5" />
                          )}
                        </button>
                        <button
                          onClick={() => {
                            if (!confirm(`确定删除该代理？\n${p.url}`)) return;
                            deleteMut.mutate(p.id);
                          }}
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
            );
          })}
        </div>
      )}
    </div>
  );
}
