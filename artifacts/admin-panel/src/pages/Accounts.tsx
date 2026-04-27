import { useState, useEffect, useRef, useMemo } from "react";
import { useQuery, useMutation, useQueryClient } from "@tanstack/react-query";
import { Plus, Trash2, CheckCircle, XCircle, RefreshCw, Users, ShieldCheck, Copy, Check, RotateCcw, ListChecks, Eraser, AlertTriangle, ChevronDown, ChevronRight, KeyRound, Unlink, ShieldX, Search, X } from "lucide-react";
import { adminFetch } from "@/lib/admin-auth";
import { Button } from "@/components/ui/button";
import { Card, CardContent } from "@/components/ui/card";
import { Badge } from "@/components/ui/badge";
import { Input } from "@/components/ui/input";
import { Label } from "@/components/ui/label";
import { Dialog, DialogContent, DialogHeader, DialogTitle, DialogTrigger, DialogFooter, DialogDescription } from "@/components/ui/dialog";
import { Tabs, TabsContent, TabsList, TabsTrigger } from "@/components/ui/tabs";
import { useToast } from "@/hooks/use-toast";

interface Account {
  index: number;
  has_jwt: boolean;
  has_quota: boolean;
  licenseId: string;
  jwt_preview: string;
  auth_preview: string;
  daily_used: number | null;
  daily_total: number | null;
  last_quota_check: number;
  external_usage_flag: boolean;
  external_usage_count: number;
  account_id: string;
  quota_status_reason: string | null;
}

const QUOTA_REASON_LABELS: Record<string, string> = {
  jwt_state_none: "订阅已失效",
  jwt_401_unrecoverable: "JWT 无效",
};

interface KeyMeta {
  key: string;
  account_id?: string | null;
  banned: boolean;
}

function fmtTokens(n: number): string {
  if (n >= 1_000_000) return `${(n / 1_000_000).toFixed(1)}M`;
  if (n >= 1_000) return `${Math.round(n / 1_000)}K`;
  return String(n);
}

function CreditsDisplay({ daily_used, daily_total, has_quota, last_quota_check }: {
  daily_used: number | null;
  daily_total: number | null;
  has_quota: boolean;
  last_quota_check: number;
}) {
  if (last_quota_check === 0 || daily_total === null || daily_used === null) {
    return <span className="text-xs text-muted-foreground">未检查</span>;
  }
  if (daily_total === 0) {
    return <span className="text-xs text-emerald-400 font-medium">无限制</span>;
  }
  const remaining = daily_total - daily_used;
  const pct = Math.max(0, Math.min(100, Math.round((remaining / daily_total) * 100)));
  const color = pct <= 10 ? "bg-destructive" : pct <= 30 ? "bg-amber-500" : "bg-emerald-500";
  const textColor = pct <= 10 ? "text-destructive" : pct <= 30 ? "text-amber-400" : "text-emerald-400";
  return (
    <div className="flex items-center gap-2 min-w-[160px]">
      <div className="flex-1 h-1.5 rounded-full bg-muted overflow-hidden">
        <div className={`h-full rounded-full ${color}`} style={{ width: `${pct}%` }} />
      </div>
      <span className={`text-xs font-mono font-medium ${textColor} whitespace-nowrap`}>
        {fmtTokens(remaining)}/{fmtTokens(daily_total)}
      </span>
    </div>
  );
}

interface ExistingKey {
  key: string;
  usage_limit: number;
  usage_count: number;
  usage_cost: number;
}

function fmtCost(v: number): string {
  return Number.isInteger(v) ? String(v) : v.toFixed(2).replace(/\.?0+$/, "");
}

function AccountCard({
  account,
  copiedIndex,
  copyJwt,
  resetQuotaMutation,
  deleteMutation,
}: {
  account: Account;
  copiedIndex: number | null;
  copyJwt: (index: number) => void;
  resetQuotaMutation: { mutate: (index: number) => void; isPending: boolean };
  deleteMutation: { mutate: (index: number) => void; isPending: boolean };
}) {
  return (
    <Card className={`border-card-border transition-all ${account.has_quota ? "glow-green" : "glow-red"}`}>
      <CardContent className="flex items-center gap-4 py-4">
        <div className="flex items-center gap-2 shrink-0">
          {account.has_quota ? (
            <CheckCircle className="w-5 h-5 text-emerald-400" />
          ) : (
            <XCircle className="w-5 h-5 text-destructive" />
          )}
          <span className="text-sm font-medium text-muted-foreground">#{account.index + 1}</span>
        </div>

        <div className="flex-1 min-w-0 space-y-1.5">
          <div className="flex items-center gap-2 flex-wrap">
            {account.licenseId && (
              <Badge variant="outline" className="text-xs border-blue-500/30 text-blue-400">
                <ShieldCheck className="w-3 h-3 mr-1" />
                {account.licenseId}
              </Badge>
            )}
            {account.external_usage_count > 0 && (
              <Badge variant="outline" className="text-xs border-amber-500/40 text-amber-400 bg-amber-500/10 font-mono px-1.5">
                {account.external_usage_count}
              </Badge>
            )}
            {account.jwt_preview && (
              <code className="text-xs text-muted-foreground font-mono bg-muted px-2 py-0.5 rounded">
                JWT: {account.jwt_preview}
              </code>
            )}
            {account.auth_preview && (
              <code className="text-xs text-muted-foreground font-mono bg-muted px-2 py-0.5 rounded">
                Auth: {account.auth_preview}
              </code>
            )}
          </div>
          <div className="flex items-center gap-3">
            <span className="text-xs text-muted-foreground shrink-0">
              {account.licenseId ? "自动刷新" : "静态 JWT"}
            </span>
            <span className="text-xs text-muted-foreground/40">·</span>
            <span className="text-xs text-muted-foreground shrink-0">AI Credits</span>
            <CreditsDisplay
              daily_used={account.daily_used}
              daily_total={account.daily_total}
              has_quota={account.has_quota}
              last_quota_check={account.last_quota_check}
            />
          </div>
        </div>

        {account.external_usage_flag && (
          <Badge
            className="shrink-0 bg-amber-500/20 text-amber-400 border-amber-500/30 flex items-center gap-1"
            title={`检测到外部调用（第 ${account.external_usage_count}/2 次）`}
          >
            <AlertTriangle className="w-3 h-3" />
            外部调用 {account.external_usage_count}/2
          </Badge>
        )}
        <Badge className={`shrink-0 ${account.has_quota ? "bg-emerald-500/20 text-emerald-400 border-emerald-500/30" : "bg-destructive/20 text-destructive border-destructive/30"}`}>
          {account.has_quota
            ? "正常"
            : (account.quota_status_reason && QUOTA_REASON_LABELS[account.quota_status_reason])
              || "超限"}
        </Badge>

        {account.has_jwt && (
          <Button variant="ghost" size="sm" className="text-muted-foreground hover:text-primary shrink-0"
            onClick={() => copyJwt(account.index)} title="复制 JWT">
            {copiedIndex === account.index
              ? <Check className="w-4 h-4 text-emerald-400" />
              : <Copy className="w-4 h-4" />}
          </Button>
        )}

        <Button variant="ghost" size="sm" className="text-muted-foreground hover:text-amber-400 shrink-0"
          onClick={() => resetQuotaMutation.mutate(account.index)}
          disabled={resetQuotaMutation.isPending}
          title="重置配额（立即重新检查）">
          <RotateCcw className={`w-4 h-4 ${resetQuotaMutation.isPending ? "animate-spin" : ""}`} />
        </Button>

        <Button variant="ghost" size="sm" className="text-muted-foreground hover:text-destructive shrink-0"
          onClick={() => deleteMutation.mutate(account.index)} disabled={deleteMutation.isPending}>
          <Trash2 className="w-4 h-4" />
        </Button>
      </CardContent>
    </Card>
  );
}

function SectionHeader({
  icon,
  label,
  count,
  expanded,
  onToggle,
  iconClass,
  hoverClass,
  badge,
}: {
  icon: React.ReactNode;
  label: string;
  count: number;
  expanded: boolean;
  onToggle: () => void;
  iconClass?: string;
  hoverClass?: string;
  badge?: React.ReactNode;
}) {
  return (
    <button
      onClick={onToggle}
      className={`w-full flex items-center justify-between px-1 py-1.5 text-sm font-medium text-foreground transition-colors group ${hoverClass ?? "hover:text-primary"}`}
    >
      <div className="flex items-center gap-2">
        <span className={iconClass}>{icon}</span>
        <span>{label}</span>
        <span className="text-xs text-muted-foreground font-normal">（{count} 个）</span>
        {badge}
      </div>
      {expanded
        ? <ChevronDown className="w-4 h-4 text-muted-foreground" />
        : <ChevronRight className="w-4 h-4 text-muted-foreground" />}
    </button>
  );
}

export default function Accounts() {
  const { toast } = useToast();
  const qc = useQueryClient();
  const [open, setOpen] = useState(false);
  const [mode, setMode] = useState<"auto" | "jwt">("auto");
  const [form, setForm] = useState({ jwt: "", licenseId: "", authorization: "" });
  const [copiedIndex, setCopiedIndex] = useState<number | null>(null);
  const [isRechecking, setIsRechecking] = useState(false);
  const [recheckProgress, setRecheckProgress] = useState<{ done: number; total: number; percent: number } | null>(null);
  const pollRef = useRef<ReturnType<typeof setInterval> | null>(null);
  const [existingKeys, setExistingKeys] = useState<ExistingKey[]>([]);
  const [showExistingKeys, setShowExistingKeys] = useState(false);
  const [copiedKey, setCopiedKey] = useState<string | null>(null);
  const [boundExpanded, setBoundExpanded] = useState(false);
  const [unboundExpanded, setUnboundExpanded] = useState(false);
  const [searchQ, setSearchQ] = useState("");

  useEffect(() => {
    if (isRechecking) {
      pollRef.current = setInterval(async () => {
        try {
          const res = await adminFetch("/admin/accounts/recheck-progress");
          if (res.ok) {
            const prog = await res.json();
            setRecheckProgress({ done: prog.done, total: prog.total, percent: prog.percent });
            if (!prog.running) {
              setIsRechecking(false);
              qc.invalidateQueries({ queryKey: ["admin-accounts"] });
              qc.invalidateQueries({ queryKey: ["admin-status"] });
            }
          }
        } catch { /* ignore */ }
        qc.invalidateQueries({ queryKey: ["admin-accounts"] });
      }, 2000);
      return () => {
        if (pollRef.current) clearInterval(pollRef.current);
      };
    } else {
      if (pollRef.current) clearInterval(pollRef.current);
      setRecheckProgress(null);
      return;
    }
  }, [isRechecking, qc]);

  const copyJwt = async (index: number) => {
    try {
      const res = await adminFetch(`/admin/accounts/${index}/jwt`);
      if (!res.ok) throw new Error("获取 JWT 失败");
      const { jwt } = await res.json();
      await navigator.clipboard.writeText(jwt);
      setCopiedIndex(index);
      toast({ title: "JWT 已复制到剪贴板" });
      setTimeout(() => setCopiedIndex(null), 2000);
    } catch {
      toast({ title: "复制失败", description: "请检查浏览器是否允许剪贴板访问", variant: "destructive" });
    }
  };

  const copyKey = async (key: string) => {
    try {
      await navigator.clipboard.writeText(key);
      setCopiedKey(key);
      toast({ title: "密钥已复制到剪贴板" });
      setTimeout(() => setCopiedKey(null), 2000);
    } catch {
      toast({ title: "复制失败", variant: "destructive" });
    }
  };

  const { data, isLoading, refetch } = useQuery<{ accounts: Account[] }>({
    queryKey: ["admin-accounts"],
    queryFn: async () => {
      const res = await adminFetch("/admin/accounts");
      if (!res.ok) throw new Error("获取账户失败");
      return res.json();
    },
  });

  const { data: keysData } = useQuery<{ keys_with_meta: KeyMeta[] }>({
    queryKey: ["admin-keys"],
    queryFn: async () => {
      const res = await adminFetch("/admin/keys");
      if (!res.ok) throw new Error("获取密钥失败");
      return res.json();
    },
  });

  const addMutation = useMutation({
    mutationFn: async () => {
      const body = mode === "jwt"
        ? { jwt: form.jwt }
        : { licenseId: form.licenseId, authorization: form.authorization };
      const res = await adminFetch("/admin/accounts", {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify(body),
      });
      if (!res.ok) {
        const err = await res.json();
        throw new Error(err.detail ?? "添加失败");
      }
      return res.json();
    },
    onSuccess: (data) => {
      if (data.already_exists) {
        setOpen(false);
        setForm({ jwt: "", licenseId: "", authorization: "" });
        setExistingKeys(data.keys ?? []);
        setShowExistingKeys(true);
      } else {
        toast({ title: "账户添加成功", description: `当前共 ${data.accounts_count} 个账户` });
        setOpen(false);
        setForm({ jwt: "", licenseId: "", authorization: "" });
        qc.invalidateQueries({ queryKey: ["admin-accounts"] });
        qc.invalidateQueries({ queryKey: ["admin-status"] });
      }
    },
    onError: (e: Error) => toast({ title: "添加失败", description: e.message, variant: "destructive" }),
  });

  const deleteMutation = useMutation({
    mutationFn: async (index: number) => {
      const res = await adminFetch(`/admin/accounts/${index}`, { method: "DELETE" });
      if (!res.ok) throw new Error("删除失败");
      return res.json();
    },
    onSuccess: () => {
      toast({ title: "账户已删除" });
      qc.invalidateQueries({ queryKey: ["admin-accounts"] });
      qc.invalidateQueries({ queryKey: ["admin-status"] });
    },
    onError: () => toast({ title: "删除失败", variant: "destructive" }),
  });

  const resetQuotaMutation = useMutation({
    mutationFn: async (index: number) => {
      const res = await adminFetch(`/admin/accounts/${index}/reset-quota`, { method: "POST" });
      if (!res.ok) throw new Error("重置失败");
      return res.json();
    },
    onSuccess: (data) => {
      const statusLine = data.has_quota ? "✓ 有配额" : "✗ 无配额";
      const jwtLine = data.jwt_refresh ? `JWT: ${data.jwt_refresh}` : "";
      const quotaLine = data.quota_check ? `查询: ${data.quota_check}` : "";
      const reasonLabel = data.quota_status_reason
        ? (QUOTA_REASON_LABELS[data.quota_status_reason] ?? data.quota_status_reason)
        : "";
      const reasonLine = !data.has_quota && reasonLabel ? `原因: ${reasonLabel}` : "";
      const usageLine = (data.daily_total != null && data.daily_total > 0)
        ? `用量: ${data.daily_used?.toLocaleString()}/${data.daily_total?.toLocaleString()}`
        : "";
      const desc = [statusLine, jwtLine, quotaLine, reasonLine, usageLine].filter(Boolean).join(" · ");
      toast({ title: "配额已重置", description: desc });
      qc.invalidateQueries({ queryKey: ["admin-accounts"] });
    },
    onError: () => toast({ title: "重置失败", variant: "destructive" }),
  });

  const recheckAllMutation = useMutation({
    mutationFn: async () => {
      const res = await adminFetch("/admin/accounts/reset-quota-all", { method: "POST" });
      if (!res.ok) throw new Error("触发失败");
      return res.json();
    },
    onSuccess: (data) => {
      toast({
        title: "全量重检已启动",
        description: `正在后台检测 ${data.total} 个账号，列表将自动刷新…`,
      });
      setIsRechecking(true);
      qc.invalidateQueries({ queryKey: ["admin-accounts"] });
    },
    onError: () => toast({ title: "启动重检失败", variant: "destructive" }),
  });

  const clearFlagsMutation = useMutation({
    mutationFn: async () => {
      const res = await adminFetch("/admin/accounts/clear-external-flags", { method: "POST" });
      if (!res.ok) throw new Error("清空失败");
      return res.json();
    },
    onSuccess: (data) => {
      toast({
        title: "标记已清空",
        description: `已清除 ${data.cleared} 个账号的外部调用标记和计数`,
      });
      qc.invalidateQueries({ queryKey: ["admin-accounts"] });
    },
    onError: () => toast({ title: "清空失败", variant: "destructive" }),
  });

  const deleteExhaustedMutation = useMutation({
    mutationFn: async () => {
      const res = await adminFetch("/admin/accounts/exhausted", { method: "DELETE" });
      if (!res.ok) throw new Error("删除失败");
      return res.json();
    },
    onSuccess: (data) => {
      toast({
        title: "清理完成",
        description: `已删除 ${data.deleted_accounts} 个无配额账户，剩余 ${data.remaining} 个账户`,
      });
      qc.invalidateQueries({ queryKey: ["admin-accounts"] });
      qc.invalidateQueries({ queryKey: ["admin-status"] });
    },
    onError: () => toast({ title: "清理失败", variant: "destructive" }),
  });

  const accounts = data?.accounts ?? [];

  const boundAccountIds = useMemo(() => {
    const keys = keysData?.keys_with_meta ?? [];
    const ids = new Set<string>();
    for (const k of keys) {
      if (k.account_id) {
        k.account_id.split(",").forEach(id => {
          const trimmed = id.trim();
          if (trimmed) ids.add(trimmed);
        });
      }
    }
    return ids;
  }, [keysData]);

  const { boundAccounts, unboundAccounts } = useMemo(() => {
    const q = searchQ.trim().toLowerCase();
    const match = (a: Account) =>
      !q ||
      a.licenseId.toLowerCase().includes(q) ||
      a.account_id.toLowerCase().includes(q) ||
      a.jwt_preview.toLowerCase().includes(q);
    return {
      boundAccounts: accounts.filter(a => boundAccountIds.has(a.account_id) && match(a)),
      unboundAccounts: accounts.filter(a => !boundAccountIds.has(a.account_id) && match(a)),
    };
  }, [accounts, boundAccountIds, searchQ]);

  const cardProps = { copiedIndex, copyJwt, resetQuotaMutation, deleteMutation };

  return (
    <div className="p-6 space-y-6">
      {/* 已存在账号的密钥弹窗 */}
      <Dialog open={showExistingKeys} onOpenChange={setShowExistingKeys}>
        <DialogContent className="sm:max-w-lg">
          <DialogHeader>
            <DialogTitle>该账号已存在 — 找回密钥</DialogTitle>
            <DialogDescription>
              该账号之前已激活，以下是绑定到此账号的 API 密钥，请妥善保存。
            </DialogDescription>
          </DialogHeader>
          <div className="space-y-3 py-2">
            {existingKeys.length === 0 ? (
              <div className="text-sm text-muted-foreground text-center py-4">
                该账号暂无绑定密钥（可能在激活时未生成）
              </div>
            ) : (
              existingKeys.map((k) => (
                <div key={k.key} className="rounded-lg border border-border bg-muted/40 p-3 space-y-2">
                  <div className="flex items-center gap-2">
                    <code className="flex-1 text-xs font-mono text-primary break-all">{k.key}</code>
                    <Button
                      variant="ghost"
                      size="sm"
                      className="shrink-0 h-7 px-2 text-muted-foreground hover:text-primary"
                      onClick={() => copyKey(k.key)}
                    >
                      {copiedKey === k.key
                        ? <Check className="w-3.5 h-3.5 text-emerald-400" />
                        : <Copy className="w-3.5 h-3.5" />}
                    </Button>
                  </div>
                  <p className="text-xs text-muted-foreground">
                    已用 {fmtCost(k.usage_cost ?? k.usage_count)} / 限额 {k.usage_limit === 0 ? "无限制" : k.usage_limit} 次/天
                  </p>
                </div>
              ))
            )}
          </div>
          <DialogFooter>
            <Button onClick={() => setShowExistingKeys(false)}>关闭</Button>
          </DialogFooter>
        </DialogContent>
      </Dialog>

      <div className="flex items-center justify-between">
        <div>
          <h1 className="text-2xl font-bold text-foreground">账户管理</h1>
          <p className="text-sm text-muted-foreground mt-1">
            管理 JetBrains AI 账户及 JWT 凭据
            {accounts.length > 0 && (
              <span className="ml-2">
                · <span className="text-emerald-400">{accounts.filter(a => a.has_quota).length}</span>
                <span className="text-muted-foreground/60"> / {accounts.length} 有配额</span>
              </span>
            )}
          </p>
        </div>
        <div className="flex items-center gap-3">
          <div className="relative">
            <Search className="absolute left-2.5 top-1/2 -translate-y-1/2 w-3.5 h-3.5 text-muted-foreground pointer-events-none" />
            <Input
              value={searchQ}
              onChange={(e) => setSearchQ(e.target.value)}
              placeholder="搜索账号标识…"
              className="h-8 pl-8 pr-7 text-sm w-44"
            />
            {searchQ && (
              <button
                onClick={() => setSearchQ("")}
                className="absolute right-2 top-1/2 -translate-y-1/2 text-muted-foreground hover:text-foreground"
              >
                <X className="w-3 h-3" />
              </button>
            )}
          </div>
          <Button variant="outline" size="sm" onClick={() => refetch()} disabled={isLoading}>
            <RefreshCw className={`w-4 h-4 mr-2 ${isLoading ? "animate-spin" : ""}`} />
            刷新
          </Button>
          <Button
            variant="outline"
            size="sm"
            onClick={() => recheckAllMutation.mutate()}
            disabled={recheckAllMutation.isPending || isRechecking}
            className={isRechecking ? "border-amber-500/50 text-amber-400" : ""}
          >
            <ListChecks className={`w-4 h-4 mr-2 ${(recheckAllMutation.isPending || isRechecking) ? "animate-pulse" : ""}`} />
            {isRechecking ? "检测中…" : "全量重检配额"}
          </Button>
          <Button
            variant="outline"
            size="sm"
            onClick={() => clearFlagsMutation.mutate()}
            disabled={clearFlagsMutation.isPending}
            className="border-amber-500/40 text-amber-400 hover:text-amber-300 hover:bg-amber-500/10 hover:border-amber-500/60"
          >
            <ShieldX className={`w-4 h-4 mr-2 ${clearFlagsMutation.isPending ? "animate-pulse" : ""}`} />
            {clearFlagsMutation.isPending ? "清空中…" : "清空外部调用标记"}
          </Button>
          <Button
            variant="outline"
            size="sm"
            onClick={() => deleteExhaustedMutation.mutate()}
            disabled={deleteExhaustedMutation.isPending}
            className="border-destructive/40 text-destructive hover:text-destructive hover:bg-destructive/10"
          >
            <Eraser className="w-4 h-4 mr-2" />
            {deleteExhaustedMutation.isPending ? "清理中…" : "删除无配额账户"}
          </Button>
          <Dialog open={open} onOpenChange={setOpen}>
            <DialogTrigger asChild>
              <Button size="sm">
                <Plus className="w-4 h-4 mr-2" />
                添加账户
              </Button>
            </DialogTrigger>
            <DialogContent className="sm:max-w-md">
              <DialogHeader>
                <DialogTitle>添加 JetBrains AI 账户</DialogTitle>
              </DialogHeader>
              <Tabs value={mode} onValueChange={(v) => setMode(v as "auto" | "jwt")} className="mt-2">
                <TabsList className="w-full">
                  <TabsTrigger value="auto" className="flex-1">自动刷新（推荐）</TabsTrigger>
                  <TabsTrigger value="jwt" className="flex-1">静态 JWT</TabsTrigger>
                </TabsList>
                <TabsContent value="auto" className="space-y-4 mt-4">
                  <div className="p-3 rounded-lg bg-primary/10 border border-primary/20 text-xs text-primary">
                    通过 Reqable/小黄鸟 抓包获取 licenseId 和 authorization，可实现 JWT 自动刷新
                  </div>
                  <div className="space-y-2">
                    <Label>License ID</Label>
                    <Input placeholder="例: O12345678" value={form.licenseId} onChange={(e) => setForm({ ...form, licenseId: e.target.value })} />
                  </div>
                  <div className="space-y-2">
                    <Label>Authorization（Bearer 后面的内容）</Label>
                    <Input placeholder="eyJhbGc..." value={form.authorization} onChange={(e) => setForm({ ...form, authorization: e.target.value })} />
                  </div>
                </TabsContent>
                <TabsContent value="jwt" className="space-y-4 mt-4">
                  <div className="p-3 rounded-lg bg-amber-500/10 border border-amber-500/20 text-xs text-amber-400">
                    静态 JWT 每日失效，需要手动更新。建议使用自动刷新模式。
                  </div>
                  <div className="space-y-2">
                    <Label>JWT Token（grazie-authenticate-jwt 的值）</Label>
                    <Input placeholder="eyJhbGc..." value={form.jwt} onChange={(e) => setForm({ ...form, jwt: e.target.value })} />
                  </div>
                </TabsContent>
              </Tabs>
              <DialogFooter className="mt-4">
                <Button variant="outline" onClick={() => setOpen(false)}>取消</Button>
                <Button onClick={() => addMutation.mutate()} disabled={addMutation.isPending}>
                  {addMutation.isPending ? "添加中..." : "确认添加"}
                </Button>
              </DialogFooter>
            </DialogContent>
          </Dialog>
        </div>
      </div>

      {isRechecking && (
        <div className="px-4 py-3 rounded-lg bg-amber-500/10 border border-amber-500/20 space-y-2">
          <div className="flex items-center gap-3 text-sm text-amber-400">
            <ListChecks className="w-4 h-4 animate-pulse shrink-0" />
            <span className="flex-1">
              {recheckProgress
                ? `正在检测配额：${recheckProgress.done} / ${recheckProgress.total} 个账号（${recheckProgress.percent}%）`
                : "正在后台重新检测所有账号的配额状态…"}
            </span>
            <Button
              variant="ghost" size="sm"
              className="text-amber-400 hover:text-red-400 h-6 px-2 shrink-0"
              onClick={async () => {
                await adminFetch("/admin/accounts/recheck-cancel", { method: "POST" }).catch(() => {});
                setIsRechecking(false);
              }}
            >
              取消
            </Button>
          </div>
          {recheckProgress && (
            <div className="w-full h-1.5 bg-amber-500/20 rounded-full overflow-hidden">
              <div
                className="h-full bg-amber-400 rounded-full transition-all duration-500"
                style={{ width: `${recheckProgress.percent}%` }}
              />
            </div>
          )}
        </div>
      )}

      {isLoading ? (
        <div className="flex items-center justify-center h-48 text-muted-foreground">
          <RefreshCw className="w-5 h-5 animate-spin mr-2" />
          加载中...
        </div>
      ) : accounts.length === 0 ? (
        <Card className="border-card-border border-dashed">
          <CardContent className="flex flex-col items-center justify-center py-16 gap-3">
            <Users className="w-10 h-10 text-muted-foreground/50" />
            <p className="text-sm text-muted-foreground">暂无 JetBrains AI 账户</p>
            <p className="text-xs text-muted-foreground/70">点击"添加账户"按钮配置账户凭据</p>
          </CardContent>
        </Card>
      ) : (
        <div className="space-y-4">
          {/* 已绑定密钥的账户 */}
          <div className="space-y-2">
            <SectionHeader
              icon={<KeyRound className="w-4 h-4" />}
              label="已绑定密钥账户"
              count={boundAccounts.length}
              expanded={boundExpanded}
              onToggle={() => setBoundExpanded(v => !v)}
              iconClass="text-emerald-400"
              hoverClass="hover:text-emerald-400"
            />
            {boundExpanded && (
              boundAccounts.length === 0 ? (
                <p className="text-xs text-muted-foreground pl-6 py-2">暂无已绑定密钥的账户</p>
              ) : (
                <div className="space-y-3">
                  {boundAccounts.map(account => (
                    <AccountCard key={account.index} account={account} {...cardProps} />
                  ))}
                </div>
              )
            )}
          </div>

          {/* 无绑定密钥的账户 */}
          <div className="space-y-2">
            <SectionHeader
              icon={<Unlink className="w-4 h-4" />}
              label="无绑定密钥账户"
              count={unboundAccounts.length}
              expanded={unboundExpanded}
              onToggle={() => setUnboundExpanded(v => !v)}
              iconClass="text-amber-400"
              hoverClass="hover:text-amber-400"
              badge={
                unboundAccounts.length > 0 ? (
                  <span className="text-xs bg-amber-500/20 text-amber-400 border border-amber-500/30 px-1.5 py-0.5 rounded-full">
                    未分配
                  </span>
                ) : undefined
              }
            />
            {unboundExpanded && (
              unboundAccounts.length === 0 ? (
                <p className="text-xs text-muted-foreground pl-6 py-2">所有账户都已绑定密钥</p>
              ) : (
                <div className="space-y-3">
                  {unboundAccounts.map(account => (
                    <AccountCard key={account.index} account={account} {...cardProps} />
                  ))}
                </div>
              )
            )}
          </div>
        </div>
      )}
    </div>
  );
}
