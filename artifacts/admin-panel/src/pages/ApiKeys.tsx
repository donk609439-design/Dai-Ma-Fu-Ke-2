import { useState, useMemo } from "react";
import { useQuery, useMutation, useQueryClient } from "@tanstack/react-query";
import { Plus, Trash2, RefreshCw, Key, Copy, Eye, EyeOff, Infinity, User, Users, Search, X, Eraser, ShieldOff, ShieldCheck, ChevronDown, ChevronRight, Ban, ShieldAlert, SlidersHorizontal, Clock, UserCog, Pencil } from "lucide-react";
import { adminFetch } from "@/lib/admin-auth";
import { Button } from "@/components/ui/button";
import { Card, CardContent } from "@/components/ui/card";
import { Input } from "@/components/ui/input";
import { Label } from "@/components/ui/label";
import { Dialog, DialogContent, DialogHeader, DialogTitle, DialogTrigger, DialogFooter } from "@/components/ui/dialog";
import { AlertDialog, AlertDialogAction, AlertDialogCancel, AlertDialogContent, AlertDialogDescription, AlertDialogFooter, AlertDialogHeader, AlertDialogTitle } from "@/components/ui/alert-dialog";
import { useToast } from "@/hooks/use-toast";

interface KeyMeta {
  key: string;
  masked: string;
  usage_limit: number | null;
  usage_count: number;
  usage_cost: number;
  account_id?: string | null;
  banned: boolean;
  banned_at?: number | null;
  is_nc_key?: boolean;
  is_low_admin_key?: boolean;
  low_admin_discord_id?: string;
}

function fmtCost(v: number): string {
  return Number.isInteger(v) ? String(v) : v.toFixed(2).replace(/\.?0+$/, "");
}

interface KeysData {
  keys: string[];
  keys_masked: string[];
  keys_with_meta: KeyMeta[];
  count: number;
  banned_count: number;
}

export default function ApiKeys() {
  const { toast } = useToast();
  const qc = useQueryClient();
  const [open, setOpen] = useState(false);
  const [newKey, setNewKey] = useState("");
  const [usageLimit, setUsageLimit] = useState<string>("");
  const [showKeys, setShowKeys] = useState<Record<string, boolean>>({});
  const [search, setSearch] = useState("");
  const [normalExpanded, setNormalExpanded] = useState(false);
  const [pendingExpanded, setPendingExpanded] = useState(false);
  const [multiExpanded, setMultiExpanded] = useState(false);
  const [bannedExpanded, setBannedExpanded] = useState(false);
  const [lowAdminExpanded, setLowAdminExpanded] = useState(false);
  const [lowAdminGroupExpanded, setLowAdminGroupExpanded] = useState<Record<string, boolean>>({});
  const [banConfirmKey, setBanConfirmKey] = useState<string | null>(null);
  const [adjustOpen, setAdjustOpen] = useState(false);
  const [fromMin, setFromMin] = useState("0");
  const [fromMax, setFromMax] = useState("100");
  const [toMin, setToMin] = useState("0");
  const [toMax, setToMax] = useState("50");
  const [previewValues, setPreviewValues] = useState<Record<string, number>>({});
  // 单 key 修改额度上限
  const [editLimitKey, setEditLimitKey] = useState<string | null>(null);
  const [editLimitValue, setEditLimitValue] = useState<string>("");
  const [editLimitUnlimited, setEditLimitUnlimited] = useState<boolean>(false);
  const [editUsageValue, setEditUsageValue] = useState<string>("");

  const { data, isLoading, refetch } = useQuery<KeysData>({
    queryKey: ["admin-keys"],
    queryFn: async () => {
      const res = await adminFetch("/admin/keys");
      if (!res.ok) throw new Error("获取密钥失败");
      return res.json();
    },
  });

  const addMutation = useMutation({
    mutationFn: async () => {
      const body: { key: string; usage_limit?: number } = { key: newKey };
      const limit = parseInt(usageLimit);
      if (!isNaN(limit) && limit > 0) body.usage_limit = limit;
      const res = await adminFetch("/admin/keys", {
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
    onSuccess: (d) => {
      toast({ title: "密钥添加成功", description: `当前共 ${d.keys_count} 个密钥` });
      setOpen(false);
      setNewKey("");
      setUsageLimit("");
      qc.invalidateQueries({ queryKey: ["admin-keys"] });
      qc.invalidateQueries({ queryKey: ["admin-status"] });
    },
    onError: (e: Error) => toast({ title: "添加失败", description: e.message, variant: "destructive" }),
  });

  const deleteMutation = useMutation({
    mutationFn: async (key: string) => {
      const res = await adminFetch(`/admin/keys/${encodeURIComponent(key)}`, { method: "DELETE" });
      if (!res.ok) throw new Error("删除失败");
      return res.json();
    },
    onSuccess: () => {
      toast({ title: "密钥已删除" });
      qc.invalidateQueries({ queryKey: ["admin-keys"] });
      qc.invalidateQueries({ queryKey: ["admin-status"] });
    },
    onError: () => toast({ title: "删除失败", variant: "destructive" }),
  });

  const deleteExhaustedMutation = useMutation({
    mutationFn: async () => {
      const res = await adminFetch("/admin/keys/exhausted", { method: "DELETE" });
      if (!res.ok) throw new Error("删除失败");
      return res.json();
    },
    onSuccess: (data) => {
      toast({
        title: "清理完成",
        description: `已删除 ${data.deleted_keys} 个已用完的密钥，剩余 ${data.remaining} 个`,
      });
      qc.invalidateQueries({ queryKey: ["admin-keys"] });
      qc.invalidateQueries({ queryKey: ["admin-status"] });
    },
    onError: () => toast({ title: "清理失败", variant: "destructive" }),
  });

  const banMutation = useMutation({
    mutationFn: async (key: string) => {
      const res = await adminFetch(`/admin/keys/${encodeURIComponent(key)}/ban`, { method: "POST" });
      if (!res.ok) throw new Error("封禁失败");
      return res.json();
    },
    onSuccess: () => {
      toast({ title: "密钥已封禁", description: "该密钥将无法继续使用" });
      setBanConfirmKey(null);
      qc.invalidateQueries({ queryKey: ["admin-keys"] });
      qc.invalidateQueries({ queryKey: ["admin-status"] });
    },
    onError: () => {
      toast({ title: "封禁失败", variant: "destructive" });
      setBanConfirmKey(null);
    },
  });

  const unbanMutation = useMutation({
    mutationFn: async (key: string) => {
      const res = await adminFetch(`/admin/keys/${encodeURIComponent(key)}/unban`, { method: "POST" });
      if (!res.ok) throw new Error("解封失败");
      return res.json();
    },
    onSuccess: () => {
      toast({ title: "密钥已解封", description: "该密钥现在可以正常使用" });
      qc.invalidateQueries({ queryKey: ["admin-keys"] });
    },
    onError: () => toast({ title: "解封失败", variant: "destructive" }),
  });

  const unbanAllMutation = useMutation({
    mutationFn: async () => {
      const res = await adminFetch("/admin/keys/unban-all", { method: "POST" });
      if (!res.ok) throw new Error("一键解封失败");
      return res.json();
    },
    onSuccess: (data) => {
      toast({
        title: "一键解封完成",
        description: `已解封 ${data.unbanned_count} 个密钥`,
      });
      qc.invalidateQueries({ queryKey: ["admin-keys"] });
      qc.invalidateQueries({ queryKey: ["admin-status"] });
    },
    onError: () => toast({ title: "一键解封失败", variant: "destructive" }),
  });

  const cleanupPendingMutation = useMutation({
    mutationFn: async () => {
      const res = await adminFetch("/admin/keys/cleanup-pending", { method: "POST" });
      if (!res.ok) throw new Error("清理失败");
      return res.json();
    },
    onSuccess: (data) => {
      const parts: string[] = [];
      if (data.expired_keys_deleted > 0) parts.push(`超时密钥 ${data.expired_keys_deleted} 个`);
      if (data.zombie_nc_keys_cleared > 0) parts.push(`僵尸 NC key ${data.zombie_nc_keys_cleared} 条`);
      toast({
        title: "清理完成",
        description: parts.length > 0 ? `已清除：${parts.join("，")}` : "暂无需要清理的无效等待 key",
      });
      qc.invalidateQueries({ queryKey: ["admin-keys"] });
    },
    onError: () => toast({ title: "清理失败", variant: "destructive" }),
  });

  const setUsageMutation = useMutation({
    mutationFn: async (changes: { key: string; usage_count: number }[]) => {
      const results = await Promise.allSettled(
        changes.map(({ key, usage_count }) =>
          adminFetch(`/admin/keys/${encodeURIComponent(key)}/set-usage`, {
            method: "PATCH",
            headers: { "Content-Type": "application/json" },
            body: JSON.stringify({ usage_count }),
          })
        )
      );
      const failed = results.filter((r) => r.status === "rejected").length;
      if (failed > 0) throw new Error(`${failed} 个密钥修改失败`);
      return results.length;
    },
    onSuccess: (n) => {
      toast({ title: "用量已调整", description: `已成功修改 ${n} 个密钥的用量` });
      setAdjustOpen(false);
      setPreviewValues({});
      qc.invalidateQueries({ queryKey: ["admin-keys"] });
    },
    onError: (e: Error) => toast({ title: "部分修改失败", description: e.message, variant: "destructive" }),
  });

  // 修改单 key 的额度上限（usage_limit）
  const setLimitMutation = useMutation({
    mutationFn: async (payload: { key: string; usage_limit: number | null; usage_count?: number }) => {
      const reqs: Promise<Response>[] = [
        adminFetch(`/admin/keys/${encodeURIComponent(payload.key)}/set-limit`, {
          method: "PATCH",
          headers: { "Content-Type": "application/json" },
          body: JSON.stringify({ usage_limit: payload.usage_limit }),
        }),
      ];
      if (payload.usage_count !== undefined) {
        reqs.push(
          adminFetch(`/admin/keys/${encodeURIComponent(payload.key)}/set-usage`, {
            method: "PATCH",
            headers: { "Content-Type": "application/json" },
            body: JSON.stringify({ usage_count: payload.usage_count }),
          })
        );
      }
      const results = await Promise.all(reqs);
      for (const r of results) {
        if (!r.ok) {
          let msg = "修改失败";
          try { msg = (await r.json()).detail ?? msg; } catch {}
          throw new Error(msg);
        }
      }
      return true;
    },
    onSuccess: () => {
      toast({ title: "已保存", description: "密钥额度已更新" });
      setEditLimitKey(null);
      qc.invalidateQueries({ queryKey: ["admin-keys"] });
      qc.invalidateQueries({ queryKey: ["admin-status"] });
    },
    onError: (e: Error) => toast({ title: "保存失败", description: e.message, variant: "destructive" }),
  });

  const openEditLimit = (m: KeyMeta) => {
    setEditLimitKey(m.key);
    if (m.usage_limit == null) {
      setEditLimitUnlimited(true);
      setEditLimitValue("");
    } else {
      setEditLimitUnlimited(false);
      setEditLimitValue(String(m.usage_limit));
    }
    setEditUsageValue(String(m.usage_count));
  };

  const submitEditLimit = () => {
    if (!editLimitKey) return;
    let limit: number | null;
    if (editLimitUnlimited) {
      limit = null;
    } else {
      const n = parseInt(editLimitValue, 10);
      if (isNaN(n) || n < 0) {
        toast({ title: "请输入有效的额度（≥ 0 的整数）", variant: "destructive" });
        return;
      }
      limit = n;
    }
    let usage: number | undefined;
    if (editUsageValue.trim() !== "") {
      const u = parseInt(editUsageValue, 10);
      if (isNaN(u) || u < 0) {
        toast({ title: "请输入有效的已用量（≥ 0 的整数）", variant: "destructive" });
        return;
      }
      usage = u;
    }
    setLimitMutation.mutate({ key: editLimitKey, usage_limit: limit, usage_count: usage });
  };

  const openAdjustDialog = () => {
    setPreviewValues({});
    setAdjustOpen(true);
  };

  const generatePreview = () => {
    const fMin = parseInt(fromMin, 10);
    const fMax = parseInt(fromMax, 10);
    const tMin = parseInt(toMin, 10);
    const tMax = parseInt(toMax, 10);
    if (isNaN(fMin) || isNaN(fMax) || isNaN(tMin) || isNaN(tMax) || fMin > fMax || tMin > tMax) {
      toast({ title: "区间参数有误", description: "请确保最小值 ≤ 最大值", variant: "destructive" });
      return;
    }
    const targets = (data?.keys_with_meta ?? []).filter(
      (m) => !m.banned && m.usage_count >= fMin && m.usage_count <= fMax
    );
    if (targets.length === 0) {
      toast({ title: "没有匹配的密钥", description: `当前用量在 ${fMin}–${fMax} 之间的密钥为 0 个` });
      return;
    }
    const vals: Record<string, number> = {};
    targets.forEach((m) => {
      vals[m.key] = Math.floor(Math.random() * (tMax - tMin + 1)) + tMin;
    });
    setPreviewValues(vals);
  };

  const submitAdjust = () => {
    const changes = Object.entries(previewValues).map(([key, usage_count]) => ({ key, usage_count }));
    if (changes.length === 0) {
      toast({ title: "请先生成随机预览" });
      return;
    }
    setUsageMutation.mutate(changes);
  };

  const copyKey = (key: string) => {
    navigator.clipboard.writeText(key);
    toast({ title: "已复制到剪贴板" });
  };

  const generateKey = () => {
    const chars = "abcdefghijklmnopqrstuvwxyz0123456789";
    const suffix = Array.from({ length: 32 }, () => chars[Math.floor(Math.random() * chars.length)]).join("");
    setNewKey("sk-" + suffix);
  };

  const usagePct = (meta: KeyMeta) => {
    if (meta.usage_limit == null) return null;
    return Math.min(100, Math.round((meta.usage_cost / meta.usage_limit) * 100));
  };

  const allKeys = data?.keys_with_meta ?? [];

  const isMultiAccount = (m: KeyMeta) => {
    if (!m.account_id) return false;
    return m.account_id.split(",").filter((s) => s.trim()).length >= 2;
  };

  const isPendingParam = (m: KeyMeta) => !m.banned && m.usage_limit === 0;

  const { normalKeys, pendingKeys, multiKeys, bannedKeys, lowAdminKeys, lowAdminGroups } = useMemo(() => {
    const q = search.trim().toLowerCase();
    const filtered = q
      ? allKeys.filter(
          (m) =>
            m.key.toLowerCase().includes(q) ||
            (m.account_id ?? "").toLowerCase().includes(q) ||
            (m.low_admin_discord_id ?? "").toLowerCase().includes(q)
        )
      : allKeys;
    const active = filtered.filter((m) => !m.banned);
    // 次级管理员 key 单独分组，从其他分组中剔除
    const lowAdmin = active.filter((m) => m.is_low_admin_key);
    const others = active.filter((m) => !m.is_low_admin_key);
    const pending = others.filter((m) => isPendingParam(m));
    const nonPending = others.filter((m) => !isPendingParam(m));
    // 按 Discord ID 分组次级管理员 key
    const groups: Record<string, KeyMeta[]> = {};
    for (const m of lowAdmin) {
      const did = (m.low_admin_discord_id ?? "").trim() || "未知";
      if (!groups[did]) groups[did] = [];
      groups[did].push(m);
    }
    const sortedGroups = Object.entries(groups)
      .map(([did, keys]) => ({
        discord_id: did,
        keys: keys.sort((a, b) => a.key.localeCompare(b.key)),
      }))
      .sort((a, b) => a.discord_id.localeCompare(b.discord_id));
    return {
      pendingKeys: pending,
      normalKeys: nonPending.filter((m) => !isMultiAccount(m)),
      multiKeys: nonPending.filter((m) => isMultiAccount(m)),
      bannedKeys: filtered
        .filter((m) => m.banned)
        .sort((a, b) => (a.banned_at ?? 0) - (b.banned_at ?? 0)),
      lowAdminKeys: lowAdmin,
      lowAdminGroups: sortedGroups,
    };
  }, [allKeys, search]);

  const renderKeyCard = (meta: KeyMeta, i: number, isBanned = false) => {
    const visible = showKeys[meta.key] ?? false;
    const displayKey = visible ? meta.key : meta.masked;
    const pct = usagePct(meta);
    const exhausted = !isBanned && meta.usage_limit != null && meta.usage_limit > 0 && meta.usage_cost >= meta.usage_limit;
    const multi = !isBanned && isMultiAccount(meta);

    return (
      <Card
        key={meta.key}
        className={`border-card-border ${
          isBanned
            ? "border-red-500/40 bg-red-500/5 opacity-80"
            : multi
            ? "border-violet-500/30 bg-violet-500/5"
            : exhausted
            ? "opacity-60"
            : ""
        }`}
      >
        <CardContent className="flex items-center gap-4 py-3">
          <div className="flex items-center gap-2 shrink-0">
            {isBanned ? (
              <Ban className="w-3.5 h-3.5 text-red-400" />
            ) : (
              <div className={`w-2 h-2 rounded-full ${exhausted ? "bg-red-400" : "bg-emerald-400"}`} />
            )}
            <span className="text-xs text-muted-foreground font-mono">#{i + 1}</span>
          </div>

          <div className="flex-1 min-w-0 space-y-1.5">
            <code className={`block w-full text-sm font-mono truncate px-3 py-1.5 rounded border ${
              isBanned
                ? "text-red-300 bg-red-950/30 border-red-500/30"
                : "text-foreground bg-muted/40 border-border"
            }`}>
              {displayKey}
            </code>

            {/* 用量条 */}
            <div className="flex items-center gap-2">
              {meta.usage_limit != null ? (
                <>
                  <div className="flex-1 h-1.5 rounded-full bg-muted overflow-hidden">
                    <div
                      className={`h-full rounded-full transition-all ${
                        isBanned
                          ? "bg-red-500"
                          : exhausted
                          ? "bg-red-400"
                          : pct! > 70
                          ? "bg-yellow-400"
                          : "bg-emerald-400"
                      }`}
                      style={{ width: `${pct}%` }}
                    />
                  </div>
                  <span className={`text-xs tabular-nums font-medium shrink-0 ${
                    isBanned ? "text-red-400" : exhausted ? "text-red-400" : "text-muted-foreground"
                  }`}>
                    {fmtCost(meta.usage_cost)} / {meta.usage_limit} 次
                    {exhausted && !isBanned && " · 已耗尽"}
                  </span>
                </>
              ) : (
                <span className="flex items-center gap-1 text-xs text-muted-foreground">
                  <Infinity className="w-3 h-3" />
                  不限次数 · 已用 {fmtCost(meta.usage_cost)} 次
                </span>
              )}
            </div>

            {/* 关联账号 */}
            {meta.account_id && (() => {
              const ids = meta.account_id.split(",").map((s) => s.trim()).filter(Boolean);
              return ids.length >= 2 ? (
                <div className="space-y-0.5">
                  {ids.map((id, idx) => (
                    <div key={idx} className="flex items-center gap-1.5 text-xs text-muted-foreground">
                      <Users className="w-3 h-3 shrink-0 text-violet-400" />
                      <span className="font-mono truncate">{id}</span>
                    </div>
                  ))}
                </div>
              ) : (
                <div className="flex items-center gap-1.5 text-xs text-muted-foreground">
                  <User className="w-3 h-3 shrink-0" />
                  <span className="font-mono truncate">{ids[0]}</span>
                </div>
              );
            })()}

            {/* NC key 标记 */}
            {meta.is_nc_key && (
              <span className="inline-flex items-center gap-1 text-xs font-semibold text-cyan-300 bg-cyan-900/30 border border-cyan-500/30 rounded px-1.5 py-0.5">
                NC 持久密钥
              </span>
            )}

            {/* 封禁提示 */}
            {isBanned && (
              <p className="text-xs text-red-400 font-medium">
                已封禁：无法继续调用 API
                {meta.banned_at ? (
                  <span className="ml-2 font-normal text-red-400/70">
                    · {new Date(meta.banned_at * 1000).toLocaleString("zh-CN", {
                      month: "2-digit", day: "2-digit",
                      hour: "2-digit", minute: "2-digit",
                    })}
                  </span>
                ) : null}
              </p>
            )}
          </div>

          <div className="flex items-center gap-1 shrink-0">
            <Button variant="ghost" size="sm" className="h-8 w-8 p-0 text-muted-foreground"
              onClick={() => setShowKeys({ ...showKeys, [meta.key]: !visible })}>
              {visible ? <EyeOff className="w-3.5 h-3.5" /> : <Eye className="w-3.5 h-3.5" />}
            </Button>
            <Button variant="ghost" size="sm" className="h-8 w-8 p-0 text-muted-foreground"
              onClick={() => copyKey(meta.key)}>
              <Copy className="w-3.5 h-3.5" />
            </Button>
            <Button
              variant="ghost"
              size="sm"
              className="h-8 w-8 p-0 text-muted-foreground hover:text-primary hover:bg-primary/10"
              onClick={() => openEditLimit(meta)}
              title="修改额度上限"
            >
              <Pencil className="w-3.5 h-3.5" />
            </Button>
            {isBanned ? (
              <Button
                variant="ghost"
                size="sm"
                className="h-8 w-8 p-0 text-emerald-500 hover:text-emerald-400 hover:bg-emerald-500/10"
                onClick={() => unbanMutation.mutate(meta.key)}
                disabled={unbanMutation.isPending}
                title="解封密钥"
              >
                <ShieldCheck className="w-3.5 h-3.5" />
              </Button>
            ) : (
              <Button
                variant="ghost"
                size="sm"
                className="h-8 w-8 p-0 text-muted-foreground hover:text-red-400 hover:bg-red-500/10"
                onClick={() => setBanConfirmKey(meta.key)}
                title="封禁密钥"
              >
                <Ban className="w-3.5 h-3.5" />
              </Button>
            )}
            <Button variant="ghost" size="sm" className="h-8 w-8 p-0 text-muted-foreground hover:text-destructive"
              onClick={() => deleteMutation.mutate(meta.key)} disabled={deleteMutation.isPending}>
              <Trash2 className="w-3.5 h-3.5" />
            </Button>
          </div>
        </CardContent>
      </Card>
    );
  };

  const banTargetMasked = banConfirmKey
    ? (banConfirmKey.length > 8 ? banConfirmKey.slice(0, 8) + "****" : "****")
    : "";

  const previewKeys = useMemo(() => {
    return (data?.keys_with_meta ?? []).filter((m) => previewValues[m.key] !== undefined);
  }, [data, previewValues]);

  const editingMeta = editLimitKey ? allKeys.find((m) => m.key === editLimitKey) : null;
  const editingMasked = editingMeta?.masked ?? (editLimitKey ? editLimitKey.slice(0, 8) + "***" : "");

  return (
    <div className="p-6 space-y-6">
      {/* 修改单 key 额度上限 */}
      <Dialog open={!!editLimitKey} onOpenChange={(o) => { if (!o) setEditLimitKey(null); }}>
        <DialogContent className="sm:max-w-md">
          <DialogHeader>
            <DialogTitle className="flex items-center gap-2">
              <Pencil className="w-4 h-4 text-primary" />
              修改密钥额度
            </DialogTitle>
          </DialogHeader>
          <div className="space-y-4 mt-2">
            <div className="rounded-md border border-border bg-muted/30 px-3 py-2">
              <p className="text-xs text-muted-foreground mb-1">目标密钥</p>
              <code className="text-sm font-mono text-foreground break-all">{editingMasked}</code>
            </div>

            <div className="space-y-2">
              <div className="flex items-center justify-between">
                <Label>额度上限（usage_limit）</Label>
                <label className="flex items-center gap-1.5 text-xs text-muted-foreground cursor-pointer select-none">
                  <input
                    type="checkbox"
                    className="accent-primary"
                    checked={editLimitUnlimited}
                    onChange={(e) => setEditLimitUnlimited(e.target.checked)}
                  />
                  不限次数
                </label>
              </div>
              <Input
                type="number"
                min={0}
                placeholder={editLimitUnlimited ? "已勾选不限次数" : "例如：50"}
                value={editLimitUnlimited ? "" : editLimitValue}
                onChange={(e) => setEditLimitValue(e.target.value)}
                disabled={editLimitUnlimited}
                className="text-sm font-mono"
              />
              <p className="text-xs text-muted-foreground">
                填写一个非负整数；勾选「不限次数」会把上限清空（设为 null）
              </p>
            </div>

            <div className="space-y-2">
              <Label>已用量（usage_count，可选）</Label>
              <Input
                type="number"
                min={0}
                placeholder="留空则不修改"
                value={editUsageValue}
                onChange={(e) => setEditUsageValue(e.target.value)}
                className="text-sm font-mono"
              />
              <p className="text-xs text-muted-foreground">
                同步把已用量调整到指定值；可用来"重置"（填 0）
              </p>
            </div>
          </div>
          <DialogFooter className="mt-4">
            <Button variant="outline" onClick={() => setEditLimitKey(null)}>取消</Button>
            <Button onClick={submitEditLimit} disabled={setLimitMutation.isPending}>
              {setLimitMutation.isPending ? "保存中…" : "保存"}
            </Button>
          </DialogFooter>
        </DialogContent>
      </Dialog>

      {/* 调整用量弹窗 */}
      <Dialog open={adjustOpen} onOpenChange={(o) => { if (!o) { setAdjustOpen(false); setPreviewValues({}); } }}>
        <DialogContent className="sm:max-w-md max-h-[80vh] flex flex-col">
          <DialogHeader>
            <DialogTitle className="flex items-center gap-2">
              <SlidersHorizontal className="w-4 h-4 text-primary" />
              随机调整密钥用量
            </DialogTitle>
          </DialogHeader>
          <p className="text-xs text-muted-foreground -mt-1">筛选当前用量在某区间的密钥，将其用量随机设置到目标区间内</p>

          <div className="space-y-4 mt-3">
            {/* 筛选区间 */}
            <div className="rounded-lg border border-border bg-muted/20 p-3 space-y-2">
              <p className="text-xs font-medium text-muted-foreground">筛选：当前用量区间</p>
              <div className="flex items-center gap-2">
                <Input
                  type="number" min={0} placeholder="最小"
                  className="h-8 text-sm text-center font-mono"
                  value={fromMin} onChange={(e) => { setFromMin(e.target.value); setPreviewValues({}); }}
                />
                <span className="text-muted-foreground text-sm shrink-0">–</span>
                <Input
                  type="number" min={0} placeholder="最大"
                  className="h-8 text-sm text-center font-mono"
                  value={fromMax} onChange={(e) => { setFromMax(e.target.value); setPreviewValues({}); }}
                />
              </div>
            </div>

            {/* 目标区间 */}
            <div className="rounded-lg border border-primary/30 bg-primary/5 p-3 space-y-2">
              <p className="text-xs font-medium text-primary/80">随机调整到</p>
              <div className="flex items-center gap-2">
                <Input
                  type="number" min={0} placeholder="最小"
                  className="h-8 text-sm text-center font-mono border-primary/30"
                  value={toMin} onChange={(e) => { setToMin(e.target.value); setPreviewValues({}); }}
                />
                <span className="text-muted-foreground text-sm shrink-0">–</span>
                <Input
                  type="number" min={0} placeholder="最大"
                  className="h-8 text-sm text-center font-mono border-primary/30"
                  value={toMax} onChange={(e) => { setToMax(e.target.value); setPreviewValues({}); }}
                />
              </div>
            </div>

            <Button variant="outline" size="sm" className="w-full" onClick={generatePreview}>
              <RefreshCw className="w-3.5 h-3.5 mr-2" />
              生成随机预览
            </Button>

            {/* 预览列表 */}
            {previewKeys.length > 0 && (
              <div className="rounded-lg border border-border overflow-hidden">
                <div className="px-3 py-2 bg-muted/30 border-b border-border flex items-center justify-between">
                  <span className="text-xs font-medium text-muted-foreground">预览（{previewKeys.length} 个密钥）</span>
                  <span className="text-xs text-muted-foreground">旧值 → 新值</span>
                </div>
                <div className="max-h-44 overflow-y-auto divide-y divide-border">
                  {previewKeys.map((m) => (
                    <div key={m.key} className="flex items-center gap-3 px-3 py-2">
                      <code className="flex-1 min-w-0 text-xs font-mono truncate text-muted-foreground">{m.masked}</code>
                      <div className="flex items-center gap-1.5 shrink-0 text-xs tabular-nums">
                        <span className="text-muted-foreground">{m.usage_count}</span>
                        <span className="text-muted-foreground/50">→</span>
                        <span className="text-primary font-semibold">{previewValues[m.key]}</span>
                      </div>
                    </div>
                  ))}
                </div>
              </div>
            )}
          </div>

          <DialogFooter className="mt-4 pt-3 border-t border-border shrink-0">
            <Button variant="outline" onClick={() => { setAdjustOpen(false); setPreviewValues({}); }}>取消</Button>
            <Button onClick={submitAdjust} disabled={setUsageMutation.isPending || previewKeys.length === 0}>
              {setUsageMutation.isPending ? "保存中…" : `确认保存${previewKeys.length > 0 ? `（${previewKeys.length} 个）` : ""}`}
            </Button>
          </DialogFooter>
        </DialogContent>
      </Dialog>

      {/* 封禁确认弹框 */}
      <AlertDialog open={!!banConfirmKey} onOpenChange={(o) => { if (!o) setBanConfirmKey(null); }}>
        <AlertDialogContent>
          <AlertDialogHeader>
            <AlertDialogTitle>确认封禁此密钥？</AlertDialogTitle>
            <AlertDialogDescription>
              密钥 <code className="font-mono text-sm bg-muted px-1.5 py-0.5 rounded">{banTargetMasked}</code> 封禁后将立即无法使用，持有此密钥的用户所有请求都会被拒绝。
              <br /><br />
              封禁后可在"已封禁密钥"区域点击解封按钮恢复。
            </AlertDialogDescription>
          </AlertDialogHeader>
          <AlertDialogFooter>
            <AlertDialogCancel>取消</AlertDialogCancel>
            <AlertDialogAction
              className="bg-destructive hover:bg-destructive/90"
              onClick={() => banConfirmKey && banMutation.mutate(banConfirmKey)}
              disabled={banMutation.isPending}
            >
              {banMutation.isPending ? "封禁中…" : "确认封禁"}
            </AlertDialogAction>
          </AlertDialogFooter>
        </AlertDialogContent>
      </AlertDialog>

      <div className="flex items-center justify-between">
        <div>
          <h1 className="text-2xl font-bold text-foreground">API 密钥管理</h1>
          <p className="text-sm text-muted-foreground mt-1">管理客户端访问此服务所需的 API 密钥</p>
        </div>
        <div className="flex items-center gap-3">
          <Button variant="outline" size="sm" onClick={() => refetch()} disabled={isLoading}>
            <RefreshCw className={`w-4 h-4 mr-2 ${isLoading ? "animate-spin" : ""}`} />
            刷新
          </Button>
          <Button
            variant="outline"
            size="sm"
            onClick={openAdjustDialog}
            disabled={isLoading || !data?.keys_with_meta?.length}
            className="border-primary/40 text-primary hover:text-primary hover:bg-primary/10"
          >
            <SlidersHorizontal className="w-4 h-4 mr-2" />
            调整用量
          </Button>
          <Button
            variant="outline"
            size="sm"
            onClick={() => deleteExhaustedMutation.mutate()}
            disabled={deleteExhaustedMutation.isPending}
            className="border-destructive/40 text-destructive hover:text-destructive hover:bg-destructive/10"
          >
            <Eraser className="w-4 h-4 mr-2" />
            {deleteExhaustedMutation.isPending ? "清理中…" : "删除已用完密钥"}
          </Button>
          <Dialog open={open} onOpenChange={setOpen}>
            <DialogTrigger asChild>
              <Button size="sm">
                <Plus className="w-4 h-4 mr-2" />
                添加密钥
              </Button>
            </DialogTrigger>
            <DialogContent className="sm:max-w-md">
              <DialogHeader>
                <DialogTitle>添加客户端 API 密钥</DialogTitle>
              </DialogHeader>
              <div className="space-y-4 mt-2">
                <div className="p-3 rounded-lg bg-primary/10 border border-primary/20 text-xs text-primary">
                  客户端需在请求头中携带此密钥才能访问 API，格式: Authorization: Bearer &lt;密钥&gt;
                </div>
                <div className="space-y-2">
                  <Label>API 密钥</Label>
                  <div className="flex gap-2">
                    <Input
                      placeholder="sk-xxxxxxxxxxxxxxxx"
                      value={newKey}
                      onChange={(e) => setNewKey(e.target.value)}
                      className="font-mono text-sm"
                    />
                    <Button type="button" variant="outline" size="sm" onClick={generateKey} className="shrink-0">
                      随机生成
                    </Button>
                  </div>
                </div>
                <div className="space-y-2">
                  <Label>使用次数限制（可选）</Label>
                  <Input
                    type="number"
                    placeholder="留空表示不限次数"
                    value={usageLimit}
                    onChange={(e) => setUsageLimit(e.target.value)}
                    min={1}
                    className="text-sm"
                  />
                  <p className="text-xs text-muted-foreground">例如填写 30，该密钥最多可使用 30 次</p>
                </div>
              </div>
              <DialogFooter className="mt-4">
                <Button variant="outline" onClick={() => setOpen(false)}>取消</Button>
                <Button onClick={() => addMutation.mutate()} disabled={addMutation.isPending || !newKey}>
                  {addMutation.isPending ? "添加中..." : "确认添加"}
                </Button>
              </DialogFooter>
            </DialogContent>
          </Dialog>
        </div>
      </div>

      {/* Summary Card */}
      <Card className="border-card-border bg-primary/5 border-primary/20">
        <CardContent className="flex items-center gap-6 py-4">
          <div className="p-2 rounded-lg bg-primary/20">
            <Key className="w-5 h-5 text-primary" />
          </div>
          <div className="flex gap-6">
            <div>
              <p className="text-sm font-medium text-foreground">{data?.count ?? 0} 个正常密钥</p>
              <p className="text-xs text-muted-foreground">可正常访问 API</p>
            </div>
            {(data?.banned_count ?? 0) > 0 && (
              <div>
                <p className="text-sm font-medium text-red-400">{data?.banned_count} 个已封禁</p>
                <p className="text-xs text-muted-foreground">无法继续使用</p>
              </div>
            )}
          </div>
        </CardContent>
      </Card>

      {/* 搜索框 */}
      <div className="relative">
        <Search className="absolute left-3 top-1/2 -translate-y-1/2 w-4 h-4 text-muted-foreground pointer-events-none" />
        <Input
          placeholder="搜索密钥或账号标识…"
          value={search}
          onChange={(e) => setSearch(e.target.value)}
          className="pl-9 pr-9 text-sm font-mono"
        />
        {search && (
          <button
            onClick={() => setSearch("")}
            className="absolute right-3 top-1/2 -translate-y-1/2 text-muted-foreground hover:text-foreground"
          >
            <X className="w-4 h-4" />
          </button>
        )}
      </div>

      {isLoading ? (
        <div className="flex items-center justify-center h-48 text-muted-foreground">
          <RefreshCw className="w-5 h-5 animate-spin mr-2" />
          加载中...
        </div>
      ) : normalKeys.length === 0 && pendingKeys.length === 0 && multiKeys.length === 0 && bannedKeys.length === 0 && lowAdminKeys.length === 0 ? (
        <Card className="border-card-border border-dashed">
          <CardContent className="flex flex-col items-center justify-center py-16 gap-3">
            {search ? (
              <>
                <Search className="w-10 h-10 text-muted-foreground/50" />
                <p className="text-sm text-muted-foreground">未找到匹配的密钥</p>
                <p className="text-xs text-muted-foreground/70">尝试更换关键词，或<button className="underline ml-1" onClick={() => setSearch("")}>清空搜索</button></p>
              </>
            ) : (
              <>
                <Key className="w-10 h-10 text-muted-foreground/50" />
                <p className="text-sm text-muted-foreground">暂无 API 密钥</p>
                <p className="text-xs text-muted-foreground/70">点击"添加密钥"按钮配置访问凭据</p>
              </>
            )}
          </CardContent>
        </Card>
      ) : (
        <div className="space-y-4">
          {/* 正常密钥区域 */}
          <div className="space-y-2">
            <button
              onClick={() => setNormalExpanded(!normalExpanded)}
              className="w-full flex items-center justify-between px-1 py-1.5 text-sm font-medium text-foreground hover:text-primary transition-colors group"
            >
              <div className="flex items-center gap-2">
                <ShieldCheck className="w-4 h-4 text-emerald-400" />
                <span>正常密钥</span>
                <span className="text-xs text-muted-foreground font-normal">（{normalKeys.length} 个）</span>
              </div>
              {normalExpanded ? (
                <ChevronDown className="w-4 h-4 text-muted-foreground group-hover:text-primary" />
              ) : (
                <ChevronRight className="w-4 h-4 text-muted-foreground group-hover:text-primary" />
              )}
            </button>

            {normalExpanded && (
              normalKeys.length === 0 ? (
                <p className="text-xs text-muted-foreground pl-6 py-2">暂无正常密钥</p>
              ) : (
                <div className="space-y-2">
                  {normalKeys.map((meta, i) => renderKeyCard(meta, i, false))}
                </div>
              )
            )}
          </div>

          {/* 等待返回参数区域 */}
          {(pendingKeys.length > 0 || !search) && (
            <div className="space-y-2">
              <div className="flex items-center justify-between px-1 py-1.5">
                <button
                  onClick={() => setPendingExpanded(!pendingExpanded)}
                  className="flex items-center gap-2 text-sm font-medium text-foreground hover:text-amber-400 transition-colors group"
                >
                  <Clock className="w-4 h-4 text-amber-400" />
                  <span>等待返回参数</span>
                  <span className="text-xs text-muted-foreground font-normal">（{pendingKeys.length} 个）</span>
                  <span className="text-[10px] px-1.5 py-0.5 rounded-full bg-amber-500/15 text-amber-400 border border-amber-500/25 font-normal">
                    额度待激活
                  </span>
                  {pendingExpanded ? (
                    <ChevronDown className="w-4 h-4 text-muted-foreground group-hover:text-amber-400" />
                  ) : (
                    <ChevronRight className="w-4 h-4 text-muted-foreground group-hover:text-amber-400" />
                  )}
                </button>
                <Button
                  variant="outline"
                  size="sm"
                  onClick={() => cleanupPendingMutation.mutate()}
                  disabled={cleanupPendingMutation.isPending}
                  className="h-7 px-2.5 text-xs border-amber-500/40 text-amber-500 hover:text-amber-400 hover:bg-amber-500/10 hover:border-amber-500/60"
                >
                  <Eraser className="w-3.5 h-3.5 mr-1.5" />
                  {cleanupPendingMutation.isPending ? "清理中…" : "清除无效等待 key"}
                </Button>
              </div>

              {pendingExpanded && (
                pendingKeys.length === 0 ? (
                  <p className="text-xs text-muted-foreground pl-6 py-2">暂无等待参数的密钥</p>
                ) : (
                  <div className="space-y-2">
                    {pendingKeys.map((meta, i) => renderKeyCard(meta, i, false))}
                  </div>
                )
              )}
            </div>
          )}

          {/* 多账号 key 区域 */}
          {(multiKeys.length > 0 || !search) && (
            <div className="space-y-2">
              <button
                onClick={() => setMultiExpanded(!multiExpanded)}
                className="w-full flex items-center justify-between px-1 py-1.5 text-sm font-medium text-foreground hover:text-violet-400 transition-colors group"
              >
                <div className="flex items-center gap-2">
                  <Users className="w-4 h-4 text-violet-400" />
                  <span>多账号 key</span>
                  <span className="text-xs text-muted-foreground font-normal">（{multiKeys.length} 个）</span>
                  <span className="text-[10px] px-1.5 py-0.5 rounded-full bg-violet-500/15 text-violet-400 border border-violet-500/25 font-normal">
                    绑定 2+ 个账号
                  </span>
                </div>
                {multiExpanded ? (
                  <ChevronDown className="w-4 h-4 text-muted-foreground group-hover:text-violet-400" />
                ) : (
                  <ChevronRight className="w-4 h-4 text-muted-foreground group-hover:text-violet-400" />
                )}
              </button>

              {multiExpanded && (
                multiKeys.length === 0 ? (
                  <p className="text-xs text-muted-foreground pl-6 py-2">暂无多账号密钥</p>
                ) : (
                  <div className="space-y-2">
                    {multiKeys.map((meta, i) => renderKeyCard(meta, i, false))}
                  </div>
                )
              )}
            </div>
          )}

          {/* 次级管理员 key 区域 */}
          {(lowAdminKeys.length > 0 || !search) && (
            <div className="space-y-2">
              <button
                onClick={() => setLowAdminExpanded(!lowAdminExpanded)}
                className="w-full flex items-center justify-between px-1 py-1.5 text-sm font-medium text-foreground hover:text-orange-400 transition-colors group"
              >
                <div className="flex items-center gap-2">
                  <UserCog className="w-4 h-4 text-orange-400" />
                  <span>次级管理员 key</span>
                  <span className="text-xs text-muted-foreground font-normal">
                    （{lowAdminKeys.length} 个 · {lowAdminGroups.length} 个 Discord 用户）
                  </span>
                  <span className="text-[10px] px-1.5 py-0.5 rounded-full bg-orange-500/15 text-orange-400 border border-orange-500/25 font-normal">
                    LOW_ADMIN 创建
                  </span>
                </div>
                {lowAdminExpanded ? (
                  <ChevronDown className="w-4 h-4 text-muted-foreground group-hover:text-orange-400" />
                ) : (
                  <ChevronRight className="w-4 h-4 text-muted-foreground group-hover:text-orange-400" />
                )}
              </button>

              {lowAdminExpanded && (
                lowAdminGroups.length === 0 ? (
                  <p className="text-xs text-muted-foreground pl-6 py-2">暂无次级管理员创建的密钥</p>
                ) : (
                  <div className="space-y-3">
                    {lowAdminGroups.map((g) => {
                      const gOpen = lowAdminGroupExpanded[g.discord_id] ?? true;
                      const sumCost = g.keys.reduce((s, k) => s + (k.usage_cost ?? 0), 0);
                      const sumLimit = g.keys.reduce(
                        (s, k) => s + (k.usage_limit ?? 0),
                        0,
                      );
                      return (
                        <div key={g.discord_id} className="rounded-lg border border-orange-500/25 bg-orange-500/5 overflow-hidden">
                          <button
                            onClick={() =>
                              setLowAdminGroupExpanded({
                                ...lowAdminGroupExpanded,
                                [g.discord_id]: !gOpen,
                              })
                            }
                            className="w-full flex items-center justify-between px-3 py-2 hover:bg-orange-500/10 transition-colors"
                          >
                            <div className="flex items-center gap-2 min-w-0">
                              <User className="w-3.5 h-3.5 text-orange-400 shrink-0" />
                              <code className="text-xs font-mono text-orange-300 truncate">
                                Discord: {g.discord_id}
                              </code>
                              <span className="text-xs text-muted-foreground shrink-0">
                                · {g.keys.length} 个密钥
                              </span>
                              <span className="text-xs text-muted-foreground shrink-0 tabular-nums">
                                · 已用 {fmtCost(sumCost)} / {sumLimit} 次
                              </span>
                            </div>
                            {gOpen ? (
                              <ChevronDown className="w-3.5 h-3.5 text-orange-400 shrink-0" />
                            ) : (
                              <ChevronRight className="w-3.5 h-3.5 text-orange-400 shrink-0" />
                            )}
                          </button>
                          {gOpen && (
                            <div className="space-y-2 px-2 pb-2">
                              {g.keys.map((meta, i) => renderKeyCard(meta, i, false))}
                            </div>
                          )}
                        </div>
                      );
                    })}
                  </div>
                )
              )}
            </div>
          )}

          {/* 封禁密钥区域 */}
          {(bannedKeys.length > 0 || !search) && (
            <div className="space-y-2">
              <div className="flex items-center justify-between px-1 py-1.5">
                <button
                  onClick={() => setBannedExpanded(!bannedExpanded)}
                  className="flex items-center gap-2 text-sm font-medium text-foreground hover:text-red-400 transition-colors group"
                >
                  <ShieldOff className="w-4 h-4 text-red-400" />
                  <span>已封禁密钥</span>
                  <span className="text-xs text-muted-foreground font-normal">（{bannedKeys.length} 个）</span>
                  {bannedExpanded ? (
                    <ChevronDown className="w-4 h-4 text-muted-foreground group-hover:text-red-400" />
                  ) : (
                    <ChevronRight className="w-4 h-4 text-muted-foreground group-hover:text-red-400" />
                  )}
                </button>
                {bannedKeys.length > 0 && (
                  <Button
                    variant="outline"
                    size="sm"
                    onClick={() => unbanAllMutation.mutate()}
                    disabled={unbanAllMutation.isPending}
                    className="h-7 px-2.5 text-xs border-emerald-500/40 text-emerald-500 hover:text-emerald-400 hover:bg-emerald-500/10 hover:border-emerald-500/60"
                  >
                    <ShieldAlert className="w-3.5 h-3.5 mr-1.5" />
                    {unbanAllMutation.isPending ? "解封中…" : "一键解封"}
                  </Button>
                )}
              </div>

              {bannedExpanded && (
                bannedKeys.length === 0 ? (
                  <p className="text-xs text-muted-foreground pl-6 py-2">暂无封禁密钥</p>
                ) : (
                  <div className="space-y-2">
                    {bannedKeys.map((meta, i) => renderKeyCard(meta, i, true))}
                  </div>
                )
              )}
            </div>
          )}
        </div>
      )}
    </div>
  );
}
