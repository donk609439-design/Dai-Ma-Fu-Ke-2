import { useState, useMemo } from "react";
import { useQuery, useMutation, useQueryClient } from "@tanstack/react-query";
import {
  Search,
  RefreshCw,
  Copy,
  Eye,
  EyeOff,
  Trash2,
  KeyRound,
  AlertTriangle,
  Ban,
  CheckCircle2,
} from "lucide-react";
import { adminFetch } from "@/lib/admin-auth";
import { Button } from "@/components/ui/button";
import { Card, CardContent } from "@/components/ui/card";
import { Input } from "@/components/ui/input";
import {
  AlertDialog,
  AlertDialogAction,
  AlertDialogCancel,
  AlertDialogContent,
  AlertDialogDescription,
  AlertDialogFooter,
  AlertDialogHeader,
  AlertDialogTitle,
} from "@/components/ui/alert-dialog";
import { useToast } from "@/hooks/use-toast";

interface PersonalKeyItem {
  dc_user_id: string;
  api_key: string;
  masked: string;
  usage_limit: number;
  usage_count: number;
  banned: boolean;
  banned_at: number | null;
  claimed_today: number;
  created_at: number | null;
  key_exists: boolean;
}

interface PersonalKeyData {
  items: PersonalKeyItem[];
  count: number;
}

function fmtTime(ts: number | null): string {
  if (!ts) return "-";
  try {
    return new Date(ts * 1000).toLocaleString("zh-CN", { hour12: false });
  } catch {
    return "-";
  }
}

export default function SelfBuiltKeys() {
  const { toast } = useToast();
  const qc = useQueryClient();
  const [search, setSearch] = useState("");
  const [shown, setShown] = useState<Record<string, boolean>>({});
  const [confirmDelete, setConfirmDelete] = useState<PersonalKeyItem | null>(null);

  const { data, isLoading, refetch, isFetching } = useQuery<PersonalKeyData>({
    queryKey: ["admin-personal-keys"],
    queryFn: async () => {
      const res = await adminFetch("/admin/personal-keys");
      if (!res.ok) throw new Error("获取自建 Key 列表失败");
      return res.json();
    },
  });

  const banMutation = useMutation({
    mutationFn: async ({ key, banned }: { key: string; banned: boolean }) => {
      const url = banned
        ? `/admin/keys/${encodeURIComponent(key)}/unban`
        : `/admin/keys/${encodeURIComponent(key)}/ban`;
      const res = await adminFetch(url, { method: "POST" });
      if (!res.ok) {
        const d = await res.json().catch(() => ({}));
        throw new Error(d.detail || "操作失败");
      }
      return res.json();
    },
    onSuccess: (_d, vars) => {
      toast({ title: vars.banned ? "已解除封禁" : "已封禁" });
      qc.invalidateQueries({ queryKey: ["admin-personal-keys"] });
      qc.invalidateQueries({ queryKey: ["admin-keys"] });
    },
    onError: (e: Error) =>
      toast({ title: "操作失败", description: e.message, variant: "destructive" }),
  });

  const deleteMutation = useMutation({
    mutationFn: async (dcUid: string) => {
      const res = await adminFetch(
        `/admin/personal-keys/${encodeURIComponent(dcUid)}`,
        { method: "DELETE" },
      );
      if (!res.ok) {
        const d = await res.json().catch(() => ({}));
        throw new Error(d.detail || "删除失败");
      }
      return res.json();
    },
    onSuccess: (d) => {
      toast({
        title: "已解除绑定并删除 Key",
        description: d.deleted_key
          ? `${String(d.deleted_key).slice(0, 12)}…`
          : undefined,
      });
      qc.invalidateQueries({ queryKey: ["admin-personal-keys"] });
      qc.invalidateQueries({ queryKey: ["admin-keys"] });
      qc.invalidateQueries({ queryKey: ["admin-status"] });
    },
    onError: (e: Error) =>
      toast({ title: "删除失败", description: e.message, variant: "destructive" }),
  });

  const items = data?.items ?? [];
  const filtered = useMemo(() => {
    const q = search.trim().toLowerCase();
    if (!q) return items;
    return items.filter(
      (it) =>
        it.dc_user_id.toLowerCase().includes(q) ||
        it.api_key.toLowerCase().includes(q),
    );
  }, [items, search]);

  const totals = useMemo(() => {
    let granted = 0;
    let used = 0;
    let banned = 0;
    let claimed = 0;
    for (const it of items) {
      granted += it.usage_limit;
      used += it.usage_count;
      if (it.banned) banned += 1;
      if (it.claimed_today > 0) claimed += 1;
    }
    return { granted, used, banned, claimed };
  }, [items]);

  return (
    <div className="space-y-4 p-4 sm:p-6">
      <div className="flex items-center justify-between flex-wrap gap-3">
        <div>
          <h1 className="text-2xl font-bold flex items-center gap-2">
            <KeyRound className="w-6 h-6 text-primary" />
            自建 Key
          </h1>
          <p className="text-sm text-muted-foreground mt-1">
            用户在【个人中心】通过 Discord 验证自助创建的专属 API Key
          </p>
        </div>
        <Button
          onClick={() => refetch()}
          variant="outline"
          size="sm"
          disabled={isFetching}
        >
          <RefreshCw className={`w-4 h-4 mr-1.5 ${isFetching ? "animate-spin" : ""}`} />
          刷新
        </Button>
      </div>

      <div className="grid grid-cols-2 sm:grid-cols-4 gap-3">
        <Card>
          <CardContent className="p-4">
            <p className="text-xs text-muted-foreground">用户总数</p>
            <p className="text-2xl font-bold mt-1">{data?.count ?? 0}</p>
          </CardContent>
        </Card>
        <Card>
          <CardContent className="p-4">
            <p className="text-xs text-muted-foreground">今日已签到</p>
            <p className="text-2xl font-bold mt-1 text-primary">{totals.claimed}</p>
          </CardContent>
        </Card>
        <Card>
          <CardContent className="p-4">
            <p className="text-xs text-muted-foreground">总发放额度</p>
            <p className="text-2xl font-bold mt-1">{totals.granted}</p>
            <p className="text-[11px] text-muted-foreground mt-0.5">
              已用 {totals.used}
            </p>
          </CardContent>
        </Card>
        <Card>
          <CardContent className="p-4">
            <p className="text-xs text-muted-foreground">已封禁</p>
            <p className="text-2xl font-bold mt-1 text-red-400">{totals.banned}</p>
          </CardContent>
        </Card>
      </div>

      <div className="relative">
        <Search className="absolute left-3 top-1/2 -translate-y-1/2 w-4 h-4 text-muted-foreground" />
        <Input
          value={search}
          onChange={(e) => setSearch(e.target.value)}
          placeholder="搜索 Discord 用户 ID 或 API Key…"
          className="pl-9"
        />
      </div>

      <Card>
        <CardContent className="p-0">
          {isLoading ? (
            <div className="p-12 text-center text-muted-foreground text-sm">
              加载中…
            </div>
          ) : filtered.length === 0 ? (
            <div className="p-12 text-center text-muted-foreground text-sm">
              {items.length === 0
                ? "暂无用户自建 Key"
                : "没有匹配的搜索结果"}
            </div>
          ) : (
            <div className="overflow-x-auto">
              <table className="w-full text-sm">
                <thead>
                  <tr className="border-b border-border bg-muted/30 text-xs uppercase tracking-wide text-muted-foreground">
                    <th className="text-left px-4 py-2.5 font-medium">Discord ID</th>
                    <th className="text-left px-4 py-2.5 font-medium">API Key</th>
                    <th className="text-right px-4 py-2.5 font-medium">额度</th>
                    <th className="text-center px-4 py-2.5 font-medium">今日</th>
                    <th className="text-center px-4 py-2.5 font-medium">状态</th>
                    <th className="text-left px-4 py-2.5 font-medium">创建时间</th>
                    <th className="text-right px-4 py-2.5 font-medium">操作</th>
                  </tr>
                </thead>
                <tbody>
                  {filtered.map((it) => {
                    const isShown = !!shown[it.api_key];
                    const remaining = Math.max(0, it.usage_limit - it.usage_count);
                    return (
                      <tr
                        key={it.dc_user_id}
                        className="border-b border-border/50 hover:bg-muted/20 transition-colors"
                      >
                        <td className="px-4 py-2.5 font-mono text-xs">
                          {it.dc_user_id}
                        </td>
                        <td className="px-4 py-2.5">
                          <div className="flex items-center gap-1.5">
                            <code className="font-mono text-xs">
                              {isShown ? it.api_key : it.masked}
                            </code>
                            <button
                              onClick={() =>
                                setShown((s) => ({ ...s, [it.api_key]: !s[it.api_key] }))
                              }
                              className="p-1 rounded hover:bg-muted/40 text-muted-foreground"
                              title={isShown ? "隐藏" : "显示"}
                            >
                              {isShown ? (
                                <EyeOff className="w-3.5 h-3.5" />
                              ) : (
                                <Eye className="w-3.5 h-3.5" />
                              )}
                            </button>
                            <button
                              onClick={() => {
                                navigator.clipboard.writeText(it.api_key);
                                toast({ title: "已复制 API Key" });
                              }}
                              className="p-1 rounded hover:bg-muted/40 text-muted-foreground"
                              title="复制"
                            >
                              <Copy className="w-3.5 h-3.5" />
                            </button>
                          </div>
                        </td>
                        <td className="px-4 py-2.5 text-right font-mono text-xs">
                          <span
                            className={
                              remaining <= 0 ? "text-red-400" : "text-foreground"
                            }
                          >
                            {it.usage_count} / {it.usage_limit}
                          </span>
                        </td>
                        <td className="px-4 py-2.5 text-center">
                          {it.claimed_today > 0 ? (
                            <span className="inline-flex items-center gap-1 text-[11px] text-green-400">
                              <CheckCircle2 className="w-3 h-3" />
                              已签到
                            </span>
                          ) : (
                            <span className="text-[11px] text-muted-foreground">
                              未签到
                            </span>
                          )}
                        </td>
                        <td className="px-4 py-2.5 text-center">
                          {!it.key_exists ? (
                            <span className="inline-flex items-center gap-1 text-[11px] text-yellow-400">
                              <AlertTriangle className="w-3 h-3" />
                              脏数据
                            </span>
                          ) : it.banned ? (
                            <span className="inline-flex items-center gap-1 text-[11px] text-red-400">
                              <Ban className="w-3 h-3" />
                              已封禁
                            </span>
                          ) : (
                            <span className="inline-flex items-center gap-1 text-[11px] text-green-400">
                              <CheckCircle2 className="w-3 h-3" />
                              正常
                            </span>
                          )}
                        </td>
                        <td className="px-4 py-2.5 text-xs text-muted-foreground whitespace-nowrap">
                          {fmtTime(it.created_at)}
                        </td>
                        <td className="px-4 py-2.5">
                          <div className="flex items-center justify-end gap-1">
                            {it.key_exists && (
                              <Button
                                variant="ghost"
                                size="sm"
                                disabled={banMutation.isPending}
                                onClick={() =>
                                  banMutation.mutate({
                                    key: it.api_key,
                                    banned: it.banned,
                                  })
                                }
                                className="h-7 text-xs"
                              >
                                {it.banned ? "解封" : "封禁"}
                              </Button>
                            )}
                            <Button
                              variant="ghost"
                              size="sm"
                              onClick={() => setConfirmDelete(it)}
                              className="h-7 text-xs text-red-400 hover:text-red-300"
                            >
                              <Trash2 className="w-3.5 h-3.5" />
                            </Button>
                          </div>
                        </td>
                      </tr>
                    );
                  })}
                </tbody>
              </table>
            </div>
          )}
        </CardContent>
      </Card>

      <AlertDialog
        open={!!confirmDelete}
        onOpenChange={(o) => !o && setConfirmDelete(null)}
      >
        <AlertDialogContent>
          <AlertDialogHeader>
            <AlertDialogTitle>解除自建 Key 绑定？</AlertDialogTitle>
            <AlertDialogDescription>
              将删除 Discord 用户{" "}
              <code className="font-mono">{confirmDelete?.dc_user_id}</code>{" "}
              的自建 Key{" "}
              <code className="font-mono">
                {confirmDelete?.masked}
              </code>{" "}
              。该用户下次进入个人中心时可重新创建一个新 Key（额度归零）。
            </AlertDialogDescription>
          </AlertDialogHeader>
          <AlertDialogFooter>
            <AlertDialogCancel>取消</AlertDialogCancel>
            <AlertDialogAction
              onClick={() => {
                if (confirmDelete) deleteMutation.mutate(confirmDelete.dc_user_id);
                setConfirmDelete(null);
              }}
              className="bg-red-500 hover:bg-red-600"
            >
              确认删除
            </AlertDialogAction>
          </AlertDialogFooter>
        </AlertDialogContent>
      </AlertDialog>
    </div>
  );
}
