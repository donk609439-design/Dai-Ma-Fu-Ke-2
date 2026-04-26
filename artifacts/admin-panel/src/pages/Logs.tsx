import { useQuery, useMutation, useQueryClient } from "@tanstack/react-query";
import { useMemo, useState } from "react";
import { adminFetch, isFullAdmin } from "@/lib/admin-auth";
import { useDiscordAuth } from "@/hooks/useDiscordAuth";
import { Button } from "@/components/ui/button";
import { Card, CardContent, CardHeader, CardTitle } from "@/components/ui/card";
import {
  RefreshCw, Trash2, ScrollText, CheckCircle2, XCircle, ShieldCheck, LogOut, MessageSquare,
} from "lucide-react";
import {
  AlertDialog, AlertDialogAction, AlertDialogCancel, AlertDialogContent,
  AlertDialogDescription, AlertDialogFooter, AlertDialogHeader, AlertDialogTitle,
  AlertDialogTrigger,
} from "@/components/ui/alert-dialog";

interface LogEntry {
  id: number;
  ts: number;
  model: string;
  key: string;
  discord_id?: string;
  prompt_tokens: number;
  completion_tokens: number;
  elapsed_ms: number;
  status: "ok" | "error";
  exempt?: boolean;
}

function formatTs(ts: number): string {
  const d = new Date(ts * 1000);
  const pad = (n: number) => String(n).padStart(2, "0");
  return `${pad(d.getMonth() + 1)}/${pad(d.getDate())} ${pad(d.getHours())}:${pad(d.getMinutes())}:${pad(d.getSeconds())}`;
}

function shortModel(model: string): string {
  return model.replace(/^anthropic-/i, "").replace(/^openai-/i, "").replace(/^google-chat-/i, "");
}

export default function Logs() {
  const fullAdmin = isFullAdmin();
  // LOW 用户场景下需要 Discord 鉴权；管理员不需要
  const { dcToken, userTag, isLoggedIn: dcLoggedIn, login: dcLogin, logout: dcLogout } =
    useDiscordAuth("activate");

  const [autoRefresh, setAutoRefresh] = useState(true);
  const qc = useQueryClient();

  // 自动注入 Discord Token（LOW 用户场景需要；管理员场景下加了也无害）
  const dcFetch = useMemo(() => {
    return (input: Parameters<typeof adminFetch>[0], init: Parameters<typeof adminFetch>[1] = {}) => {
      const headers = new Headers(init.headers);
      if (dcToken) headers.set("X-Discord-Token", dcToken);
      return adminFetch(input, { ...init, headers });
    };
  }, [dcToken]);

  // 是否启用查询：管理员永远启用；LOW 用户必须先 Discord 登录
  const queryEnabled = fullAdmin || !!dcToken;

  const { data, isLoading, error } = useQuery<{ logs: LogEntry[]; total: number }>({
    queryKey: ["call-logs", fullAdmin ? "admin" : (dcToken ?? "")],
    queryFn: async () => {
      const r = await dcFetch("/admin/logs?limit=200");
      if (!r.ok) {
        let detail = `HTTP ${r.status}`;
        try { const j = await r.json(); detail = j.detail || j.error || detail; } catch {}
        throw new Error(detail);
      }
      return r.json();
    },
    enabled: queryEnabled,
    refetchInterval: autoRefresh && queryEnabled ? 3000 : false,
    retry: false,
  });

  const clearMut = useMutation({
    mutationFn: async () => {
      const r = await dcFetch("/admin/logs", { method: "DELETE" });
      if (!r.ok) {
        let detail = `HTTP ${r.status}`;
        try { const j = await r.json(); detail = j.detail || j.error || detail; } catch {}
        throw new Error(detail);
      }
    },
    onSuccess: () => qc.invalidateQueries({ queryKey: ["call-logs"] }),
  });

  // LOW 用户未登录 Discord：仅展示登录卡片
  if (!fullAdmin && !dcLoggedIn) {
    return (
      <div className="p-6 max-w-2xl mx-auto">
        <Card className="border-card-border">
          <CardHeader>
            <CardTitle className="text-lg flex items-center gap-2">
              <ShieldCheck className="w-5 h-5 text-primary" />
              查看调用日志需要 Discord 验证
            </CardTitle>
          </CardHeader>
          <CardContent className="space-y-4">
            <p className="text-sm text-muted-foreground leading-relaxed">
              调用日志按 Discord 账号隔离。请先使用 Discord 登录，登录后只会展示你名下密钥的调用记录。
            </p>
            <Button onClick={dcLogin} className="gap-2">
              <MessageSquare className="w-4 h-4" />
              使用 Discord 登录
            </Button>
          </CardContent>
        </Card>
      </div>
    );
  }

  const logs = data?.logs ?? [];

  return (
    <div className="p-6 space-y-5">
      <div className="flex items-center justify-between flex-wrap gap-3">
        <div>
          <h1 className="text-2xl font-bold text-foreground">调用日志</h1>
          <p className="text-sm text-muted-foreground mt-1">
            {fullAdmin
              ? `全部调用记录，共 ${data?.total ?? 0} 条（内存最近 500 条，重启后清空）`
              : `你名下密钥的调用记录，共 ${data?.total ?? 0} 条（按 Discord 账号过滤）`}
          </p>
          <p className="text-xs text-muted-foreground mt-1">
            提示：单次调用输入 &lt; 200 token 且 输出 &lt; 200 token 时不计入用量（标记「豁免」）。
          </p>
        </div>
        <div className="flex items-center gap-2 flex-wrap">
          {!fullAdmin && dcLoggedIn && (
            <span className="inline-flex items-center gap-1.5 text-xs px-2.5 py-1 rounded-full bg-primary/10 text-primary border border-primary/20">
              <ShieldCheck className="w-3.5 h-3.5" />
              {userTag || "Discord 已登录"}
            </span>
          )}
          {!fullAdmin && (
            <Button variant="ghost" size="sm" onClick={dcLogout} className="gap-1.5">
              <LogOut className="w-3.5 h-3.5" /> 退出
            </Button>
          )}
          <Button
            variant={autoRefresh ? "default" : "outline"}
            size="sm"
            onClick={() => setAutoRefresh(v => !v)}
          >
            <RefreshCw className={`w-4 h-4 mr-2 ${autoRefresh ? "animate-spin" : ""}`} />
            {autoRefresh ? "自动刷新中" : "自动刷新"}
          </Button>
          <Button
            variant="outline"
            size="sm"
            onClick={() => qc.invalidateQueries({ queryKey: ["call-logs"] })}
            disabled={isLoading}
          >
            <RefreshCw className={`w-4 h-4 mr-1 ${isLoading ? "animate-spin" : ""}`} />
          </Button>
          <AlertDialog>
            <AlertDialogTrigger asChild>
              <Button variant="outline" size="sm" className="text-destructive hover:text-destructive">
                <Trash2 className="w-4 h-4 mr-2" />
                清空
              </Button>
            </AlertDialogTrigger>
            <AlertDialogContent>
              <AlertDialogHeader>
                <AlertDialogTitle>确认清空日志？</AlertDialogTitle>
                <AlertDialogDescription>
                  {fullAdmin
                    ? "此操作将清除所有调用日志，不可恢复。"
                    : "此操作仅清除你名下密钥的调用记录，其他用户的日志不受影响。"}
                </AlertDialogDescription>
              </AlertDialogHeader>
              <AlertDialogFooter>
                <AlertDialogCancel>取消</AlertDialogCancel>
                <AlertDialogAction onClick={() => clearMut.mutate()}>确认清空</AlertDialogAction>
              </AlertDialogFooter>
            </AlertDialogContent>
          </AlertDialog>
        </div>
      </div>

      {error && (
        <div className="rounded-md border border-destructive/40 bg-destructive/10 px-4 py-2 text-sm text-destructive">
          {(error as Error).message}
        </div>
      )}

      <Card className="border-card-border">
        <CardHeader className="pb-2">
          <CardTitle className="text-base font-semibold flex items-center gap-2">
            <ScrollText className="w-4 h-4 text-primary" />
            请求记录
          </CardTitle>
        </CardHeader>
        <CardContent className="p-0">
          {logs.length === 0 ? (
            <div className="flex flex-col items-center justify-center py-16 text-muted-foreground gap-2">
              <ScrollText className="w-8 h-8 opacity-30" />
              <span className="text-sm">暂无调用记录</span>
            </div>
          ) : (
            <div className="overflow-x-auto">
              <table className="w-full text-sm">
                <thead>
                  <tr className="border-b border-border bg-muted/30">
                    <th className="text-left px-4 py-2.5 text-xs text-muted-foreground font-medium whitespace-nowrap">时间</th>
                    <th className="text-left px-4 py-2.5 text-xs text-muted-foreground font-medium whitespace-nowrap">模型</th>
                    <th className="text-left px-4 py-2.5 text-xs text-muted-foreground font-medium whitespace-nowrap">密钥</th>
                    {fullAdmin && (
                      <th className="text-left px-4 py-2.5 text-xs text-muted-foreground font-medium whitespace-nowrap">Discord</th>
                    )}
                    <th className="text-right px-4 py-2.5 text-xs text-muted-foreground font-medium whitespace-nowrap">输入 tokens</th>
                    <th className="text-right px-4 py-2.5 text-xs text-muted-foreground font-medium whitespace-nowrap">输出 tokens</th>
                    <th className="text-right px-4 py-2.5 text-xs text-muted-foreground font-medium whitespace-nowrap">耗时</th>
                    <th className="text-center px-4 py-2.5 text-xs text-muted-foreground font-medium whitespace-nowrap">计费</th>
                    <th className="text-center px-4 py-2.5 text-xs text-muted-foreground font-medium whitespace-nowrap">状态</th>
                  </tr>
                </thead>
                <tbody>
                  {logs.map((log, i) => (
                    <tr
                      key={log.id}
                      className={`border-b border-border/50 transition-colors hover:bg-muted/20 ${i === 0 ? "bg-primary/5" : ""}`}
                    >
                      <td className="px-4 py-2.5 font-mono text-xs text-muted-foreground whitespace-nowrap">
                        {formatTs(log.ts)}
                      </td>
                      <td className="px-4 py-2.5">
                        <span className="font-mono text-xs text-foreground bg-muted/50 px-1.5 py-0.5 rounded">
                          {shortModel(log.model)}
                        </span>
                      </td>
                      <td className="px-4 py-2.5 font-mono text-xs text-muted-foreground whitespace-nowrap">
                        {log.key}
                      </td>
                      {fullAdmin && (
                        <td className="px-4 py-2.5 font-mono text-xs text-muted-foreground whitespace-nowrap">
                          {log.discord_id || <span className="opacity-40">-</span>}
                        </td>
                      )}
                      <td className="px-4 py-2.5 text-right font-mono text-xs text-blue-400">
                        {log.prompt_tokens.toLocaleString()}
                      </td>
                      <td className="px-4 py-2.5 text-right font-mono text-xs text-emerald-400">
                        {log.completion_tokens.toLocaleString()}
                      </td>
                      <td className="px-4 py-2.5 text-right font-mono text-xs text-muted-foreground whitespace-nowrap">
                        {log.elapsed_ms >= 1000
                          ? `${(log.elapsed_ms / 1000).toFixed(1)}s`
                          : `${log.elapsed_ms}ms`}
                      </td>
                      <td className="px-4 py-2.5 text-center">
                        {log.exempt ? (
                          <span className="inline-flex items-center gap-1 text-[10px] px-1.5 py-0.5 rounded-full bg-amber-500/15 text-amber-400 border border-amber-500/30 whitespace-nowrap">
                            豁免
                          </span>
                        ) : log.status === "ok" ? (
                          <span className="inline-flex items-center gap-1 text-[10px] px-1.5 py-0.5 rounded-full bg-blue-500/15 text-blue-400 border border-blue-500/30 whitespace-nowrap">
                            计费
                          </span>
                        ) : (
                          <span className="text-[10px] text-muted-foreground">—</span>
                        )}
                      </td>
                      <td className="px-4 py-2.5 text-center">
                        {log.status === "ok" ? (
                          <CheckCircle2 className="w-4 h-4 text-emerald-400 mx-auto" />
                        ) : (
                          <XCircle className="w-4 h-4 text-destructive mx-auto" />
                        )}
                      </td>
                    </tr>
                  ))}
                </tbody>
              </table>
            </div>
          )}
        </CardContent>
      </Card>
    </div>
  );
}
