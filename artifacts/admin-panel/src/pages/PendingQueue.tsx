import { useState, useEffect, useMemo, useRef } from "react";
import { useQuery, useMutation, useQueryClient } from "@tanstack/react-query";
import { adminFetch } from "@/lib/admin-auth";
import { useToast } from "@/hooks/use-toast";
import {
  AlertDialog, AlertDialogAction, AlertDialogCancel,
  AlertDialogContent, AlertDialogDescription, AlertDialogFooter,
  AlertDialogHeader, AlertDialogTitle, AlertDialogTrigger,
} from "@/components/ui/alert-dialog";
import {
  Loader2, RefreshCw, Hourglass, Trash2, Copy, Check,
  ChevronDown, ChevronRight, KeyRound, Clock, Mail, Timer,
  ScrollText, Shield, Users,
} from "lucide-react";

interface PendingRecord {
  id: string;
  email: string;
  pending_lids: string[];
  pending_count: number;
  bound_ids: string[];
  bound_count: number;
  pending_nc_key: string;
  is_low_admin?: boolean;
  last_updated: number;
}

interface RetryLogEntry {
  ts: number;
  msg: string;
  level: "info" | "success" | "warn" | "error" | "pending";
}

interface PendingData {
  records: PendingRecord[];
  last_retry_at: number;
  next_retry_at: number;
  interval: number;
  server_time: number;
  logs: RetryLogEntry[];
  logs_low?: RetryLogEntry[];
}

type ViewTab = "main" | "low";

const LEVEL_STYLE: Record<string, string> = {
  success: "text-emerald-400",
  error:   "text-red-400",
  warn:    "text-amber-400",
  pending: "text-sky-400",
  info:    "text-muted-foreground",
};

function RelativeTime({ ts }: { ts: number }) {
  const now = Date.now() / 1000;
  const diff = Math.round(now - ts);
  if (diff < 60) return <span>{diff} 秒前</span>;
  if (diff < 3600) return <span>{Math.floor(diff / 60)} 分钟前</span>;
  return <span>{Math.floor(diff / 3600)} 小时前</span>;
}

function AbsTime({ ts }: { ts: number }) {
  const d = new Date(ts * 1000);
  const hh = String(d.getHours()).padStart(2, "0");
  const mm = String(d.getMinutes()).padStart(2, "0");
  const ss = String(d.getSeconds()).padStart(2, "0");
  return <span>{hh}:{mm}:{ss}</span>;
}

function CopyBtn({ text }: { text: string }) {
  const [copied, setCopied] = useState(false);
  const copy = () => {
    navigator.clipboard.writeText(text).catch(() => {});
    setCopied(true);
    setTimeout(() => setCopied(false), 1500);
  };
  return (
    <button
      onClick={copy}
      className="inline-flex items-center gap-1 px-1.5 py-0.5 rounded text-xs border border-border hover:bg-muted/40 transition-colors text-muted-foreground"
    >
      {copied ? <Check className="w-3 h-3 text-emerald-400" /> : <Copy className="w-3 h-3" />}
    </button>
  );
}

function Countdown({ nextRetryAt, serverTime }: { nextRetryAt: number; serverTime: number }) {
  const clockOffset = useRef(serverTime - Date.now() / 1000);
  const [secsLeft, setSecsLeft] = useState(() => {
    const now = Date.now() / 1000 + clockOffset.current;
    return Math.max(0, Math.round(nextRetryAt - now));
  });

  useEffect(() => {
    clockOffset.current = serverTime - Date.now() / 1000;
    const update = () => {
      const now = Date.now() / 1000 + clockOffset.current;
      setSecsLeft(Math.max(0, Math.round(nextRetryAt - now)));
    };
    update();
    const id = setInterval(update, 1000);
    return () => clearInterval(id);
  }, [nextRetryAt, serverTime]);

  const mins = Math.floor(secsLeft / 60);
  const secs = secsLeft % 60;

  if (secsLeft <= 0) return <span className="text-emerald-400 font-medium">重试中...</span>;
  return (
    <span className="font-mono tabular-nums">
      {mins > 0 ? <>{mins} 分 </> : null}{String(secs).padStart(2, "0")} 秒
    </span>
  );
}

export default function PendingQueue() {
  const queryClient = useQueryClient();
  const { toast } = useToast();
  const [expanded, setExpanded] = useState<Record<string, boolean>>({});
  const [showLog, setShowLog] = useState(true);
  // Tab：'main' = 普通用户队列；'low' = LOW 用户专属队列（独立日志/记录）
  const [tab, setTab] = useState<ViewTab>("main");

  const { data, isLoading, isFetching, refetch } = useQuery<PendingData>({
    queryKey: ["admin-pending-nc"],
    queryFn: async () => {
      const res = await adminFetch("/admin/pending-nc");
      if (!res.ok) throw new Error(`HTTP ${res.status}`);
      return res.json();
    },
    refetchInterval: 30_000,
  });

  const deleteMutation = useMutation({
    mutationFn: async (id: string) => {
      const res = await adminFetch(`/admin/pending-nc/${encodeURIComponent(id)}`, { method: "DELETE" });
      if (!res.ok) throw new Error(`HTTP ${res.status}`);
    },
    onSuccess: () => {
      queryClient.invalidateQueries({ queryKey: ["admin-pending-nc"] });
      toast({ title: "已清除该排队记录" });
    },
    onError: (e: any) => {
      toast({ title: "清除失败", description: e.message, variant: "destructive" });
    },
  });

  // 全部记录：按 is_low_admin 分流
  const allRecords = data?.records ?? [];
  const mainRecords = useMemo(() => allRecords.filter((r) => !r.is_low_admin), [allRecords]);
  const lowRecords = useMemo(() => allRecords.filter((r) => !!r.is_low_admin), [allRecords]);
  const records = tab === "low" ? lowRecords : mainRecords;
  const logsRaw = tab === "low" ? (data?.logs_low ?? []) : (data?.logs ?? []);
  const logs = data ? [...logsRaw].reverse() : [];

  const toggleExpand = (id: string) =>
    setExpanded((prev) => ({ ...prev, [id]: !prev[id] }));

  return (
    <div className="space-y-4">
      {/* 页头 */}
      <div className="flex items-center justify-between flex-wrap gap-2">
        <div className="flex items-center gap-2">
          <Hourglass className="w-5 h-5 text-amber-400" />
          <h1 className="text-lg font-semibold">排队记录</h1>
          {records.length > 0 && (
            <span className="text-xs px-2 py-0.5 rounded-full bg-amber-500/15 text-amber-400 border border-amber-500/30">
              {records.length} 条
            </span>
          )}
        </div>
        <button
          onClick={() => refetch()}
          disabled={isFetching}
          className="flex items-center gap-1.5 px-3 py-1.5 rounded-md border border-border text-xs text-muted-foreground hover:bg-muted/30 transition-colors disabled:opacity-50"
        >
          <RefreshCw className={`w-3.5 h-3.5 ${isFetching ? "animate-spin" : ""}`} />
          刷新
        </button>
      </div>

      {/* 主 / LOW 切换 Tab */}
      <div className="flex items-center gap-1 border-b border-border" data-testid="pending-tabs">
        <button
          onClick={() => setTab("main")}
          data-testid="pending-tab-main"
          className={`flex items-center gap-1.5 px-3 py-2 text-sm border-b-2 transition-colors ${
            tab === "main"
              ? "border-primary text-primary"
              : "border-transparent text-muted-foreground hover:text-foreground"
          }`}
        >
          <Users className="w-4 h-4" />
          普通用户
          <span className="ml-1 text-xs px-1.5 py-0 rounded bg-muted/40">
            {mainRecords.length}
          </span>
        </button>
        <button
          onClick={() => setTab("low")}
          data-testid="pending-tab-low"
          className={`flex items-center gap-1.5 px-3 py-2 text-sm border-b-2 transition-colors ${
            tab === "low"
              ? "border-emerald-500 text-emerald-400"
              : "border-transparent text-muted-foreground hover:text-foreground"
          }`}
        >
          <Shield className="w-4 h-4" />
          LOW 用户
          <span className="ml-1 text-xs px-1.5 py-0 rounded bg-emerald-500/15 text-emerald-400">
            {lowRecords.length}
          </span>
        </button>
      </div>

      {/* 倒计时条 */}
      {data && (
        <div className="flex items-center gap-2 px-4 py-2.5 rounded-lg border border-border bg-muted/10 text-sm">
          <Timer className="w-4 h-4 text-amber-400 shrink-0" />
          <span className="text-muted-foreground">距离下一次批量重试：</span>
          <span className="text-foreground">
            <Countdown nextRetryAt={data.next_retry_at} serverTime={data.server_time} />
          </span>
          {data.last_retry_at > 0 && (
            <span className="ml-auto text-xs text-muted-foreground flex items-center gap-1">
              <Clock className="w-3 h-3" />
              上次：<RelativeTime ts={data.last_retry_at} />
            </span>
          )}
        </div>
      )}

      <p className="text-xs text-muted-foreground">
        NC 许可证尚未被 Grazie 信任时自动排队。后台每 5 分钟重试一次，全部信任后密钥额度自动升至 {tab === "low" ? 16 : 25}。
      </p>

      {/* 加载中 */}
      {isLoading && (
        <div className="flex justify-center py-10 text-muted-foreground">
          <Loader2 className="w-6 h-6 animate-spin" />
        </div>
      )}

      {/* 排队记录列表 */}
      {!isLoading && records.length === 0 && (
        <div className="flex flex-col items-center justify-center py-10 text-muted-foreground gap-2">
          <Hourglass className="w-8 h-8 opacity-30" />
          <p className="text-sm">暂无排队记录</p>
        </div>
      )}

      {records.map((rec) => (
        <div key={rec.id} className="rounded-lg border border-border bg-card overflow-hidden">
          <div className="flex items-center gap-3 px-4 py-3">
            <button onClick={() => toggleExpand(rec.id)} className="text-muted-foreground hover:text-foreground transition-colors">
              {expanded[rec.id] ? <ChevronDown className="w-4 h-4" /> : <ChevronRight className="w-4 h-4" />}
            </button>
            <div className="flex items-center gap-1.5 flex-1 min-w-0">
              <Mail className="w-3.5 h-3.5 text-muted-foreground shrink-0" />
              <span className="text-sm font-medium truncate">{rec.email || rec.id}</span>
              {rec.is_low_admin && (
                <span
                  className="ml-1 inline-flex items-center gap-1 text-[10px] px-1.5 py-0.5 rounded bg-emerald-500/15 text-emerald-400 border border-emerald-500/30 shrink-0"
                  title="该记录由 LOW_ADMIN 用户激活，将走 LOW CF 池与独立并发"
                  data-testid={`badge-low-${rec.id}`}
                >
                  <Shield className="w-2.5 h-2.5" />
                  LOW
                </span>
              )}
            </div>
            <span className="text-xs px-2 py-0.5 rounded-full bg-amber-500/10 text-amber-400 border border-amber-500/25 shrink-0">
              ⏳ {rec.pending_count} 个待信任
            </span>
            {rec.bound_count > 0 && (
              <span className="text-xs px-2 py-0.5 rounded-full bg-emerald-500/10 text-emerald-400 border border-emerald-500/25 shrink-0">
                ✓ {rec.bound_count} 已信任
              </span>
            )}
            <span className="text-xs text-muted-foreground shrink-0 flex items-center gap-1">
              <Clock className="w-3 h-3" /><RelativeTime ts={rec.last_updated} />
            </span>
            <button
              onClick={() => deleteMutation.mutate(rec.id)}
              disabled={deleteMutation.isPending}
              title="清除此排队记录"
              className="shrink-0 p-1.5 rounded hover:bg-red-500/10 text-muted-foreground hover:text-red-400 transition-colors disabled:opacity-40"
            >
              <Trash2 className="w-3.5 h-3.5" />
            </button>
          </div>

          {expanded[rec.id] && (
            <div className="border-t border-border px-4 py-3 space-y-3 bg-muted/10">
              {rec.pending_nc_key && (
                <div className="space-y-1">
                  <p className="text-xs text-muted-foreground font-medium flex items-center gap-1">
                    <KeyRound className="w-3 h-3" /> 预签 API 密钥（当前额度 0，激活后升至 {rec.is_low_admin ? 16 : 25}）
                  </p>
                  <div className="flex items-center gap-2">
                    <code className="text-xs font-mono text-primary bg-primary/10 px-2 py-1 rounded border border-primary/20 break-all flex-1">
                      {rec.pending_nc_key}
                    </code>
                    <CopyBtn text={rec.pending_nc_key} />
                  </div>
                </div>
              )}
              <div className="space-y-1">
                <p className="text-xs text-muted-foreground font-medium">⏳ 待信任 licenseId（{rec.pending_count} 个）</p>
                <div className="flex flex-wrap gap-1.5">
                  {rec.pending_lids.map((lid) => (
                    <span key={lid} className="inline-flex items-center gap-1 text-xs font-mono px-2 py-0.5 rounded bg-amber-500/10 text-amber-300 border border-amber-500/20">
                      {lid}<CopyBtn text={lid} />
                    </span>
                  ))}
                </div>
              </div>
              {rec.bound_ids.length > 0 && (
                <div className="space-y-1">
                  <p className="text-xs text-muted-foreground font-medium">✓ 已信任 licenseId（{rec.bound_count} 个）</p>
                  <div className="flex flex-wrap gap-1.5">
                    {rec.bound_ids.map((lid) => (
                      <span key={lid} className="inline-flex items-center gap-1 text-xs font-mono px-2 py-0.5 rounded bg-emerald-500/10 text-emerald-300 border border-emerald-500/20">
                        {lid}<CopyBtn text={lid} />
                      </span>
                    ))}
                  </div>
                </div>
              )}
              <p className="text-xs text-muted-foreground/50 font-mono">行 ID：{rec.id}</p>
            </div>
          )}
        </div>
      ))}

      {/* 重试日志 */}
      {data && (
        <div className="rounded-lg border border-border bg-card overflow-hidden">
          <button
            onClick={() => setShowLog((v) => !v)}
            className="w-full flex items-center justify-between px-4 py-3 hover:bg-muted/20 transition-colors"
          >
            <div className="flex items-center gap-2">
              <ScrollText className="w-4 h-4 text-sky-400" />
              <span className="text-sm font-medium">重试日志</span>
              <span className="text-xs text-muted-foreground">（最近 {logs.length} 条，新→旧）</span>
            </div>
            {showLog ? <ChevronDown className="w-4 h-4 text-muted-foreground" /> : <ChevronRight className="w-4 h-4 text-muted-foreground" />}
          </button>

          {showLog && (
            <div className="border-t border-border bg-black/20 max-h-72 overflow-y-auto">
              {logs.length === 0 ? (
                <p className="text-xs text-muted-foreground px-4 py-3">服务启动后首次重试（约 2 分钟）才会有日志</p>
              ) : (
                <table className="w-full text-xs">
                  <tbody>
                    {logs.map((entry, i) => (
                      <tr key={i} className="border-b border-border/30 last:border-0 hover:bg-muted/10">
                        <td className="px-3 py-1.5 font-mono text-muted-foreground/60 whitespace-nowrap w-16 select-none">
                          <AbsTime ts={entry.ts} />
                        </td>
                        <td className={`px-3 py-1.5 ${LEVEL_STYLE[entry.level] ?? "text-muted-foreground"}`}>
                          {entry.msg}
                        </td>
                      </tr>
                    ))}
                  </tbody>
                </table>
              )}
            </div>
          )}
        </div>
      )}
    </div>
  );
}
