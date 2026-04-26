import { useQuery } from "@tanstack/react-query";
import { RefreshCw, BarChart3, Zap, AlertCircle, Clock, Activity } from "lucide-react";
import { adminFetch } from "@/lib/admin-auth";
import { Button } from "@/components/ui/button";
import { Card, CardContent, CardHeader, CardTitle } from "@/components/ui/card";
import { Badge } from "@/components/ui/badge";

interface AccountStat {
  calls: number;
  errors: number;
  prompt_tokens: number;
  completion_tokens: number;
  avg_ttft_ms: number | null;
  avg_total_ms: number | null;
  p90_ttft_ms: number | null;
  p90_total_ms: number | null;
}

interface StatsResponse {
  uptime_seconds: number;
  total: {
    calls: number;
    errors: number;
    prompt_tokens: number;
    completion_tokens: number;
  };
  accounts: Record<string, AccountStat>;
}

function formatUptime(seconds: number): string {
  const d = Math.floor(seconds / 86400);
  const h = Math.floor((seconds % 86400) / 3600);
  const m = Math.floor((seconds % 3600) / 60);
  const s = Math.floor(seconds % 60);
  if (d > 0) return `${d}天 ${h}小时 ${m}分`;
  if (h > 0) return `${h}小时 ${m}分 ${s}秒`;
  if (m > 0) return `${m}分 ${s}秒`;
  return `${s}秒`;
}

function formatMs(ms: number | null): string {
  if (ms === null) return "—";
  if (ms >= 1000) return `${(ms / 1000).toFixed(2)}s`;
  return `${ms.toFixed(0)}ms`;
}

function formatTokens(n: number): string {
  if (n >= 1_000_000) return `${(n / 1_000_000).toFixed(2)}M`;
  if (n >= 1_000) return `${(n / 1_000).toFixed(1)}k`;
  return String(n);
}

function StatCard({ label, value, sub, icon: Icon, accent }: {
  label: string; value: string; sub?: string; icon: any; accent?: string;
}) {
  return (
    <Card className="border-card-border">
      <CardContent className="pt-5 pb-4">
        <div className="flex items-start justify-between">
          <div>
            <p className="text-xs text-muted-foreground mb-1">{label}</p>
            <p className={`text-2xl font-bold ${accent ?? "text-foreground"}`}>{value}</p>
            {sub && <p className="text-xs text-muted-foreground mt-1">{sub}</p>}
          </div>
          <div className={`p-2 rounded-lg ${accent ? "bg-primary/10" : "bg-muted/50"}`}>
            <Icon className={`w-4 h-4 ${accent ?? "text-muted-foreground"}`} />
          </div>
        </div>
      </CardContent>
    </Card>
  );
}

export default function Stats() {
  const { data, isLoading, refetch, error } = useQuery<StatsResponse>({
    queryKey: ["admin-stats"],
    queryFn: async () => {
      const res = await adminFetch("/admin/stats");
      if (!res.ok) throw new Error("获取统计数据失败");
      return res.json();
    },
    refetchInterval: 10000,
  });

  const total = data?.total;
  const accounts = data ? Object.entries(data.accounts) : [];
  const errorRate = total && total.calls + total.errors > 0
    ? ((total.errors / (total.calls + total.errors)) * 100).toFixed(1)
    : "0.0";

  return (
    <div className="p-6 space-y-6">
      <div className="flex items-center justify-between">
        <div>
          <h1 className="text-2xl font-bold text-foreground">用量统计</h1>
          <p className="text-sm text-muted-foreground mt-1">各账户后端调用数据与延迟分析（内存统计，重启后重置）</p>
        </div>
        <Button variant="outline" size="sm" onClick={() => refetch()} disabled={isLoading}>
          <RefreshCw className={`w-4 h-4 mr-2 ${isLoading ? "animate-spin" : ""}`} />
          刷新
        </Button>
      </div>

      {error ? (
        <Card className="border-destructive/30 bg-destructive/10">
          <CardContent className="flex items-center gap-2 py-4 text-destructive">
            <AlertCircle className="w-4 h-4" />
            <span className="text-sm">获取统计数据失败，请确认服务正在运行</span>
          </CardContent>
        </Card>
      ) : (
        <>
          {/* 总览卡片 */}
          <div className="grid grid-cols-2 lg:grid-cols-4 gap-4">
            <StatCard
              label="服务运行时长"
              value={data ? formatUptime(data.uptime_seconds) : "—"}
              icon={Clock}
            />
            <StatCard
              label="总调用次数"
              value={total ? String(total.calls) : "—"}
              sub={`错误率 ${errorRate}%`}
              icon={Activity}
              accent="text-primary"
            />
            <StatCard
              label="Prompt Tokens（估算）"
              value={total ? formatTokens(total.prompt_tokens) : "—"}
              icon={BarChart3}
            />
            <StatCard
              label="Completion Tokens（估算）"
              value={total ? formatTokens(total.completion_tokens) : "—"}
              icon={Zap}
            />
          </div>

          {/* 各账户统计 */}
          <Card className="border-card-border">
            <CardHeader className="pb-3">
              <CardTitle className="text-base flex items-center gap-2">
                <BarChart3 className="w-4 h-4 text-primary" />
                各账户明细
                <Badge variant="outline" className="ml-auto text-xs">
                  {accounts.length} 个账户
                </Badge>
              </CardTitle>
            </CardHeader>
            <CardContent>
              {accounts.length === 0 ? (
                <div className="flex flex-col items-center justify-center py-12 text-muted-foreground gap-2">
                  <BarChart3 className="w-8 h-8 opacity-30" />
                  <p className="text-sm">暂无统计数据</p>
                  <p className="text-xs opacity-70">发起一次 API 调用后数据将出现在这里</p>
                </div>
              ) : (
                <div className="overflow-x-auto">
                  <table className="w-full text-sm">
                    <thead>
                      <tr className="border-b border-border text-left">
                        <th className="pb-2 pr-4 text-xs text-muted-foreground font-medium">账户标识</th>
                        <th className="pb-2 pr-4 text-xs text-muted-foreground font-medium text-right">调用次数</th>
                        <th className="pb-2 pr-4 text-xs text-muted-foreground font-medium text-right">错误次数</th>
                        <th className="pb-2 pr-4 text-xs text-muted-foreground font-medium text-right">Prompt Tokens</th>
                        <th className="pb-2 pr-4 text-xs text-muted-foreground font-medium text-right">Completion Tokens</th>
                        <th className="pb-2 pr-4 text-xs text-muted-foreground font-medium text-right">均值 TTFT</th>
                        <th className="pb-2 pr-4 text-xs text-muted-foreground font-medium text-right">P90 TTFT</th>
                        <th className="pb-2 pr-4 text-xs text-muted-foreground font-medium text-right">均值总耗时</th>
                        <th className="pb-2 text-xs text-muted-foreground font-medium text-right">P90 总耗时</th>
                      </tr>
                    </thead>
                    <tbody className="divide-y divide-border/50">
                      {accounts.map(([id, stat]) => (
                        <tr key={id} className="hover:bg-muted/20 transition-colors">
                          <td className="py-3 pr-4">
                            <code className="text-xs font-mono text-primary bg-primary/10 px-2 py-0.5 rounded truncate max-w-[160px] block">
                              {id}
                            </code>
                          </td>
                          <td className="py-3 pr-4 text-right font-mono">{stat.calls}</td>
                          <td className="py-3 pr-4 text-right font-mono">
                            <span className={stat.errors > 0 ? "text-destructive" : "text-muted-foreground"}>
                              {stat.errors}
                            </span>
                          </td>
                          <td className="py-3 pr-4 text-right font-mono text-muted-foreground">
                            {formatTokens(stat.prompt_tokens)}
                          </td>
                          <td className="py-3 pr-4 text-right font-mono text-muted-foreground">
                            {formatTokens(stat.completion_tokens)}
                          </td>
                          <td className="py-3 pr-4 text-right font-mono text-emerald-400">
                            {formatMs(stat.avg_ttft_ms)}
                          </td>
                          <td className="py-3 pr-4 text-right font-mono text-muted-foreground">
                            {formatMs(stat.p90_ttft_ms)}
                          </td>
                          <td className="py-3 pr-4 text-right font-mono text-blue-400">
                            {formatMs(stat.avg_total_ms)}
                          </td>
                          <td className="py-3 text-right font-mono text-muted-foreground">
                            {formatMs(stat.p90_total_ms)}
                          </td>
                        </tr>
                      ))}
                    </tbody>
                  </table>
                </div>
              )}
            </CardContent>
          </Card>

          <p className="text-xs text-muted-foreground text-center">
            Token 数为按字符估算值（÷4），非精确计数；TTFT 仅流式请求有数据；P90 需至少 10 次调用才显示
          </p>
        </>
      )}
    </div>
  );
}
