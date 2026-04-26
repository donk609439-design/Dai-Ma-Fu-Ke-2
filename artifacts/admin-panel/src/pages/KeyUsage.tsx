import { useState } from "react";
import { Search, Key, Loader2, BarChart2, Infinity } from "lucide-react";

interface UsageResult {
  masked: string;
  usage_count: number;
  usage_cost: number;
  usage_limit: number | null;
}

function fmtCost(v: number): string {
  return Number.isInteger(v) ? String(v) : v.toFixed(2).replace(/\.?0+$/, "");
}

export default function KeyUsage() {
  const [inputKey, setInputKey] = useState("");
  const [result, setResult] = useState<UsageResult | null>(null);
  const [error, setError] = useState<string | null>(null);
  const [loading, setLoading] = useState(false);

  const handleQuery = async (e: React.FormEvent) => {
    e.preventDefault();
    const key = inputKey.trim();
    if (!key) return;
    setLoading(true);
    setResult(null);
    setError(null);
    try {
      const res = await fetch(`/key/usage?key=${encodeURIComponent(key)}`);
      if (!res.ok) {
        const data = await res.json().catch(() => ({}));
        throw new Error(data.detail || "密钥不存在或无效");
      }
      setResult(await res.json());
    } catch (e: any) {
      setError(e.message || "查询失败");
    } finally {
      setLoading(false);
    }
  };

  const pct =
    result && result.usage_limit
      ? Math.min(100, Math.round((result.usage_cost / result.usage_limit) * 100))
      : null;

  const barColor =
    pct === null
      ? "bg-primary"
      : pct >= 90
      ? "bg-red-500"
      : pct >= 70
      ? "bg-amber-500"
      : "bg-emerald-500";

  return (
    <div className="p-6 max-w-lg mx-auto">
      <div className="flex items-center gap-3 mb-6">
        <div className="flex items-center justify-center w-10 h-10 rounded-lg bg-primary/10">
          <BarChart2 className="w-5 h-5 text-primary" />
        </div>
        <div>
          <h1 className="text-xl font-semibold text-foreground">用量查询</h1>
          <p className="text-sm text-muted-foreground">输入您的 API 密钥，查看当前调用次数</p>
        </div>
      </div>

      <form onSubmit={handleQuery} className="flex gap-2 mb-6">
        <div className="relative flex-1">
          <Key className="absolute left-3 top-1/2 -translate-y-1/2 w-4 h-4 text-muted-foreground" />
          <input
            type="password"
            value={inputKey}
            onChange={(e) => setInputKey(e.target.value)}
            placeholder="输入您的 API 密钥..."
            className="w-full pl-9 pr-4 py-2.5 rounded-md border border-input bg-background text-sm text-foreground placeholder:text-muted-foreground focus:outline-none focus:ring-2 focus:ring-primary/30"
          />
        </div>
        <button
          type="submit"
          disabled={loading || !inputKey.trim()}
          className="flex items-center gap-2 px-4 py-2.5 rounded-md bg-primary text-primary-foreground text-sm font-medium hover:bg-primary/90 disabled:opacity-50 disabled:cursor-not-allowed transition-colors shrink-0"
        >
          {loading ? <Loader2 className="w-4 h-4 animate-spin" /> : <Search className="w-4 h-4" />}
          查询
        </button>
      </form>

      {error && (
        <div className="p-4 rounded-lg border border-red-500/30 bg-red-500/10 text-sm text-red-400">
          {error}
        </div>
      )}

      {result && (
        <div className="rounded-lg border border-border bg-card p-5 space-y-4">
          <div className="flex items-center gap-2.5">
            <div className="flex items-center justify-center w-8 h-8 rounded-md bg-primary/10">
              <Key className="w-4 h-4 text-primary" />
            </div>
            <div>
              <p className="text-xs text-muted-foreground mb-0.5">密钥（已脱敏）</p>
              <code className="text-sm font-mono text-foreground tracking-wide">{result.masked}</code>
            </div>
          </div>

          <div className="space-y-2">
            <div className="flex items-center justify-between text-sm">
              <span className="text-muted-foreground">已使用</span>
              <span className="font-semibold text-foreground">
                {fmtCost(result.usage_cost)}
                {result.usage_limit !== null ? (
                  <span className="text-muted-foreground font-normal"> / {result.usage_limit.toLocaleString()} 次</span>
                ) : (
                  <span className="text-muted-foreground font-normal"> 次</span>
                )}
              </span>
            </div>

            {result.usage_limit !== null ? (
              <>
                <div className="w-full h-2.5 rounded-full bg-muted overflow-hidden">
                  <div
                    className={`h-full rounded-full transition-all ${barColor}`}
                    style={{ width: `${pct}%` }}
                  />
                </div>
                <p className="text-xs text-muted-foreground text-right">{pct}% 已消耗</p>
              </>
            ) : (
              <div className="flex items-center gap-1.5 text-xs text-muted-foreground">
                <Infinity className="w-3.5 h-3.5" />
                无调用次数限制
              </div>
            )}
          </div>

          {result.usage_limit !== null && pct !== null && pct >= 90 && (
            <div className="text-xs text-amber-400 bg-amber-500/10 border border-amber-500/20 rounded-md px-3 py-2">
              用量即将耗尽，请联系管理员续期或申请新密钥
            </div>
          )}
        </div>
      )}

      <p className="mt-6 text-xs text-muted-foreground text-center">
        此页面不展示完整密钥信息，请妥善保管您的密钥
      </p>
    </div>
  );
}
