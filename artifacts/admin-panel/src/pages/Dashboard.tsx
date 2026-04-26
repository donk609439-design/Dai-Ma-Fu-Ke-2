import { useQuery } from "@tanstack/react-query";
import { useState } from "react";
import { RefreshCw, Server, Users, Key, Cpu, CheckCircle, XCircle, AlertCircle, Activity, Database, ArrowDownToLine, ClipboardPaste, ChevronDown, ChevronUp, Stethoscope, Upload } from "lucide-react";
import { adminFetch, getAdminKey } from "@/lib/admin-auth";
import { Button } from "@/components/ui/button";
import { Card, CardContent, CardHeader, CardTitle } from "@/components/ui/card";
import { Badge } from "@/components/ui/badge";
import { Input } from "@/components/ui/input";
import { Label } from "@/components/ui/label";
import { Textarea } from "@/components/ui/textarea";
import { useToast } from "@/hooks/use-toast";

interface StatusData {
  status: string;
  accounts_count: number;
  keys_count: number;
  models_count: number;
  current_account_index: number;
}

interface MigrateResult {
  success: boolean;
  imported_accounts: number;
  imported_keys: number;
  total_accounts_now: number;
  total_keys_now: number;
  imported_prizes?: number;
  imported_saint_points?: number;
  imported_saint_donations?: number;
  imported_user_items?: number;
  imported_pokeballs?: number;
  imported_cf_proxies?: number;
  error?: string;
  degraded?: boolean;
  accounts_skipped?: boolean;
  export_all_error?: string;
  degraded_hint?: string;
}

interface ExtraImportResult {
  success: boolean;
  imported_prizes?: number;
  imported_saint_points?: number;
  imported_saint_donations?: number;
  imported_user_items?: number;
  imported_pokeballs?: number;
  imported_user_passwords?: number;
  imported_donated_jb_accounts?: number;
  imported_cf_proxies?: number;
  imported_accounts?: number;
  imported_keys?: number;
  error?: string;
}

function formatBytes(n: number): string {
  if (n < 1024) return `${n} B`;
  if (n < 1024 * 1024) return `${(n / 1024).toFixed(1)} KB`;
  if (n < 1024 * 1024 * 1024) return `${(n / 1024 / 1024).toFixed(1)} MB`;
  return `${(n / 1024 / 1024 / 1024).toFixed(2)} GB`;
}

const SQL_INSTRUCTIONS = [
  {
    label: "账号（jb_accounts）★ 当源端 export-all 崩溃时必填",
    sql: `\\t on
\\pset format unaligned
SELECT COALESCE(json_agg(row_to_json(t)), '[]'::json) FROM (
  SELECT id, license_id, auth_token, jwt,
         COALESCE(has_quota, TRUE) AS has_quota,
         COALESCE(last_updated, 0) AS last_updated,
         COALESCE(last_quota_check, 0) AS last_quota_check,
         daily_used, daily_total,
         COALESCE(external_usage_count, 0) AS external_usage_count
  FROM jb_accounts ORDER BY id
) t;`,
    field: "accounts",
  },
  {
    label: "客户端密钥（jb_client_keys）",
    sql: `\\t on
\\pset format unaligned
SELECT COALESCE(json_agg(row_to_json(t)), '[]'::json) FROM (
  SELECT key,
         COALESCE(usage_limit, 0) AS usage_limit,
         COALESCE(usage_count, 0) AS usage_count,
         account_id,
         COALESCE(banned, FALSE) AS banned,
         banned_at,
         COALESCE(is_nc_key, FALSE) AS is_nc_key,
         COALESCE(is_low_admin_key, FALSE) AS is_low_admin_key,
         COALESCE(low_admin_discord_id, '') AS low_admin_discord_id
  FROM jb_client_keys ORDER BY key
) t;`,
    field: "keys",
  },
  {
    label: "背包物品（user_items）",
    sql: `SELECT json_agg(row_to_json(t)) FROM (SELECT owner_key, prize_name, metadata, used FROM user_items ORDER BY id) t;`,
    field: "user_items",
  },
  {
    label: "宝可梦球（pokeballs）",
    sql: `SELECT json_agg(json_build_object('ball_key',p.ball_key,'name',p.name,'capacity',p.capacity,'total_used',p.total_used,'rr_index',p.rr_index,'members',COALESCE((SELECT json_agg(pm.member_key) FROM pokeball_members pm WHERE pm.pokeball_id=p.id),'[]'::json))) FROM pokeballs p;`,
    field: "pokeballs",
  },
  {
    label: "圣人积分（saint_points）",
    sql: `SELECT json_agg(row_to_json(t)) FROM (SELECT password, points FROM saint_points ORDER BY password) t;`,
    field: "saint_points",
  },
  {
    label: "捐献记录（saint_donations）",
    sql: `SELECT json_agg(row_to_json(t)) FROM (SELECT account_id, password FROM saint_donations ORDER BY account_id) t;`,
    field: "saint_donations",
  },
];

export default function Dashboard() {
  const { toast } = useToast();
  const [sourceUrl, setSourceUrl] = useState("");
  const [sourceKey, setSourceKey] = useState("");
  const [migrating, setMigrating] = useState(false);
  const [migrateResult, setMigrateResult] = useState<MigrateResult | null>(null);

  const [jsonFields, setJsonFields] = useState<Record<string, string>>({});
  const [jsonFiles, setJsonFiles] = useState<Record<string, File>>({});
  const [jsonImporting, setJsonImporting] = useState(false);
  const [jsonProgress, setJsonProgress] = useState<string>("");
  const [jsonResult, setJsonResult] = useState<ExtraImportResult | null>(null);
  const [showSql, setShowSql] = useState(false);

  const [probing, setProbing] = useState(false);
  const [probeResult, setProbeResult] = useState<Record<string, unknown> | null>(null);

  const handleProbe = async () => {
    if (!sourceUrl.trim()) return;
    setProbing(true);
    setProbeResult(null);
    try {
      const res = await adminFetch("/admin/migration-probe", {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({
          source_url: sourceUrl.trim().replace(/\/$/, ""),
          source_admin_key: sourceKey.trim() || getAdminKey(),
        }),
      });
      const data = await res.json();
      setProbeResult(data);
    } catch (e: unknown) {
      setProbeResult({ ok: false, error: String(e) });
    } finally {
      setProbing(false);
    }
  };

  const handleMigrate = async () => {
    if (!sourceUrl.trim()) return;
    setMigrating(true);
    setMigrateResult(null);
    try {
      const res = await adminFetch("/admin/accounts/import-from-source", {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({
          source_url: sourceUrl.trim().replace(/\/$/, ""),
          source_admin_key: sourceKey.trim() || getAdminKey(),
        }),
      });
      const data = await res.json();
      setMigrateResult(res.ok ? data : { success: false, error: data.error || `HTTP ${res.status}`, imported_accounts: 0, imported_keys: 0, total_accounts_now: 0, total_keys_now: 0 });
    } catch (e: unknown) {
      setMigrateResult({ success: false, error: String(e), imported_accounts: 0, imported_keys: 0, total_accounts_now: 0, total_keys_now: 0 });
    } finally {
      setMigrating(false);
    }
  };

  const handleJsonImport = async () => {
    setJsonImporting(true);
    setJsonResult(null);
    setJsonProgress("");
    const totals: ExtraImportResult = { success: true };
    const errors: string[] = [];
    try {
      // 收集所有需要上传的字段：文件优先于粘贴文本
      const tasks: Array<{ field: string; label: string; body: BodyInit; sizeHint: string }> = [];
      for (const item of SQL_INSTRUCTIONS) {
        const file = jsonFiles[item.field];
        const text = (jsonFields[item.field] || "").trim();
        if (file) {
          tasks.push({ field: item.field, label: item.label, body: file, sizeHint: formatBytes(file.size) });
        } else if (text) {
          tasks.push({ field: item.field, label: item.label, body: text, sizeHint: `${text.length.toLocaleString()} 字符` });
        }
      }
      if (tasks.length === 0) {
        setJsonResult({ success: false, error: "请至少粘贴一个字段或上传一个文件" });
        return;
      }
      // 逐字段串行 POST。文件直接作为 fetch body —— 浏览器底层流式上传，
      // 不会把 79MB+ 内容 materialize 到 JS 字符串里。
      let i = 0;
      for (const t of tasks) {
        i++;
        setJsonProgress(`(${i}/${tasks.length}) 正在上传 ${t.label}（${t.sizeHint}）…`);
        try {
          const res = await adminFetch(
            `/admin/extra-import-stream?field=${encodeURIComponent(t.field)}`,
            { method: "POST", headers: { "Content-Type": "application/json" }, body: t.body },
          );
          const data = await res.json().catch(() => ({}));
          if (!res.ok) {
            errors.push(`${t.label}: ${data.error || `HTTP ${res.status}`}`);
          } else {
            const totalsAny = totals as unknown as Record<string, unknown>;
            for (const k of Object.keys(data)) {
              if (k.startsWith("imported_") && typeof data[k] === "number") {
                const cur = totalsAny[k];
                totalsAny[k] = (typeof cur === "number" ? cur : 0) + data[k];
              }
            }
          }
        } catch (e: unknown) {
          errors.push(`${t.label}: ${(e as Error).message || String(e)}`);
        }
      }
      totals.success = errors.length === 0;
      if (errors.length) totals.error = errors.join("； ");
      setJsonResult(totals);
      if (errors.length === 0) {
        // 成功后清空所有 slot，避免重复提交
        setJsonFiles({});
        setJsonFields({});
      }
    } finally {
      setJsonImporting(false);
      setJsonProgress("");
    }
  };

  const { data: status, isLoading, error, refetch } = useQuery<StatusData>({
    queryKey: ["admin-status"],
    queryFn: async () => {
      const res = await adminFetch("/admin/status");
      if (!res.ok) throw new Error("无法连接到 AI 服务");
      return res.json();
    },
    refetchInterval: 30000,
  });

  const { data: modelsData } = useQuery<{ models: string[]; anthropic_model_mappings?: Record<string, string> }>({
    queryKey: ["models-list"],
    queryFn: async () => {
      const res = await adminFetch("/admin/models");
      if (!res.ok) throw new Error("无法获取模型列表");
      return res.json();
    },
  });

  const stats = [
    { label: "JetBrains 账户", value: status?.accounts_count ?? "-", icon: Users, color: "text-blue-400", bg: "bg-blue-500/10" },
    { label: "客户端 API 密钥", value: status?.keys_count ?? "-", icon: Key, color: "text-emerald-400", bg: "bg-emerald-500/10" },
    { label: "可用模型数量", value: status?.models_count ?? "-", icon: Cpu, color: "text-purple-400", bg: "bg-purple-500/10" },
    { label: "当前账户索引", value: status?.current_account_index ?? "-", icon: Activity, color: "text-amber-400", bg: "bg-amber-500/10" },
  ];

  return (
    <div className="p-6 space-y-6">
      <div className="flex items-center justify-between">
        <div>
          <h1 className="text-2xl font-bold text-foreground">控制台</h1>
          <p className="text-sm text-muted-foreground mt-1">JetBrains AI API 服务状态总览</p>
        </div>
        <div className="flex items-center gap-3">
          <Button variant="outline" size="sm" onClick={() => refetch()} disabled={isLoading}>
            <RefreshCw className={`w-4 h-4 mr-2 ${isLoading ? "animate-spin" : ""}`} />
            刷新
          </Button>
        </div>
      </div>

      {/* Service Status Banner */}
      <Card className={`border ${error ? "border-destructive/50 bg-destructive/5" : "border-emerald-500/30 bg-emerald-500/5"}`}>
        <CardContent className="flex items-center gap-3 py-4">
          {error ? (
            <XCircle className="w-5 h-5 text-destructive shrink-0" />
          ) : (
            <CheckCircle className="w-5 h-5 text-emerald-400 shrink-0" />
          )}
          <div>
            <p className={`text-sm font-medium ${error ? "text-destructive" : "text-emerald-400"}`}>
              {error ? "服务连接失败" : "服务运行正常"}
            </p>
            <p className="text-xs text-muted-foreground">
              {error ? String(error) : "JetBrains AI OpenAI Compatible API 服务正在运行"}
            </p>
          </div>
          {!error && status && (
            <Badge className="ml-auto bg-emerald-500/20 text-emerald-400 border-emerald-500/30">
              {status.status}
            </Badge>
          )}
        </CardContent>
      </Card>

      {/* Stats Grid */}
      <div className="grid grid-cols-2 lg:grid-cols-4 gap-4">
        {stats.map(({ label, value, icon: Icon, color, bg }) => (
          <Card key={label} className="border-card-border">
            <CardContent className="pt-5">
              <div className="flex items-center justify-between mb-3">
                <span className="text-sm text-muted-foreground">{label}</span>
                <div className={`p-2 rounded-lg ${bg}`}>
                  <Icon className={`w-4 h-4 ${color}`} />
                </div>
              </div>
              <p className="text-3xl font-bold text-foreground">{value}</p>
            </CardContent>
          </Card>
        ))}
      </div>

      {/* Models List */}
      <Card className="border-card-border">
        <CardHeader className="pb-3">
          <CardTitle className="text-base font-semibold flex items-center gap-2">
            <Cpu className="w-4 h-4 text-primary" />
            可用模型列表
          </CardTitle>
        </CardHeader>
        <CardContent>
          {modelsData?.models?.length ? (
            <div className="grid grid-cols-1 sm:grid-cols-2 lg:grid-cols-3 gap-2">
              {modelsData.models.map((modelId) => (
                <div key={modelId} className="flex items-center gap-2 p-3 rounded-lg bg-muted/40 border border-border">
                  <div className="w-2 h-2 rounded-full bg-emerald-400 shrink-0" />
                  <span className="text-sm font-mono text-foreground truncate">{modelId}</span>
                </div>
              ))}
            </div>
          ) : (
            <div className="flex items-center justify-center py-8 text-muted-foreground">
              <AlertCircle className="w-4 h-4 mr-2" />
              <span className="text-sm">暂无可用模型</span>
            </div>
          )}
        </CardContent>
      </Card>

      {/* API Endpoints */}
      <Card className="border-card-border">
        <CardHeader className="pb-3">
          <CardTitle className="text-base font-semibold flex items-center gap-2">
            <Server className="w-4 h-4 text-primary" />
            API 接口地址
          </CardTitle>
        </CardHeader>
        <CardContent className="space-y-2">
          {[
            { method: "GET", path: "/v1/models", desc: "获取可用模型列表" },
            { method: "POST", path: "/v1/chat/completions", desc: "OpenAI 兼容聊天接口" },
            { method: "POST", path: "/v1/messages", desc: "Anthropic 兼容消息接口" },
          ].map(({ method, path, desc }) => (
            <div key={path} className="flex items-center gap-3 p-3 rounded-lg bg-muted/40 border border-border">
              <Badge variant="outline" className={`text-xs font-mono shrink-0 ${method === "GET" ? "border-blue-500/50 text-blue-400" : "border-emerald-500/50 text-emerald-400"}`}>
                {method}
              </Badge>
              <code className="text-sm text-foreground font-mono">{path}</code>
              <span className="text-xs text-muted-foreground ml-auto">{desc}</span>
            </div>
          ))}
        </CardContent>
      </Card>

      {/* 数据迁移 */}
      <Card className="border-card-border">
        <CardHeader className="pb-3">
          <CardTitle className="text-base font-semibold flex items-center gap-2">
            <Database className="w-4 h-4 text-primary" />
            数据迁移（从旧环境导入）
          </CardTitle>
        </CardHeader>
        <CardContent className="space-y-4">
          <p className="text-xs text-muted-foreground">
            从另一个部署实例拉取全部数据（账号、密钥、奖品管理、抽奖积分、背包物品），写入本库后立即生效，不影响已有数据（冲突时以源数据覆盖）。
          </p>
          <div className="space-y-3">
            <div className="space-y-1.5">
              <Label className="text-xs text-muted-foreground">源环境 URL（例如 https://xxx.replit.dev）</Label>
              <Input
                placeholder="https://your-old-project.replit.dev"
                value={sourceUrl}
                onChange={e => setSourceUrl(e.target.value)}
                className="font-mono text-sm"
              />
            </div>
            <div className="space-y-1.5">
              <Label className="text-xs text-muted-foreground">源环境 ADMIN_KEY（留空则使用当前环境的 KEY）</Label>
              <Input
                placeholder="留空 = 使用当前 ADMIN_KEY"
                type="password"
                value={sourceKey}
                onChange={e => setSourceKey(e.target.value)}
                className="font-mono text-sm"
              />
            </div>
          </div>
          <div className="flex gap-2">
            <Button
              onClick={handleMigrate}
              disabled={migrating || !sourceUrl.trim()}
              className="flex-1"
            >
              <ArrowDownToLine className={`w-4 h-4 mr-2 ${migrating ? "animate-bounce" : ""}`} />
              {migrating ? "迁移中，请稍候…" : "开始迁移"}
            </Button>
            <Button
              onClick={handleProbe}
              disabled={probing || !sourceUrl.trim()}
              variant="outline"
              className="border-amber-500/40 text-amber-500 hover:text-amber-400 hover:bg-amber-500/10"
              title="只调用源端 export-all 不导入数据，用于查看源端真实响应"
            >
              <Stethoscope className={`w-4 h-4 mr-2 ${probing ? "animate-pulse" : ""}`} />
              {probing ? "诊断中…" : "诊断源端"}
            </Button>
          </div>

          {probeResult && (() => {
            const ok = probeResult.ok === true;
            const status = probeResult.status_code as number | undefined;
            const body = (probeResult.body_truncated_8kb as string) ?? "";
            const summary = probeResult.json_top_level_summary as Record<string, string> | null;
            const stage = probeResult.stage as string | undefined;
            const errorMsg = probeResult.error as string | undefined;
            return (
              <div className={`rounded-lg p-4 text-xs space-y-3 border ${ok ? "bg-emerald-500/5 border-emerald-500/30" : "bg-amber-500/5 border-amber-500/30"}`}>
                <div className="flex items-center gap-2 text-sm font-medium">
                  <Stethoscope className={`w-4 h-4 ${ok ? "text-emerald-400" : "text-amber-400"}`} />
                  <span className={ok ? "text-emerald-400" : "text-amber-400"}>
                    诊断结果：{stage === "network" ? "网络层失败" : ok ? `源端正常（HTTP ${status}）` : `源端异常（HTTP ${status ?? "?"}）`}
                  </span>
                </div>
                {errorMsg && (
                  <div className="text-destructive font-mono break-all">{errorMsg}</div>
                )}
                {probeResult.url ? (
                  <div className="text-muted-foreground">
                    URL：<code className="font-mono break-all text-foreground/80">{String(probeResult.url)}</code>
                  </div>
                ) : null}
                {probeResult.content_type ? (
                  <div className="text-muted-foreground">
                    Content-Type：<code className="font-mono">{String(probeResult.content_type)}</code>
                    {" · "}长度：<code className="font-mono">{String(probeResult.content_length_bytes ?? 0)}</code> 字节
                    {probeResult.server_header ? <> · Server：<code className="font-mono">{String(probeResult.server_header)}</code></> : null}
                  </div>
                ) : null}
                {summary && (
                  <div>
                    <div className="text-muted-foreground mb-1">JSON 顶层字段：</div>
                    <pre className="bg-muted/40 border border-border rounded p-2 font-mono text-[11px] overflow-x-auto">
{Object.entries(summary).map(([k, v]) => `${k}: ${v}`).join("\n")}
                    </pre>
                  </div>
                )}
                {body && (
                  <div>
                    <div className="text-muted-foreground mb-1">响应正文（截前 8KB）：</div>
                    <pre className="bg-muted/40 border border-border rounded p-2 font-mono text-[11px] max-h-80 overflow-auto whitespace-pre-wrap break-all">
{body}
                    </pre>
                  </div>
                )}
              </div>
            );
          })()}

          {migrateResult && (
            <div className={`rounded-lg p-4 text-sm space-y-1 ${
              migrateResult.success
                ? (migrateResult.degraded
                    ? "bg-amber-500/10 border border-amber-500/30"
                    : "bg-emerald-500/10 border border-emerald-500/30")
                : "bg-destructive/10 border border-destructive/30"
            }`}>
              {migrateResult.success ? (
                <>
                  <p className={`font-medium flex items-center gap-2 ${migrateResult.degraded ? "text-amber-400" : "text-emerald-400"}`}>
                    {migrateResult.degraded ? <AlertCircle className="w-4 h-4" /> : <CheckCircle className="w-4 h-4" />}
                    {migrateResult.degraded ? "迁移部分完成（降级模式）" : "迁移完成"}
                  </p>
                  <p className="text-muted-foreground">导入账号：<span className="text-foreground font-mono">{migrateResult.imported_accounts}</span>　导入密钥：<span className="text-foreground font-mono">{migrateResult.imported_keys}</span></p>
                  <p className="text-muted-foreground">当前账号总数：<span className="text-foreground font-mono">{migrateResult.total_accounts_now}</span>　密钥总数：<span className="text-foreground font-mono">{migrateResult.total_keys_now}</span></p>
                  {(migrateResult.imported_prizes !== undefined) && (
                    <p className="text-muted-foreground">
                      导入奖品：<span className="text-foreground font-mono">{migrateResult.imported_prizes}</span>　
                      圣人积分：<span className="text-foreground font-mono">{migrateResult.imported_saint_points ?? 0}</span>　
                      捐献记录：<span className="text-foreground font-mono">{migrateResult.imported_saint_donations ?? 0}</span>
                    </p>
                  )}
                  {(migrateResult.imported_user_items !== undefined) && (
                    <p className="text-muted-foreground">
                      背包物品：<span className="text-foreground font-mono">{migrateResult.imported_user_items}</span>　
                      宝可梦球：<span className="text-foreground font-mono">{migrateResult.imported_pokeballs ?? 0}</span>　
                      CF 代理：<span className="text-foreground font-mono">{migrateResult.imported_cf_proxies ?? 0}</span>
                    </p>
                  )}
                  {migrateResult.degraded && (
                    <div className="mt-3 pt-3 border-t border-amber-500/30 space-y-2">
                      <p className="text-xs text-amber-300/90">
                        <strong>源端 export-all 失败：</strong>
                        <code className="font-mono break-all ml-1">{migrateResult.export_all_error}</code>
                      </p>
                      <p className="text-xs text-amber-200/80 leading-relaxed">
                        {migrateResult.degraded_hint}
                      </p>
                      <p className="text-xs text-amber-100/90">
                        ↓ 请在下方"JSON 粘贴导入"卡片里，按指引在源端 Shell 执行 SQL 拿到账号 JSON 后粘贴到"账号（jb_accounts）"输入框，最后点击"导入 JSON"。
                      </p>
                    </div>
                  )}
                </>
              ) : (
                <p className="text-destructive flex items-center gap-2">
                  <XCircle className="w-4 h-4" /> 迁移失败：{migrateResult.error}
                </p>
              )}
            </div>
          )}
        </CardContent>
      </Card>

      {/* JSON 粘贴导入 */}
      <Card className="border-card-border">
        <CardHeader className="pb-3">
          <CardTitle className="text-base font-semibold flex items-center gap-2">
            <ClipboardPaste className="w-4 h-4 text-primary" />
            JSON 粘贴导入（背包 / 抽奖 / 奖品）
          </CardTitle>
        </CardHeader>
        <CardContent className="space-y-4">
          <p className="text-xs text-muted-foreground">
            若旧项目无法通过 URL 自动拉取背包/抽奖数据，可在旧项目 Shell 中执行以下 SQL，将输出的 JSON 粘贴到对应输入框后导入。
          </p>

          {/* SQL 指令折叠 */}
          <button
            className="flex items-center gap-1.5 text-xs text-primary hover:underline"
            onClick={() => setShowSql(v => !v)}
          >
            {showSql ? <ChevronUp className="w-3.5 h-3.5" /> : <ChevronDown className="w-3.5 h-3.5" />}
            {showSql ? "收起 SQL 指令" : "查看在旧项目 Shell 执行的 SQL 指令"}
          </button>

          {showSql && (
            <div className="space-y-3 rounded-lg bg-muted/30 border border-border p-3">
              <p className="text-xs text-amber-400 font-medium">在旧项目的 Shell 中先执行：<code className="bg-muted px-1 rounded">psql $DATABASE_URL</code>，然后逐条执行：</p>
              {SQL_INSTRUCTIONS.map(item => (
                <div key={item.field} className="space-y-1">
                  <p className="text-xs text-muted-foreground font-medium">{item.label}</p>
                  <pre className="text-xs font-mono bg-background border border-border rounded p-2 overflow-x-auto whitespace-pre-wrap break-all text-foreground/80 select-all">{item.sql}</pre>
                </div>
              ))}
              <p className="text-xs text-muted-foreground">将每条 SQL 的输出结果（以 <code className="bg-muted px-1 rounded">[</code> 开头的 JSON）复制到下方对应输入框。</p>
            </div>
          )}

          {/* 粘贴/上传区 */}
          <div className="space-y-3">
            {SQL_INSTRUCTIONS.map(item => {
              const val = jsonFields[item.field] || "";
              const file = jsonFiles[item.field];
              const hasFile = !!file;
              const hasText = val.length > 0;
              return (
                <div key={item.field} className="space-y-1.5">
                  <div className="flex items-center justify-between gap-2 flex-wrap">
                    <Label className="text-xs text-muted-foreground">{item.label} JSON（留空跳过）</Label>
                    <div className="flex items-center gap-2">
                      {hasFile && (
                        <span className="text-[10px] text-emerald-400 font-mono">
                          已绑定 {file.name}（{formatBytes(file.size)}）
                        </span>
                      )}
                      {!hasFile && hasText && (
                        <span className="text-[10px] text-muted-foreground font-mono">
                          {val.length.toLocaleString()} 字符
                        </span>
                      )}
                      <input
                        type="file"
                        accept=".json,application/json,.txt,text/plain"
                        id={`upload-${item.field}`}
                        className="hidden"
                        onChange={(e) => {
                          const f = e.target.files?.[0];
                          if (!f) return;
                          // 关键：不读取文件内容，只保存 File 引用，
                          // 提交时由 fetch 直接流式上传（哪怕几百 MB 也不卡浏览器）
                          setJsonFiles(prev => ({ ...prev, [item.field]: f }));
                          // 清空对应文本框，避免歧义
                          setJsonFields(prev => ({ ...prev, [item.field]: "" }));
                          toast({
                            title: "已绑定文件（未读取内容）",
                            description: `${f.name} - ${formatBytes(f.size)}，点击「上传导入」时才会发送`,
                          });
                          // 清空 input 让同一文件可以再次选
                          e.target.value = "";
                        }}
                      />
                      <Button
                        type="button"
                        size="sm"
                        variant="outline"
                        className="h-6 px-2 text-[11px]"
                        onClick={() => document.getElementById(`upload-${item.field}`)?.click()}
                      >
                        <Upload className="w-3 h-3 mr-1" />
                        {hasFile ? "更换文件" : "上传 JSON 文件"}
                      </Button>
                      {(hasFile || hasText) && (
                        <Button
                          type="button"
                          size="sm"
                          variant="ghost"
                          className="h-6 px-2 text-[11px] text-muted-foreground hover:text-destructive"
                          onClick={() => {
                            setJsonFiles(prev => {
                              const next = { ...prev };
                              delete next[item.field];
                              return next;
                            });
                            setJsonFields(prev => ({ ...prev, [item.field]: "" }));
                          }}
                        >
                          清空
                        </Button>
                      )}
                    </div>
                  </div>
                  {hasFile ? (
                    <div className="font-mono text-xs h-20 rounded-md border border-emerald-500/40 bg-emerald-500/5 px-3 py-2 flex items-center gap-2 text-emerald-400">
                      <Upload className="w-4 h-4 shrink-0" />
                      <span className="truncate">
                        {file.name} · {formatBytes(file.size)} · 提交时直接流式上传，不会读入浏览器内存
                      </span>
                    </div>
                  ) : (
                    <Textarea
                      placeholder={`粘贴 ${item.label} 的 JSON 数组，或点击右上角"上传 JSON 文件"（推荐用于大文件）…`}
                      value={val}
                      onChange={e => setJsonFields(prev => ({ ...prev, [item.field]: e.target.value }))}
                      className="font-mono text-xs h-20 resize-y"
                    />
                  )}
                </div>
              );
            })}
          </div>

          <Button
            onClick={handleJsonImport}
            disabled={jsonImporting || (Object.values(jsonFields).every(v => !v.trim()) && Object.keys(jsonFiles).length === 0)}
            className="w-full"
            variant="secondary"
          >
            <ClipboardPaste className={`w-4 h-4 mr-2 ${jsonImporting ? "animate-pulse" : ""}`} />
            {jsonImporting ? (jsonProgress || "导入中…") : "上传 / 粘贴导入"}
          </Button>

          {jsonResult && (
            <div className={`rounded-lg p-4 text-sm space-y-1 ${jsonResult.success ? "bg-emerald-500/10 border border-emerald-500/30" : "bg-destructive/10 border border-destructive/30"}`}>
              {jsonResult.success ? (
                <>
                  <p className="font-medium text-emerald-400 flex items-center gap-2">
                    <CheckCircle className="w-4 h-4" /> 导入完成
                  </p>
                  <p className="text-muted-foreground">
                    奖品：<span className="text-foreground font-mono">{jsonResult.imported_prizes}</span>　
                    背包物品：<span className="text-foreground font-mono">{jsonResult.imported_user_items}</span>　
                    宝可梦球：<span className="text-foreground font-mono">{jsonResult.imported_pokeballs}</span>
                  </p>
                  <p className="text-muted-foreground">
                    圣人积分：<span className="text-foreground font-mono">{jsonResult.imported_saint_points}</span>　
                    捐献记录：<span className="text-foreground font-mono">{jsonResult.imported_saint_donations}</span>
                  </p>
                </>
              ) : (
                <p className="text-destructive flex items-center gap-2">
                  <XCircle className="w-4 h-4" /> 导入失败：{jsonResult.error}
                </p>
              )}
            </div>
          )}
        </CardContent>
      </Card>
    </div>
  );
}
