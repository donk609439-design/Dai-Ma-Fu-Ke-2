import { useState, useCallback } from "react";
import { useQuery, useMutation, useQueryClient } from "@tanstack/react-query";
import { adminFetch } from "@/lib/admin-auth";
import { useToast } from "@/hooks/use-toast";
import {
  Send, Settings, CheckCircle2, XCircle, AlertCircle, Eye, EyeOff,
  Plus, Trash2, ToggleLeft, ToggleRight, Copy, Users, Key,
  RefreshCw, Loader2, KeyRound, Clock, Download, ClipboardList,
  ChevronDown, ChevronRight, Eraser,
} from "lucide-react";

// ────────────────────────────────────────────────────────────────
//  Types
// ────────────────────────────────────────────────────────────────
interface ClientConfig {
  endpoint: string;
  partner_id: string;
  has_secret: boolean;
}

interface PushResult {
  email: string;
  status: "accepted" | "rejected";
  id?: string;
  reason?: string;
}

interface PushResponse {
  status_code: number;
  response: {
    success: boolean;
    accepted: number;
    rejected_count: number;
    results: PushResult[];
    activation_mode: string;
  };
}

interface PartnerKey {
  id: string;
  enabled: boolean;
  notes: string;
  created_at: string;
  total_contributions: number;
  active_contributions: number;
}

interface ImportResult {
  success: boolean;
  partner_status: string;
  imported: number;
  acc_ids: string[];
  message?: string;
}

// ────────────────────────────────────────────────────────────────
//  Push log — types + localStorage hook
// ────────────────────────────────────────────────────────────────
interface PushLogEntry {
  id: string;
  ts: number;
  input_count: number;
  activation_mode: string;
  status_code: number;
  response: PushResponse["response"] | null;
  error?: string;
}

const PUSH_LOG_KEY = "partner_push_log_v1";
const PUSH_LOG_MAX = 100;

function loadPushLog(): PushLogEntry[] {
  try {
    return JSON.parse(localStorage.getItem(PUSH_LOG_KEY) || "[]");
  } catch {
    return [];
  }
}

function savePushLog(entries: PushLogEntry[]) {
  localStorage.setItem(PUSH_LOG_KEY, JSON.stringify(entries.slice(0, PUSH_LOG_MAX)));
}

function usePushLog() {
  const [entries, setEntries] = useState<PushLogEntry[]>(loadPushLog);

  const append = useCallback((entry: PushLogEntry) => {
    setEntries(prev => {
      const next = [entry, ...prev].slice(0, PUSH_LOG_MAX);
      savePushLog(next);
      return next;
    });
  }, []);

  const clear = useCallback(() => {
    setEntries([]);
    localStorage.removeItem(PUSH_LOG_KEY);
  }, []);

  return { entries, append, clear };
}

// ────────────────────────────────────────────────────────────────
//  Push log — display component
// ────────────────────────────────────────────────────────────────
function PushLogPanel({ entries, onClear }: { entries: PushLogEntry[]; onClear: () => void }) {
  const [expanded, setExpanded] = useState<Set<string>>(new Set());

  const toggle = (id: string) =>
    setExpanded(prev => {
      const next = new Set(prev);
      next.has(id) ? next.delete(id) : next.add(id);
      return next;
    });

  const fmt = (ts: number) => {
    const d = new Date(ts);
    return `${d.toLocaleDateString()} ${d.toLocaleTimeString()}`;
  };

  return (
    <div className="rounded-xl border border-border bg-card overflow-hidden">
      <div className="flex items-center justify-between px-4 py-3 border-b border-border bg-muted/10">
        <div className="flex items-center gap-2">
          <ClipboardList className="w-4 h-4 text-muted-foreground" />
          <span className="text-sm font-semibold">推送日志</span>
          <span className="text-xs text-muted-foreground/60">最近 {PUSH_LOG_MAX} 条，页面关闭前保留</span>
        </div>
        {entries.length > 0 && (
          <button
            onClick={onClear}
            className="flex items-center gap-1 text-xs text-muted-foreground hover:text-red-400 transition-colors"
          >
            <Eraser className="w-3.5 h-3.5" />
            清空
          </button>
        )}
      </div>

      {entries.length === 0 ? (
        <div className="py-8 text-center text-muted-foreground/50 text-sm">暂无记录，推送后将在此显示</div>
      ) : (
        <div className="divide-y divide-border max-h-[480px] overflow-y-auto">
          {entries.map(entry => {
            const open = expanded.has(entry.id);
            const r = entry.response;
            const ok = entry.status_code >= 200 && entry.status_code < 300;
            return (
              <div key={entry.id}>
                {/* summary row */}
                <button
                  onClick={() => toggle(entry.id)}
                  className="w-full flex items-center gap-3 px-4 py-2.5 hover:bg-muted/10 transition-colors text-left"
                >
                  {open
                    ? <ChevronDown className="w-3.5 h-3.5 text-muted-foreground shrink-0" />
                    : <ChevronRight className="w-3.5 h-3.5 text-muted-foreground shrink-0" />}

                  {/* timestamp */}
                  <span className="text-[11px] text-muted-foreground/70 font-mono shrink-0 w-36">{fmt(entry.ts)}</span>

                  {/* http status badge */}
                  <span className={`text-[10px] font-bold px-1.5 py-0.5 rounded shrink-0 ${ok ? "bg-green-500/20 text-green-400" : "bg-red-500/20 text-red-400"}`}>
                    HTTP {entry.status_code}
                  </span>

                  {/* accepted / rejected */}
                  {r ? (
                    <>
                      <span className="text-xs text-green-400 shrink-0">✓ {r.accepted}</span>
                      <span className="text-xs text-red-400 shrink-0">✗ {r.rejected_count}</span>
                      <span className="text-[11px] text-muted-foreground/60 shrink-0">
                        {r.activation_mode === "immediate" ? "立即激活" : "屯池"}
                      </span>
                    </>
                  ) : (
                    <span className="text-xs text-red-400 shrink-0 flex-1 truncate">{entry.error || "推送失败"}</span>
                  )}

                  {/* input count */}
                  <span className="ml-auto text-[10px] text-muted-foreground/50 shrink-0">{entry.input_count} 条输入</span>
                </button>

                {/* expanded detail */}
                {open && (
                  <div className="px-4 pb-3 bg-muted/5 border-t border-border/50 space-y-2">
                    {/* raw JSON */}
                    <div className="mt-2">
                      <p className="text-[10px] text-muted-foreground/60 mb-1 font-medium">返回参数（完整）</p>
                      <pre className="bg-background border border-border rounded-lg px-3 py-2 text-[11px] font-mono text-foreground/80 overflow-x-auto whitespace-pre-wrap break-all leading-relaxed">
                        {JSON.stringify(
                          { status_code: entry.status_code, response: entry.response, error: entry.error },
                          null, 2
                        )}
                      </pre>
                    </div>

                    {/* per-account results table */}
                    {r?.results && r.results.length > 0 && (
                      <div>
                        <p className="text-[10px] text-muted-foreground/60 mb-1 font-medium">逐账号结果</p>
                        <div className="rounded-lg border border-border overflow-hidden">
                          <table className="w-full text-[11px]">
                            <thead>
                              <tr className="border-b border-border bg-muted/20">
                                <th className="text-left px-3 py-1.5 text-muted-foreground font-medium">邮箱</th>
                                <th className="text-center px-2 py-1.5 text-muted-foreground font-medium">状态</th>
                                <th className="text-left px-2 py-1.5 text-muted-foreground font-medium">ID / 原因</th>
                              </tr>
                            </thead>
                            <tbody className="divide-y divide-border/50">
                              {r.results.map((res, i) => (
                                <tr key={i} className="hover:bg-muted/5">
                                  <td className="px-3 py-1.5 font-mono truncate max-w-[160px]" title={res.email}>{res.email}</td>
                                  <td className="px-2 py-1.5 text-center">
                                    {res.status === "accepted"
                                      ? <CheckCircle2 className="w-3 h-3 text-green-400 mx-auto" />
                                      : <XCircle className="w-3 h-3 text-red-400 mx-auto" />}
                                  </td>
                                  <td className="px-2 py-1.5 font-mono text-muted-foreground/70 truncate max-w-[140px]"
                                    title={res.id || res.reason}>
                                    {res.id ? `#${res.id.slice(0, 10)}` : (res.reason || "—")}
                                  </td>
                                </tr>
                              ))}
                            </tbody>
                          </table>
                        </div>
                      </div>
                    )}
                  </div>
                )}
              </div>
            );
          })}
        </div>
      )}
    </div>
  );
}

// ────────────────────────────────────────────────────────────────
//  Shared: single config display row
// ────────────────────────────────────────────────────────────────
function CfgRow({ label, value, onCopy }: { label: string; value: string; onCopy?: () => void }) {
  const masked = value === "••••••••（已配置）" || value === "—" || value === "未配置";
  return (
    <div className="flex items-start gap-3 py-2 border-b border-border/50 last:border-0">
      <span className="text-muted-foreground w-24 shrink-0 text-xs pt-0.5">{label}</span>
      <span className="font-mono text-xs break-all flex-1">{value}</span>
      {!masked && onCopy && (
        <button onClick={onCopy} className="shrink-0 text-muted-foreground hover:text-foreground">
          <Copy className="w-3.5 h-3.5" />
        </button>
      )}
    </div>
  );
}

// ────────────────────────────────────────────────────────────────
//  Config panel — 主对端（DB 存储，可编辑）
// ────────────────────────────────────────────────────────────────
function PrimaryConfigBlock() {
  const { toast } = useToast();
  const qc = useQueryClient();
  const [showSecret, setShowSecret] = useState(false);
  const [form, setForm] = useState({ endpoint: "", partner_id: "", hmac_secret: "" });
  const [editing, setEditing] = useState(false);

  const { data: cfg, isLoading } = useQuery<ClientConfig>({
    queryKey: ["partner-client-config"],
    queryFn: async () => {
      const res = await adminFetch("/admin/partner-client-config");
      if (!res.ok) throw new Error("获取失败");
      const data = await res.json();
      setForm(f => ({ ...f, endpoint: data.endpoint, partner_id: data.partner_id }));
      return data;
    },
  });

  const saveMutation = useMutation({
    mutationFn: async () => {
      const res = await adminFetch("/admin/partner-client-config", {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify(form),
      });
      if (!res.ok) throw new Error("保存失败");
      return res.json();
    },
    onSuccess: () => {
      qc.invalidateQueries({ queryKey: ["partner-client-config"] });
      toast({ title: "配置已保存" });
      setEditing(false);
      setForm(f => ({ ...f, hmac_secret: "" }));
    },
    onError: (e: Error) => toast({ title: "保存失败", description: e.message, variant: "destructive" }),
  });

  if (isLoading) return <div className="py-4 text-center text-muted-foreground text-xs">加载中…</div>;

  return (
    <div className="rounded-xl border border-border bg-card p-4 space-y-4">
      <div className="flex items-center justify-between">
        <div className="flex items-center gap-2">
          <span className="text-xs font-bold px-1.5 py-0.5 rounded bg-primary/20 text-primary">主</span>
          <h3 className="text-sm font-semibold">对端配置（数据库存储）</h3>
        </div>
        <button
          onClick={() => setEditing(v => !v)}
          className="text-xs px-3 py-1.5 rounded-lg border border-border hover:bg-muted/30 transition-colors"
        >
          {editing ? "取消编辑" : "编辑"}
        </button>
      </div>

      {editing ? (
        <div className="space-y-3">
          {[
            { key: "endpoint", label: "Endpoint URL", placeholder: "https://api.rokwuky.com/api/partner/contribute/submit" },
            { key: "partner_id", label: "Partner ID", placeholder: "partner-orange-xxxxxxxxxxxxxxxx" },
          ].map(({ key, label, placeholder }) => (
            <div key={key}>
              <label className="block text-xs font-medium text-muted-foreground mb-1">{label}</label>
              <input
                className="w-full bg-background border border-border rounded-lg px-3 py-2 text-sm font-mono focus:outline-none focus:ring-2 focus:ring-primary/50"
                placeholder={placeholder}
                value={(form as any)[key]}
                onChange={e => setForm(f => ({ ...f, [key]: e.target.value }))}
              />
            </div>
          ))}
          <div>
            <label className="block text-xs font-medium text-muted-foreground mb-1">
              HMAC Secret <span className="opacity-60">（留空表示不更改）</span>
            </label>
            <div className="relative">
              <input
                type={showSecret ? "text" : "password"}
                className="w-full bg-background border border-border rounded-lg px-3 py-2 pr-10 text-sm font-mono focus:outline-none focus:ring-2 focus:ring-primary/50"
                placeholder="64-hex secret"
                value={form.hmac_secret}
                onChange={e => setForm(f => ({ ...f, hmac_secret: e.target.value }))}
              />
              <button type="button" onClick={() => setShowSecret(v => !v)} className="absolute right-2 top-1/2 -translate-y-1/2 text-muted-foreground hover:text-foreground">
                {showSecret ? <EyeOff className="w-4 h-4" /> : <Eye className="w-4 h-4" />}
              </button>
            </div>
          </div>
          <button onClick={() => saveMutation.mutate()} disabled={saveMutation.isPending}
            className="w-full py-2 rounded-lg bg-primary text-primary-foreground text-sm font-medium hover:bg-primary/90 disabled:opacity-50">
            {saveMutation.isPending ? "保存中…" : "保存配置"}
          </button>
        </div>
      ) : (
        <div className="space-y-0">
          <CfgRow label="Endpoint" value={cfg?.endpoint || "—"} onCopy={() => { navigator.clipboard.writeText(cfg!.endpoint); toast({ title: "已复制" }); }} />
          <CfgRow label="Partner ID" value={cfg?.partner_id || "—"} onCopy={() => { navigator.clipboard.writeText(cfg!.partner_id); toast({ title: "已复制" }); }} />
          <CfgRow label="HMAC Secret" value={cfg?.has_secret ? "••••••••（已配置）" : "未配置"} />
          <div className="flex items-center gap-2 pt-2">
            {cfg?.endpoint && cfg?.partner_id && cfg?.has_secret
              ? <><CheckCircle2 className="w-4 h-4 text-green-400" /><span className="text-xs text-green-400">配置完整，可以推送</span></>
              : <><AlertCircle className="w-4 h-4 text-yellow-400" /><span className="text-xs text-yellow-400">配置不完整，请点编辑填写</span></>
            }
          </div>
        </div>
      )}
    </div>
  );
}

// ────────────────────────────────────────────────────────────────
//  Config panel — 第二对端（环境变量，只读）
// ────────────────────────────────────────────────────────────────
interface ClientConfig2 extends ClientConfig { configured: boolean; }

function SecondaryConfigBlock() {
  const { data: cfg2, isLoading } = useQuery<ClientConfig2>({
    queryKey: ["partner-client-config2"],
    queryFn: async () => {
      const res = await adminFetch("/admin/partner-client-config2");
      if (!res.ok) throw new Error("获取失败");
      return res.json();
    },
  });
  const { toast } = useToast();

  if (isLoading) return <div className="py-4 text-center text-muted-foreground text-xs">加载中…</div>;

  return (
    <div className="rounded-xl border border-border bg-card p-4 space-y-4">
      <div className="flex items-center gap-2">
        <span className="text-xs font-bold px-1.5 py-0.5 rounded bg-muted text-muted-foreground">副</span>
        <h3 className="text-sm font-semibold">对端配置（环境变量 PARTNER2_*）</h3>
        <span className="ml-auto text-[10px] text-muted-foreground/60 border border-border/40 rounded px-1.5 py-0.5">只读</span>
      </div>
      <div className="space-y-0">
        <CfgRow label="Endpoint" value={cfg2?.endpoint || "—"} onCopy={cfg2?.endpoint ? () => { navigator.clipboard.writeText(cfg2!.endpoint); toast({ title: "已复制" }); } : undefined} />
        <CfgRow label="Partner ID" value={cfg2?.partner_id || "—"} onCopy={cfg2?.partner_id ? () => { navigator.clipboard.writeText(cfg2!.partner_id); toast({ title: "已复制" }); } : undefined} />
        <CfgRow label="HMAC Secret" value={cfg2?.has_secret ? "••••••••（已配置）" : "未配置"} />
        <div className="flex items-center gap-2 pt-2">
          {cfg2?.configured
            ? <><CheckCircle2 className="w-4 h-4 text-green-400" /><span className="text-xs text-green-400">已配置，参与自注册负载均衡（50/50）</span></>
            : <><AlertCircle className="w-4 h-4 text-muted-foreground/60" /><span className="text-xs text-muted-foreground/60">未配置，设置 PARTNER2_* 环境变量后生效</span></>
          }
        </div>
      </div>
    </div>
  );
}

// ────────────────────────────────────────────────────────────────
//  Config panel — 合并两块
// ────────────────────────────────────────────────────────────────
function ConfigPanel() {
  return (
    <div className="space-y-4">
      <h2 className="text-sm font-semibold">对端配置</h2>
      <PrimaryConfigBlock />
      <SecondaryConfigBlock />
    </div>
  );
}

// ────────────────────────────────────────────────────────────────
//  Per-account key query row
// ────────────────────────────────────────────────────────────────
function AccountKeyRow({ email, contributionId, accepted, reason }: { email: string; contributionId?: string; accepted: boolean; reason?: string }) {
  const { toast } = useToast();
  const [importing, setImporting] = useState(false);
  const [result, setResult] = useState<ImportResult | null>(null);

  const doImport = async () => {
    setImporting(true);
    setResult(null);
    try {
      const res = await adminFetch("/admin/partner-import-by-email", {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({ email }),
      });
      const d = await res.json();
      if (!res.ok) throw new Error(d.detail || "导入失败");
      setResult(d);
    } catch (e: any) {
      toast({ title: "导入失败", description: e.message, variant: "destructive" });
    } finally {
      setImporting(false);
    }
  };

  return (
    <div className="px-4 py-2.5 space-y-2">
      <div className="flex items-center gap-2 text-xs">
        {accepted
          ? <CheckCircle2 className="w-3.5 h-3.5 text-green-400 shrink-0" />
          : <XCircle className="w-3.5 h-3.5 text-red-400 shrink-0" />}
        <span className="font-mono flex-1 truncate">{email}</span>
        {!accepted && reason && (
          <span className="text-red-400/80 text-[10px] shrink-0 max-w-[120px] truncate" title={reason}>{reason}</span>
        )}
        {contributionId && (
          <span className="text-muted-foreground/60 font-mono text-[10px] shrink-0 max-w-[100px] truncate" title={contributionId}>
            #{contributionId.slice(0, 8)}
          </span>
        )}
        {accepted && (
          <button
            onClick={doImport}
            disabled={importing}
            className="shrink-0 flex items-center gap-1 px-2 py-0.5 rounded border border-border text-muted-foreground hover:text-foreground hover:bg-muted/30 transition-colors text-[11px]"
          >
            {importing ? <Loader2 className="w-3 h-3 animate-spin" /> : <Download className="w-3 h-3" />}
            立即导入
          </button>
        )}
      </div>

      {result && (
        <div className="ml-5 space-y-1">
          {result.success && result.acc_ids.length > 0 ? (
            <>
              <p className="text-[10px] text-green-400/80">已导入 {result.imported} 条凭证到池</p>
              {result.acc_ids.map((k, i) => (
                <div key={i} className="flex items-center gap-2 bg-primary/10 border border-primary/20 rounded px-2 py-1">
                  <KeyRound className="w-3 h-3 text-primary shrink-0" />
                  <code className="text-[11px] font-mono flex-1 break-all">{k}</code>
                  <button
                    onClick={() => { navigator.clipboard.writeText(k); toast({ title: "已复制" }); }}
                    className="shrink-0 text-muted-foreground hover:text-foreground"
                  >
                    <Copy className="w-3 h-3" />
                  </button>
                </div>
              ))}
            </>
          ) : (
            <div className="flex items-center gap-1.5 text-xs text-muted-foreground">
              <Clock className="w-3 h-3" />
              <span>{result.message || `对方状态: ${result.partner_status}，激活后再试`}</span>
            </div>
          )}
        </div>
      )}
    </div>
  );
}

// ────────────────────────────────────────────────────────────────
//  Push panel (with log)
// ────────────────────────────────────────────────────────────────
const PLACEHOLDER = `email1@example.com,password1
email2@example.com,password2`;

function PushPanel() {
  const { toast } = useToast();
  const [text, setText] = useState("");
  const [mode, setMode] = useState<"immediate" | "stockpile">("immediate");
  const [result, setResult] = useState<PushResponse | null>(null);
  const { entries: logEntries, append: appendLog, clear: clearLog } = usePushLog();

  const parse = () => {
    const lines = text.trim().split("\n").filter(l => l.trim());
    return lines.map(l => {
      const parts = l.split(",");
      return { email: (parts[0] || "").trim(), password: (parts[1] || "").trim() };
    }).filter(a => a.email && a.password);
  };

  const pushMutation = useMutation({
    mutationFn: async () => {
      const accs = parse();
      if (accs.length === 0) throw new Error("请输入有效的账号（每行：邮箱,密码）");
      if (accs.length > 100) throw new Error("单次最多 100 条");
      const res = await adminFetch("/admin/partner-client-push", {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({ accounts: accs, activation_mode: mode }),
      });
      const data = await res.json();
      if (!res.ok) throw new Error(data.detail || "推送失败");
      return { data: data as PushResponse, inputCount: accs.length };
    },
    onSuccess: ({ data, inputCount }) => {
      setResult(data);
      appendLog({
        id: `${Date.now()}-${Math.random().toString(36).slice(2, 7)}`,
        ts: Date.now(),
        input_count: inputCount,
        activation_mode: mode,
        status_code: data.status_code,
        response: data.response ?? null,
      });
      const r = data.response;
      toast({
        title: `推送完成 HTTP ${data.status_code}`,
        description: `接受 ${r?.accepted ?? "?"} 条，拒绝 ${r?.rejected_count ?? "?"} 条`,
      });
    },
    onError: (e: Error) => {
      appendLog({
        id: `${Date.now()}-${Math.random().toString(36).slice(2, 7)}`,
        ts: Date.now(),
        input_count: parse().length,
        activation_mode: mode,
        status_code: 0,
        response: null,
        error: e.message,
      });
      toast({ title: "推送失败", description: e.message, variant: "destructive" });
    },
  });

  return (
    <div className="space-y-4">
      <div>
        <div className="flex items-center justify-between mb-1">
          <label className="text-sm font-semibold">账号列表</label>
          <span className="text-xs text-muted-foreground">每行：邮箱,密码</span>
        </div>
        <textarea
          className="w-full h-40 bg-background border border-border rounded-lg px-3 py-2 text-sm font-mono focus:outline-none focus:ring-2 focus:ring-primary/50 resize-y"
          placeholder={PLACEHOLDER}
          value={text}
          onChange={e => { setText(e.target.value); setResult(null); }}
        />
        <p className="text-xs text-muted-foreground mt-1">
          {text.trim() ? `已输入 ${text.trim().split("\n").filter(l => l.includes(",")).length} 行` : ""}
        </p>
      </div>

      <div>
        <label className="block text-sm font-semibold mb-2">激活模式</label>
        <div className="flex gap-3">
          {(["immediate", "stockpile"] as const).map(m => (
            <button
              key={m}
              onClick={() => setMode(m)}
              className={`flex-1 py-2 rounded-lg border text-sm font-medium transition-colors ${mode === m ? "border-primary bg-primary/10 text-primary" : "border-border hover:bg-muted/30 text-muted-foreground"}`}
            >
              {m === "immediate" ? "立即激活" : "屯池（暂不激活）"}
            </button>
          ))}
        </div>
      </div>

      <button
        onClick={() => pushMutation.mutate()}
        disabled={pushMutation.isPending || !text.trim()}
        className="w-full py-2.5 rounded-lg bg-primary text-primary-foreground text-sm font-semibold hover:bg-primary/90 transition-colors disabled:opacity-50 flex items-center justify-center gap-2"
      >
        <Send className="w-4 h-4" />
        {pushMutation.isPending ? "推送中…" : "推送到对端"}
      </button>

      {/* Current push result */}
      {result && (
        <div className="bg-card border border-border rounded-xl overflow-hidden">
          <div className="px-4 py-3 border-b border-border flex items-center justify-between">
            <span className="text-sm font-semibold">本次推送结果</span>
            <span className={`text-xs px-2 py-0.5 rounded-full font-medium ${result.status_code === 200 ? "bg-green-500/20 text-green-400" : "bg-red-500/20 text-red-400"}`}>
              HTTP {result.status_code}
            </span>
          </div>
          {result.response?.results && (
            <>
              <div className="px-4 py-2 flex gap-4 text-sm border-b border-border bg-muted/10">
                <span className="text-green-400">✓ 接受 {result.response.accepted}</span>
                <span className="text-red-400">✗ 拒绝 {result.response.rejected_count}</span>
                <span className="text-muted-foreground">模式: {result.response.activation_mode === "immediate" ? "立即激活" : "屯池"}</span>
              </div>
              <p className="px-4 pt-2 text-[11px] text-muted-foreground/70">
                后台每 60 秒自动拉取一次凭证写入池，也可点「立即导入」手动触发（通常 1~15 分钟可激活）
              </p>
              <div className="divide-y divide-border max-h-80 overflow-y-auto">
                {result.response.results.map((r, i) => (
                  <AccountKeyRow
                    key={i}
                    email={r.email}
                    contributionId={r.id}
                    accepted={r.status === "accepted"}
                    reason={r.reason}
                  />
                ))}
              </div>
            </>
          )}
          {!result.response?.results && (
            <pre className="px-4 py-3 text-xs text-red-400 overflow-x-auto">
              {JSON.stringify(result.response, null, 2)}
            </pre>
          )}
        </div>
      )}

      {/* Push history log */}
      <div className="pt-2">
        <PushLogPanel entries={logEntries} onClear={clearLog} />
      </div>
    </div>
  );
}

// ────────────────────────────────────────────────────────────────
//  Server-side key management (secondary tab)
// ────────────────────────────────────────────────────────────────
function CopyBtn({ text }: { text: string }) {
  const { toast } = useToast();
  return (
    <button onClick={() => { navigator.clipboard.writeText(text); toast({ title: "已复制" }); }} className="p-1 rounded hover:bg-muted/40 text-muted-foreground hover:text-foreground transition-colors">
      <Copy className="w-3.5 h-3.5" />
    </button>
  );
}

function NewKeyModal({ onClose, onCreated }: { onClose: () => void; onCreated: () => void }) {
  const { toast } = useToast();
  const [form, setForm] = useState({ name: "", notes: "" });
  const [result, setResult] = useState<{ partner_id: string; hmac_secret: string } | null>(null);
  const [saving, setSaving] = useState(false);

  const submit = async () => {
    setSaving(true);
    try {
      const res = await adminFetch("/admin/partner-keys", {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify(form),
      });
      if (!res.ok) throw new Error((await res.json()).detail || "创建失败");
      setResult(await res.json());
      onCreated();
    } catch (e: any) {
      toast({ title: "创建失败", description: e.message, variant: "destructive" });
    } finally {
      setSaving(false);
    }
  };

  return (
    <div className="fixed inset-0 bg-black/60 flex items-center justify-center z-50">
      <div className="bg-card border border-border rounded-xl p-6 w-full max-w-lg shadow-2xl">
        {result ? (
          <>
            <h2 className="text-lg font-bold mb-4 text-green-400">Key 已创建 — 请立刻保存！</h2>
            <p className="text-xs text-muted-foreground mb-4">HMAC Secret 仅显示一次。</p>
            <div className="space-y-3">
              {[{ label: "Partner ID", value: result.partner_id }, { label: "HMAC Secret", value: result.hmac_secret }].map(({ label, value }) => (
                <div key={label}>
                  <p className="text-xs text-muted-foreground mb-1">{label}</p>
                  <div className="flex items-center gap-2 bg-background border border-border rounded-lg px-3 py-2">
                    <code className="text-xs flex-1 break-all font-mono">{value}</code>
                    <CopyBtn text={value} />
                  </div>
                </div>
              ))}
            </div>
            <button onClick={onClose} className="mt-5 w-full py-2 rounded-lg bg-primary text-primary-foreground text-sm font-medium">我已保存，关闭</button>
          </>
        ) : (
          <>
            <h2 className="text-lg font-bold mb-5">签发接收方 Key（给其他合作方）</h2>
            <div className="space-y-4">
              <div>
                <label className="block text-xs font-medium text-muted-foreground mb-1">名称（英文）</label>
                <input className="w-full bg-background border border-border rounded-lg px-3 py-2 text-sm" placeholder="例：partner-b" value={form.name} onChange={e => setForm(f => ({ ...f, name: e.target.value }))} />
              </div>
              <div>
                <label className="block text-xs font-medium text-muted-foreground mb-1">备注</label>
                <input className="w-full bg-background border border-border rounded-lg px-3 py-2 text-sm" value={form.notes} onChange={e => setForm(f => ({ ...f, notes: e.target.value }))} />
              </div>
            </div>
            <div className="flex gap-3 mt-5">
              <button onClick={onClose} className="flex-1 py-2 rounded-lg border border-border text-sm">取消</button>
              <button onClick={submit} disabled={saving || !form.name.trim()} className="flex-1 py-2 rounded-lg bg-primary text-primary-foreground text-sm disabled:opacity-50">{saving ? "生成中…" : "生成"}</button>
            </div>
          </>
        )}
      </div>
    </div>
  );
}

function ServerKeysPanel() {
  const qc = useQueryClient();
  const { toast } = useToast();
  const [showNew, setShowNew] = useState(false);
  const [confirmDeleteId, setConfirmDeleteId] = useState<string | null>(null);

  const { data: keys = [], isLoading } = useQuery<PartnerKey[]>({
    queryKey: ["partner-keys"],
    queryFn: async () => {
      const res = await adminFetch("/admin/partner-keys");
      if (!res.ok) throw new Error("获取失败");
      return res.json();
    },
  });

  const toggleMutation = useMutation({
    mutationFn: async ({ id, enabled }: { id: string; enabled: boolean }) => {
      await adminFetch(`/admin/partner-keys/${id}`, { method: "PUT", headers: { "Content-Type": "application/json" }, body: JSON.stringify({ enabled }) });
    },
    onSuccess: () => qc.invalidateQueries({ queryKey: ["partner-keys"] }),
  });

  const deleteMutation = useMutation({
    mutationFn: async (id: string) => { await adminFetch(`/admin/partner-keys/${id}`, { method: "DELETE" }); },
    onSuccess: () => {
      qc.invalidateQueries({ queryKey: ["partner-keys"] });
      setConfirmDeleteId(null);
      toast({ title: "已删除" });
    },
    onError: () => toast({ title: "删除失败", variant: "destructive" }),
  });

  return (
    <div className="space-y-4">
      <div className="flex items-center justify-between">
        <p className="text-xs text-muted-foreground">管理允许向我方推送账号的合作伙伴</p>
        <button onClick={() => setShowNew(true)} className="flex items-center gap-1.5 px-3 py-1.5 bg-primary text-primary-foreground rounded-lg text-xs font-medium hover:bg-primary/90">
          <Plus className="w-3.5 h-3.5" /> 签发新 Key
        </button>
      </div>
      {isLoading ? (
        <div className="py-6 text-center text-muted-foreground text-sm">加载中…</div>
      ) : keys.length === 0 ? (
        <div className="py-6 text-center text-muted-foreground text-sm">暂无接收方合作伙伴</div>
      ) : (
        <table className="w-full text-sm">
          <thead>
            <tr className="border-b border-border">
              <th className="text-left py-2 font-medium text-muted-foreground text-xs">Partner ID</th>
              <th className="text-center py-2 font-medium text-muted-foreground text-xs">投稿/激活</th>
              <th className="text-center py-2 font-medium text-muted-foreground text-xs">状态</th>
              <th className="text-center py-2 font-medium text-muted-foreground text-xs">操作</th>
            </tr>
          </thead>
          <tbody className="divide-y divide-border">
            {keys.map(k => (
              <tr key={k.id} className="hover:bg-muted/10">
                <td className="py-2"><code className="text-xs font-mono text-muted-foreground">{k.id}</code></td>
                <td className="py-2 text-center text-xs"><span className="text-foreground">{k.total_contributions}</span> / <span className="text-green-400">{k.active_contributions}</span></td>
                <td className="py-2 text-center">
                  <button onClick={() => toggleMutation.mutate({ id: k.id, enabled: !k.enabled })}>
                    {k.enabled ? <ToggleRight className="w-5 h-5 text-primary mx-auto" /> : <ToggleLeft className="w-5 h-5 text-muted-foreground mx-auto" />}
                  </button>
                </td>
                <td className="py-2 text-center">
                  {confirmDeleteId === k.id ? (
                    <span className="inline-flex items-center gap-1 text-xs">
                      <button
                        onClick={() => deleteMutation.mutate(k.id)}
                        disabled={deleteMutation.isPending}
                        className="px-1.5 py-0.5 rounded bg-red-500 text-white hover:bg-red-600 disabled:opacity-50"
                      >
                        确认
                      </button>
                      <button
                        onClick={() => setConfirmDeleteId(null)}
                        className="px-1.5 py-0.5 rounded border border-border text-muted-foreground hover:bg-muted/30"
                      >
                        取消
                      </button>
                    </span>
                  ) : (
                    <button
                      onClick={() => setConfirmDeleteId(k.id)}
                      className="p-1 rounded hover:bg-red-500/10 text-muted-foreground hover:text-red-400"
                    >
                      <Trash2 className="w-3.5 h-3.5" />
                    </button>
                  )}
                </td>
              </tr>
            ))}
          </tbody>
        </table>
      )}
      {showNew && <NewKeyModal onClose={() => setShowNew(false)} onCreated={() => qc.invalidateQueries({ queryKey: ["partner-keys"] })} />}
    </div>
  );
}

// ────────────────────────────────────────────────────────────────
//  Main page
// ────────────────────────────────────────────────────────────────
type Tab = "push" | "config" | "receive";

export default function Partners() {
  const [tab, setTab] = useState<Tab>("push");

  const tabs: { id: Tab; label: string; icon: React.ElementType }[] = [
    { id: "push", label: "推送账号", icon: Send },
    { id: "config", label: "对端配置", icon: Settings },
    { id: "receive", label: "接收方管理", icon: Users },
  ];

  return (
    <div className="space-y-5">
      <div>
        <h1 className="text-2xl font-bold">合作伙伴</h1>
        <p className="text-sm text-muted-foreground mt-1">向对端推送 JB 账号 / 管理接收方 Key</p>
      </div>

      {/* Tabs */}
      <div className="flex gap-1 bg-muted/20 rounded-xl p-1 w-fit">
        {tabs.map(({ id, label, icon: Icon }) => (
          <button
            key={id}
            onClick={() => setTab(id)}
            className={`flex items-center gap-2 px-4 py-2 rounded-lg text-sm font-medium transition-colors ${tab === id ? "bg-card shadow text-foreground" : "text-muted-foreground hover:text-foreground"}`}
          >
            <Icon className="w-4 h-4" />
            {label}
          </button>
        ))}
      </div>

      {/* Tab content */}
      <div className="bg-card border border-border rounded-xl p-5">
        {tab === "push" && <PushPanel />}
        {tab === "config" && <ConfigPanel />}
        {tab === "receive" && <ServerKeysPanel />}
      </div>
    </div>
  );
}
