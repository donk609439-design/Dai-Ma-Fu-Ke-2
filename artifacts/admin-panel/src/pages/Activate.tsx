import { useState, useRef, useEffect, useMemo } from "react";
import { UserPlus, Play, CheckCircle, XCircle, Loader2, Mail, Lock, TerminalSquare, Copy, Check, AlertTriangle, FlaskConical, LogIn, LogOut, Layers, Settings2, Sparkles, ShieldCheck, Wand2 } from "lucide-react";
import { useQueryClient } from "@tanstack/react-query";
import { getAdminKey, isFullAdmin, getAdminRole, adminFetch } from "@/lib/admin-auth";
import { useDiscordAuth } from "@/hooks/useDiscordAuth";

type TaskStatus = "idle" | "running" | "success" | "failed" | "quota_rejected" | "donated_blocked";

interface LogEntry {
  msg: string;
  time: string;
}

/**
 * 解析 AI Credits 行并返回剩余百分比（0-100），解析失败返回 null
 * 原始格式：Account XXXXX AI Credits: 300,000/300,000 (used 0) → has_quota=True
 */
function parseCreditsPercent(msg: string): number | null {
  const match = msg.match(/AI Credits:\s*([\d,]+)\/([\d,]+)/);
  if (!match) return null;
  const avail = parseInt(match[1].replace(/,/g, ""), 10);
  const total = parseInt(match[2].replace(/,/g, ""), 10);
  return total > 0 ? Math.round((avail / total) * 100) : 0;
}

/**
 * 管理员视图专用：仅把 AI Credits 数字替换为百分比，其余原样输出
 */
function fmtAdminLog(msg: string): string {
  if (msg.includes("AI Credits:")) {
    const pct = parseCreditsPercent(msg);
    if (pct !== null) {
      return msg.replace(/AI Credits:\s*[\d,]+\/[\d,]+(\s*\([^)]*\))?/, `AI Credits: ${pct}%`);
    }
  }
  return msg;
}

/**
 * 对游客隐藏所有 IDE/JetBrains 技术细节，伪装成「自动绑卡→解绑→激活」流程
 * 返回 null 表示完全过滤掉该行
 */
function disguiseLog(msg: string): string | null {
  const m = msg.trim();

  // ── 完全过滤 ──
  if (m.startsWith("[DEBUG]")) return null;
  if (m.startsWith("[ide-jwt]")) return null;
  if (m.startsWith("pending licenseId")) return null;
  if (m.includes("Account") && m.includes("AI Credits")) {
    const pct = parseCreditsPercent(m);
    return pct !== null ? `账号额度剩余 ${pct}%` : null;
  }
  if (m.match(/✓ [A-Z]{2} 许可证获取成功/)) return null;
  if (m.includes("✓ 获得") && m.includes("IDE 许可证")) return null;
  if (m.startsWith("[nc-create]") && m.includes("账号全量")) return null;
  if (m.includes("最终 License IDs:")) return "✓ 账号凭证获取成功";
  if (m.includes("licenseId") && !m.startsWith("✓") && !m.startsWith("⏳")) return null;

  // ── nc-create 系列 → 解绑 ──
  if (m.startsWith("[nc-create]") && m.includes("开始创建")) return "正在逐步释放临时付款绑定...";
  if (m.startsWith("[nc-create]") && m.includes("新增 licenseId")) return "✓ 临时付款方式已全部自动解绑";
  if (m.match(/^\[nc-create:[A-Z]+\]/)) return "✓ 解绑步骤完成";
  if (m.startsWith("[nc-create:AIP]")) return "✓ 权益配置完成";
  if (m.startsWith("[8]") && m.includes("NC licenseId")) return "正在核验账号权益有效性...";

  // ── 步骤标题 ──
  if (m.startsWith("[1/8]") || m.includes("JBA 登录")) return "正在验证账号身份...";
  if (m.startsWith("[2/8]") || (m.includes("AI 状态") && m.includes("检查"))) return "正在检查账号状态...";
  if (m.startsWith("[3/8]") || m.includes("OAuth PKCE")) return "正在建立安全会话...";
  if (m.startsWith("[4/8]") || m.includes("Hub user_id")) return "正在同步账号数据...";
  if (m.startsWith("[5/8]") || m.includes("AI Pro 一个月试用")) return "正在为账号申请 AI Pro 一个月试用...";
  if (m.startsWith("[6/8]") || (m.includes("licenseId") && m.includes("等待"))) return "正在获取账号凭证...";
  if (m.startsWith("[7/8]") || m.includes("Grazie 注册")) return "正在注册服务账号...";
  if (m.startsWith("[8/8]") || m.includes("provide-access")) return "正在签发服务凭证...";

  // ── 子步骤 ──
  if (m.includes("✓ 登录成功") || m.includes("登录成功")) return "✓ 账号身份验证通过";
  if (m.includes("showAIPlans=True") || m.includes("尚未激活")) return "检测到权益未开通，准备自动激活...";
  if (m.includes("showAIPlans=False") || m.includes("已激活")) return "账号状态已确认";
  if (m.includes("授权码获取成功")) return "✓ 安全会话建立成功";
  if (m.includes("正在交换 token")) return "正在处理授权令牌...";
  if (m.includes("id_token 获取成功")) return "✓ 授权令牌获取成功";
  if (m.includes("user_id:")) return "✓ 账号数据同步完成";
  if (m.includes("将依次尝试") || m.includes("付费IDE")) return "准备发起临时绑卡请求...";
  if (m.match(/\[无卡\] 尝试 [A-Z]+/)) return "正在处理绑卡步骤...";
  if (m.match(/\[无卡\] [A-Z]+: OK/)) return "✓ 绑卡验证通过";
  if (m.includes("✓ 共获得") && m.includes("IDE 许可证")) return "✓ 绑卡验证全部通过";
  if (m.includes("等待 10s") || m.includes("许可证同步到账号")) return "正在同步账单数据，请稍候...";
  if (m.includes("register: HTTP 200")) return "✓ 服务注册成功";
  if (m.includes("正在检查账号额度")) return "正在核对账号权益额度...";
  if (m.includes("⏳") && m.includes("NC licenseId 尚待信任")) return "⏳ 权益配置中，约 5-30 分钟后自动完成...";
  if (m.includes("✓ 已为您生成专属 API 密钥")) {
    const quotaMatch = m.match(/额度 (.+?)）/);
    const quota = quotaMatch ? `（${quotaMatch[1]}）` : "";
    return `✓ 账号激活成功！专属 API 密钥已生成${quota}`;
  }
  if (m.includes("⏳ NC 许可证已创建并记录") || m.includes("约30-60分钟后后台自动入池")) {
    return "⏳ 账号权益处理中，系统将在后台自动完成并为您分配 API 密钥（约 5-30 分钟）";
  }

  // ── 通用过滤 ──
  if (m.includes("[RATE_LIMIT]") || m.includes("速率限制") || m.includes("429")) return "⚠ 服务器繁忙，正在等待后重试，请稍候...";
  if (m.startsWith("[WARN]") || m.startsWith("⚠")) return "⚠ 部分步骤需要重试，正在处理...";
  if (m.startsWith("[EXCEPTION]")) return "✗ 发生异常，请检查账号信息后重试";
  if (m.startsWith("✗")) return "✗ 操作失败，请检查账号信息是否正确";
  if (m.match(/^\[[A-Z0-9/_\-:]+\]/)) return null;
  if (msg.startsWith("  ") && !m.startsWith("✓") && !m.startsWith("✗") && !m.startsWith("⏳") && !m.startsWith("⚠")) return null;

  return msg;
}

export default function Activate() {
  const queryClient = useQueryClient();
  // 仅完整管理员（admin）走「无卡激活」管理员模式；
  // 次级管理员（low_admin）落入用户面板，但额外展示绿色提示框
  const isAdmin = isFullAdmin();
  const isLowAdmin = !isAdmin && !!getAdminKey() && getAdminRole() === "low_admin";

  // 非管理员须先通过 Discord 验证（仅验服务器成员，不限身份组）
  const { dcToken, userTag, isLoggedIn: dcLoggedIn, login: dcLogin, logout: dcLogout } = useDiscordAuth("activate");

  const [email, setEmail] = useState("");
  const [password, setPassword] = useState("");
  const [status, setStatus] = useState<TaskStatus>("idle");
  const [logs, setLogs] = useState<LogEntry[]>([]);
  const [error, setError] = useState<string | null>(null);
  const [successInfo, setSuccessInfo] = useState<{ licenseId?: string; generatedKey?: string; isExisting?: boolean; isPending?: boolean } | null>(null);
  const [keyCopied, setKeyCopied] = useState(false);
  const logsEndRef = useRef<HTMLDivElement>(null);
  const logsContainerRef = useRef<HTMLDivElement>(null);
  const esRef = useRef<EventSource | null>(null);

  useEffect(() => {
    const el = logsContainerRef.current;
    if (el) {
      el.scrollTop = el.scrollHeight;
    }
  }, [logs]);

  useEffect(() => {
    return () => { esRef.current?.close(); };
  }, []);

  const addLog = (msg: string) => {
    setLogs((prev) => [...prev, { msg, time: new Date().toLocaleTimeString("zh-CN", { hour12: false }) }]);
  };

  const handleSubmit = async (e: React.FormEvent) => {
    e.preventDefault();
    if (!email || !password) return;

    setStatus("running");
    setLogs([]);
    setError(null);
    setSuccessInfo(null);
    esRef.current?.close();

    try {
      const body: Record<string, string> = { email, password };
      // 普通访客 + LOW 用户均须 Discord 验证；仅完整管理员可跳过
      // LOW 用户的 Discord ID 用来在多个 LOW CF 子池间挑选自己的子池
      if (!isAdmin && dcToken) body.discord_token = dcToken;
      // 关键：adminFetch 风格，已登录用户（含 low_admin）会自动带上 X-Admin-Key
      // → 后端据此识别 is_low_admin，跳过 20/日 限制并使用 LOW 专属 CF 池
      const headers: Record<string, string> = { "Content-Type": "application/json" };
      const storedKey = getAdminKey();
      if (storedKey) headers["X-Admin-Key"] = storedKey;
      const res = await fetch("/admin/activate", {
        method: "POST",
        headers,
        body: JSON.stringify(body),
      });
      if (!res.ok) {
        const data = await res.json().catch(() => ({}));
        throw new Error(data.detail || data.error?.message || `HTTP ${res.status}`);
      }
      const postData = await res.json();
      const { task_id } = postData;

      // ★ 新流程：不再预签 key，等待激活流程结束后由 done 事件返回 generated_key

      const es = new EventSource(`/admin/activate/${task_id}/stream`);
      esRef.current = es;

      es.onmessage = (ev) => {
        try {
          const data = JSON.parse(ev.data);
          if (data.type === "log") {
            addLog(data.msg);
          } else if (data.type === "done") {
            es.close();
            if (data.status === "success") {
              setStatus("success");
              const isPending = !data.result?.jwt && !!data.result?.pending_nc_lids?.length;
              setSuccessInfo({
                licenseId: data.result?.license_id,
                generatedKey: data.generated_key ?? undefined,
                isExisting: data.is_existing_key ?? false,
                isPending,
              });
              queryClient.invalidateQueries({ queryKey: ["/admin/accounts"] });
              queryClient.invalidateQueries({ queryKey: ["/admin/status"] });
              queryClient.invalidateQueries({ queryKey: ["admin-keys"] });
            } else if (data.status === "quota_rejected") {
              setStatus("quota_rejected");
            } else if (data.status === "donated_blocked") {
              setStatus("donated_blocked");
            } else {
              setStatus("failed");
              setError(data.result?.error || "激活失败");
            }
          }
        } catch {}
      };

      es.onerror = () => {
        es.close();
        if (status === "running") {
          setStatus("failed");
          setError("与服务器的连接中断");
        }
      };
    } catch (e: any) {
      setStatus("failed");
      setError(e.message || "请求失败");
    }
  };

  const handleReset = () => {
    esRef.current?.close();
    setStatus("idle");
    setLogs([]);
    setError(null);
    setSuccessInfo(null);
    setKeyCopied(false);
  };

  const copyKey = (key: string) => {
    navigator.clipboard.writeText(key);
    setKeyCopied(true);
    setTimeout(() => setKeyCopied(false), 2000);
  };

  // 诊断探测（仅管理员）
  const [probeStatus, setProbeStatus] = useState<"idle" | "running" | "done" | "error">("idle");
  const [probeResult, setProbeResult] = useState<Record<string, any> | null>(null);
  const [probeType, setProbeType] = useState<"grazie" | "ides">("grazie");
  const [showProbe, setShowProbe] = useState(false);

  const runProbe = async (type: "grazie" | "ides") => {
    if (!email || !password) { alert("请先填写邮箱和密码"); return; }
    setProbeType(type);
    setProbeStatus("running");
    setProbeResult(null);
    setShowProbe(true);
    try {
      const endpoint = type === "ides" ? "/admin/activate/ides-probe" : "/admin/activate/grazie-probe";
      const res = await fetch(endpoint, {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({ email, password }),
      });
      const data = await res.json().catch(() => ({ error: `HTTP ${res.status}` }));
      setProbeResult(data);
      setProbeStatus("done");
    } catch (e: any) {
      setProbeResult({ error: e.message });
      setProbeStatus("error");
    }
  };

  const isRunning = status === "running";

  const logLineColor = (msg: string) => {
    if (msg.startsWith("✓") || msg.includes("成功")) return "text-emerald-400";
    if (msg.startsWith("✗")) return "text-red-400 font-medium";
    if (msg.includes("[FAIL]") || msg.includes("[EXCEPTION]")) return "text-red-400";
    if (msg.includes("[WARN]") || msg.startsWith("⚠")) return "text-yellow-400";
    if (msg.match(/^\[(\d+)\/8\]/)) return "text-sky-400 font-semibold";
    return "text-gray-300";
  };

  // 渲染日志：管理员看原始日志，游客看伪装后的日志
  const renderLogs = () => {
    if (isAdmin) {
      return logs.map((entry, i) => {
        const display = fmtAdminLog(entry.msg);
        return (
          <div key={i} className="flex gap-3">
            <span className="text-gray-600 shrink-0 select-none">{entry.time}</span>
            <span className={logLineColor(entry.msg)}>{display}</span>
          </div>
        );
      });
    }
    // 游客：过滤 + 伪装
    const disguised: { time: string; msg: string }[] = [];
    for (const entry of logs) {
      const d = disguiseLog(entry.msg);
      if (d !== null) disguised.push({ time: entry.time, msg: d });
    }
    return disguised.map((entry, i) => (
      <div key={i} className="flex gap-3">
        <span className="text-gray-600 shrink-0 select-none">{entry.time}</span>
        <span className={logLineColor(entry.msg)}>{entry.msg}</span>
      </div>
    ));
  };

  return (
    <div className="premium-page-shell mx-auto max-w-5xl space-y-6">
      <section className="premium-hero-panel rounded-[2rem] p-5 sm:p-7">
        <div className="relative z-10 grid gap-6 lg:grid-cols-[1fr_300px] lg:items-center">
          <div>
            <div className="mb-4 inline-flex items-center gap-2 rounded-full border border-white/70 bg-white/60 px-3 py-1.5 text-xs font-black uppercase tracking-[0.2em] text-orange-700 shadow-sm backdrop-blur">
              <Sparkles className="h-3.5 w-3.5" />
              Account Activation Lab
            </div>
            <h1 className="text-3xl font-black tracking-tight text-foreground sm:text-5xl">
              账号<span className="citrus-text">激活</span>
            </h1>
            <p className="mt-3 max-w-2xl text-sm leading-6 text-muted-foreground sm:text-base">
              {isAdmin ? "将 JetBrains AI 账号接入系统池，自动完成凭证校验、额度检查与密钥签发。" : "自动完成账号验证、绑卡解绑与专属 API 密钥生成，流程进度实时可见。"}
            </p>
            <div className="mt-5 flex flex-wrap gap-2">
              <span className="inline-flex items-center gap-2 rounded-2xl bg-white/58 px-3 py-2 text-xs font-bold text-foreground ring-1 ring-white/65">
                <ShieldCheck className="h-4 w-4 text-cyan-700" />
                Discord Guard
              </span>
              <span className="inline-flex items-center gap-2 rounded-2xl bg-white/58 px-3 py-2 text-xs font-bold text-foreground ring-1 ring-white/65">
                <Wand2 className="h-4 w-4 text-orange-600" />
                Auto Provision
              </span>
              <span className="inline-flex items-center gap-2 rounded-2xl bg-white/58 px-3 py-2 text-xs font-bold text-foreground ring-1 ring-white/65">
                <TerminalSquare className="h-4 w-4 text-emerald-700" />
                Live Logs
              </span>
            </div>
          </div>
          <div className="rounded-[1.75rem] border border-white/70 bg-white/56 p-4 shadow-xl shadow-orange-950/5 backdrop-blur-xl">
            <div className="flex items-center gap-3">
              <div className="citrus-orb grid h-14 w-14 place-items-center rounded-2xl">
                <UserPlus className="h-6 w-6 text-white" />
              </div>
              <div>
                <p className="text-xs font-black uppercase tracking-[0.22em] text-muted-foreground">Mode</p>
                <p className="mt-1 text-lg font-black text-foreground">{isAdmin ? "Admin No-card" : isLowAdmin ? "LOW Admin" : "Guest Auto"}</p>
              </div>
            </div>
            <p className="mt-4 text-xs leading-5 text-muted-foreground">
              密钥生成后请立即复制保存；激活过程中的预签密钥会在凭证确认后自动升级额度。
            </p>
          </div>
        </div>
      </section>

      {/* 说明卡片（统一文案，管理员 / 用户 / LOW 一致） */}
      <div className="premium-surface rounded-[1.75rem] p-5 text-sm text-muted-foreground space-y-1.5">
        <p className="font-black text-cyan-700 flex items-center gap-2">
          <span className="inline-block w-2 h-2 rounded-full bg-cyan-500 animate-pulse shadow-[0_0_16px_rgba(6,182,212,0.6)]" />
          账户激活 · AI Pro 一个月试用
        </p>
        <ul className="list-disc list-inside space-y-1">
          <li className="text-emerald-300/80">仅支持<span className="font-semibold"> 已绑定信用卡 </span>的 JetBrains 账号（领取 AI Pro 试用须通过付款验证，<span className="font-semibold">不会产生扣费</span>）</li>
          <li>系统自动为账号申请 <span className="font-semibold text-emerald-300">JetBrains AI Pro 一个月免费试用</span>，单账号约可获得 <span className="font-semibold">1M Tokens</span> 额度</li>
          <li>激活成功后账号自动入池，签发额度 <span className="font-semibold text-emerald-300">{isAdmin ? 25 : isLowAdmin ? 16 : 25}</span> 的专属 API 密钥</li>
          <li>全程仅读取账号凭证，<span className="font-semibold">不会修改你的账号密码、绑卡或任何账号设置</span></li>
          <li className="text-amber-400/80">请妥善保管账号信息；密钥生成后请立即复制保存</li>
        </ul>
      </div>

      {/* 次级管理员专属提示（仅 low_admin 可见） */}
      {isLowAdmin && (
        <div className="premium-surface rounded-[1.75rem] p-5 text-sm space-y-1.5">
          <p className="font-black text-cyan-700 flex items-center gap-2">
            <span className="inline-block w-2 h-2 rounded-full bg-cyan-500 shadow-[0_0_16px_rgba(6,182,212,0.6)]" />
            次级管理员模式
          </p>
          <ul className="list-disc list-inside space-y-1 text-emerald-300/90">
            <li>请先 <span className="font-semibold">Discord 登录</span>，您的 Discord 账号将用于划分专属 LOW CF 子池；激活不计入 20 次/日 限额</li>
            <li>您发起的激活将自动使用<span className="font-semibold">「我的 CF 池」</span>中按当前 Discord 账号配置的 Worker，与系统主池及其他 Discord 用户完全隔离</li>
            <li>请先在左侧「我的 CF 池」中为当前 Discord 账号添加至少一个可用的 Worker URL，否则将走直连可能受限</li>
          </ul>
        </div>
      )}

      {/* LOW 用户专属：个人专属密钥（Discord 登录后出现，额度累加） */}
      {isLowAdmin && (
        <LowPersonalKey
          dcToken={dcToken ?? ""}
          dcLoggedIn={dcLoggedIn}
          isLowAdmin={isLowAdmin}
        />
      )}

      {/* LOW 用户专属：per-Discord 并发设置（影响单条 + 批量激活） */}
      {(isLowAdmin || isAdmin) && (
        <LowConcurrencyConfig dcToken={dcToken ?? ""} isLowAdmin={isLowAdmin} />
      )}

      {/* LOW 用户专属：批量激活面板（管理员同样可见以便代为操作） */}
      {(isLowAdmin || isAdmin) && (
        <LowBatchPanel
          queryClient={queryClient}
          dcToken={dcToken ?? ""}
          dcLoggedIn={dcLoggedIn}
          isLowAdmin={isLowAdmin}
        />
      )}

      {/* Discord 登录门控（普通访客 + LOW 用户均须 Discord 登录；仅完整管理员跳过） */}
      {!isAdmin && (
        <div className="premium-surface rounded-[1.75rem] p-4">
          {dcLoggedIn ? (
            <div className="flex items-center gap-3 flex-wrap">
              <div className="grid h-10 w-10 place-items-center rounded-2xl bg-indigo-100 text-indigo-700 ring-1 ring-indigo-200">
                <ShieldCheck className="w-4 h-4" />
              </div>
              <div>
                <p className="text-xs font-black uppercase tracking-[0.18em] text-indigo-500">Discord verified</p>
                <p className="text-sm font-bold text-foreground">{userTag}</p>
              </div>
              <button
                type="button"
                onClick={dcLogout}
                className="ml-auto flex items-center gap-1.5 px-3 py-2 rounded-2xl border border-white/70 bg-white/60 text-xs font-bold text-muted-foreground hover:bg-white transition-colors"
              >
                <LogOut className="w-3.5 h-3.5" />
                退出
              </button>
            </div>
          ) : (
            <div className="flex flex-col items-center gap-4 py-8 rounded-[1.5rem] border border-dashed border-indigo-300/70 bg-indigo-50/60 text-center">
              <div className="grid h-12 w-12 place-items-center rounded-2xl bg-indigo-100 text-indigo-700 ring-1 ring-indigo-200">
                <LogIn className="w-5 h-5" />
              </div>
              <div>
                <p className="text-sm font-black text-foreground">需要 Discord 验证才能激活账号</p>
                <p className="mt-1 text-xs text-muted-foreground">仅验证服务器成员身份，不读取敏感信息</p>
              </div>
              <button
                type="button"
                onClick={dcLogin}
                className="flex items-center gap-2 px-5 py-3 rounded-2xl text-sm font-black bg-indigo-600 text-white shadow-lg shadow-indigo-500/20 hover:bg-indigo-700 transition-colors"
              >
                <LogIn className="w-4 h-4" />
                Discord 登录
              </button>
            </div>
          )}
        </div>
      )}

      {/* 输入表单（非管理员须先登录 DC） */}
      <form onSubmit={handleSubmit} className="premium-surface rounded-[1.75rem] p-5 space-y-4">
        <div>
          <label className="block text-sm font-medium text-foreground mb-1.5">JetBrains 邮箱</label>
          <div className="relative">
            <Mail className="absolute left-3 top-1/2 -translate-y-1/2 w-4 h-4 text-muted-foreground" />
            <input
              type="email"
              value={email}
              onChange={(e) => setEmail(e.target.value)}
              placeholder="user@example.com"
              disabled={isRunning}
              required
              className="w-full pl-10 pr-4 py-3 rounded-2xl border border-white/70 bg-white/70 text-sm text-foreground placeholder:text-muted-foreground shadow-inner focus:outline-none focus:ring-4 focus:ring-orange-200/60 disabled:opacity-50"
            />
          </div>
        </div>
        <div>
          <label className="block text-sm font-medium text-foreground mb-1.5">JetBrains 密码</label>
          <div className="relative">
            <Lock className="absolute left-3 top-1/2 -translate-y-1/2 w-4 h-4 text-muted-foreground" />
            <input
              type="password"
              value={password}
              onChange={(e) => setPassword(e.target.value)}
              placeholder="••••••••"
              disabled={isRunning}
              required
              className="w-full pl-10 pr-4 py-3 rounded-2xl border border-white/70 bg-white/70 text-sm text-foreground placeholder:text-muted-foreground shadow-inner focus:outline-none focus:ring-4 focus:ring-orange-200/60 disabled:opacity-50"
            />
          </div>
        </div>
        <div className="flex flex-wrap gap-3">
          <button
            type="submit"
            disabled={isRunning || !email || !password || (!isAdmin && !dcLoggedIn)}
            className="flex items-center gap-2 px-5 py-3 rounded-2xl bg-gradient-to-r from-orange-400 to-orange-600 text-white text-sm font-black shadow-lg shadow-orange-500/20 hover:-translate-y-0.5 disabled:opacity-50 disabled:cursor-not-allowed disabled:hover:translate-y-0 transition-all"
          >
            {isRunning ? (
              <><Loader2 className="w-4 h-4 animate-spin" />激活中...</>
            ) : (
              <><Play className="w-4 h-4" />开始激活</>
            )}
          </button>
          {status !== "idle" && (
            <button
              type="button"
              onClick={handleReset}
              disabled={isRunning}
              className="px-4 py-3 rounded-2xl border border-white/70 bg-white/60 text-sm font-bold text-muted-foreground hover:bg-white disabled:opacity-50 transition-colors"
            >
              重置
            </button>
          )}
          {/* 诊断按钮仅管理员可见 */}
          {isAdmin && (
            <>
              <button
                type="button"
                onClick={() => runProbe("grazie")}
                disabled={isRunning || probeStatus === "running" || !email || !password}
                title="Grazie 端点综合诊断"
                className="flex items-center gap-2 px-4 py-2.5 rounded-md border border-violet-500/40 bg-violet-500/10 text-violet-400 text-sm font-medium hover:bg-violet-500/20 disabled:opacity-50 disabled:cursor-not-allowed transition-colors"
              >
                {probeStatus === "running" && probeType === "grazie" ? (
                  <><Loader2 className="w-4 h-4 animate-spin" />探测中...</>
                ) : (
                  <><FlaskConical className="w-4 h-4" />Grazie 诊断</>
                )}
              </button>
              <button
                type="button"
                onClick={() => runProbe("ides")}
                disabled={isRunning || probeStatus === "running" || !email || !password}
                title="IDES 端点全流程探测"
                className="flex items-center gap-2 px-4 py-2.5 rounded-md border border-amber-500/40 bg-amber-500/10 text-amber-400 text-sm font-medium hover:bg-amber-500/20 disabled:opacity-50 disabled:cursor-not-allowed transition-colors"
              >
                {probeStatus === "running" && probeType === "ides" ? (
                  <><Loader2 className="w-4 h-4 animate-spin" />IDES 探测中...</>
                ) : (
                  <><FlaskConical className="w-4 h-4" />IDES 探测</>
                )}
              </button>
            </>
          )}
        </div>
      </form>

      {/* 运行中且已预签 key：提前展示密钥（0 额度，稍后自动升级）*/}
      {status === "running" && successInfo?.generatedKey && successInfo.isPending && (
        <div className="mb-4 p-4 rounded-lg border border-amber-500/30 bg-amber-500/5 space-y-2">
          <div className="flex items-center gap-2">
            <div className="w-1.5 h-1.5 rounded-full bg-amber-400 animate-pulse" />
            <p className="text-sm font-semibold text-amber-300">您的 API 密钥（激活中，当前额度 0）</p>
          </div>
          <p className="text-xs text-muted-foreground">凭证确认后额度自动升至 {isLowAdmin ? 16 : 25}，请妥善保存此密钥</p>
          <div className="flex items-center gap-2 mt-1">
            <code className="flex-1 text-xs font-mono text-amber-300 bg-amber-500/10 px-3 py-2 rounded border border-amber-500/20 break-all">
              {successInfo.generatedKey}
            </code>
            <button
              onClick={() => copyKey(successInfo.generatedKey!)}
              className="shrink-0 flex items-center gap-1.5 px-3 py-2 rounded border border-amber-500/30 text-xs font-medium text-amber-400 hover:bg-amber-500/10 transition-colors"
            >
              {keyCopied ? <><Check className="w-3.5 h-3.5" />已复制</> : <><Copy className="w-3.5 h-3.5" />复制</>}
            </button>
          </div>
        </div>
      )}

      {/* 成功提示 */}
      {status === "success" && (
        <div className="mb-4 space-y-3">
          <div className={`flex items-start gap-3 p-4 rounded-lg border text-emerald-400 ${successInfo?.isExisting ? "bg-amber-500/10 border-amber-500/30 text-amber-400" : "bg-emerald-500/10 border-emerald-500/30"}`}>
            <CheckCircle className="w-5 h-5 shrink-0 mt-0.5" />
            <div>
              <p className="font-medium">
                {successInfo?.isExisting ? "该账号已存在 — 找回密钥" : "激活成功！账号已自动添加"}
              </p>
              {isAdmin && successInfo?.licenseId && (
                <p className="text-xs mt-0.5 opacity-70">licenseId: {successInfo.licenseId}</p>
              )}
            </div>
          </div>
          {successInfo?.generatedKey && (
            <div className="p-4 rounded-lg border border-primary/30 bg-primary/5 space-y-2">
              <div className="flex items-center gap-2">
                <div className={`w-1.5 h-1.5 rounded-full ${successInfo.isPending ? "bg-amber-400" : "bg-primary"} animate-pulse`} />
                <p className="text-sm font-semibold text-foreground">
                  {successInfo.isExisting
                    ? "您的 API 密钥（找回）"
                    : successInfo.isPending
                      ? `您的专属 API 密钥（激活中，额度将升为 ${isLowAdmin ? 16 : 25}）`
                      : `您的专属 API 密钥（额度 ${isLowAdmin ? 16 : 25}）`}
                </p>
              </div>
              <p className="text-xs text-muted-foreground">
                {successInfo.isExisting
                  ? "这是您账号绑定的密钥，请妥善保存。"
                  : successInfo.isPending
                    ? `凭证确认中，约 1-5 分钟后额度自动升至 ${isLowAdmin ? 16 : 25}，密钥请妥善保存`
                    : "请妥善保存，此密钥不会再次显示"}
              </p>
              <div className="flex items-center gap-2 mt-1">
                <code className="flex-1 text-xs font-mono text-primary bg-primary/10 px-3 py-2 rounded border border-primary/20 break-all">
                  {successInfo.generatedKey}
                </code>
                <button
                  onClick={() => copyKey(successInfo.generatedKey!)}
                  className="shrink-0 flex items-center gap-1.5 px-3 py-2 rounded border border-primary/30 text-xs font-medium text-primary hover:bg-primary/10 transition-colors"
                >
                  {keyCopied ? <><Check className="w-3.5 h-3.5" />已复制</> : <><Copy className="w-3.5 h-3.5" />复制</>}
                </button>
              </div>
              <p className="text-xs text-muted-foreground/70">使用方式：<code className="text-primary/70">Authorization: Bearer {successInfo.generatedKey.slice(0, 16)}...</code></p>
            </div>
          )}
        </div>
      )}

      {status === "quota_rejected" && (
        <div className="mb-4 flex items-start gap-3 p-4 rounded-lg bg-amber-500/10 border border-amber-500/30 text-amber-400">
          <XCircle className="w-5 h-5 shrink-0 mt-0.5" />
          <div>
            <p className="font-medium">账号额度不足，无法激活</p>
            <p className="text-sm mt-1.5 text-amber-400/80 leading-relaxed">
              该账号已使用过额度，不是满配额状态，不符合激活要求。<br />
              仅允许从未使用过的、满配额的全新账号入池，请更换账号后重试。
            </p>
          </div>
        </div>
      )}

      {status === "donated_blocked" && (
        <div className="mb-4 flex items-start gap-3 p-4 rounded-lg bg-red-500/10 border border-red-500/30 text-red-400">
          <XCircle className="w-5 h-5 shrink-0 mt-0.5" />
          <div>
            <p className="font-medium">此账号已被封锁，无法重新激活</p>
            <p className="text-sm mt-1.5 text-red-400/80 leading-relaxed">
              该账号曾向系统捐献过 Key，已被永久标记，请使用其他账号。
            </p>
          </div>
        </div>
      )}

      {status === "failed" && error && (
        <div className="mb-4 flex items-start gap-3 p-4 rounded-lg bg-red-500/10 border border-red-500/30 text-red-400">
          <XCircle className="w-5 h-5 shrink-0 mt-0.5" />
          <div>
            <p className="font-medium">激活失败</p>
            <p className="text-sm mt-0.5 text-red-400/80 leading-relaxed">
              {isAdmin
                ? error
                : (error?.includes("RATE_LIMIT") || error?.includes("速率限制") || error?.includes("429"))
                  ? "服务器当前请求过多，请等待 1-2 分钟后重试。"
                  : (error?.includes("PAYMENT_PROOF_REQUIRED") || error?.includes("payment_proof") || error?.includes("绑定信用卡") || error?.includes("绑卡"))
                    ? "该账号尚未绑定信用卡，无法领取 AI Pro 试用。请先到 JetBrains 账号设置中绑定一张可用的信用卡（不会扣费），然后再回来重试。"
                    : (error?.includes("COUNTRY_IS_RESTRICTED") || error?.includes("country"))
                      ? "该账号所在地区受限，无法领取 AI Pro 试用，请更换账号或调整账号地区后重试。"
                      : (error?.includes("登录失败") || error?.includes("登录异常") || error?.toLowerCase().includes("login") || error?.includes("invalid_login"))
                        ? "账号验证未通过，请检查邮箱和密码是否正确后重试。"
                        : (error || "激活失败，请稍后重试。")}
            </p>
          </div>
        </div>
      )}

      {/* 日志面板 */}
      {logs.length > 0 && (
        <div className="premium-surface rounded-[1.75rem] overflow-hidden">
          <div className="flex items-center gap-2 px-4 py-3 bg-white/45 border-b border-white/60">
            <TerminalSquare className="w-4 h-4 text-muted-foreground" />
            <span className="text-xs font-medium text-muted-foreground">
              {isAdmin ? "激活日志" : "操作日志"}
            </span>
            {isRunning && <Loader2 className="w-3 h-3 animate-spin text-primary ml-auto" />}
          </div>
          <div ref={logsContainerRef} className="premium-terminal p-4 h-80 overflow-y-auto font-mono text-xs space-y-0.5">
            {renderLogs()}
            <div ref={logsEndRef} />
          </div>
        </div>
      )}

      {/* 诊断探测结果面板（仅管理员） */}
      {isAdmin && showProbe && (
        <div className="mt-4 rounded-lg border border-violet-500/30 overflow-hidden">
          <div className="flex items-center gap-2 px-4 py-2.5 bg-violet-500/10 border-b border-violet-500/20">
            <FlaskConical className="w-4 h-4 text-violet-400" />
            <span className="text-xs font-medium text-violet-400">
              {probeType === "ides" ? "IDES 端点探测结果" : "Grazie 诊断结果"}
            </span>
            {probeStatus === "running" && <Loader2 className="w-3 h-3 animate-spin text-violet-400 ml-auto" />}
            <button
              onClick={() => setShowProbe(false)}
              className="ml-auto text-muted-foreground hover:text-foreground text-xs"
            >
              ✕
            </button>
          </div>
          <div className="bg-[#0d1117] p-4 h-96 overflow-y-auto font-mono text-xs">
            {probeStatus === "running" && (
              <div className="text-violet-400 animate-pulse">正在探测，请稍候（约 30-60 秒）...</div>
            )}
            {probeResult && (
              <div className="space-y-1">
                {Object.entries(probeResult).map(([key, value]) => {
                  const valStr = typeof value === "object" ? JSON.stringify(value, null, 2) : String(value);
                  const isSuccess = valStr.includes('"token"') || (valStr.includes('HTTP 200') && !valStr.includes('error'));
                  const isError = valStr.includes('HTTP 4') || valStr.includes('HTTP 5') || valStr.includes('error') || valStr.includes('失败');
                  const isKey = key.startsWith("tokens_api") || key.includes("license_id") || key.includes("probe");
                  return (
                    <div key={key} className={`border-b border-white/5 pb-1 mb-1 ${isKey ? "mt-2" : ""}`}>
                      <div className={`font-bold text-xs mb-0.5 ${isKey ? "text-amber-400" : "text-gray-500"}`}>
                        [{key}]
                      </div>
                      <pre className={`whitespace-pre-wrap break-all text-xs leading-relaxed ${
                        isSuccess ? "text-emerald-400" : isError ? "text-red-400" : "text-gray-300"
                      }`}>
                        {valStr.length > 1000 ? valStr.slice(0, 1000) + "\n...(截断)" : valStr}
                      </pre>
                    </div>
                  );
                })}
              </div>
            )}
          </div>
        </div>
      )}
    </div>
  );
}

// ────────────────────────────────────────────────────────────
// LOW 用户批量激活面板（管理员同样可见）
// 调用 GET /admin/low-config + PATCH /admin/low-config + POST /admin/activate-batch
// ────────────────────────────────────────────────────────────
interface LowConfigResp {
  concurrency: number;
  concurrency_default: number;
  concurrency_max: number;
  batch_max: number;
  cooldown_seconds: number;
  last_batch_at: number;
  cooldown_remaining: number;
  server_time: number;
  discord_id?: string;
  discord_settings?: Record<string, number>;
}

interface BatchAcct {
  email: string;
  password: string;
}

interface BatchRow {
  email: string;
  task_id: string;
  preissued_key: string;
  status: "running" | "success" | "failed";
  lastLog: string;
}

function parseBatchInput(text: string): BatchAcct[] {
  const out: BatchAcct[] = [];
  text.split(/\r?\n/).forEach((raw) => {
    const line = raw.trim();
    if (!line) return;
    // 支持 email:password 或 email,password 或 email\tpassword
    const m = line.match(/^(\S+?)[\s,:]+(.+)$/);
    if (!m) return;
    const email = m[1].trim();
    const password = m[2].trim();
    if (email && password && email.includes("@")) {
      out.push({ email, password });
    }
  });
  return out;
}

// ────────────────────────────────────────────────────────────
// LOW 用户个人专属密钥：创建一次，所有激活累加配额到这把 key
// ────────────────────────────────────────────────────────────
function LowPersonalKey({
  dcToken,
  dcLoggedIn,
  isLowAdmin,
}: {
  dcToken: string;
  dcLoggedIn: boolean;
  isLowAdmin: boolean;
}) {
  const [keyInfo, setKeyInfo] = useState<{
    key: string | null;
    usage_limit: number;
    usage_count: number;
  } | null>(null);
  const [creating, setCreating] = useState(false);
  const [copied, setCopied] = useState(false);
  const [err, setErr] = useState<string | null>(null);
  const [importing, setImporting] = useState(false);
  const [exporting, setExporting] = useState(false);
  const [importMsg, setImportMsg] = useState<string | null>(null);
  const importFileRef = useRef<HTMLInputElement | null>(null);

  const extraHeaders = (): Record<string, string> =>
    dcToken ? { "X-Discord-Token": dcToken } : {};

  const fetchKey = async () => {
    try {
      const res = await adminFetch("/admin/low-user-key", { headers: extraHeaders() });
      if (!res.ok) return;
      const data = await res.json();
      setKeyInfo({
        key: data.key ?? null,
        usage_limit: data.usage_limit ?? 0,
        usage_count: data.usage_count ?? 0,
      });
    } catch {}
  };

  useEffect(() => {
    if (isLowAdmin && dcLoggedIn) fetchKey();
  }, [isLowAdmin, dcLoggedIn, dcToken]); // eslint-disable-line react-hooks/exhaustive-deps

  const createKey = async () => {
    setCreating(true);
    setErr(null);
    try {
      const res = await adminFetch("/admin/low-user-key", {
        method: "POST",
        headers: extraHeaders(),
      });
      if (!res.ok) {
        const d = await res.json().catch(() => ({}));
        throw new Error(d.detail || `HTTP ${res.status}`);
      }
      const data = await res.json();
      setKeyInfo({ key: data.key, usage_limit: data.usage_limit, usage_count: data.usage_count });
    } catch (e: any) {
      setErr(e.message);
    } finally {
      setCreating(false);
    }
  };

  const copyKey = () => {
    if (keyInfo?.key) {
      navigator.clipboard.writeText(keyInfo.key);
      setCopied(true);
      setTimeout(() => setCopied(false), 2000);
    }
  };

  const exportKey = async () => {
    setExporting(true);
    setErr(null);
    setImportMsg(null);
    try {
      const res = await adminFetch("/admin/low-user-key/export", { headers: extraHeaders() });
      if (!res.ok) {
        const d = await res.json().catch(() => ({}));
        throw new Error(d.detail || d.error?.message || `HTTP ${res.status}`);
      }
      const data = await res.json();
      // 触发浏览器下载
      const blob = new Blob([JSON.stringify(data, null, 2)], { type: "application/json" });
      const url = URL.createObjectURL(blob);
      const a = document.createElement("a");
      a.href = url;
      const ts = new Date().toISOString().slice(0, 10);
      a.download = `jbai-personal-key-${ts}.json`;
      document.body.appendChild(a);
      a.click();
      document.body.removeChild(a);
      URL.revokeObjectURL(url);
    } catch (e: any) {
      setErr(e.message);
    } finally {
      setExporting(false);
    }
  };

  const triggerImport = () => {
    setErr(null);
    setImportMsg(null);
    importFileRef.current?.click();
  };

  const handleImportFile = async (e: React.ChangeEvent<HTMLInputElement>) => {
    const file = e.target.files?.[0];
    e.target.value = ""; // 允许重复选择同一文件
    if (!file) return;
    if (keyInfo?.key) {
      setErr("您当前已有个人密钥，请先点击下方「删除并重置」再导入");
      return;
    }
    setImporting(true);
    setErr(null);
    setImportMsg(null);
    try {
      const text = await file.text();
      let payload: unknown;
      try {
        payload = JSON.parse(text);
      } catch {
        throw new Error("文件内容不是合法的 JSON");
      }
      const res = await adminFetch("/admin/low-user-key/import", {
        method: "POST",
        headers: { ...extraHeaders(), "Content-Type": "application/json" },
        body: JSON.stringify(payload),
      });
      if (!res.ok) {
        const d = await res.json().catch(() => ({}));
        throw new Error(d.detail || d.error?.message || `HTTP ${res.status}`);
      }
      const data = await res.json();
      setKeyInfo({
        key: data.key,
        usage_limit: data.usage_limit ?? 0,
        usage_count: data.usage_count ?? 0,
      });
      setImportMsg(`导入成功，恢复了 ${data.account_count ?? 0} 个账号绑定`);
    } catch (err2: any) {
      setErr(err2.message);
    } finally {
      setImporting(false);
    }
  };

  const deleteKey = async () => {
    if (!confirm("确认删除当前个人密钥？删除后额度立即清零，无法恢复（除非有导出的备份）。")) return;
    setErr(null);
    setImportMsg(null);
    try {
      const res = await adminFetch("/admin/low-user-key", {
        method: "DELETE",
        headers: extraHeaders(),
      });
      if (!res.ok) {
        const d = await res.json().catch(() => ({}));
        throw new Error(d.detail || `HTTP ${res.status}`);
      }
      setKeyInfo({ key: null, usage_limit: 0, usage_count: 0 });
    } catch (e: any) {
      setErr(e.message);
    }
  };

  if (!isLowAdmin) return null;
  if (!dcLoggedIn) return null;

  return (
    <div className="mb-5 rounded-lg border border-violet-500/30 bg-violet-500/[0.03] p-4 space-y-3">
      <div className="flex items-center justify-between">
        <p className="text-sm font-semibold text-violet-400">我的专属 API 密钥</p>
        {keyInfo?.key && (
          <span className="text-xs text-muted-foreground">
            已用 {keyInfo.usage_count} / 额度 {keyInfo.usage_limit}
          </span>
        )}
      </div>

      {keyInfo === null ? (
        <p className="text-xs text-muted-foreground">加载中...</p>
      ) : keyInfo.key ? (
        <div className="space-y-2">
          <div className="flex items-center gap-2 bg-black/20 rounded px-3 py-2 text-xs font-mono text-violet-300 break-all">
            <span className="flex-1">{keyInfo.key}</span>
            <button
              type="button"
              onClick={copyKey}
              className="shrink-0 text-xs text-violet-400 hover:text-violet-300 transition-colors"
            >
              {copied ? "已复制" : "复制"}
            </button>
          </div>
          <p className="text-xs text-muted-foreground">
            单次激活 +16 额度，批量激活每成功一个账号 +16 额度，全部累加到此密钥。
          </p>
          <div className="flex flex-wrap gap-2 pt-1">
            <button
              type="button"
              onClick={exportKey}
              disabled={exporting}
              className="px-3 py-1 rounded text-xs font-semibold border border-violet-500/40 text-violet-300 hover:bg-violet-500/10 disabled:opacity-50 transition-colors"
            >
              {exporting ? "导出中..." : "导出备份"}
            </button>
            <button
              type="button"
              onClick={deleteKey}
              className="px-3 py-1 rounded text-xs font-semibold border border-red-500/40 text-red-300 hover:bg-red-500/10 transition-colors"
            >
              删除并重置
            </button>
          </div>
          {err && <p className="text-xs text-red-400">{err}</p>}
          {importMsg && <p className="text-xs text-emerald-400">{importMsg}</p>}
        </div>
      ) : (
        <div className="space-y-2">
          <p className="text-xs text-muted-foreground">
            您还没有个人专属密钥。创建后，所有激活的额度将自动累加到这把密钥，无需每次保存新 key。也可以从之前导出的备份文件恢复。
          </p>
          <div className="flex flex-wrap gap-2">
            <button
              type="button"
              onClick={createKey}
              disabled={creating}
              className="px-4 py-1.5 rounded text-xs font-semibold bg-violet-600 hover:bg-violet-500 text-white disabled:opacity-50 transition-colors"
            >
              {creating ? "创建中..." : "创建我的专属密钥"}
            </button>
            <button
              type="button"
              onClick={triggerImport}
              disabled={importing}
              className="px-4 py-1.5 rounded text-xs font-semibold border border-violet-500/40 text-violet-300 hover:bg-violet-500/10 disabled:opacity-50 transition-colors"
            >
              {importing ? "导入中..." : "从备份导入"}
            </button>
          </div>
          {err && <p className="text-xs text-red-400">{err}</p>}
          {importMsg && <p className="text-xs text-emerald-400">{importMsg}</p>}
        </div>
      )}
      {/* 隐藏的文件选择器，由「从备份导入」按钮触发 */}
      <input
        ref={importFileRef}
        type="file"
        accept="application/json,.json"
        onChange={handleImportFile}
        className="hidden"
      />
    </div>
  );
}

// ────────────────────────────────────────────────────────────
// LOW 用户并发设置（per-Discord-account）：单条与批量激活共用同一线程池
// isLowAdmin=true → 必须带 X-Discord-Token，服务端自动按 Discord 账号隔离
// isAdmin=true    → 无需 Discord token，操作全局默认（可选 discord_id 精确定位）
// ────────────────────────────────────────────────────────────
function LowConcurrencyConfig({
  dcToken,
  isLowAdmin,
}: {
  dcToken: string;
  isLowAdmin: boolean;
}) {
  const [config, setConfig] = useState<LowConfigResp | null>(null);
  const [concInput, setConcInput] = useState<number>(3);
  const [savingConc, setSavingConc] = useState(false);
  const [errorMsg, setErrorMsg] = useState<string | null>(null);

  const discordHeaders = (): Record<string, string> =>
    isLowAdmin && dcToken ? { "X-Discord-Token": dcToken } : {};

  const refreshConfig = async () => {
    try {
      const res = await adminFetch("/admin/low-config", {
        headers: discordHeaders(),
      });
      if (!res.ok) {
        const data = await res.json().catch(() => ({}));
        throw new Error(data.detail || `HTTP ${res.status}`);
      }
      const data: LowConfigResp = await res.json();
      setConfig(data);
      setConcInput(data.concurrency);
    } catch (e: any) {
      setErrorMsg(`无法读取并发配置：${e.message}`);
    }
  };

  useEffect(() => {
    refreshConfig();
    // 当 dcToken 变化（LOW 用户登录/注销）时重新拉取对应 Discord 账号的并发
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [dcToken]);

  const saveConcurrency = async () => {
    if (!config) return;
    const v = Math.max(1, Math.min(config.concurrency_max, Math.floor(concInput)));
    setSavingConc(true);
    setErrorMsg(null);
    try {
      const res = await adminFetch("/admin/low-config", {
        method: "PATCH",
        headers: { "Content-Type": "application/json", ...discordHeaders() },
        body: JSON.stringify({ concurrency: v }),
      });
      if (!res.ok) {
        const data = await res.json().catch(() => ({}));
        throw new Error(data.detail || `HTTP ${res.status}`);
      }
      await refreshConfig();
    } catch (e: any) {
      setErrorMsg(`保存并发失败：${e.message}`);
    } finally {
      setSavingConc(false);
    }
  };

  return (
    <div
      className="mb-3 rounded-lg border border-emerald-500/30 bg-emerald-500/[0.03] px-4 py-3"
      data-testid="low-concurrency-config"
    >
      <div className="flex items-center gap-3 flex-wrap" data-testid="low-config-row">
        <Settings2 className="w-4 h-4 text-emerald-400" />
        <span className="text-sm font-semibold text-emerald-400">
          {isLowAdmin ? "我的并发配置：" : "LOW 全局默认并发："}
        </span>
        <input
          type="number"
          min={1}
          max={config?.concurrency_max ?? 50}
          value={concInput}
          onChange={(e) => setConcInput(parseInt(e.target.value || "1", 10))}
          data-testid="low-concurrency-input"
          className="w-20 px-2 py-1 rounded border border-input bg-background text-sm font-mono focus:outline-none focus:ring-2 focus:ring-emerald-500/30"
        />
        <span className="text-xs text-muted-foreground">
          （1 - {config?.concurrency_max ?? 50}；同时作用于
          <span className="text-emerald-300">单条激活</span>与
          <span className="text-emerald-300">批量激活</span>；
          当前 = <span className="text-emerald-400 font-mono">{config?.concurrency ?? "..."}</span>
          {isLowAdmin && config?.discord_id ? (
            <span className="ml-1 text-zinc-500">（Discord {config.discord_id}）</span>
          ) : null}）
        </span>
        <button
          type="button"
          onClick={saveConcurrency}
          disabled={savingConc || !config || concInput === config.concurrency}
          data-testid="low-concurrency-save"
          className="ml-auto px-3 py-1 rounded text-xs font-medium border border-emerald-500/30 text-emerald-400 hover:bg-emerald-500/10 disabled:opacity-40 transition-colors"
        >
          {savingConc ? "保存中..." : "保存并发"}
        </button>
      </div>
      {errorMsg && (
        <div className="mt-2 text-xs text-red-400" data-testid="low-config-error">
          {errorMsg}
        </div>
      )}
    </div>
  );
}

function LowBatchPanel({
  queryClient,
  dcToken,
  dcLoggedIn,
  isLowAdmin,
}: {
  queryClient: ReturnType<typeof useQueryClient>;
  dcToken: string;
  dcLoggedIn: boolean;
  isLowAdmin: boolean;
}) {
  const [open, setOpen] = useState(false);
  const [text, setText] = useState("");
  const [config, setConfig] = useState<LowConfigResp | null>(null);
  const [submitting, setSubmitting] = useState(false);
  const [rows, setRows] = useState<BatchRow[]>([]);
  const [errorMsg, setErrorMsg] = useState<string | null>(null);
  const [now, setNow] = useState<number>(Date.now() / 1000);
  const sourcesRef = useRef<EventSource[]>([]);

  // 解析输入框得到的账号数 + 校验
  const parsed = useMemo(() => parseBatchInput(text), [text]);
  const overLimit = config ? parsed.length > config.batch_max : false;

  // 1 秒一次的本地时钟，用于显示冷却倒计时
  useEffect(() => {
    const id = setInterval(() => setNow(Date.now() / 1000), 1000);
    return () => clearInterval(id);
  }, []);

  const cooldownRemaining = useMemo(() => {
    if (!config) return 0;
    const offset = now - config.server_time;
    const left = config.cooldown_remaining - offset;
    return Math.max(0, Math.round(left));
  }, [config, now]);

  // 拉取配置（首次展开 + 每次提交后）
  // LOW 用户须带 X-Discord-Token，服务端自动按 Discord 账号作用域返回 per-Discord 并发
  const refreshConfig = async () => {
    try {
      const extraHeaders: Record<string, string> =
        isLowAdmin && dcToken ? { "X-Discord-Token": dcToken } : {};
      const res = await adminFetch("/admin/low-config", { headers: extraHeaders });
      if (!res.ok) {
        const data = await res.json().catch(() => ({}));
        throw new Error(data.detail || `HTTP ${res.status}`);
      }
      const data: LowConfigResp = await res.json();
      setConfig(data);
    } catch (e: any) {
      setErrorMsg(`无法读取 LOW 配置：${e.message}`);
    }
  };

  useEffect(() => {
    if (open && !config) refreshConfig();
  }, [open]);  // eslint-disable-line react-hooks/exhaustive-deps

  // 关闭时关闭所有 EventSource
  useEffect(() => {
    return () => {
      sourcesRef.current.forEach((es) => es.close());
      sourcesRef.current = [];
    };
  }, []);

  const submitBatch = async () => {
    if (parsed.length === 0 || overLimit) return;
    setSubmitting(true);
    setErrorMsg(null);
    setRows([]);
    sourcesRef.current.forEach((es) => es.close());
    sourcesRef.current = [];

    // LOW 用户必须先 Discord 登录（用于划分 LOW CF 子池）
    if (isLowAdmin && !dcLoggedIn) {
      setErrorMsg("请先在上方完成 Discord 登录后再批量激活");
      setSubmitting(false);
      return;
    }

    try {
      const body: Record<string, any> = { accounts: parsed };
      if (isLowAdmin && dcToken) body.discord_token = dcToken;
      const res = await adminFetch("/admin/activate-batch", {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify(body),
      });
      if (!res.ok) {
        const data = await res.json().catch(() => ({}));
        throw new Error(data.detail || `HTTP ${res.status}`);
      }
      const data: { started: BatchRow[] } = await res.json();
      const initialRows: BatchRow[] = data.started.map((s: any) => ({
        email: s.email,
        task_id: s.task_id,
        preissued_key: s.preissued_key,
        status: "running",
        lastLog: "等待启动...",
      }));
      setRows(initialRows);

      // 为每个 task 建立独立 SSE 连接读取最后一条日志
      initialRows.forEach((row, idx) => {
        const es = new EventSource(`/admin/activate/${row.task_id}/stream`);
        sourcesRef.current.push(es);
        es.onmessage = (ev) => {
          try {
            const d = JSON.parse(ev.data);
            if (d.type === "log") {
              setRows((prev) => {
                const next = [...prev];
                if (next[idx]) next[idx] = { ...next[idx], lastLog: d.msg };
                return next;
              });
            } else if (d.type === "done") {
              es.close();
              setRows((prev) => {
                const next = [...prev];
                if (next[idx]) {
                  next[idx] = {
                    ...next[idx],
                    status: d.status === "success" ? "success" : "failed",
                    lastLog:
                      d.status === "success"
                        ? "✓ 激活完成"
                        : `✗ ${d.result?.error || d.status || "失败"}`,
                  };
                }
                return next;
              });
              queryClient.invalidateQueries({ queryKey: ["admin-keys"] });
              queryClient.invalidateQueries({ queryKey: ["admin-pending-nc"] });
            }
          } catch {}
        };
        es.onerror = () => {
          es.close();
        };
      });

      await refreshConfig();
    } catch (e: any) {
      setErrorMsg(`批量启动失败：${e.message}`);
    } finally {
      setSubmitting(false);
    }
  };

  const cd_mins = Math.floor(cooldownRemaining / 60);
  const cd_secs = cooldownRemaining % 60;

  return (
    <div className="mb-5 rounded-lg border border-emerald-500/30 bg-emerald-500/[0.03] overflow-hidden">
      <button
        type="button"
        onClick={() => setOpen((v) => !v)}
        data-testid="low-batch-toggle"
        className="w-full flex items-center gap-2 px-4 py-3 hover:bg-emerald-500/[0.06] transition-colors"
      >
        <Layers className="w-4 h-4 text-emerald-400" />
        <span className="text-sm font-semibold text-emerald-400">批量激活（LOW 用户专属）</span>
        <span className="text-xs text-muted-foreground ml-auto">
          {open ? "▼" : "▶"} 单批最多 {config?.batch_max ?? 50} 个 / 每小时 1 次
        </span>
      </button>

      {open && (
        <div className="border-t border-emerald-500/20 px-4 py-4 space-y-4">
          {/* 输入区 */}
          <div>
            <label className="block text-xs text-muted-foreground mb-1.5">
              账号列表（每行一个，<code className="text-emerald-300">email:password</code> 或 <code className="text-emerald-300">email,password</code>，
              单批最多 <span className="text-emerald-400">{config?.batch_max ?? 50}</span> 个）
            </label>
            <textarea
              value={text}
              onChange={(e) => setText(e.target.value)}
              rows={8}
              data-testid="low-batch-textarea"
              placeholder="user1@example.com:pass1&#10;user2@example.com:pass2&#10;..."
              className="w-full px-3 py-2 rounded border border-input bg-background text-xs font-mono focus:outline-none focus:ring-2 focus:ring-emerald-500/30"
            />
            <div className="mt-1 text-xs flex items-center gap-3 flex-wrap">
              <span className={overLimit ? "text-red-400" : "text-muted-foreground"}>
                解析到 <span className="font-mono">{parsed.length}</span> 个账号
                {overLimit && `（超出上限 ${config?.batch_max}）`}
              </span>
              {cooldownRemaining > 0 && (
                <span className="text-amber-400">
                  冷却中：{cd_mins > 0 ? `${cd_mins} 分 ` : ""}{String(cd_secs).padStart(2, "0")} 秒后可再次发起
                </span>
              )}
            </div>
          </div>

          {/* 提交按钮 */}
          <div className="flex items-center gap-2">
            <button
              type="button"
              onClick={submitBatch}
              disabled={submitting || parsed.length === 0 || overLimit || cooldownRemaining > 0}
              data-testid="low-batch-submit"
              className="flex items-center gap-2 px-4 py-2 rounded bg-emerald-500/15 border border-emerald-500/40 text-emerald-400 text-sm font-medium hover:bg-emerald-500/25 disabled:opacity-40 disabled:cursor-not-allowed transition-colors"
            >
              {submitting ? <><Loader2 className="w-4 h-4 animate-spin" />提交中...</> : <><Play className="w-4 h-4" />启动批量激活</>}
            </button>
            {rows.length > 0 && (
              <span className="text-xs text-muted-foreground">
                运行中 {rows.filter((r) => r.status === "running").length} / 完成 {rows.filter((r) => r.status === "success").length} / 失败 {rows.filter((r) => r.status === "failed").length}
              </span>
            )}
          </div>

          {errorMsg && (
            <div className="flex items-start gap-2 p-3 rounded border border-red-500/30 bg-red-500/10 text-xs text-red-400">
              <AlertTriangle className="w-3.5 h-3.5 shrink-0 mt-0.5" />
              <span>{errorMsg}</span>
            </div>
          )}

          {/* 结果列表 */}
          {rows.length > 0 && (
            <div className="rounded border border-border overflow-hidden" data-testid="low-batch-results">
              <table className="w-full text-xs">
                <thead className="bg-muted/30">
                  <tr>
                    <th className="text-left px-3 py-2 font-medium text-muted-foreground w-8">#</th>
                    <th className="text-left px-3 py-2 font-medium text-muted-foreground">账号</th>
                    <th className="text-left px-3 py-2 font-medium text-muted-foreground w-24">状态</th>
                    <th className="text-left px-3 py-2 font-medium text-muted-foreground">最新日志</th>
                  </tr>
                </thead>
                <tbody>
                  {rows.map((r, i) => (
                    <tr key={r.task_id} className="border-t border-border/50">
                      <td className="px-3 py-1.5 font-mono text-muted-foreground">{i + 1}</td>
                      <td className="px-3 py-1.5 font-mono text-foreground">{r.email}</td>
                      <td className="px-3 py-1.5">
                        {r.status === "running" && (
                          <span className="inline-flex items-center gap-1 text-amber-400">
                            <Loader2 className="w-3 h-3 animate-spin" />运行中
                          </span>
                        )}
                        {r.status === "success" && <span className="text-emerald-400">✓ 完成</span>}
                        {r.status === "failed" && <span className="text-red-400">✗ 失败</span>}
                      </td>
                      <td className="px-3 py-1.5 text-muted-foreground truncate max-w-[24rem]">{r.lastLog}</td>
                    </tr>
                  ))}
                </tbody>
              </table>
            </div>
          )}
        </div>
      )}
    </div>
  );
}
