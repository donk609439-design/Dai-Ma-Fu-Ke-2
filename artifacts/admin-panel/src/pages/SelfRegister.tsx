import { useState, useEffect, useRef } from "react";
import { Loader2, CheckCircle2, XCircle, Copy, KeyRound, CreditCard, Eye, EyeOff, RotateCcw, Clock, ShieldCheck } from "lucide-react";
import { useToast } from "@/hooks/use-toast";

type Stage = "discord" | "form" | "processing" | "active" | "failed";

// ───── Session persistence ─────
const SR_SESSION_KEY = "sr_dc_auth";
const SR_SESSION_TTL = 30 * 60 * 1000; // 30 minutes

function loadSrSession(): { token: string; tag: string } | null {
  try {
    const raw = sessionStorage.getItem(SR_SESSION_KEY);
    if (!raw) return null;
    const obj = JSON.parse(raw) as { token: string; tag: string; ts: number };
    if (Date.now() - obj.ts > SR_SESSION_TTL) { sessionStorage.removeItem(SR_SESSION_KEY); return null; }
    return { token: obj.token, tag: obj.tag };
  } catch { return null; }
}

function saveSrSession(token: string, tag: string) {
  sessionStorage.setItem(SR_SESSION_KEY, JSON.stringify({ token, tag, ts: Date.now() }));
}

function clearSrSession() {
  sessionStorage.removeItem(SR_SESSION_KEY);
}

// ───── Discord helpers ─────
async function verifyDiscordToken(token: string): Promise<{ valid: boolean; user_tag?: string }> {
  try {
    const res = await fetch(`/key/discord-verify?token=${encodeURIComponent(token)}`);
    if (!res.ok) return { valid: false };
    return res.json();
  } catch {
    return { valid: false };
  }
}

// ───── Registration helpers ─────
async function submitAccount(email: string, password: string, discordToken: string) {
  const res = await fetch(`/key/self-register`, {
    method: "POST",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify({ email, password, discord_token: discordToken }),
  });
  const data = await res.json();
  if (!res.ok) throw new Error(data.error?.message || data.detail || "提交失败，请稍后重试");
  return data as { status: string; message?: string; keys?: string[]; key?: string };
}

async function pollStatus(email: string) {
  const res = await fetch(`/key/self-register-status?email=${encodeURIComponent(email)}`);
  if (!res.ok) return null;
  return res.json() as Promise<{ status: string; message?: string; keys?: string[]; key?: string }>;
}

// ───── Sub-components ─────
function CopyButton({ text }: { text: string }) {
  const { toast } = useToast();
  const [copied, setCopied] = useState(false);
  return (
    <button
      onClick={() => {
        navigator.clipboard.writeText(text);
        setCopied(true);
        toast({ title: "已复制到剪贴板" });
        setTimeout(() => setCopied(false), 2000);
      }}
      className="shrink-0 p-1.5 rounded-lg hover:bg-white/10 text-white/70 hover:text-white transition-colors"
    >
      {copied ? <CheckCircle2 className="w-4 h-4 text-green-400" /> : <Copy className="w-4 h-4" />}
    </button>
  );
}

function KeyCard({ apiKey, active }: { apiKey: string; active: boolean }) {
  return (
    <div className={`rounded-xl border px-4 py-3 space-y-2 ${active ? "bg-green-500/10 border-green-500/30" : "bg-yellow-500/10 border-yellow-500/30"}`}>
      <div className="flex items-center gap-2">
        <KeyRound className={`w-4 h-4 shrink-0 ${active ? "text-green-400" : "text-yellow-400"}`} />
        <span className={`text-xs font-medium ${active ? "text-green-400" : "text-yellow-400"}`}>
          {active ? "API Key（可用）" : "API Key（待激活）"}
        </span>
        {!active && (
          <span className="ml-auto flex items-center gap-1 text-[10px] text-yellow-400/80">
            <Clock className="w-3 h-3" /> 等待凭证确认
          </span>
        )}
      </div>
      <div className="flex items-center gap-2 bg-black/20 rounded-lg px-3 py-2">
        <code className="text-xs font-mono flex-1 break-all text-foreground">{apiKey}</code>
        <CopyButton text={apiKey} />
      </div>
      {!active && (
        <p className="text-[11px] text-yellow-400/70">
          凭证确认后（通常 1~5 分钟）额度自动升为 25 次
        </p>
      )}
    </div>
  );
}

// ───── Main component ─────
export default function SelfRegister() {
  const { toast } = useToast();

  const [stage, setStage] = useState<Stage>(() => (loadSrSession() ? "form" : "discord"));
  const [discordTag, setDiscordTag] = useState(() => loadSrSession()?.tag ?? "");
  const [discordToken, setDiscordToken] = useState(() => loadSrSession()?.token ?? "");
  const [discordError, setDiscordError] = useState("");

  const [email, setEmail] = useState("");
  const [password, setPassword] = useState("");
  const [showPw, setShowPw] = useState(false);

  const [preKey, setPreKey] = useState("");
  const [activeKeys, setActiveKeys] = useState<string[]>([]);
  const [errorMsg, setErrorMsg] = useState("");
  const [dotCount, setDotCount] = useState(1);
  const pollerRef = useRef<ReturnType<typeof setInterval> | null>(null);
  const emailRef = useRef("");

  // ── Handle Discord OAuth callback params ──
  useEffect(() => {
    const sp = new URLSearchParams(window.location.search);
    const token = sp.get("discord_token");
    const tag = sp.get("tag");
    const err = sp.get("discord_error");

    if (token) {
      // Clean URL immediately
      window.history.replaceState({}, "", window.location.pathname);
      // Verify the token is legit before proceeding
      verifyDiscordToken(token).then((r) => {
        if (r.valid) {
          const resolvedTag = tag || r.user_tag || "";
          saveSrSession(token, resolvedTag);
          setDiscordToken(token);
          setDiscordTag(resolvedTag);
          setStage("form");
        } else {
          setDiscordError("验证令牌无效，请重新授权");
        }
      });
    } else if (err) {
      window.history.replaceState({}, "", window.location.pathname);
      const msgs: Record<string, string> = {
        not_member: `您的 Discord 账号（${tag}）不是本服务器成员，请先加入后重试`,
        no_required_role: `您的 Discord 账号（${tag}）没有所需身份组，请确认账号权限后重试`,
        access_denied: "您取消了 Discord 授权",
        token_failed: "Discord Token 交换失败，请重试",
        member_check_failed: "无法验证服务器成员资格，请重试",
        invalid_state: "授权已过期，请重新发起",
        no_token: "Discord 未返回 Token，请重试",
      };
      setDiscordError(msgs[err] || `Discord 错误: ${err}`);
    }
  }, []);

  // ── Processing dot animation ──
  useEffect(() => {
    if (stage !== "processing") return;
    const t = setInterval(() => setDotCount(d => (d % 3) + 1), 600);
    return () => clearInterval(t);
  }, [stage]);

  // ── Status poller ──
  useEffect(() => {
    if (stage !== "processing") {
      if (pollerRef.current) { clearInterval(pollerRef.current); pollerRef.current = null; }
      return;
    }
    pollerRef.current = setInterval(async () => {
      const res = await pollStatus(emailRef.current);
      if (!res) return;
      if (res.status === "active") {
        clearInterval(pollerRef.current!);
        setActiveKeys(res.keys || (preKey ? [preKey] : []));
        setStage("active");
      } else if (res.status === "failed") {
        clearInterval(pollerRef.current!);
        setErrorMsg(res.message || "激活失败，请检查账号信息后重试");
        setStage("failed");
      } else if (res.status === "processing" && res.key && !preKey) {
        setPreKey(res.key);
      }
    }, 6000);
    return () => { if (pollerRef.current) clearInterval(pollerRef.current); };
  }, [stage, preKey]);

  const handleDiscordLogin = () => {
    window.location.href = "/key/discord-auth";
  };

  const handleSubmit = async () => {
    if (!email.includes("@")) { toast({ title: "请输入正确的邮箱", variant: "destructive" }); return; }
    if (password.length < 6) { toast({ title: "密码不能少于6位", variant: "destructive" }); return; }
    emailRef.current = email.trim().toLowerCase();
    try {
      const res = await submitAccount(email.trim(), password, discordToken);
      if (res.status === "active") { setActiveKeys(res.keys || []); setStage("active"); return; }
      if (res.status === "failed") { setErrorMsg(res.message || "激活失败"); setStage("failed"); return; }
      if (res.key) setPreKey(res.key);
      setStage("processing");
    } catch (e: any) {
      setErrorMsg(e.message || "提交失败，请稍后重试");
      setStage("failed");
    }
  };

  const reset = () => {
    clearSrSession();
    setStage("discord");
    setEmail(""); setPassword(""); setPreKey(""); setActiveKeys("" as any); setErrorMsg("");
    setDiscordTag(""); setDiscordToken(""); setDiscordError("");
    setActiveKeys([]);
  };

  return (
    <div className="min-h-[60vh] flex items-start justify-center pt-8">
      <div className="w-full max-w-md space-y-3">
        {/* Header */}
        <div className="text-center mb-6">
          <div className="inline-flex items-center justify-center w-14 h-14 rounded-2xl bg-primary/15 mb-4">
            <CreditCard className="w-7 h-7 text-primary" />
          </div>
          <h1 className="text-2xl font-bold">自助绑卡</h1>
          <p className="text-sm text-muted-foreground mt-2">提交您的账号，系统自动为您完成绑定并签发 API Key</p>
        </div>

        {/* Step indicator */}
        <div className="flex items-center gap-2 mb-4">
          {[
            { label: "Discord 验证", key: "discord" },
            { label: "提交账号", key: "form" },
            { label: "完成", key: "active" },
          ].map((s, i) => {
            const isDone =
              (s.key === "discord" && ["form", "processing", "active"].includes(stage)) ||
              (s.key === "form" && ["processing", "active"].includes(stage)) ||
              (s.key === "active" && stage === "active");
            const isCurrent = stage === s.key || (s.key === "form" && stage === "processing");
            return (
              <div key={s.key} className="flex items-center gap-2 flex-1">
                <div className={`w-6 h-6 rounded-full flex items-center justify-center text-xs font-bold shrink-0 ${isDone ? "bg-green-500 text-white" : isCurrent ? "bg-primary text-primary-foreground" : "bg-muted text-muted-foreground"}`}>
                  {isDone ? <CheckCircle2 className="w-3.5 h-3.5" /> : i + 1}
                </div>
                <span className={`text-xs ${isCurrent ? "text-foreground font-medium" : "text-muted-foreground"}`}>{s.label}</span>
                {i < 2 && <div className={`flex-1 h-px ${isDone ? "bg-green-500/50" : "bg-border"}`} />}
              </div>
            );
          })}
        </div>

        {/* ─── Discord stage ─── */}
        {stage === "discord" && (
          <div className="bg-card border border-border rounded-2xl p-6 shadow-lg space-y-5">
            <div className="flex items-center gap-3">
              <div className="w-10 h-10 rounded-xl bg-[#5865F2]/15 flex items-center justify-center">
                <svg className="w-5 h-5" viewBox="0 0 24 24" fill="#5865F2">
                  <path d="M20.317 4.37a19.791 19.791 0 0 0-4.885-1.515.074.074 0 0 0-.079.037c-.21.375-.444.864-.608 1.25a18.27 18.27 0 0 0-5.487 0 12.64 12.64 0 0 0-.617-1.25.077.077 0 0 0-.079-.037A19.736 19.736 0 0 0 3.677 4.37a.07.07 0 0 0-.032.027C.533 9.046-.32 13.58.099 18.057a.082.082 0 0 0 .031.057 19.9 19.9 0 0 0 5.993 3.03.078.078 0 0 0 .084-.028c.462-.63.874-1.295 1.226-1.994a.076.076 0 0 0-.041-.106 13.107 13.107 0 0 1-1.872-.892.077.077 0 0 1-.008-.128 10.2 10.2 0 0 0 .372-.292.074.074 0 0 1 .077-.01c3.928 1.793 8.18 1.793 12.062 0a.074.074 0 0 1 .078.01c.12.098.246.198.373.292a.077.077 0 0 1-.006.127 12.299 12.299 0 0 1-1.873.892.077.077 0 0 0-.041.107c.36.698.772 1.362 1.225 1.993a.076.076 0 0 0 .084.028 19.839 19.839 0 0 0 6.002-3.03.077.077 0 0 0 .032-.054c.5-5.177-.838-9.674-3.549-13.66a.061.061 0 0 0-.031-.03z" />
                </svg>
              </div>
              <div>
                <p className="font-semibold text-sm">Discord 服务器成员验证</p>
                <p className="text-xs text-muted-foreground">仅限我们 Discord 服务器成员注册</p>
              </div>
            </div>

            {discordError && (
              <div className="flex items-start gap-2 bg-red-500/10 border border-red-500/20 rounded-xl px-4 py-3">
                <XCircle className="w-4 h-4 text-red-400 shrink-0 mt-0.5" />
                <p className="text-sm text-red-400">{discordError}</p>
              </div>
            )}

            <div className="bg-muted/20 rounded-xl px-4 py-3 text-xs text-muted-foreground space-y-1">
              <p>点击下方按钮，授权我们读取您的 Discord 账号信息及服务器成员资格。</p>
              <p className="text-muted-foreground/60">我们仅验证服务器成员资格，不会存储您的 Discord 密码。</p>
            </div>

            <button
              onClick={handleDiscordLogin}
              className="w-full py-3 rounded-xl bg-[#5865F2] text-white text-sm font-semibold hover:bg-[#4752c4] transition-colors flex items-center justify-center gap-2"
            >
              <svg className="w-4 h-4" viewBox="0 0 24 24" fill="currentColor">
                <path d="M20.317 4.37a19.791 19.791 0 0 0-4.885-1.515.074.074 0 0 0-.079.037c-.21.375-.444.864-.608 1.25a18.27 18.27 0 0 0-5.487 0 12.64 12.64 0 0 0-.617-1.25.077.077 0 0 0-.079-.037A19.736 19.736 0 0 0 3.677 4.37a.07.07 0 0 0-.032.027C.533 9.046-.32 13.58.099 18.057a.082.082 0 0 0 .031.057 19.9 19.9 0 0 0 5.993 3.03.078.078 0 0 0 .084-.028c.462-.63.874-1.295 1.226-1.994a.076.076 0 0 0-.041-.106 13.107 13.107 0 0 1-1.872-.892.077.077 0 0 1-.008-.128 10.2 10.2 0 0 0 .372-.292.074.074 0 0 1 .077-.01c3.928 1.793 8.18 1.793 12.062 0a.074.074 0 0 1 .078.01c.12.098.246.198.373.292a.077.077 0 0 1-.006.127 12.299 12.299 0 0 1-1.873.892.077.077 0 0 0-.041.107c.36.698.772 1.362 1.225 1.993a.076.076 0 0 0 .084.028 19.839 19.839 0 0 0 6.002-3.03.077.077 0 0 0 .032-.054c.5-5.177-.838-9.674-3.549-13.66a.061.061 0 0 0-.031-.03z" />
              </svg>
              使用 Discord 登录验证
            </button>
          </div>
        )}

        {/* ─── Form stage ─── */}
        {stage === "form" && (
          <div className="bg-card border border-border rounded-2xl p-6 shadow-lg space-y-4">
            {discordTag && (
              <div className="flex items-center gap-2 bg-[#5865F2]/10 border border-[#5865F2]/20 rounded-xl px-4 py-2.5">
                <ShieldCheck className="w-4 h-4 text-[#5865F2] shrink-0" />
                <span className="text-xs text-[#5865F2]">Discord 验证通过：<span className="font-medium">{discordTag}</span></span>
              </div>
            )}
            <div>
              <label className="block text-sm font-medium mb-1.5">账号邮箱</label>
              <input
                type="email"
                className="w-full bg-background border border-border rounded-xl px-4 py-2.5 text-sm focus:outline-none focus:ring-2 focus:ring-primary/50"
                placeholder="your@email.com"
                value={email}
                onChange={e => setEmail(e.target.value)}
                onKeyDown={e => e.key === "Enter" && handleSubmit()}
              />
            </div>
            <div>
              <label className="block text-sm font-medium mb-1.5">账号密码</label>
              <div className="relative">
                <input
                  type={showPw ? "text" : "password"}
                  className="w-full bg-background border border-border rounded-xl px-4 py-2.5 pr-10 text-sm focus:outline-none focus:ring-2 focus:ring-primary/50"
                  placeholder="请输入密码"
                  value={password}
                  onChange={e => setPassword(e.target.value)}
                  onKeyDown={e => e.key === "Enter" && handleSubmit()}
                />
                <button type="button" onClick={() => setShowPw(v => !v)} className="absolute right-3 top-1/2 -translate-y-1/2 text-muted-foreground hover:text-foreground">
                  {showPw ? <EyeOff className="w-4 h-4" /> : <Eye className="w-4 h-4" />}
                </button>
              </div>
            </div>
            <button
              onClick={handleSubmit}
              disabled={!email || !password}
              className="w-full py-2.5 rounded-xl bg-primary text-primary-foreground text-sm font-semibold hover:bg-primary/90 transition-colors disabled:opacity-40"
            >
              提交绑定
            </button>
            <p className="text-xs text-center text-muted-foreground">提交后立刻获得 Key，凭证确认后（约 1~5 分钟）自动开通额度</p>
          </div>
        )}

        {/* ─── Processing stage ─── */}
        {stage === "processing" && (
          <div className="bg-card border border-border rounded-2xl p-6 shadow-lg space-y-5">
            <div className="flex items-center gap-3">
              <div className="relative flex-shrink-0">
                <div className="absolute inset-0 rounded-full bg-yellow-500/20 animate-ping" />
                <div className="relative w-10 h-10 rounded-full bg-yellow-500/15 flex items-center justify-center">
                  <Loader2 className="w-5 h-5 text-yellow-400 animate-spin" />
                </div>
              </div>
              <div>
                <p className="font-semibold text-sm">等待凭证确认{".".repeat(dotCount)}</p>
                <p className="text-xs text-muted-foreground mt-0.5">{emailRef.current}</p>
              </div>
            </div>
            {preKey && <KeyCard apiKey={preKey} active={false} />}
            {!preKey && (
              <div className="flex items-center gap-2 bg-muted/20 rounded-xl px-4 py-3 text-sm text-muted-foreground">
                <Loader2 className="w-4 h-4 animate-spin shrink-0" />
                <span>正在分配 Key，请稍候…</span>
              </div>
            )}
            <p className="text-xs text-muted-foreground text-center">凭证确认后页面自动更新，无需刷新</p>
          </div>
        )}

        {/* ─── Active stage ─── */}
        {stage === "active" && (
          <div className="bg-card border border-border rounded-2xl p-6 shadow-lg space-y-5">
            <div className="flex items-center gap-3">
              <div className="w-10 h-10 rounded-xl bg-green-500/15 flex items-center justify-center">
                <CheckCircle2 className="w-6 h-6 text-green-400" />
              </div>
              <div>
                <p className="font-semibold text-green-400">绑定成功！</p>
                <p className="text-xs text-muted-foreground">您的 API Key 已开通 25 次额度</p>
              </div>
            </div>
            {activeKeys.length > 0 ? (
              <div className="space-y-2">
                {activeKeys.map((k, i) => <KeyCard key={i} apiKey={k} active={true} />)}
              </div>
            ) : (
              <div className="bg-yellow-500/10 border border-yellow-500/20 rounded-xl px-4 py-3">
                <p className="text-sm text-yellow-400">绑定已完成，请在「用量查询」页面查看您的 Key</p>
              </div>
            )}
            <p className="text-xs text-muted-foreground text-center">请妥善保存您的 API Key</p>
            <button onClick={reset} className="w-full py-2 rounded-xl border border-border text-sm text-muted-foreground hover:text-foreground hover:bg-muted/20 transition-colors flex items-center justify-center gap-2">
              <RotateCcw className="w-3.5 h-3.5" /> 重新提交
            </button>
          </div>
        )}

        {/* ─── Failed stage ─── */}
        {stage === "failed" && (
          <div className="bg-card border border-border rounded-2xl p-6 shadow-lg space-y-5">
            <div className="flex items-center gap-3">
              <div className="w-10 h-10 rounded-xl bg-red-500/15 flex items-center justify-center">
                <XCircle className="w-6 h-6 text-red-400" />
              </div>
              <div>
                <p className="font-semibold text-red-400">绑定失败</p>
                <p className="text-xs text-muted-foreground mt-0.5">{errorMsg}</p>
              </div>
            </div>
            {/* 若 Discord 仍有效，直接返回表单；否则完整重置 */}
            {discordToken ? (
              <button
                onClick={() => { setEmail(""); setPassword(""); setPreKey(""); setErrorMsg(""); setStage("form"); }}
                className="w-full py-2.5 rounded-xl bg-primary text-primary-foreground text-sm font-semibold hover:bg-primary/90 transition-colors flex items-center justify-center gap-2"
              >
                <RotateCcw className="w-3.5 h-3.5" /> 重新填写（无需重新验证 Discord）
              </button>
            ) : (
              <button onClick={reset} className="w-full py-2.5 rounded-xl bg-primary text-primary-foreground text-sm font-semibold hover:bg-primary/90 transition-colors flex items-center justify-center gap-2">
                <RotateCcw className="w-3.5 h-3.5" /> 重新填写
              </button>
            )}
          </div>
        )}
      </div>
    </div>
  );
}
