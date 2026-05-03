import { useState, useEffect, useCallback } from "react";
import {
  Loader2,
  CheckCircle2,
  XCircle,
  Copy,
  KeyRound,
  UserCircle,
  Sparkles,
  ShieldCheck,
  RefreshCcw,
  Gift,
  LogOut,
} from "lucide-react";
import { useToast } from "@/hooks/use-toast";
import { useDiscordAuth } from "@/hooks/useDiscordAuth";

type Status = {
  has_key: boolean;
  api_key?: string;
  usage_limit?: number;
  usage_count?: number;
  claimed_today: number;
  daily_quota: number;
  user_tag?: string;
};

async function fetchStatus(token: string): Promise<Status> {
  const r = await fetch(`/key/personal-center?discord_token=${encodeURIComponent(token)}`);
  if (!r.ok) {
    const d = await r.json().catch(() => ({}));
    throw new Error(d.detail || `HTTP ${r.status}`);
  }
  return r.json();
}

async function postCreate(token: string) {
  const r = await fetch(`/key/personal-center/create`, {
    method: "POST",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify({ discord_token: token }),
  });
  const d = await r.json().catch(() => ({}));
  if (!r.ok) throw new Error(d.detail || `HTTP ${r.status}`);
  return d;
}

async function postClaim(token: string) {
  const r = await fetch(`/key/personal-center/claim`, {
    method: "POST",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify({ discord_token: token }),
  });
  const d = await r.json().catch(() => ({}));
  if (!r.ok) throw new Error(d.detail || `HTTP ${r.status}`);
  return d;
}

function CopyBtn({ text }: { text: string }) {
  const { toast } = useToast();
  const [copied, setCopied] = useState(false);
  return (
    <button
      onClick={() => {
        navigator.clipboard.writeText(text);
        setCopied(true);
        toast({ title: "已复制到剪贴板" });
        setTimeout(() => setCopied(false), 1800);
      }}
      className="shrink-0 p-2 rounded-lg hover:bg-white/10 text-white/70 hover:text-white transition-colors"
    >
      {copied ? (
        <CheckCircle2 className="w-4 h-4 text-green-400" />
      ) : (
        <Copy className="w-4 h-4" />
      )}
    </button>
  );
}

const DC_ERR_MSGS: Record<string, string> = {
  not_member: "您的 Discord 账号不是本服务器成员，请先加入服务器后重试",
  no_required_role: "您的 Discord 账号没有所需身份组，请先获取身份组后重试",
  access_denied: "您取消了 Discord 授权",
  token_failed: "Discord Token 交换失败，请重试",
  user_failed: "获取 Discord 用户信息失败，请重试",
  member_check_failed: "无法验证服务器成员资格，请重试",
  invalid_state: "授权已过期，请重新发起",
  no_token: "Discord 未返回 Token，请重试",
};

export default function PersonalCenter() {
  const { toast } = useToast();
  // 在 hook 清理 URL 参数前先抢读 discord_error
  const [discordError] = useState<string>(() => {
    if (typeof window === "undefined") return "";
    const sp = new URLSearchParams(window.location.search);
    const err = sp.get("discord_error") || "";
    const tag = sp.get("tag") || "";
    if (!err) return "";
    const base = DC_ERR_MSGS[err] || `Discord 错误: ${err}`;
    return tag ? `${base}（${decodeURIComponent(tag)}）` : base;
  });
  const { dcToken, userTag, isLoggedIn, login, logout } = useDiscordAuth("personal-center", "register");

  const [status, setStatus] = useState<Status | null>(null);
  const [loading, setLoading] = useState(false);
  const [creating, setCreating] = useState(false);
  const [claiming, setClaiming] = useState(false);

  const refresh = useCallback(async (silent = false) => {
    if (!dcToken) return;
    if (!silent) setLoading(true);
    try {
      const s = await fetchStatus(dcToken);
      setStatus(s);
    } catch (e: any) {
      const msg = e.message || "";
      if (msg.includes("Discord 验证")) {
        logout();
        toast({ title: "登录已过期", description: "请重新使用 Discord 登录", variant: "destructive" });
      } else if (!silent) {
        toast({ title: "加载失败", description: msg, variant: "destructive" });
      }
    } finally {
      if (!silent) setLoading(false);
    }
  }, [dcToken, logout, toast]);

  useEffect(() => {
    if (dcToken) refresh();
  }, [dcToken, refresh]);

  const handleCreate = async () => {
    if (!dcToken) return;
    setCreating(true);
    try {
      await postCreate(dcToken);
      toast({ title: "创建成功" });
      await refresh(true);
    } catch (e: any) {
      toast({ title: "创建失败", description: e.message, variant: "destructive" });
    } finally {
      setCreating(false);
    }
  };

  const handleClaim = async () => {
    if (!dcToken) return;
    setClaiming(true);
    try {
      const d = await postClaim(dcToken);
      toast({ title: `签到成功，+${d.daily_quota} 额度已到账` });
      await refresh(true);
    } catch (e: any) {
      toast({ title: "签到失败", description: e.message, variant: "destructive" });
    } finally {
      setClaiming(false);
    }
  };

  // ─── Discord 未登录 ───
  if (!isLoggedIn) {
    return (
      <div className="min-h-[60vh] flex items-start justify-center pt-8">
        <div className="w-full max-w-md space-y-3">
          <div className="text-center mb-6">
            <div className="inline-flex items-center justify-center w-14 h-14 rounded-2xl bg-primary/15 mb-4">
              <UserCircle className="w-7 h-7 text-primary" />
            </div>
            <h1 className="text-2xl font-bold">个人中心</h1>
            <p className="text-sm text-muted-foreground mt-2">
              专属个人 Key + 每日 40 次免费额度
            </p>
          </div>

          <div className="bg-card border border-border rounded-2xl p-6 shadow-lg space-y-5">
            <div className="flex items-center gap-3">
              <div className="w-10 h-10 rounded-xl bg-[#5865F2]/15 flex items-center justify-center">
                <svg className="w-5 h-5" viewBox="0 0 24 24" fill="#5865F2">
                  <path d="M20.317 4.37a19.791 19.791 0 0 0-4.885-1.515.074.074 0 0 0-.079.037c-.21.375-.444.864-.608 1.25a18.27 18.27 0 0 0-5.487 0 12.64 12.64 0 0 0-.617-1.25.077.077 0 0 0-.079-.037A19.736 19.736 0 0 0 3.677 4.37a.07.07 0 0 0-.032.027C.533 9.046-.32 13.58.099 18.057a.082.082 0 0 0 .031.057 19.9 19.9 0 0 0 5.993 3.03.078.078 0 0 0 .084-.028c.462-.63.874-1.295 1.226-1.994a.076.076 0 0 0-.041-.106 13.107 13.107 0 0 1-1.872-.892.077.077 0 0 1-.008-.128 10.2 10.2 0 0 0 .372-.292.074.074 0 0 1 .077-.01c3.928 1.793 8.18 1.793 12.062 0a.074.074 0 0 1 .078.01c.12.098.246.198.373.292a.077.077 0 0 1-.006.127 12.299 12.299 0 0 1-1.873.892.077.077 0 0 0-.041.107c.36.698.772 1.362 1.225 1.993a.076.076 0 0 0 .084.028 19.839 19.839 0 0 0 6.002-3.03.077.077 0 0 0 .032-.054c.5-5.177-.838-9.674-3.549-13.66a.061.061 0 0 0-.031-.03z" />
                </svg>
              </div>
              <div>
                <p className="font-semibold text-sm">Discord 服务器成员验证</p>
                <p className="text-xs text-muted-foreground">仅限服务器成员使用</p>
              </div>
            </div>

            {discordError && (
              <div className="flex items-start gap-2 bg-red-500/10 border border-red-500/20 rounded-xl px-4 py-3">
                <XCircle className="w-4 h-4 text-red-400 shrink-0 mt-0.5" />
                <p className="text-sm text-red-400">{discordError}</p>
              </div>
            )}

            <div className="bg-muted/20 rounded-xl px-4 py-3 text-xs text-muted-foreground space-y-1">
              <p>登录后您可以：</p>
              <p>· 创建 1 个专属个人 API Key（每账号唯一）</p>
              <p>· 每天签到一次，一次性领取 40 次调用额度</p>
              <p className="text-amber-400/80">· 仅限拥有指定身份组的服务器成员使用</p>
            </div>

            <button
              onClick={login}
              className="w-full py-3 rounded-xl bg-[#5865F2] text-white text-sm font-semibold hover:bg-[#4752c4] transition-colors flex items-center justify-center gap-2"
            >
              使用 Discord 登录验证
            </button>
          </div>
        </div>
      </div>
    );
  }

  // ─── 已登录 ───
  const claimedToday = (status?.claimed_today ?? 0) > 0;
  const claimDisabled = !status?.has_key || claiming || claimedToday;

  return (
    <div className="min-h-[60vh] flex items-start justify-center pt-8 pb-12">
      <div className="w-full max-w-xl space-y-4">
        <div className="text-center mb-2">
          <div className="inline-flex items-center justify-center w-14 h-14 rounded-2xl bg-primary/15 mb-3">
            <UserCircle className="w-7 h-7 text-primary" />
          </div>
          <h1 className="text-2xl font-bold">个人中心</h1>
          <p className="text-sm text-muted-foreground mt-2">
            管理您的专属 Key 和每日额度
          </p>
        </div>

        <div className="flex items-center gap-2 bg-[#5865F2]/10 border border-[#5865F2]/20 rounded-xl px-4 py-2.5">
          <ShieldCheck className="w-4 h-4 text-[#5865F2] shrink-0" />
          <span className="text-xs text-[#5865F2] flex-1 truncate">
            Discord 验证通过：<span className="font-medium">{userTag || "-"}</span>
          </span>
          <button
            onClick={() => refresh()}
            disabled={loading}
            className="text-[11px] text-[#5865F2] hover:underline flex items-center gap-1 disabled:opacity-50"
          >
            <RefreshCcw className={`w-3 h-3 ${loading ? "animate-spin" : ""}`} /> 刷新
          </button>
          <button
            onClick={logout}
            className="text-[11px] text-[#5865F2] hover:underline flex items-center gap-1"
          >
            <LogOut className="w-3 h-3" /> 退出
          </button>
        </div>

        {loading && !status && (
          <div className="bg-card border border-border rounded-2xl p-8 flex items-center justify-center">
            <Loader2 className="w-5 h-5 animate-spin text-muted-foreground" />
          </div>
        )}

        {status && !status.has_key && (
          <div className="bg-card border border-border rounded-2xl p-6 shadow-lg space-y-4">
            <div className="flex items-center gap-3">
              <div className="w-10 h-10 rounded-xl bg-yellow-500/15 flex items-center justify-center">
                <KeyRound className="w-5 h-5 text-yellow-400" />
              </div>
              <div>
                <p className="font-semibold text-sm">您还没有个人 Key</p>
                <p className="text-xs text-muted-foreground mt-0.5">
                  每个 Discord 账号仅可创建 1 个专属 Key
                </p>
              </div>
            </div>
            <button
              onClick={handleCreate}
              disabled={creating}
              className="w-full py-2.5 rounded-xl bg-primary text-primary-foreground text-sm font-semibold hover:bg-primary/90 transition-colors disabled:opacity-50 flex items-center justify-center gap-2"
            >
              {creating ? <Loader2 className="w-4 h-4 animate-spin" /> : <Sparkles className="w-4 h-4" />}
              {creating ? "创建中…" : "创建我的专属 Key"}
            </button>
          </div>
        )}

        {status?.has_key && (
          <>
            <div className="bg-card border border-border rounded-2xl p-6 shadow-lg space-y-4">
              <div className="flex items-center gap-2">
                <KeyRound className="w-4 h-4 text-green-400" />
                <span className="text-xs font-medium text-green-400">
                  您的专属 API Key
                </span>
              </div>
              <div className="flex items-center gap-2 bg-black/20 rounded-lg px-3 py-2">
                <code className="text-xs font-mono flex-1 break-all text-foreground">
                  {status.api_key}
                </code>
                <CopyBtn text={status.api_key || ""} />
              </div>
              <div className="grid grid-cols-2 gap-3">
                <div className="bg-muted/30 rounded-xl px-3 py-3">
                  <p className="text-[11px] text-muted-foreground mb-1">总额度</p>
                  <p className="text-lg font-bold">{status.usage_limit ?? 0}</p>
                </div>
                <div className="bg-muted/30 rounded-xl px-3 py-3">
                  <p className="text-[11px] text-muted-foreground mb-1">已使用</p>
                  <p className="text-lg font-bold">{status.usage_count ?? 0}</p>
                </div>
              </div>
            </div>

            <div className="bg-card border border-border rounded-2xl p-6 shadow-lg space-y-4">
              <div className="flex items-center gap-3">
                <div className="w-10 h-10 rounded-xl bg-primary/15 flex items-center justify-center">
                  <Gift className="w-5 h-5 text-primary" />
                </div>
                <div className="flex-1">
                  <p className="font-semibold text-sm">每日签到</p>
                  <p className="text-xs text-muted-foreground mt-0.5">
                    {claimedToday
                      ? `今日已签到（+${status.daily_quota} 额度已发放）`
                      : `每天可签到一次，一次性发放 ${status.daily_quota} 次调用额度`}
                  </p>
                </div>
              </div>

              <button
                onClick={handleClaim}
                disabled={claimDisabled}
                className="w-full py-2.5 rounded-xl bg-primary text-primary-foreground text-sm font-semibold hover:bg-primary/90 transition-colors disabled:opacity-50 flex items-center justify-center gap-2"
              >
                {claiming ? (
                  <Loader2 className="w-4 h-4 animate-spin" />
                ) : (
                  <Sparkles className="w-4 h-4" />
                )}
                {claimedToday
                  ? "今日已签到"
                  : claiming
                    ? "签到中…"
                    : `签到领取 +${status.daily_quota} 额度`}
              </button>

              <p className="text-[11px] text-muted-foreground text-center leading-relaxed">
                每天签到一次，一次性发放 {status.daily_quota} 次调用额度。
                <br />
                次日 0 点（UTC）自动重置。
              </p>
            </div>
          </>
        )}
      </div>
    </div>
  );
}
