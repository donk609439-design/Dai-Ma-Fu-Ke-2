import { useState, useEffect } from "react";
import { useLocation } from "wouter";
import { Shield, LogOut, Eye, EyeOff, Loader2, UserPlus, RefreshCw } from "lucide-react";
import { getAdminKey, setAdminKey, clearAdminKey, adminFetch, getApiBase, setAdminRole, type AdminRole } from "@/lib/admin-auth";

interface AdminGateProps {
  children: React.ReactNode;
}

export default function AdminGate({ children }: AdminGateProps) {
  const [, navigate] = useLocation();
  const [authed, setAuthed] = useState<boolean | null>(null);
  const [inputKey, setInputKey] = useState("");
  const [show, setShow] = useState(false);
  const [error, setError] = useState("");
  const [loading, setLoading] = useState(false);
  // 是否处于"有存储 key 但验证失败"的重连模式
  const [reconnecting, setReconnecting] = useState(false);

  const doVerify = (stored: string, retries: number, cancelled: { v: boolean }) => {
    const verify = async (r: number): Promise<void> => {
      try {
        const res = await adminFetch("/admin/status");
        if (cancelled.v) return;
        if (res.ok) {
          // 同步身份：admin / low_admin。低权用户走用户面板（由上游路由判定）。
          try {
            const j = await res.clone().json();
            const role: AdminRole = j?.role === "low_admin" ? "low_admin" : "admin";
            setAdminRole(role);
          } catch {
            setAdminRole("admin");
          }
          setReconnecting(false);
          setAuthed(true);
        } else if ((res.status === 503 || res.status === 502) && r > 0) {
          await new Promise((x) => setTimeout(x, 1500));
          if (!cancelled.v) return verify(r - 1);
        } else if (res.status === 401 || res.status === 403) {
          // 明确的认证失败 → key 无效，清除并要求重新输入
          clearAdminKey();
          setReconnecting(false);
          setAuthed(false);
          setError("ADMIN_KEY 已失效，请重新输入");
        } else {
          // 其他错误（500、网关超时等）→ 保留 key，进入重连模式
          setReconnecting(true);
          setAuthed(false);
          setError("服务暂时不可用，请点击「重新连接」");
        }
      } catch {
        if (cancelled.v) return;
        if (r > 0) {
          await new Promise((x) => setTimeout(x, 1500));
          if (!cancelled.v) return verify(r - 1);
        }
        // 网络异常 → 保留 key，进入重连模式
        setReconnecting(true);
        setAuthed(false);
        setError("连接服务器失败，请点击「重新连接」");
      }
    };
    return verify(retries);
  };

  useEffect(() => {
    const stored = getAdminKey();
    if (!stored) {
      setAuthed(false);
      return;
    }
    const cancelled = { v: false };
    doVerify(stored, 3, cancelled);
    return () => { cancelled.v = true; };
  }, []);

  const handleLogin = async (e: React.FormEvent) => {
    e.preventDefault();
    if (!inputKey.trim()) return;
    setLoading(true);
    setError("");

    const base = getApiBase();

    const tryLogin = async (retries: number): Promise<void> => {
      try {
        const res = await fetch(`${base}/admin/status`, {
          headers: { "X-Admin-Key": inputKey.trim() },
        });
        if (res.ok) {
          setAdminKey(inputKey.trim());
          // 同步身份；低权用户登录后由上层路由器跳转至用户面板（/activate）
          try {
            const j = await res.clone().json();
            const role: AdminRole = j?.role === "low_admin" ? "low_admin" : "admin";
            setAdminRole(role);
            if (role === "low_admin") {
              setAuthed(true);
              navigate("/activate");
              return;
            }
          } catch {
            setAdminRole("admin");
          }
          setAuthed(true);
          return;
        }
        if ((res.status === 503 || res.status === 502) && retries > 0) {
          setError("AI 服务正在启动，请稍等...");
          await new Promise((r) => setTimeout(r, 2000));
          return tryLogin(retries - 1);
        }
        if (res.status === 503 || res.status === 502) {
          setError("AI 服务启动超时，请刷新页面重试");
        } else {
          setError("ADMIN_KEY 不正确，请重试");
        }
      } catch {
        if (retries > 0) {
          await new Promise((r) => setTimeout(r, 2000));
          return tryLogin(retries - 1);
        }
        setError("连接服务器失败，请检查 API 服务地址和网络");
      }
    };

    try {
      await tryLogin(10);
    } finally {
      setLoading(false);
    }
  };

  const handleLogout = () => {
    clearAdminKey();
    setReconnecting(false);
    setAuthed(false);
    setInputKey("");
    setError("");
  };

  const handleReconnect = () => {
    const stored = getAdminKey();
    if (!stored) {
      setReconnecting(false);
      return;
    }
    setLoading(true);
    setError("");
    const cancelled = { v: false };
    doVerify(stored, 5, cancelled).finally(() => {
      if (!cancelled.v) setLoading(false);
    });
  };

  if (authed === null) {
    return (
      <div className="flex items-center justify-center h-full min-h-64 text-muted-foreground">
        <Loader2 className="w-5 h-5 animate-spin mr-2" />
        验证中...
      </div>
    );
  }

  // 有存储 key 但暂时连不上 → 重连界面（不要求重新输入 key）
  if (!authed && reconnecting) {
    return (
      <div className="flex items-center justify-center min-h-screen bg-background">
        <div className="w-full max-w-sm mx-4">
          <div className="flex flex-col items-center gap-3 mb-8">
            <div className="flex items-center justify-center w-14 h-14 rounded-2xl bg-yellow-500/10 border border-yellow-500/20">
              <RefreshCw className="w-7 h-7 text-yellow-500" />
            </div>
            <div className="text-center">
              <h1 className="text-xl font-semibold text-foreground">连接中断</h1>
              <p className="text-sm text-muted-foreground mt-1">服务暂时不可用，您的登录状态已保留</p>
            </div>
          </div>

          {error && (
            <p className="text-sm text-red-400 text-center mb-4">{error}</p>
          )}

          <div className="space-y-3">
            <button
              onClick={handleReconnect}
              disabled={loading}
              className="w-full py-2.5 rounded-lg bg-primary text-primary-foreground text-sm font-medium hover:bg-primary/90 disabled:opacity-50 disabled:cursor-not-allowed transition-colors flex items-center justify-center gap-2"
            >
              {loading ? <><Loader2 className="w-4 h-4 animate-spin" />重连中...</> : <><RefreshCw className="w-4 h-4" />重新连接</>}
            </button>

            <button
              type="button"
              onClick={handleLogout}
              className="w-full py-2.5 rounded-lg border border-border bg-transparent text-sm text-muted-foreground hover:text-foreground hover:bg-muted/30 transition-colors"
            >
              切换账号
            </button>
          </div>
        </div>
      </div>
    );
  }

  if (!authed) {
    return (
      <div className="flex items-center justify-center min-h-screen bg-background">
        <div className="w-full max-w-sm mx-4">
          <div className="flex flex-col items-center gap-3 mb-8">
            <div className="flex items-center justify-center w-14 h-14 rounded-2xl bg-primary/10 border border-primary/20">
              <Shield className="w-7 h-7 text-primary" />
            </div>
            <div className="text-center">
              <h1 className="text-xl font-semibold text-foreground">管理员验证</h1>
              <p className="text-sm text-muted-foreground mt-1">请输入 ADMIN_KEY 以访问管理面板</p>
            </div>
          </div>

          <form onSubmit={handleLogin} className="space-y-4">
            <div className="relative">
              <input
                type={show ? "text" : "password"}
                value={inputKey}
                onChange={(e) => setInputKey(e.target.value)}
                placeholder="输入 ADMIN_KEY..."
                autoFocus
                className="w-full px-4 py-3 pr-11 rounded-lg border border-input bg-background text-sm text-foreground placeholder:text-muted-foreground focus:outline-none focus:ring-2 focus:ring-primary/30"
              />
              <button
                type="button"
                onClick={() => setShow(!show)}
                className="absolute right-3 top-1/2 -translate-y-1/2 text-muted-foreground hover:text-foreground"
              >
                {show ? <EyeOff className="w-4 h-4" /> : <Eye className="w-4 h-4" />}
              </button>
            </div>

            {error && (
              <p className="text-sm text-red-400 text-center">{error}</p>
            )}

            <button
              type="submit"
              disabled={loading || !inputKey.trim()}
              className="w-full py-2.5 rounded-lg bg-primary text-primary-foreground text-sm font-medium hover:bg-primary/90 disabled:opacity-50 disabled:cursor-not-allowed transition-colors flex items-center justify-center gap-2"
            >
              {loading ? <><Loader2 className="w-4 h-4 animate-spin" />验证中...</> : "进入管理面板"}
            </button>

            <button
              type="button"
              onClick={() => navigate("/activate")}
              className="w-full py-2.5 rounded-lg border border-border bg-transparent text-sm text-muted-foreground hover:text-foreground hover:bg-muted/30 transition-colors flex items-center justify-center gap-2"
            >
              <UserPlus className="w-4 h-4" />
              账号激活（无需登录）
            </button>
          </form>
        </div>
      </div>
    );
  }

  return (
    <>
      {children}
      <button
        onClick={handleLogout}
        className="fixed bottom-4 right-4 z-50 flex items-center gap-1.5 px-3 py-1.5 rounded-md border border-border bg-background/80 backdrop-blur text-xs text-muted-foreground hover:text-foreground hover:bg-muted/50 transition-colors"
        title="退出登录"
      >
        <LogOut className="w-3.5 h-3.5" />
        退出
      </button>
    </>
  );
}
