import { useState } from "react";
import { useDiscordAuth } from "@/hooks/useDiscordAuth";
import { useToast } from "@/hooks/use-toast";
import { LogIn, LogOut, Send, HeartHandshake, CheckCircle2 } from "lucide-react";

export default function Donate() {
  const { dcToken, userTag, isLoggedIn, login, logout } = useDiscordAuth("donate");
  const { toast } = useToast();

  const [email, setEmail] = useState("");
  const [password, setPassword] = useState("");
  const [submitting, setSubmitting] = useState(false);
  const [submitted, setSubmitted] = useState(false);

  async function handleSubmit(e: React.FormEvent) {
    e.preventDefault();
    if (!dcToken) return;
    if (!email.trim() || !password.trim()) {
      toast({ title: "请填写完整信息", variant: "destructive" });
      return;
    }
    setSubmitting(true);
    try {
      const res = await fetch("/key/donate-jb-account", {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({
          discord_token: dcToken,
          email: email.trim(),
          password: password.trim(),
        }),
      });
      const data = await res.json();
      if (!res.ok) throw new Error(data.detail || "提交失败");
      setSubmitted(true);
      setEmail("");
      setPassword("");
    } catch (err: any) {
      toast({ title: "提交失败", description: err.message, variant: "destructive" });
    } finally {
      setSubmitting(false);
    }
  }

  return (
    <div className="max-w-lg mx-auto py-8 px-4">
      <div className="flex items-center gap-3 mb-6">
        <div className="flex items-center justify-center w-10 h-10 rounded-xl bg-pink-100 dark:bg-pink-900/30">
          <HeartHandshake className="w-5 h-5 text-pink-600 dark:text-pink-400" />
        </div>
        <div>
          <h1 className="text-lg font-semibold">捐号助力</h1>
          <p className="text-sm text-muted-foreground">捐献 R一串账号，审核通过后获得 10 圣人点数</p>
        </div>
      </div>

      {!isLoggedIn ? (
        <div className="rounded-xl border bg-card p-8 text-center space-y-4">
          <p className="text-sm text-muted-foreground">请先通过 Discord 登录，以便关联您的账号</p>
          <button
            onClick={login}
            className="inline-flex items-center gap-2 px-4 py-2 rounded-lg bg-indigo-600 text-white text-sm font-medium hover:bg-indigo-700 transition-colors"
          >
            <LogIn className="w-4 h-4" />
            Discord 登录
          </button>
        </div>
      ) : submitted ? (
        <div className="rounded-xl border bg-card p-8 text-center space-y-3">
          <CheckCircle2 className="w-12 h-12 text-green-500 mx-auto" />
          <p className="font-medium">提交成功！</p>
          <p className="text-sm text-muted-foreground">管理员审核通过后，将自动向您的账号奖励 10 圣人点数。</p>
          <button
            onClick={() => setSubmitted(false)}
            className="text-sm text-primary hover:underline"
          >
            继续捐献
          </button>
        </div>
      ) : (
        <div className="rounded-xl border bg-card overflow-hidden">
          <div className="flex items-center justify-between px-4 py-3 border-b bg-muted/40">
            <span className="text-sm font-medium">已登录：{userTag}</span>
            <button
              onClick={logout}
              className="inline-flex items-center gap-1.5 text-xs text-muted-foreground hover:text-foreground transition-colors"
            >
              <LogOut className="w-3.5 h-3.5" />
              退出
            </button>
          </div>

          <div className="p-5 space-y-4">
            <div className="rounded-lg bg-amber-50 dark:bg-amber-900/20 border border-amber-200 dark:border-amber-800 p-4 space-y-1.5">
              <p className="text-sm font-semibold text-amber-800 dark:text-amber-200">捐号要求</p>
              <p className="text-sm text-amber-700 dark:text-amber-300">1. 必须是绑了卡的R一串账号</p>
              <p className="text-sm text-amber-700 dark:text-amber-300">2. 余额需要大于20刀</p>
            </div>

            <form onSubmit={handleSubmit} className="space-y-3">
              <div>
                <label className="block text-sm font-medium mb-1">邮箱</label>
                <input
                  type="email"
                  value={email}
                  onChange={(e) => setEmail(e.target.value)}
                  placeholder="example@example.com"
                  required
                  className="w-full px-3 py-2 rounded-md border bg-background text-sm focus:outline-none focus:ring-2 focus:ring-primary/30"
                />
              </div>
              <div>
                <label className="block text-sm font-medium mb-1">密码</label>
                <input
                  type="text"
                  value={password}
                  onChange={(e) => setPassword(e.target.value)}
                  placeholder="账号密码"
                  required
                  className="w-full px-3 py-2 rounded-md border bg-background text-sm focus:outline-none focus:ring-2 focus:ring-primary/30"
                />
              </div>
              <button
                type="submit"
                disabled={submitting}
                className="w-full flex items-center justify-center gap-2 px-4 py-2.5 rounded-lg bg-primary text-primary-foreground text-sm font-medium hover:bg-primary/90 transition-colors disabled:opacity-50"
              >
                <Send className="w-4 h-4" />
                {submitting ? "提交中…" : "提交捐号"}
              </button>
            </form>
          </div>
        </div>
      )}
    </div>
  );
}
