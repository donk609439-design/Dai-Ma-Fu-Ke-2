import { Switch, Route, Router as WouterRouter, Link, useLocation } from "wouter";
import { QueryClient, QueryClientProvider } from "@tanstack/react-query";
import { Toaster } from "@/components/ui/toaster";
import { TooltipProvider } from "@/components/ui/tooltip";
import Dashboard from "@/pages/Dashboard";
import Accounts from "@/pages/Accounts";
import ApiKeys from "@/pages/ApiKeys";
import Models from "@/pages/Models";
import Docs from "@/pages/Docs";
import Stats from "@/pages/Stats";
import Activate from "@/pages/Activate";
import KeyUsage from "@/pages/KeyUsage";
import Logs from "@/pages/Logs";
import Lottery from "@/pages/Lottery";
import Backpack from "@/pages/Backpack";
import Prizes from "@/pages/Prizes";
import Partners from "@/pages/Partners";
import ProxyPool from "@/pages/ProxyPool";
import LowCfPool from "@/pages/LowCfPool";
import LowCfPoolAdmin from "@/pages/LowCfPoolAdmin";
import SelfRegister from "@/pages/SelfRegister";
import Donate from "@/pages/Donate";
import DonatedAccounts from "@/pages/DonatedAccounts";
import PendingQueue from "@/pages/PendingQueue";
import AdminGate from "@/components/AdminGate";
import { getAdminKey, useAdminRole } from "@/lib/admin-auth";
import { LayoutDashboard, Users, Key, Cpu, BookOpen, Zap, BarChart3, UserPlus, Menu, X, Search, ScrollText, Ticket, Package, Gift, Handshake, CreditCard, HeartHandshake, ShieldCheck, Globe, Hourglass } from "lucide-react";
import { cn } from "@/lib/utils";
import { useState, useEffect } from "react";

const queryClient = new QueryClient({
  defaultOptions: {
    queries: {
      retry: (failureCount, error: any) => {
        // 503/502 自动重试（服务启动中），其余错误最多重试 1 次
        const status = error?.status ?? error?.response?.status;
        if (status === 503 || status === 502) return failureCount < 3;
        return failureCount < 1;
      },
      retryDelay: (attempt) =>
        Math.min(1000 * 2 ** attempt + Math.random() * 1000, 15000),
      staleTime: 30_000,        // 30s 内不重新获取
      gcTime: 5 * 60_000,      // 5min 缓存保留
      refetchOnWindowFocus: false,
      refetchOnReconnect: true,
    },
  },
});

const GUEST_PATHS = ["/activate", "/self-register", "/my-key", "/lottery", "/backpack", "/donate"];

const adminNavItems = [
  { path: "/", label: "控制台", icon: LayoutDashboard },
  { path: "/accounts", label: "账户管理", icon: Users },
  { path: "/activate", label: "账号激活", icon: UserPlus },
  { path: "/pending-queue", label: "排队记录", icon: Hourglass },
  { path: "/self-register", label: "自助绑卡", icon: CreditCard },
  { path: "/keys", label: "API 密钥", icon: Key },
  { path: "/models", label: "模型配置", icon: Cpu },
  { path: "/stats", label: "用量统计", icon: BarChart3 },
  { path: "/logs", label: "调用日志", icon: ScrollText },
  { path: "/docs", label: "接口文档", icon: BookOpen },
  { path: "/prizes", label: "奖品管理", icon: Gift },
  { path: "/partners", label: "合作伙伴", icon: Handshake },
  { path: "/donated-accounts", label: "后备隐藏能源", icon: ShieldCheck },
  { path: "/proxy-pool", label: "CF代理池", icon: Globe },
  { path: "/proxy-pool/low-users", label: "  └ LOW 用户池", icon: Globe },
  { path: "/my-key", label: "用量查询", icon: Search },
  { path: "/lottery", label: "抽奖机", icon: Ticket },
  { path: "/backpack", label: "我的背包", icon: Package },
  { path: "/donate", label: "捐号助力", icon: HeartHandshake },
];

const guestNavItems = [
  { path: "/activate", label: "账号激活", icon: UserPlus },
  { path: "/self-register", label: "自助绑卡", icon: CreditCard },
  { path: "/my-key", label: "用量查询", icon: Search },
  { path: "/lottery", label: "抽奖机", icon: Ticket },
  { path: "/backpack", label: "我的背包", icon: Package },
  { path: "/donate", label: "捐号助力", icon: HeartHandshake },
];

// 次级管理员（low_admin）：用户面板 + 专属 CF 池入口
const lowAdminNavItems = [
  { path: "/activate", label: "账号激活", icon: UserPlus },
  { path: "/self-register", label: "自助绑卡", icon: CreditCard },
  { path: "/my-key", label: "用量查询", icon: Search },
  { path: "/lottery", label: "抽奖机", icon: Ticket },
  { path: "/backpack", label: "我的背包", icon: Package },
  { path: "/donate", label: "捐号助力", icon: HeartHandshake },
  { path: "/my-cf-pool", label: "我的 CF 池", icon: Globe },
  { path: "/logs", label: "调用日志", icon: ScrollText },
];

type NavMode = "admin" | "guest" | "low_admin";

function NavLinks({ onNavigate, mode }: { onNavigate?: () => void; mode: NavMode }) {
  const [location] = useLocation();
  const items =
    mode === "admin" ? adminNavItems
    : mode === "low_admin" ? lowAdminNavItems
    : guestNavItems;
  return (
    <nav className="flex-1 px-3 py-4 space-y-1">
      {items.map(({ path, label, icon: Icon }) => {
        const active = location === path;
        return (
          <Link key={path} href={path}>
            <span
              onClick={onNavigate}
              className={cn(
                "group flex items-center gap-3 px-3 py-2.5 rounded-xl text-sm font-medium transition-all duration-200 cursor-pointer",
                active
                  ? "bg-primary text-primary-foreground shadow-sm shadow-orange-200/70"
                  : "text-sidebar-foreground hover:bg-sidebar-accent hover:text-sidebar-accent-foreground hover:translate-x-0.5"
              )}
            >
              <Icon className={cn("w-4 h-4 shrink-0 transition-transform", active ? "scale-105" : "group-hover:scale-105")} />
              {label}
            </span>
          </Link>
        );
      })}
    </nav>
  );
}

function Sidebar({ mode }: { mode: NavMode }) {
  const subtitle = mode === "admin" ? "管理控制台" : "用户中心";
  return (
    <aside className="hidden md:flex flex-col w-60 h-full bg-sidebar/85 backdrop-blur-xl border-r border-sidebar-border shrink-0 overflow-y-auto">
      <div className="flex items-center gap-3 px-5 py-5 border-b border-sidebar-border">
        <div className="citrus-gradient flex items-center justify-center w-9 h-9 rounded-2xl shadow-lg shadow-orange-200/70">
          <Zap className="w-4.5 h-4.5 text-white" />
        </div>
        <div>
          <p className="text-sm font-bold text-sidebar-foreground leading-tight tracking-tight">JetBrains AI</p>
          <p className="text-xs text-muted-foreground leading-tight">{subtitle}</p>
        </div>
      </div>
      <NavLinks mode={mode} />
      <div className="px-4 py-3 border-t border-sidebar-border">
        <p className="text-xs text-muted-foreground">🍊 v3.0.0 · Citrus UI</p>
      </div>
    </aside>
  );
}

function MobileHeader({ onMenuOpen }: { onMenuOpen: () => void }) {
  return (
    <header className="md:hidden fixed top-0 left-0 right-0 z-30 flex items-center justify-between px-4 h-14 bg-sidebar/90 backdrop-blur-xl border-b border-sidebar-border">
      <div className="flex items-center gap-2.5">
        <div className="citrus-gradient flex items-center justify-center w-7 h-7 rounded-xl shadow-md shadow-orange-200/70">
          <Zap className="w-3.5 h-3.5 text-white" />
        </div>
        <p className="text-sm font-semibold text-sidebar-foreground">JetBrains AI 管理控制台</p>
      </div>
      <button
        onClick={onMenuOpen}
        className="p-2 rounded-md text-sidebar-foreground hover:bg-sidebar-accent transition-colors"
        aria-label="打开菜单"
      >
        <Menu className="w-5 h-5" />
      </button>
    </header>
  );
}

function MobileDrawer({ open, onClose, mode }: { open: boolean; onClose: () => void; mode: NavMode }) {
  useEffect(() => {
    if (open) {
      document.body.style.overflow = "hidden";
    } else {
      document.body.style.overflow = "";
    }
    return () => { document.body.style.overflow = ""; };
  }, [open]);

  return (
    <>
      {open && (
        <div
          className="md:hidden fixed inset-0 z-40 bg-black/50"
          onClick={onClose}
        />
      )}
      <div
        className={cn(
          "md:hidden fixed top-0 left-0 bottom-0 z-50 w-64 bg-sidebar/95 backdrop-blur-xl border-r border-sidebar-border flex flex-col transition-transform duration-300",
          open ? "translate-x-0" : "-translate-x-full"
        )}
      >
        <div className="flex items-center justify-between px-5 py-4 border-b border-sidebar-border">
          <div className="flex items-center gap-2.5">
            <div className="citrus-gradient flex items-center justify-center w-7 h-7 rounded-xl shadow-md shadow-orange-200/70">
              <Zap className="w-3.5 h-3.5 text-white" />
            </div>
            <p className="text-sm font-semibold text-sidebar-foreground">JetBrains AI</p>
          </div>
          <button
            onClick={onClose}
            className="p-1.5 rounded-md text-sidebar-foreground hover:bg-sidebar-accent transition-colors"
          >
            <X className="w-4 h-4" />
          </button>
        </div>
        <NavLinks onNavigate={onClose} mode={mode} />
        <div className="px-4 py-3 border-t border-sidebar-border">
          <p className="text-xs text-muted-foreground">v3.0.0 · MIT License</p>
        </div>
      </div>
    </>
  );
}

function Layout({ children, mode }: { children: React.ReactNode; mode: NavMode }) {
  const [mobileOpen, setMobileOpen] = useState(false);
  return (
    <div className="app-shell h-screen flex overflow-hidden">
      <Sidebar mode={mode} />
      <MobileDrawer open={mobileOpen} onClose={() => setMobileOpen(false)} mode={mode} />
      <div className="flex-1 flex flex-col min-w-0 overflow-hidden">
        <MobileHeader onMenuOpen={() => setMobileOpen(true)} />
        <main className="flex-1 overflow-y-auto p-4 md:p-7 pt-16 md:pt-7">
          {children}
        </main>
      </div>
    </div>
  );
}

function FullscreenLayout({ children, mode }: { children: React.ReactNode; mode: NavMode }) {
  const [sidebarOpen, setSidebarOpen] = useState(false);

  useEffect(() => {
    document.body.style.overflow = sidebarOpen ? "hidden" : "";
    return () => { document.body.style.overflow = ""; };
  }, [sidebarOpen]);

  return (
    <div className="min-h-screen w-full relative">
      {/* 菜单开关按钮 */}
      <button
        onClick={() => setSidebarOpen((v) => !v)}
        className="fixed top-4 left-4 z-50 flex items-center gap-1.5 px-3 py-2 rounded-xl bg-white/85 backdrop-blur-sm shadow-md border border-orange-200 hover:bg-white transition-colors"
        style={{ color: "#92400e" }}
      >
        {sidebarOpen ? <X className="w-4 h-4" /> : <Menu className="w-4 h-4" />}
        <span className="text-xs font-medium">{sidebarOpen ? "收起" : "菜单"}</span>
      </button>

      {/* 半透明遮罩 */}
      {sidebarOpen && (
        <div
          className="fixed inset-0 bg-black/30 z-40 backdrop-blur-sm"
          onClick={() => setSidebarOpen(false)}
        />
      )}

      {/* 抽屉菜单（全平台通用，不依赖 Sidebar 的 hidden md:flex） */}
      <div
        className="fixed left-0 top-0 h-full z-50 flex flex-col bg-sidebar/95 backdrop-blur-xl border-r border-sidebar-border transition-transform duration-300 ease-in-out"
        style={{
          width: "min(256px, 80vw)",
          transform: sidebarOpen ? "translateX(0)" : "translateX(-100%)",
        }}
      >
        <div className="flex items-center justify-between px-5 py-4 border-b border-sidebar-border shrink-0">
          <div className="flex items-center gap-2.5">
            <div className="citrus-gradient flex items-center justify-center w-7 h-7 rounded-xl shadow-md shadow-orange-200/70">
              <Zap className="w-3.5 h-3.5 text-white" />
            </div>
            <p className="text-sm font-semibold text-sidebar-foreground">JetBrains AI</p>
          </div>
          <button
            onClick={() => setSidebarOpen(false)}
            className="p-1.5 rounded-md text-sidebar-foreground hover:bg-sidebar-accent transition-colors"
          >
            <X className="w-4 h-4" />
          </button>
        </div>
        <NavLinks onNavigate={() => setSidebarOpen(false)} mode={mode} />
        <div className="px-4 py-3 border-t border-sidebar-border shrink-0">
          <p className="text-xs text-muted-foreground">v3.0.0 · MIT License</p>
        </div>
      </div>

      {children}
    </div>
  );
}

function AdminRoutes() {
  return (
    <Switch>
      <Route path="/" component={Dashboard} />
      <Route path="/accounts" component={Accounts} />
      <Route path="/keys" component={ApiKeys} />
      <Route path="/models" component={Models} />
      <Route path="/stats" component={Stats} />
      <Route path="/logs" component={Logs} />
      <Route path="/docs" component={Docs} />
      <Route path="/prizes" component={Prizes} />
      <Route path="/partners" component={Partners} />
      <Route path="/donated-accounts" component={DonatedAccounts} />
      <Route path="/proxy-pool" component={ProxyPool} />
      <Route path="/proxy-pool/low-users" component={LowCfPoolAdmin} />
      <Route path="/activate" component={Activate} />
      <Route path="/pending-queue" component={PendingQueue} />
      <Route path="/self-register" component={SelfRegister} />
      <Route path="/my-key" component={KeyUsage} />
      <Route path="/backpack" component={Backpack} />
      <Route path="/donate" component={Donate} />
    </Switch>
  );
}

// 次级管理员路由：用户面板可见页 + 专属 CF 池
// 任何不在白名单内的路径（含管理员专属 /accounts、/keys 等）一律重定向到 /activate
function LowAdminRedirect() {
  const [, navigate] = useLocation();
  useEffect(() => {
    navigate("/activate", { replace: true });
  }, [navigate]);
  return null;
}

function LowAdminRoutes() {
  return (
    <Switch>
      <Route path="/activate" component={Activate} />
      <Route path="/self-register" component={SelfRegister} />
      <Route path="/my-key" component={KeyUsage} />
      <Route path="/backpack" component={Backpack} />
      <Route path="/donate" component={Donate} />
      <Route path="/my-cf-pool" component={LowCfPool} />
      <Route path="/logs" component={Logs} />
      <Route component={LowAdminRedirect} />
    </Switch>
  );
}

function Router() {
  const [location] = useLocation();
  // 响应式订阅 role 变更（同窗口 + 跨标签页）
  const role = useAdminRole();
  const hasAdminKey = !!getAdminKey();
  const isGuestPath = GUEST_PATHS.includes(location);

  // 已有 key 但身份未确认：交给 AdminGate 走 /admin/status 校验，避免先闪管理员页面
  if (hasAdminKey && role === "unknown") {
    return (
      <AdminGate>
        <div />
      </AdminGate>
    );
  }

  // 完整管理员：保留全功能管理面板
  if (hasAdminKey && role === "admin") {
    if (location === "/lottery") {
      return (
        <AdminGate>
          <FullscreenLayout mode="admin">
            <Lottery />
          </FullscreenLayout>
        </AdminGate>
      );
    }
    return (
      <AdminGate>
        <Layout mode="admin">
          <AdminRoutes />
        </Layout>
      </AdminGate>
    );
  }

  // 次级管理员：用户面板布局 + 专属 CF 池入口；其余管理员页一律不可见
  if (hasAdminKey && role === "low_admin") {
    if (location === "/lottery") {
      return (
        <AdminGate>
          <FullscreenLayout mode="low_admin">
            <Lottery />
          </FullscreenLayout>
        </AdminGate>
      );
    }
    return (
      <AdminGate>
        <Layout mode="low_admin">
          <LowAdminRoutes />
        </Layout>
      </AdminGate>
    );
  }

  // 未登录：访客模式（公共页面）
  if (location === "/lottery") {
    return (
      <FullscreenLayout mode="guest">
        <Lottery />
      </FullscreenLayout>
    );
  }

  if (isGuestPath) {
    return (
      <Layout mode="guest">
        <Switch>
          <Route path="/activate" component={Activate} />
          <Route path="/self-register" component={SelfRegister} />
          <Route path="/my-key" component={KeyUsage} />
          <Route path="/backpack" component={Backpack} />
          <Route path="/donate" component={Donate} />
        </Switch>
      </Layout>
    );
  }

  // 仅管理员可见的页面但未登录 → 显示登录界面
  return (
    <AdminGate>
      <Layout mode="admin">
        <Switch>
          <Route path="/" component={Dashboard} />
          <Route path="/accounts" component={Accounts} />
          <Route path="/keys" component={ApiKeys} />
          <Route path="/models" component={Models} />
          <Route path="/stats" component={Stats} />
          <Route path="/logs" component={Logs} />
          <Route path="/docs" component={Docs} />
          <Route path="/prizes" component={Prizes} />
          <Route path="/partners" component={Partners} />
          <Route path="/donated-accounts" component={DonatedAccounts} />
          <Route path="/proxy-pool" component={ProxyPool} />
          <Route path="/proxy-pool/low-users" component={LowCfPoolAdmin} />
        </Switch>
      </Layout>
    </AdminGate>
  );
}

function App() {
  return (
    <QueryClientProvider client={queryClient}>
      <TooltipProvider>
        <WouterRouter base={import.meta.env.BASE_URL.replace(/\/$/, "")}>
          <Router />
        </WouterRouter>
        <Toaster />
      </TooltipProvider>
    </QueryClientProvider>
  );
}

export default App;
