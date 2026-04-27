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
import {
  LayoutDashboard,
  Users,
  Key,
  Cpu,
  BookOpen,
  Zap,
  BarChart3,
  UserPlus,
  Menu,
  X,
  Search,
  ScrollText,
  Ticket,
  Package,
  Gift,
  Handshake,
  CreditCard,
  HeartHandshake,
  ShieldCheck,
  Globe,
  Hourglass,
  Sparkles,
  Command,
  ChevronRight,
  Shield,
} from "lucide-react";
import { cn } from "@/lib/utils";
import { type ReactNode, useState, useEffect } from "react";

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
      staleTime: 30_000, // 30s 内不重新获取
      gcTime: 5 * 60_000, // 5min 缓存保留
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

function getModeMeta(mode: NavMode) {
  if (mode === "admin") {
    return {
      title: "JetBrains AI",
      subtitle: "Citrus Admin Console",
      badge: "ADMIN",
      description: "账户、模型、密钥与流量调度中心",
    };
  }

  if (mode === "low_admin") {
    return {
      title: "JetBrains AI",
      subtitle: "LOW Control Room",
      badge: "LOW",
      description: "专属池与个人用量管理入口",
    };
  }

  return {
    title: "Orange AI",
    subtitle: "Citrus User Portal",
    badge: "GUEST",
    description: "激活、查询、抽奖与账号助力",
  };
}

function NavLinks({ onNavigate, mode }: { onNavigate?: () => void; mode: NavMode }) {
  const [location] = useLocation();
  const items =
    mode === "admin"
      ? adminNavItems
      : mode === "low_admin"
        ? lowAdminNavItems
        : guestNavItems;

  return (
    <nav className="flex-1 overflow-y-auto px-3 py-4">
      <div className="mb-3 flex items-center justify-between px-3">
        <span className="text-[11px] font-bold uppercase tracking-[0.24em] text-sidebar-foreground/45">
          Navigation
        </span>
        <Command className="h-3.5 w-3.5 text-sidebar-foreground/35" />
      </div>

      <div className="space-y-1.5">
        {items.map(({ path, label, icon: Icon }) => {
          const active = location === path;
          const nested = label.trim().startsWith("└");
          return (
            <Link key={path} href={path}>
              <span
                onClick={onNavigate}
                className={cn(
                  "premium-nav-link group relative flex items-center gap-3 rounded-2xl px-3 py-2.5 text-sm font-semibold transition-all duration-300 cursor-pointer",
                  nested && "ml-4 text-xs",
                  active
                    ? "premium-nav-link-active text-white shadow-lg shadow-orange-500/20"
                    : "text-sidebar-foreground/72 hover:bg-white/55 hover:text-sidebar-foreground hover:shadow-sm hover:shadow-orange-900/5"
                )}
              >
                <span
                  className={cn(
                    "grid h-8 w-8 shrink-0 place-items-center rounded-xl transition-all duration-300",
                    active
                      ? "bg-white/20 text-white ring-1 ring-white/20"
                      : "bg-white/55 text-orange-700/75 ring-1 ring-orange-900/5 group-hover:scale-105 group-hover:bg-orange-50"
                  )}
                >
                  <Icon className="h-4 w-4" />
                </span>
                <span className="min-w-0 flex-1 truncate">{label}</span>
                <ChevronRight
                  className={cn(
                    "h-3.5 w-3.5 shrink-0 opacity-0 transition-all duration-300",
                    active ? "translate-x-0 opacity-80" : "-translate-x-1 group-hover:translate-x-0 group-hover:opacity-50"
                  )}
                />
              </span>
            </Link>
          );
        })}
      </div>
    </nav>
  );
}

function SidebarBrand({ mode }: { mode: NavMode }) {
  const meta = getModeMeta(mode);

  return (
    <div className="px-4 pb-4 pt-5">
      <div className="premium-brand-card relative overflow-hidden rounded-[1.65rem] p-4">
        <div className="relative z-10 flex items-start gap-3">
          <div className="citrus-orb grid h-12 w-12 shrink-0 place-items-center rounded-2xl">
            <Zap className="h-5 w-5 text-white drop-shadow" />
          </div>
          <div className="min-w-0 flex-1">
            <div className="mb-1 flex items-center gap-2">
              <p className="truncate text-base font-black tracking-tight text-sidebar-foreground">
                {meta.title}
              </p>
              <span className="rounded-full bg-white/60 px-2 py-0.5 text-[10px] font-black tracking-wider text-orange-700 ring-1 ring-white/55">
                {meta.badge}
              </span>
            </div>
            <p className="text-xs font-semibold text-orange-700/80">{meta.subtitle}</p>
            <p className="mt-2 text-xs leading-relaxed text-sidebar-foreground/55">
              {meta.description}
            </p>
          </div>
        </div>

        <div className="relative z-10 mt-4 grid grid-cols-3 gap-2 text-center">
          {[
            ["AI", "Core"],
            ["99", "Uptime"],
            ["CN", "Edge"],
          ].map(([value, label]) => (
            <div key={label} className="rounded-2xl bg-white/48 px-2 py-2 ring-1 ring-white/50 backdrop-blur">
              <p className="text-xs font-black text-sidebar-foreground">{value}</p>
              <p className="mt-0.5 text-[10px] font-medium text-sidebar-foreground/45">{label}</p>
            </div>
          ))}
        </div>
      </div>
    </div>
  );
}

function SidebarFooter({ mode }: { mode: NavMode }) {
  return (
    <div className="px-4 pb-4 pt-3">
      <div className="rounded-3xl border border-white/55 bg-white/48 p-3 shadow-sm shadow-orange-950/5 backdrop-blur-xl">
        <div className="flex items-center gap-3">
          <div className="grid h-9 w-9 place-items-center rounded-2xl bg-orange-100 text-orange-700 ring-1 ring-orange-200/70">
            {mode === "admin" ? <Shield className="h-4 w-4" /> : <Sparkles className="h-4 w-4" />}
          </div>
          <div className="min-w-0">
            <p className="truncate text-xs font-bold text-sidebar-foreground">Citrus UI 4.0</p>
            <p className="truncate text-[11px] text-sidebar-foreground/48">Premium glass edition</p>
          </div>
        </div>
      </div>
    </div>
  );
}

function Sidebar({ mode }: { mode: NavMode }) {
  return (
    <aside className="premium-sidebar hidden h-full w-[18rem] shrink-0 flex-col overflow-hidden md:flex">
      <SidebarBrand mode={mode} />
      <NavLinks mode={mode} />
      <SidebarFooter mode={mode} />
    </aside>
  );
}

function MobileHeader({ onMenuOpen, mode }: { onMenuOpen: () => void; mode: NavMode }) {
  const meta = getModeMeta(mode);

  return (
    <header className="mobile-glass-header fixed left-0 right-0 top-0 z-30 flex h-16 items-center justify-between px-4 md:hidden">
      <div className="flex min-w-0 items-center gap-3">
        <div className="citrus-orb grid h-9 w-9 shrink-0 place-items-center rounded-2xl">
          <Zap className="h-4 w-4 text-white" />
        </div>
        <div className="min-w-0">
          <p className="truncate text-sm font-black tracking-tight text-sidebar-foreground">{meta.title}</p>
          <p className="truncate text-[11px] font-medium text-sidebar-foreground/52">{meta.subtitle}</p>
        </div>
      </div>
      <button
        onClick={onMenuOpen}
        className="rounded-2xl bg-white/65 p-2.5 text-sidebar-foreground shadow-sm ring-1 ring-orange-900/10 transition hover:bg-white"
        aria-label="打开菜单"
      >
        <Menu className="h-5 w-5" />
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
    return () => {
      document.body.style.overflow = "";
    };
  }, [open]);

  return (
    <>
      {open && (
        <div
          className="fixed inset-0 z-40 bg-stone-950/45 backdrop-blur-sm md:hidden"
          onClick={onClose}
        />
      )}
      <div
        className={cn(
          "premium-sidebar fixed bottom-0 left-0 top-0 z-50 flex w-[18rem] max-w-[82vw] flex-col overflow-hidden transition-transform duration-300 md:hidden",
          open ? "translate-x-0" : "-translate-x-full"
        )}
      >
        <div className="flex items-center justify-between px-5 py-4">
          <div className="flex items-center gap-2.5">
            <div className="citrus-orb grid h-8 w-8 place-items-center rounded-xl">
              <Zap className="h-3.5 w-3.5 text-white" />
            </div>
            <p className="text-sm font-black text-sidebar-foreground">JetBrains AI</p>
          </div>
          <button
            onClick={onClose}
            className="rounded-xl bg-white/60 p-2 text-sidebar-foreground shadow-sm ring-1 ring-orange-900/10 transition hover:bg-white"
            aria-label="关闭菜单"
          >
            <X className="h-4 w-4" />
          </button>
        </div>
        <SidebarBrand mode={mode} />
        <NavLinks onNavigate={onClose} mode={mode} />
        <SidebarFooter mode={mode} />
      </div>
    </>
  );
}

function Layout({ children, mode }: { children: ReactNode; mode: NavMode }) {
  const [mobileOpen, setMobileOpen] = useState(false);

  return (
    <div className="app-shell relative flex h-screen overflow-hidden">
      <div className="shell-ambient shell-ambient-one" />
      <div className="shell-ambient shell-ambient-two" />
      <div className="shell-grid" />

      <Sidebar mode={mode} />
      <MobileDrawer open={mobileOpen} onClose={() => setMobileOpen(false)} mode={mode} />

      <div className="relative z-10 flex min-w-0 flex-1 flex-col overflow-hidden">
        <MobileHeader onMenuOpen={() => setMobileOpen(true)} mode={mode} />
        <main className="premium-main-scroll flex-1 overflow-y-auto px-3 pb-5 pt-20 sm:px-5 md:px-7 md:py-7">
          <div className="mx-auto w-full max-w-[1500px]">
            {children}
          </div>
        </main>
      </div>
    </div>
  );
}

function FullscreenLayout({ children, mode }: { children: ReactNode; mode: NavMode }) {
  const [sidebarOpen, setSidebarOpen] = useState(false);

  useEffect(() => {
    document.body.style.overflow = sidebarOpen ? "hidden" : "";
    return () => {
      document.body.style.overflow = "";
    };
  }, [sidebarOpen]);

  return (
    <div className="app-shell relative min-h-screen w-full overflow-hidden">
      <div className="shell-ambient shell-ambient-one" />
      <div className="shell-ambient shell-ambient-two" />
      <div className="shell-grid" />

      {/* 菜单开关按钮 */}
      <button
        onClick={() => setSidebarOpen((v) => !v)}
        className="fixed left-4 top-4 z-50 flex items-center gap-2 rounded-2xl border border-white/60 bg-white/72 px-3.5 py-2.5 text-xs font-black text-orange-800 shadow-xl shadow-orange-950/10 backdrop-blur-xl transition hover:-translate-y-0.5 hover:bg-white"
      >
        {sidebarOpen ? <X className="h-4 w-4" /> : <Menu className="h-4 w-4" />}
        <span>{sidebarOpen ? "收起菜单" : "打开菜单"}</span>
      </button>

      {/* 半透明遮罩 */}
      {sidebarOpen && (
        <div
          className="fixed inset-0 z-40 bg-stone-950/40 backdrop-blur-sm"
          onClick={() => setSidebarOpen(false)}
        />
      )}

      {/* 抽屉菜单（全平台通用，不依赖 Sidebar 的 hidden md:flex） */}
      <div
        className="premium-sidebar fixed left-0 top-0 z-50 flex h-full flex-col overflow-hidden transition-transform duration-300 ease-in-out"
        style={{
          width: "min(288px, 82vw)",
          transform: sidebarOpen ? "translateX(0)" : "translateX(-100%)",
        }}
      >
        <div className="flex items-center justify-between px-5 py-4">
          <div className="flex items-center gap-2.5">
            <div className="citrus-orb grid h-8 w-8 place-items-center rounded-xl">
              <Zap className="h-3.5 w-3.5 text-white" />
            </div>
            <p className="text-sm font-black text-sidebar-foreground">JetBrains AI</p>
          </div>
          <button
            onClick={() => setSidebarOpen(false)}
            className="rounded-xl bg-white/60 p-2 text-sidebar-foreground shadow-sm ring-1 ring-orange-900/10 transition hover:bg-white"
            aria-label="关闭菜单"
          >
            <X className="h-4 w-4" />
          </button>
        </div>
        <SidebarBrand mode={mode} />
        <NavLinks onNavigate={() => setSidebarOpen(false)} mode={mode} />
        <SidebarFooter mode={mode} />
      </div>

      <div className="relative z-10">{children}</div>
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
