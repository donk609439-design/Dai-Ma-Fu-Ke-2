import { useQuery } from "@tanstack/react-query";
import {
  RefreshCw,
  Server,
  Users,
  Key,
  Cpu,
  CheckCircle,
  XCircle,
  AlertCircle,
  Activity,
  Sparkles,
  Gauge,
  RadioTower,
  ShieldCheck,
} from "lucide-react";
import { adminFetch } from "@/lib/admin-auth";
import { Button } from "@/components/ui/button";
import { Card, CardContent, CardHeader, CardTitle } from "@/components/ui/card";
import { Badge } from "@/components/ui/badge";
import { MigrationPanel } from "./MigrationPanel";

interface StatusData {
  status: string;
  accounts_count: number;
  keys_count: number;
  models_count: number;
  current_account_index: number;
}

export default function Dashboard() {
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
    {
      label: "JetBrains 账户",
      value: status?.accounts_count ?? "-",
      icon: Users,
      color: "text-blue-600",
      bg: "bg-blue-500/10",
      hint: "账号轮换资源",
    },
    {
      label: "客户端 API 密钥",
      value: status?.keys_count ?? "-",
      icon: Key,
      color: "text-cyan-700",
      bg: "bg-cyan-500/10",
      hint: "接入凭据数量",
    },
    {
      label: "可用模型数量",
      value: status?.models_count ?? "-",
      icon: Cpu,
      color: "text-purple-600",
      bg: "bg-purple-500/10",
      hint: "模型映射能力",
    },
    {
      label: "当前账户索引",
      value: status?.current_account_index ?? "-",
      icon: Activity,
      color: "text-orange-600",
      bg: "bg-orange-500/10",
      hint: "实时调度指针",
    },
  ];

  return (
    <div className="space-y-6">
      <section className="hero-card rounded-[2rem] p-5 sm:p-6 lg:p-7">
        <div className="relative z-10 flex flex-col gap-6 lg:flex-row lg:items-center lg:justify-between">
          <div className="max-w-3xl">
            <div className="mb-4 inline-flex items-center gap-2 rounded-full border border-white/70 bg-white/58 px-3 py-1.5 text-xs font-bold text-orange-700 shadow-sm backdrop-blur">
              <Sparkles className="h-3.5 w-3.5" />
              Premium Citrus Console
            </div>
            <h1 className="text-balance text-3xl font-black tracking-tight text-foreground sm:text-4xl lg:text-5xl">
              橘子机控制台
            </h1>
            <p className="mt-3 max-w-2xl text-sm leading-6 text-muted-foreground sm:text-base">
              JetBrains AI API 服务状态、账号池、模型映射与 OpenAI/Anthropic 兼容接口的一站式运营视图。
            </p>

            <div className="mt-5 flex flex-wrap items-center gap-3">
              <div className="inline-flex items-center gap-2 rounded-2xl bg-white/58 px-3.5 py-2 text-xs font-bold text-foreground ring-1 ring-white/65 backdrop-blur">
                <RadioTower className="h-4 w-4 text-orange-600" />
                30s 自动巡检
              </div>
              <div className="inline-flex items-center gap-2 rounded-2xl bg-white/58 px-3.5 py-2 text-xs font-bold text-foreground ring-1 ring-white/65 backdrop-blur">
                <Gauge className="h-4 w-4 text-cyan-700" />
                智能负载调度
              </div>
              <div className="inline-flex items-center gap-2 rounded-2xl bg-white/58 px-3.5 py-2 text-xs font-bold text-foreground ring-1 ring-white/65 backdrop-blur">
                <ShieldCheck className="h-4 w-4 text-emerald-700" />
                Admin Guard
              </div>
            </div>
          </div>

          <div className="rounded-[1.75rem] border border-white/70 bg-white/58 p-4 shadow-xl shadow-orange-950/5 backdrop-blur-xl lg:min-w-[280px]">
            <div className="mb-3 flex items-center justify-between">
              <span className="text-xs font-black uppercase tracking-[0.2em] text-muted-foreground">Service</span>
              <Badge className={error ? "bg-destructive/10 text-destructive border-destructive/30" : "bg-cyan-500/15 text-cyan-700 border-cyan-500/30"}>
                {error ? "Offline" : status?.status ?? "Online"}
              </Badge>
            </div>
            <div className="flex items-center gap-3">
              <div className={`grid h-12 w-12 place-items-center rounded-2xl ${error ? "bg-destructive/10 text-destructive" : "bg-cyan-500/10 text-cyan-700"}`}>
                {error ? <XCircle className="h-6 w-6" /> : <CheckCircle className="h-6 w-6" />}
              </div>
              <div>
                <p className="text-sm font-black text-foreground">{error ? "服务连接失败" : "服务运行正常"}</p>
                <p className="mt-1 text-xs leading-5 text-muted-foreground">
                  {error ? String(error) : "OpenAI Compatible API 已就绪"}
                </p>
              </div>
            </div>
            <Button
              variant="outline"
              size="sm"
              onClick={() => refetch()}
              disabled={isLoading}
              className="mt-4 w-full rounded-2xl border-white/70 bg-white/58 font-bold shadow-sm hover:bg-white"
            >
              <RefreshCw className={`mr-2 h-4 w-4 ${isLoading ? "animate-spin" : ""}`} />
              刷新运行状态
            </Button>
          </div>
        </div>
      </section>

      {/* Service Status Banner */}
      <Card className={`overflow-hidden border ${error ? "border-destructive/50 bg-destructive/5" : "border-emerald-500/30 bg-emerald-500/5"}`}>
        <CardContent className="flex flex-col gap-3 py-4 sm:flex-row sm:items-center">
          {error ? (
            <XCircle className="h-5 w-5 shrink-0 text-destructive" />
          ) : (
            <CheckCircle className="h-5 w-5 shrink-0 text-cyan-700" />
          )}
          <div>
            <p className={`text-sm font-bold ${error ? "text-destructive" : "text-cyan-700"}`}>
              {error ? "服务连接失败" : "服务运行正常"}
            </p>
            <p className="text-xs text-muted-foreground">
              {error ? String(error) : "JetBrains AI OpenAI Compatible API 服务正在运行"}
            </p>
          </div>
          {!error && status && (
            <Badge className="sm:ml-auto bg-cyan-500/15 text-cyan-700 border-cyan-500/30">
              {status.status}
            </Badge>
          )}
        </CardContent>
      </Card>

      {/* Stats Grid */}
      <div className="grid grid-cols-1 gap-4 sm:grid-cols-2 xl:grid-cols-4">
        {stats.map(({ label, value, icon: Icon, color, bg, hint }) => (
          <Card key={label} className="metric-card rounded-[1.6rem]">
            <CardContent className="p-5">
              <div className="mb-5 flex items-center justify-between">
                <div>
                  <span className="text-sm font-bold text-muted-foreground">{label}</span>
                  <p className="mt-1 text-xs text-muted-foreground/75">{hint}</p>
                </div>
                <div className={`rounded-2xl p-3 ${bg}`}>
                  <Icon className={`h-5 w-5 ${color}`} />
                </div>
              </div>
              <div className="flex items-end justify-between">
                <p className="text-4xl font-black tracking-tight text-foreground">{value}</p>
                <div className="h-2 w-16 overflow-hidden rounded-full bg-orange-100">
                  <div className="h-full w-2/3 rounded-full bg-gradient-to-r from-orange-400 to-orange-600" />
                </div>
              </div>
            </CardContent>
          </Card>
        ))}
      </div>

      <div className="grid grid-cols-1 gap-6 xl:grid-cols-[minmax(0,1.15fr)_minmax(360px,0.85fr)]">
        {/* Models List */}
        <Card className="border-card-border rounded-[1.6rem]">
          <CardHeader className="pb-3">
            <CardTitle className="flex items-center gap-2 text-base font-black">
              <Cpu className="h-4 w-4 text-primary" />
              可用模型列表
            </CardTitle>
          </CardHeader>
          <CardContent>
            {modelsData?.models?.length ? (
              <div className="grid grid-cols-1 gap-2 sm:grid-cols-2 xl:grid-cols-3">
                {modelsData.models.map((modelId) => (
                  <div
                    key={modelId}
                    className="group flex items-center gap-2 rounded-2xl border border-white/70 bg-white/48 p-3 shadow-sm backdrop-blur transition hover:-translate-y-0.5 hover:bg-white/68 hover:shadow-md"
                  >
                    <div className="h-2.5 w-2.5 shrink-0 rounded-full bg-cyan-500 shadow-[0_0_16px_rgba(6,182,212,0.6)]" />
                    <span className="truncate font-mono text-sm font-semibold text-foreground">{modelId}</span>
                  </div>
                ))}
              </div>
            ) : (
              <div className="flex items-center justify-center py-10 text-muted-foreground">
                <AlertCircle className="mr-2 h-4 w-4" />
                <span className="text-sm">暂无可用模型</span>
              </div>
            )}
          </CardContent>
        </Card>

        {/* API Endpoints */}
        <Card className="border-card-border rounded-[1.6rem]">
          <CardHeader className="pb-3">
            <CardTitle className="flex items-center gap-2 text-base font-black">
              <Server className="h-4 w-4 text-primary" />
              API 接口地址
            </CardTitle>
          </CardHeader>
          <CardContent className="space-y-2.5">
            {[
              { method: "GET", path: "/v1/models", desc: "获取可用模型列表" },
              { method: "POST", path: "/v1/chat/completions", desc: "OpenAI 兼容聊天接口" },
              { method: "POST", path: "/v1/messages", desc: "Anthropic 兼容消息接口" },
            ].map(({ method, path, desc }) => (
              <div key={path} className="rounded-2xl border border-white/70 bg-white/48 p-3 shadow-sm backdrop-blur">
                <div className="flex items-center gap-3">
                  <Badge
                    variant="outline"
                    className={`shrink-0 font-mono text-xs ${
                      method === "GET"
                        ? "border-blue-500/50 text-blue-600"
                        : "border-cyan-500/50 text-cyan-700"
                    }`}
                  >
                    {method}
                  </Badge>
                  <code className="truncate font-mono text-sm font-bold text-foreground">{path}</code>
                </div>
                <p className="mt-2 pl-0 text-xs text-muted-foreground sm:pl-[4.6rem]">{desc}</p>
              </div>
            ))}
          </CardContent>
        </Card>
      </div>

      {/* 数据迁移（流式 NDJSON） */}
      <MigrationPanel />
    </div>
  );
}
