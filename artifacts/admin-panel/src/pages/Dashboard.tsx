import { useQuery } from "@tanstack/react-query";
import { RefreshCw, Server, Users, Key, Cpu, CheckCircle, XCircle, AlertCircle, Activity } from "lucide-react";
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

      {/* 数据迁移（流式 NDJSON） */}
      <MigrationPanel />
    </div>
  );
}
