import { useState, useEffect } from "react";
import { useQuery, useMutation, useQueryClient } from "@tanstack/react-query";
import { Save, RefreshCw, Cpu, Plus, Trash2, ArrowRight } from "lucide-react";
import { adminFetch } from "@/lib/admin-auth";
import { Button } from "@/components/ui/button";
import { Card, CardContent, CardHeader, CardTitle } from "@/components/ui/card";
import { Input } from "@/components/ui/input";
import { Label } from "@/components/ui/label";
import { Badge } from "@/components/ui/badge";
import { useToast } from "@/hooks/use-toast";

interface ModelsConfig {
  models: string[];
  anthropic_model_mappings?: Record<string, string>;
}

const DEFAULT_MODELS = [
  "anthropic-claude-3.7-sonnet",
  "anthropic-claude-4-sonnet",
  "google-chat-gemini-pro-2.5",
  "openai-o4-mini",
  "openai-o3-mini",
  "openai-o3",
  "openai-o1",
  "openai-gpt-4o",
  "anthropic-claude-3.5-sonnet",
  "openai-gpt4.1",
];

export default function Models() {
  const { toast } = useToast();
  const qc = useQueryClient();
  const [models, setModels] = useState<string[]>([]);
  const [mappings, setMappings] = useState<Record<string, string>>({});
  const [newModel, setNewModel] = useState("");
  const [newMapKey, setNewMapKey] = useState("");
  const [newMapValue, setNewMapValue] = useState("");
  const [isDirty, setIsDirty] = useState(false);

  const { data: queryData, isLoading, refetch } = useQuery<ModelsConfig>({
    queryKey: ["admin-models"],
    queryFn: async () => {
      const res = await adminFetch("/admin/models");
      if (!res.ok) throw new Error("获取模型配置失败");
      return res.json();
    },
  });

  useEffect(() => {
    if (queryData) {
      setModels(queryData.models ?? []);
      setMappings(queryData.anthropic_model_mappings ?? {});
      setIsDirty(false);
    }
  }, [queryData]);

  const saveMutation = useMutation({
    mutationFn: async () => {
      const res = await adminFetch("/admin/models", {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({ models, anthropic_model_mappings: mappings }),
      });
      if (!res.ok) throw new Error("保存失败");
      return res.json();
    },
    onSuccess: (d) => {
      toast({ title: "模型配置已保存", description: `共 ${d.models_count} 个模型` });
      setIsDirty(false);
      qc.invalidateQueries({ queryKey: ["admin-models"] });
      qc.invalidateQueries({ queryKey: ["admin-status"] });
      qc.invalidateQueries({ queryKey: ["models-list"] });
    },
    onError: () => toast({ title: "保存失败", variant: "destructive" }),
  });

  const addModel = () => {
    if (!newModel.trim() || models.includes(newModel.trim())) return;
    setModels([...models, newModel.trim()]);
    setNewModel("");
    setIsDirty(true);
  };

  const removeModel = (m: string) => {
    setModels(models.filter((x) => x !== m));
    setIsDirty(true);
  };

  const addMapping = () => {
    if (!newMapKey.trim() || !newMapValue.trim()) return;
    setMappings({ ...mappings, [newMapKey.trim()]: newMapValue.trim() });
    setNewMapKey("");
    setNewMapValue("");
    setIsDirty(true);
  };

  const removeMapping = (key: string) => {
    const next = { ...mappings };
    delete next[key];
    setMappings(next);
    setIsDirty(true);
  };

  const loadDefaults = () => {
    setModels(DEFAULT_MODELS);
    setIsDirty(true);
  };

  return (
    <div className="p-6 space-y-6">
      <div className="flex items-center justify-between">
        <div>
          <h1 className="text-2xl font-bold text-foreground">模型配置</h1>
          <p className="text-sm text-muted-foreground mt-1">管理可用的 AI 模型列表及名称映射规则</p>
        </div>
        <div className="flex items-center gap-3">
          <Button variant="outline" size="sm" onClick={() => { refetch(); setIsDirty(false); }} disabled={isLoading}>
            <RefreshCw className={`w-4 h-4 mr-2 ${isLoading ? "animate-spin" : ""}`} />
            重置
          </Button>
          <Button variant="outline" size="sm" onClick={loadDefaults}>
            加载默认
          </Button>
          <Button size="sm" onClick={() => saveMutation.mutate()} disabled={!isDirty || saveMutation.isPending}>
            <Save className="w-4 h-4 mr-2" />
            {saveMutation.isPending ? "保存中..." : "保存配置"}
          </Button>
        </div>
      </div>

      {isDirty && (
        <div className="p-3 rounded-lg bg-amber-500/10 border border-amber-500/20 text-sm text-amber-400">
          有未保存的更改，请点击"保存配置"按钮应用。
        </div>
      )}

      {/* Models List */}
      <Card className="border-card-border">
        <CardHeader className="pb-3">
          <CardTitle className="text-base font-semibold flex items-center gap-2">
            <Cpu className="w-4 h-4 text-primary" />
            模型列表
            <Badge variant="outline" className="ml-auto text-xs">{models.length} 个</Badge>
          </CardTitle>
        </CardHeader>
        <CardContent className="space-y-4">
          <div className="flex gap-2">
            <Input
              placeholder="输入模型 ID，例: anthropic-claude-3.5-sonnet"
              value={newModel}
              onChange={(e) => setNewModel(e.target.value)}
              onKeyDown={(e) => e.key === "Enter" && addModel()}
              className="font-mono text-sm"
            />
            <Button onClick={addModel} disabled={!newModel.trim()}>
              <Plus className="w-4 h-4" />
            </Button>
          </div>

          {models.length === 0 ? (
            <div className="text-center py-8 text-muted-foreground text-sm">
              暂无模型，请添加或点击"加载默认"
            </div>
          ) : (
            <div className="space-y-1.5 max-h-64 overflow-y-auto">
              {models.map((m) => (
                <div key={m} className="flex items-center gap-2 p-2.5 rounded-lg bg-muted/40 border border-border group">
                  <div className="w-2 h-2 rounded-full bg-primary shrink-0" />
                  <code className="flex-1 text-sm font-mono text-foreground">{m}</code>
                  <Button variant="ghost" size="sm" className="h-6 w-6 p-0 opacity-0 group-hover:opacity-100 text-muted-foreground hover:text-destructive"
                    onClick={() => removeModel(m)}>
                    <Trash2 className="w-3 h-3" />
                  </Button>
                </div>
              ))}
            </div>
          )}
        </CardContent>
      </Card>

      {/* Anthropic Model Mappings */}
      <Card className="border-card-border">
        <CardHeader className="pb-3">
          <CardTitle className="text-base font-semibold flex items-center gap-2">
            <ArrowRight className="w-4 h-4 text-primary" />
            Anthropic 模型映射
            <Badge variant="outline" className="ml-auto text-xs">{Object.keys(mappings).length} 条</Badge>
          </CardTitle>
        </CardHeader>
        <CardContent className="space-y-4">
          <p className="text-xs text-muted-foreground">当使用 Anthropic SDK 时，将客户端请求的模型名称映射到 JetBrains AI 的实际模型 ID</p>
          <div className="flex gap-2">
            <div className="flex-1 flex gap-2 items-center">
              <Input placeholder="来源 (如: claude-3-5-sonnet)" value={newMapKey} onChange={(e) => setNewMapKey(e.target.value)} className="font-mono text-sm" />
              <ArrowRight className="w-4 h-4 text-muted-foreground shrink-0" />
              <Input placeholder="目标 (如: anthropic-claude-3.5-sonnet)" value={newMapValue} onChange={(e) => setNewMapValue(e.target.value)} className="font-mono text-sm" />
            </div>
            <Button onClick={addMapping} disabled={!newMapKey.trim() || !newMapValue.trim()}>
              <Plus className="w-4 h-4" />
            </Button>
          </div>

          {Object.keys(mappings).length === 0 ? (
            <div className="text-center py-8 text-muted-foreground text-sm">暂无映射规则</div>
          ) : (
            <div className="space-y-1.5">
              {Object.entries(mappings).map(([from, to]) => (
                <div key={from} className="flex items-center gap-3 p-2.5 rounded-lg bg-muted/40 border border-border group">
                  <code className="text-sm font-mono text-blue-400">{from}</code>
                  <ArrowRight className="w-3.5 h-3.5 text-muted-foreground shrink-0" />
                  <code className="flex-1 text-sm font-mono text-emerald-400">{to}</code>
                  <Button variant="ghost" size="sm" className="h-6 w-6 p-0 opacity-0 group-hover:opacity-100 text-muted-foreground hover:text-destructive"
                    onClick={() => removeMapping(from)}>
                    <Trash2 className="w-3 h-3" />
                  </Button>
                </div>
              ))}
            </div>
          )}
        </CardContent>
      </Card>
    </div>
  );
}
