import { useState, useEffect } from "react";
import { useQuery, useMutation, useQueryClient } from "@tanstack/react-query";
import { Save, Megaphone, AlertCircle, CheckCircle2 } from "lucide-react";
import { adminFetch } from "@/lib/admin-auth";
import { Button } from "@/components/ui/button";
import { Card, CardContent, CardHeader, CardTitle } from "@/components/ui/card";
import { Switch } from "@/components/ui/switch";
import { Label } from "@/components/ui/label";
import { Textarea } from "@/components/ui/textarea";
import { Badge } from "@/components/ui/badge";
import { useToast } from "@/hooks/use-toast";

interface AnnouncementState {
  enabled: boolean;
  content: string;
  updated_at: number;
}

export default function Announcement() {
  const { toast } = useToast();
  const qc = useQueryClient();
  const [enabled, setEnabled] = useState(false);
  const [content, setContent] = useState("");
  const [isDirty, setIsDirty] = useState(false);

  const { data, isLoading } = useQuery<AnnouncementState>({
    queryKey: ["admin-announcement"],
    queryFn: async () => {
      const res = await adminFetch("/admin/announcement");
      if (!res.ok) throw new Error("获取公告配置失败");
      return res.json();
    },
  });

  useEffect(() => {
    if (data) {
      setEnabled(!!data.enabled);
      setContent(data.content || "");
      setIsDirty(false);
    }
  }, [data]);

  const saveMutation = useMutation({
    mutationFn: async () => {
      const res = await adminFetch("/admin/announcement", {
        method: "PUT",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({ enabled, content }),
      });
      if (!res.ok) {
        const t = await res.text();
        throw new Error(t || "保存失败");
      }
      return res.json();
    },
    onSuccess: () => {
      toast({
        title: "公告已保存",
        description: enabled ? "已启用：用户请求消息将立刻收到公告内容" : "已禁用：恢复正常 AI 回答",
      });
      setIsDirty(false);
      qc.invalidateQueries({ queryKey: ["admin-announcement"] });
    },
    onError: (e: any) => toast({ title: "保存失败", description: String(e?.message || e), variant: "destructive" }),
  });

  const updatedAtText = data?.updated_at
    ? new Date(data.updated_at * 1000).toLocaleString("zh-CN")
    : "—";

  return (
    <div className="space-y-6">
      <div className="flex items-center gap-3">
        <div className="grid h-10 w-10 place-items-center rounded-2xl bg-orange-100 text-orange-700 ring-1 ring-orange-200/70">
          <Megaphone className="h-5 w-5" />
        </div>
        <div>
          <h1 className="text-2xl font-black tracking-tight">公告劫持</h1>
          <p className="text-sm text-muted-foreground">
            启用后，用户每次请求消息时立刻返回公告内容，<span className="font-semibold text-orange-700">不会调用 AI</span>。
          </p>
        </div>
      </div>

      <Card className="border-orange-200/70 bg-amber-50/40">
        <CardContent className="flex items-start gap-3 py-4 text-sm text-amber-900">
          <AlertCircle className="h-4 w-4 shrink-0 mt-0.5" />
          <div className="space-y-1">
            <p className="font-bold">作用范围</p>
            <p>
              覆盖三类接口：<code className="rounded bg-amber-200/60 px-1">/v1/chat/completions</code>、
              <code className="rounded bg-amber-200/60 px-1">/v1/responses</code>、
              <code className="rounded bg-amber-200/60 px-1">/v1/messages</code>。
              支持流式与非流式，公告期间不消耗账号配额，也不计入用户 key 的用量。
            </p>
          </div>
        </CardContent>
      </Card>

      <Card>
        <CardHeader>
          <CardTitle className="flex items-center justify-between">
            <span>公告配置</span>
            {data && (
              <Badge variant={data.enabled ? "default" : "secondary"} className={data.enabled ? "bg-orange-600" : ""}>
                {data.enabled ? "已启用" : "未启用"}
              </Badge>
            )}
          </CardTitle>
        </CardHeader>
        <CardContent className="space-y-5">
          <div className="flex items-center justify-between rounded-2xl border bg-card p-4">
            <div className="space-y-1">
              <Label htmlFor="ann-enabled" className="text-base font-bold">启用公告劫持</Label>
              <p className="text-xs text-muted-foreground">
                打开后立即生效；客户端流式与非流式调用均会收到公告内容。
              </p>
            </div>
            <Switch
              id="ann-enabled"
              checked={enabled}
              onCheckedChange={(v) => {
                setEnabled(v);
                setIsDirty(true);
              }}
              disabled={isLoading}
            />
          </div>

          <div className="space-y-2">
            <Label htmlFor="ann-content" className="text-sm font-bold">公告内容</Label>
            <Textarea
              id="ann-content"
              value={content}
              onChange={(e) => {
                setContent(e.target.value);
                setIsDirty(true);
              }}
              placeholder="例如：服务正在维护中，预计 30 分钟后恢复，敬请耐心等待。"
              rows={10}
              className="font-mono text-sm"
              disabled={isLoading}
            />
            <p className="text-xs text-muted-foreground">
              支持任意文本（含换行、emoji、Markdown 源码）。客户端会原样收到。
            </p>
          </div>

          <div className="flex items-center justify-between border-t pt-4">
            <div className="flex items-center gap-2 text-xs text-muted-foreground">
              <CheckCircle2 className="h-3.5 w-3.5" />
              上次更新：{updatedAtText}
            </div>
            <Button
              onClick={() => saveMutation.mutate()}
              disabled={!isDirty || saveMutation.isPending || isLoading}
              className="gap-2"
            >
              <Save className="h-4 w-4" />
              {saveMutation.isPending ? "保存中…" : "保存配置"}
            </Button>
          </div>
        </CardContent>
      </Card>
    </div>
  );
}
