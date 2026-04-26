import { useState } from "react";
import { BookOpen, Copy, Send, ChevronDown, ChevronRight } from "lucide-react";
import { Button } from "@/components/ui/button";
import { Card, CardContent, CardHeader, CardTitle } from "@/components/ui/card";
import { Badge } from "@/components/ui/badge";
import { Tabs, TabsContent, TabsList, TabsTrigger } from "@/components/ui/tabs";
import { Textarea } from "@/components/ui/textarea";
import { Input } from "@/components/ui/input";
import { Label } from "@/components/ui/label";
import { useToast } from "@/hooks/use-toast";

interface Endpoint {
  method: "GET" | "POST" | "DELETE";
  path: string;
  desc: string;
  auth: string;
  requestExample?: string;
  responseExample: string;
}

const endpoints: Endpoint[] = [
  {
    method: "GET",
    path: "/v1/models",
    desc: "获取所有可用模型列表（OpenAI 格式）",
    auth: "Authorization: Bearer <client-key> 或 x-api-key: <client-key>",
    responseExample: JSON.stringify({ object: "list", data: [{ id: "anthropic-claude-3.5-sonnet", object: "model", created: 1700000000, owned_by: "jetbrains-ai" }] }, null, 2),
  },
  {
    method: "POST",
    path: "/v1/chat/completions",
    desc: "OpenAI 兼容的聊天完成接口，支持流式输出和 Function Calling",
    auth: "Authorization: Bearer <client-key>",
    requestExample: JSON.stringify({ model: "anthropic-claude-3.5-sonnet", messages: [{ role: "user", content: "你好" }], stream: false }, null, 2),
    responseExample: JSON.stringify({ id: "chatcmpl-xxx", object: "chat.completion", model: "anthropic-claude-3.5-sonnet", choices: [{ index: 0, message: { role: "assistant", content: "你好！有什么可以帮助你的？" }, finish_reason: "stop" }] }, null, 2),
  },
  {
    method: "POST",
    path: "/v1/messages",
    desc: "Anthropic 兼容的消息接口，支持流式输出",
    auth: "x-api-key: <client-key>",
    requestExample: JSON.stringify({ model: "anthropic-claude-3.5-sonnet", messages: [{ role: "user", content: "你好" }], max_tokens: 1024, stream: false }, null, 2),
    responseExample: JSON.stringify({ id: "msg_xxx", type: "message", role: "assistant", content: [{ type: "text", text: "你好！有什么可以帮助你的？" }], model: "anthropic-claude-3.5-sonnet", stop_reason: "end_turn" }, null, 2),
  },
];

function EndpointCard({ ep }: { ep: Endpoint }) {
  const [expanded, setExpanded] = useState(false);
  const { toast } = useToast();
  const copyCode = (code: string) => { navigator.clipboard.writeText(code); toast({ title: "已复制" }); };
  const methodColor = ep.method === "GET" ? "border-blue-500/50 text-blue-400 bg-blue-500/10" : "border-emerald-500/50 text-emerald-400 bg-emerald-500/10";

  return (
    <Card className="border-card-border">
      <div className="flex items-center gap-3 p-4 cursor-pointer select-none" onClick={() => setExpanded(!expanded)}>
        <Badge className={`text-xs font-mono shrink-0 border ${methodColor}`}>{ep.method}</Badge>
        <code className="text-sm font-mono text-foreground flex-1">{ep.path}</code>
        <span className="text-xs text-muted-foreground hidden sm:block">{ep.desc}</span>
        {expanded ? <ChevronDown className="w-4 h-4 text-muted-foreground shrink-0" /> : <ChevronRight className="w-4 h-4 text-muted-foreground shrink-0" />}
      </div>
      {expanded && (
        <CardContent className="border-t border-border pt-4 space-y-4">
          <div>
            <p className="text-xs font-medium text-muted-foreground mb-1">接口说明</p>
            <p className="text-sm text-foreground">{ep.desc}</p>
          </div>
          <div>
            <p className="text-xs font-medium text-muted-foreground mb-1">认证方式</p>
            <code className="text-xs font-mono text-primary bg-primary/10 px-2 py-1 rounded">{ep.auth}</code>
          </div>
          {ep.requestExample && (
            <div>
              <div className="flex items-center justify-between mb-1">
                <p className="text-xs font-medium text-muted-foreground">请求示例</p>
                <Button variant="ghost" size="sm" className="h-6 text-xs text-muted-foreground" onClick={() => copyCode(ep.requestExample!)}>
                  <Copy className="w-3 h-3 mr-1" />复制
                </Button>
              </div>
              <pre className="text-xs font-mono bg-muted/60 border border-border rounded-lg p-3 overflow-x-auto text-foreground">{ep.requestExample}</pre>
            </div>
          )}
          <div>
            <div className="flex items-center justify-between mb-1">
              <p className="text-xs font-medium text-muted-foreground">响应示例</p>
              <Button variant="ghost" size="sm" className="h-6 text-xs text-muted-foreground" onClick={() => copyCode(ep.responseExample)}>
                <Copy className="w-3 h-3 mr-1" />复制
              </Button>
            </div>
            <pre className="text-xs font-mono bg-muted/60 border border-border rounded-lg p-3 overflow-x-auto text-foreground">{ep.responseExample}</pre>
          </div>
          <div>
            <p className="text-xs font-medium text-muted-foreground mb-1">cURL 示例</p>
            <div className="flex items-start gap-2">
              <pre className="text-xs font-mono bg-muted/60 border border-border rounded-lg p-3 overflow-x-auto text-foreground flex-1">{ep.method === "GET"
                ? `curl -H "Authorization: Bearer sk-your-key" \\\n  http://localhost:8000${ep.path}`
                : `curl -X POST \\\n  -H "Authorization: Bearer sk-your-key" \\\n  -H "Content-Type: application/json" \\\n  -d '${ep.requestExample?.split("\n").join("")}' \\\n  http://localhost:8000${ep.path}`
              }</pre>
            </div>
          </div>
        </CardContent>
      )}
    </Card>
  );
}

function ApiTester() {
  const { toast } = useToast();
  const [apiKey, setApiKey] = useState("");
  const [model, setModel] = useState("anthropic-claude-3.5-sonnet");
  const [prompt, setPrompt] = useState("你好，请简单介绍一下你自己。");
  const [response, setResponse] = useState("");
  const [loading, setLoading] = useState(false);

  const sendRequest = async () => {
    if (!apiKey || !prompt) {
      toast({ title: "请填写 API 密钥和消息内容", variant: "destructive" });
      return;
    }
    setLoading(true);
    setResponse("");
    try {
      const res = await fetch("/v1/chat/completions", {
        method: "POST",
        headers: {
          "Content-Type": "application/json",
          "Authorization": `Bearer ${apiKey}`,
        },
        body: JSON.stringify({ model, messages: [{ role: "user", content: prompt }], stream: false }),
      });
      const data = await res.json();
      if (!res.ok) {
        setResponse(JSON.stringify(data, null, 2));
        toast({ title: "请求失败", description: data.detail ?? "未知错误", variant: "destructive" });
      } else {
        const content = data.choices?.[0]?.message?.content ?? JSON.stringify(data, null, 2);
        setResponse(content);
        toast({ title: "请求成功" });
      }
    } catch (e: any) {
      setResponse(`错误: ${e.message}`);
      toast({ title: "请求异常", description: e.message, variant: "destructive" });
    } finally {
      setLoading(false);
    }
  };

  return (
    <div className="space-y-4">
      <div className="grid grid-cols-1 sm:grid-cols-2 gap-4">
        <div className="space-y-2">
          <Label>客户端 API 密钥</Label>
          <Input placeholder="sk-xxxxxxxxxxxxxxxx" value={apiKey} onChange={(e) => setApiKey(e.target.value)} className="font-mono text-sm" type="password" />
        </div>
        <div className="space-y-2">
          <Label>模型 ID</Label>
          <Input placeholder="anthropic-claude-3.5-sonnet" value={model} onChange={(e) => setModel(e.target.value)} className="font-mono text-sm" />
        </div>
      </div>
      <div className="space-y-2">
        <Label>消息内容</Label>
        <Textarea placeholder="输入要发送的消息..." value={prompt} onChange={(e) => setPrompt(e.target.value)} rows={3} />
      </div>
      <Button onClick={sendRequest} disabled={loading} className="w-full">
        <Send className={`w-4 h-4 mr-2 ${loading ? "animate-pulse" : ""}`} />
        {loading ? "请求中..." : "发送请求"}
      </Button>
      {response && (
        <div className="space-y-2">
          <Label>响应结果</Label>
          <div className="p-3 rounded-lg bg-muted/60 border border-border min-h-[100px] text-sm text-foreground whitespace-pre-wrap font-mono">
            {response}
          </div>
        </div>
      )}
    </div>
  );
}

export default function Docs() {
  return (
    <div className="p-6 space-y-6">
      <div>
        <h1 className="text-2xl font-bold text-foreground">接口文档</h1>
        <p className="text-sm text-muted-foreground mt-1">API 接口说明与在线测试工具</p>
      </div>

      <Tabs defaultValue="docs">
        <TabsList>
          <TabsTrigger value="docs">
            <BookOpen className="w-4 h-4 mr-2" />
            接口文档
          </TabsTrigger>
          <TabsTrigger value="test">
            <Send className="w-4 h-4 mr-2" />
            在线测试
          </TabsTrigger>
        </TabsList>

        <TabsContent value="docs" className="space-y-3 mt-4">
          <Card className="border-card-border bg-primary/5 border-primary/20">
            <CardContent className="py-4">
              <p className="text-sm text-foreground font-medium mb-1">基础 URL</p>
              <code className="text-sm font-mono text-primary">http://&lt;your-host&gt;:8000</code>
              <p className="text-xs text-muted-foreground mt-2">所有接口均兼容 OpenAI SDK 和 Anthropic SDK，只需修改 base_url 即可使用。</p>
            </CardContent>
          </Card>
          {endpoints.map((ep) => (
            <EndpointCard key={ep.path} ep={ep} />
          ))}
        </TabsContent>

        <TabsContent value="test" className="mt-4">
          <Card className="border-card-border">
            <CardHeader className="pb-3">
              <CardTitle className="text-base font-semibold flex items-center gap-2">
                <Send className="w-4 h-4 text-primary" />
                在线测试
              </CardTitle>
            </CardHeader>
            <CardContent>
              <ApiTester />
            </CardContent>
          </Card>
        </TabsContent>
      </Tabs>
    </div>
  );
}
