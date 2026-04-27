import { useState } from "react";
import { useQuery, useQueryClient } from "@tanstack/react-query";
import { Package, Zap, Plus, Trash2, Copy, Check, ChevronDown, ChevronUp, X, CreditCard, LogIn, LogOut, Download, Sparkles, ShieldCheck, Archive, Gem, Wand2 } from "lucide-react";
import { cn } from "@/lib/utils";
import { useToast } from "@/hooks/use-toast";
import { useDiscordAuth } from "@/hooks/useDiscordAuth";

interface BackpackItem {
  id: number;
  prize_name: string;
  metadata: Record<string, unknown>;
  used: boolean;
  used_at: string | null;
  created_at: string;
}

interface Pokeball {
  id: number;
  ball_key: string;
  name: string;
  capacity: number;
  total_used: number;
  members: string[];
  created_at: string;
}

interface BackpackData {
  items: BackpackItem[];
  pokeballs: Pokeball[];
}

function maskKey(key: string) {
  if (key.length <= 12) return key.slice(0, 4) + "****";
  return key.slice(0, 10) + "****" + key.slice(-4);
}

function isQuota(name: string) { return /额度/.test(name); }

function CopyButton({ text }: { text: string }) {
  const [copied, setCopied] = useState(false);
  const copy = () => {
    navigator.clipboard.writeText(text).then(() => {
      setCopied(true);
      setTimeout(() => setCopied(false), 2000);
    });
  };
  return (
    <button onClick={copy} className="rounded-xl bg-white/60 p-1.5 text-muted-foreground shadow-sm ring-1 ring-white/70 transition hover:bg-white hover:text-foreground">
      {copied ? <Check className="w-3.5 h-3.5 text-cyan-600" /> : <Copy className="w-3.5 h-3.5" />}
    </button>
  );
}

/** 从奖品名称中解析容量数字，与后端逻辑对齐 */
function parsePrizeCap(name: string): number | null {
  const patterns = [
    /宝可梦球?[^0-9]*[【\[(（]容量[：:]?\s*(\d+)[】\]）)]/,
    /宝可梦球?[^0-9]*[【\[(（]\s*(\d+)\s*[】\]）)]/,
    /宝可梦球?[^0-9]*容量[：:]?\s*(\d+)/,
    /宝可梦球?[^0-9]*(\d+)/,
  ];
  for (const p of patterns) {
    const m = name.match(p);
    if (m) return parseInt(m[1], 10);
  }
  return null;
}

/* ── Pokémon-style 8-bit activation jingle ── */
function playPokeballActivateSound() {
  const Ctx = window.AudioContext || (window as unknown as { webkitAudioContext: typeof AudioContext }).webkitAudioContext;
  const ctx = new Ctx();
  // Classic Pokémon Center healing chime melody (simplified, square wave for 8-bit feel)
  // Notes: C5 E5 G5 C6  G5 E5 C5  E5 G5 C6(hold)
  const seq = [
    { f: 523.25, t: 0.00, d: 0.11 },
    { f: 659.25, t: 0.11, d: 0.11 },
    { f: 783.99, t: 0.22, d: 0.11 },
    { f: 1046.5, t: 0.33, d: 0.20 },
    { f: 783.99, t: 0.55, d: 0.10 },
    { f: 659.25, t: 0.65, d: 0.10 },
    { f: 523.25, t: 0.75, d: 0.18 },
    { f: 659.25, t: 0.96, d: 0.08 },
    { f: 783.99, t: 1.04, d: 0.08 },
    { f: 1046.5, t: 1.12, d: 0.38 },
  ];
  seq.forEach(({ f, t, d }) => {
    const osc = ctx.createOscillator();
    const gain = ctx.createGain();
    osc.type = "square";
    osc.frequency.value = f;
    const start = ctx.currentTime + t;
    gain.gain.setValueAtTime(0.06, start);
    gain.gain.setValueAtTime(0.06, start + d - 0.015);
    gain.gain.exponentialRampToValueAtTime(0.0001, start + d);
    osc.connect(gain).connect(ctx.destination);
    osc.start(start); osc.stop(start + d + 0.02);
  });
  setTimeout(() => { try { ctx.close(); } catch { /* ignore */ } }, 2500);
}

/* ── Activate pokeball modal ── */
function ActivateModal({ item, ownerKey, onClose, onSuccess }: {
  item: BackpackItem;
  ownerKey: string;
  onClose: () => void;
  onSuccess: () => void;
}) {
  const guessedCap = (item.metadata.capacity as number | undefined) ?? parsePrizeCap(item.prize_name);
  const [name, setName] = useState("");
  const [manualCap, setManualCap] = useState(guessedCap ? String(guessedCap) : "");
  const [status, setStatus] = useState<"idle" | "loading" | "ok" | "error">("idle");
  const [msg, setMsg] = useState("");
  const [ballKey, setBallKey] = useState("");
  const capacity = guessedCap ?? parseInt(manualCap, 10);

  const handleActivate = async () => {
    if (!name.trim()) return;
    const capVal = guessedCap ?? parseInt(manualCap, 10);
    if (!capVal || capVal <= 0) { setStatus("error"); setMsg("请输入有效的容量数字"); return; }
    setStatus("loading");
    try {
      const res = await fetch("/key/pokeball/create", {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({ password: ownerKey, item_id: item.id, name: name.trim(), capacity: capVal }),
      });
      const d = await res.json();
      if (res.ok) {
        setBallKey(d.ball_key);
        setStatus("ok");
        playPokeballActivateSound();
      } else {
        setStatus("error");
        setMsg(d.detail || "激活失败");
      }
    } catch {
      setStatus("error");
      setMsg("网络错误，请重试");
    }
  };

  return (
    <div className="fixed inset-0 z-50 flex items-center justify-center bg-black/50 backdrop-blur-sm">
      <div className="bg-card border border-border rounded-2xl shadow-2xl p-6 w-full max-w-md mx-4 relative">
        <button onClick={onClose} className="absolute top-3 right-3 p-1 text-muted-foreground hover:text-foreground">
          <X className="w-5 h-5" />
        </button>
        <div className="flex items-center gap-3 mb-5">
          <div className="text-3xl">🔮</div>
          <div>
            <h2 className="text-base font-bold text-foreground">激活宝可梦球</h2>
            <p className="text-xs text-muted-foreground">{item.prize_name}{guessedCap ? ` · 容量 ${guessedCap}` : ""}</p>
          </div>
        </div>

        {status === "ok" ? (
          <div className="space-y-4">
            <div className="p-3 bg-green-500/10 border border-green-500/20 rounded-lg">
              <p className="text-xs text-green-400 font-medium mb-1">✓ 宝可梦球已激活！</p>
              <p className="text-xs text-muted-foreground">以下是您的聚合 Key，请妥善保存：</p>
            </div>
            <div className="flex items-center gap-2 bg-muted/50 rounded-lg px-3 py-2.5">
              <code className="flex-1 text-xs text-primary font-mono break-all">{ballKey}</code>
              <CopyButton text={ballKey} />
            </div>
            <p className="text-xs text-muted-foreground">您可以在背包页面管理成员 Key，请求将在成员间轮询分发。</p>
            <button
              onClick={() => { onSuccess(); onClose(); }}
              className="w-full py-2.5 rounded-lg bg-primary text-primary-foreground text-sm font-medium hover:bg-primary/90"
            >
              完成
            </button>
          </div>
        ) : (
          <div className="space-y-4">
            <div>
              <label className="text-xs font-medium text-muted-foreground mb-1.5 block">为宝可梦球命名</label>
              <input
                type="text"
                value={name}
                onChange={e => setName(e.target.value)}
                placeholder="例如：我的主力 Key 组"
                className="w-full px-3 py-2.5 rounded-lg border border-input bg-background text-sm text-foreground placeholder:text-muted-foreground focus:outline-none focus:ring-2 focus:ring-primary/30"
                onKeyDown={e => e.key === "Enter" && handleActivate()}
                autoFocus
              />
            </div>
            {!guessedCap && (
              <div>
                <label className="text-xs font-medium text-muted-foreground mb-1.5 block">
                  宝可梦球容量 <span className="text-red-400">*</span>
                  <span className="ml-1 font-normal opacity-60">（未能从奖品名自动识别，请手动输入）</span>
                </label>
                <input
                  type="number"
                  min="1"
                  value={manualCap}
                  onChange={e => setManualCap(e.target.value)}
                  placeholder="例如：100"
                  className="w-full px-3 py-2.5 rounded-lg border border-input bg-background text-sm text-foreground placeholder:text-muted-foreground focus:outline-none focus:ring-2 focus:ring-primary/30"
                />
              </div>
            )}
            {status === "error" && <p className="text-xs text-red-400">{msg}</p>}
            <div className="flex gap-2">
              <button onClick={onClose} className="flex-1 py-2.5 rounded-lg border border-border text-sm text-muted-foreground hover:bg-muted/30">取消</button>
              <button
                onClick={handleActivate}
                disabled={!name.trim() || status === "loading"}
                className="flex-1 py-2.5 rounded-lg bg-primary text-primary-foreground text-sm font-medium hover:bg-primary/90 disabled:opacity-50"
              >
                {status === "loading" ? "激活中…" : "激活"}
              </button>
            </div>
          </div>
        )}
      </div>
    </div>
  );
}

/* ── Pokeball card ── */
function PokeballCard({ pb, ownerKey, onRefresh }: { pb: Pokeball; ownerKey: string; onRefresh: () => void }) {
  const [expanded, setExpanded] = useState(false);
  const [addKey, setAddKey] = useState("");
  const [addStatus, setAddStatus] = useState<"idle" | "loading" | "ok" | "error">("idle");
  const [addMsg, setAddMsg] = useState("");
  const [confirming, setConfirming] = useState(false);
  const [deleting, setDeleting] = useState(false);
  const { toast } = useToast();

  const pct = Math.round((pb.total_used / pb.capacity) * 100);

  const handleAddMember = async () => {
    if (!addKey.trim()) return;
    setAddStatus("loading"); setAddMsg("");
    try {
      const res = await fetch(`/key/pokeball/${pb.ball_key}/members`, {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({ password: ownerKey, member_key: addKey.trim() }),
      });
      const d = await res.json();
      if (res.ok) {
        setAddStatus("ok"); setAddMsg(""); setAddKey("");
        toast({ title: "成员已添加" });
        onRefresh();
      } else {
        setAddStatus("error"); setAddMsg(d.detail || "添加失败");
      }
    } catch {
      setAddStatus("error"); setAddMsg("网络错误");
    }
  };

  const handleRemoveMember = async (mk: string) => {
    try {
      await fetch(`/key/pokeball/${pb.ball_key}/members/${encodeURIComponent(mk)}?password=${encodeURIComponent(ownerKey)}`, {
        method: "DELETE",
      });
      toast({ title: "成员已移除" });
      onRefresh();
    } catch {
      toast({ title: "移除失败", variant: "destructive" });
    }
  };

  const handleDelete = async () => {
    setDeleting(true);
    try {
      const res = await fetch(`/key/pokeball/${pb.ball_key}?password=${encodeURIComponent(ownerKey)}`, {
        method: "DELETE",
      });
      if (res.ok) {
        toast({ title: "宝可梦球已删除" });
        onRefresh();
      } else {
        const d = await res.json();
        toast({ title: d.detail || "删除失败", variant: "destructive" });
        setConfirming(false);
      }
    } catch {
      toast({ title: "网络错误", variant: "destructive" });
      setConfirming(false);
    } finally {
      setDeleting(false);
    }
  };

  return (
    <div className="backpack-item-card rounded-[1.65rem] overflow-hidden">
      {/* header */}
      <div className="p-4">
        <div className="flex items-start justify-between gap-3 mb-3">
          <div className="flex items-center gap-2.5">
            <div className="backpack-orb grid h-12 w-12 place-items-center rounded-2xl text-2xl">🔮</div>
            <div>
              <p className="font-semibold text-foreground text-sm">{pb.name}</p>
              <p className="text-xs text-muted-foreground">容量 {pb.capacity} · {pb.members.length} 个成员</p>
            </div>
          </div>
          <div className="flex items-center gap-2 shrink-0">
            <span className="text-xs px-2.5 py-1 rounded-full bg-orange-100 text-orange-700 font-black ring-1 ring-orange-200/70">激活中</span>
            {!confirming && (
              <button
                onClick={() => setConfirming(true)}
                title="删除宝可梦球"
                className="p-1 text-muted-foreground hover:text-red-400 transition-colors"
              >
                <Trash2 className="w-4 h-4" />
              </button>
            )}
          </div>
        </div>

        {/* inline delete confirmation */}
        {confirming && (
          <div className="mb-3 p-3 rounded-lg border border-red-500/30 bg-red-500/5 space-y-2">
            <p className="text-xs text-red-400 font-medium">删除后此宝可梦球将立即失效，所有成员 Key 将从轮询池中移除，此操作不可撤销。</p>
            <div className="flex gap-2">
              <button
                onClick={() => setConfirming(false)}
                disabled={deleting}
                className="flex-1 py-1.5 rounded-lg border border-border text-xs text-muted-foreground hover:bg-muted/30 disabled:opacity-50"
              >
                取消
              </button>
              <button
                onClick={handleDelete}
                disabled={deleting}
                className="flex-1 py-1.5 rounded-lg bg-red-500 text-white text-xs font-medium hover:bg-red-600 disabled:opacity-50"
              >
                {deleting ? "删除中…" : "确认删除"}
              </button>
            </div>
          </div>
        )}

        {/* key display */}
        <div className="flex items-center gap-1.5 bg-white/58 rounded-2xl px-3 py-2 mb-3 ring-1 ring-white/70">
          <code className="flex-1 text-xs text-primary font-mono truncate">{pb.ball_key}</code>
          <CopyButton text={pb.ball_key} />
        </div>

        {/* usage bar */}
        <div className="space-y-1">
          <div className="flex justify-between text-xs text-muted-foreground">
            <span>已用 {pb.total_used} / {pb.capacity}</span>
            <span>{pct}%</span>
          </div>
          <div className="h-2 rounded-full bg-orange-100 overflow-hidden">
            <div className="h-full rounded-full bg-gradient-to-r from-orange-400 to-orange-600 transition-all" style={{ width: `${Math.min(pct, 100)}%` }} />
          </div>
        </div>
      </div>

      {/* expand toggle */}
      <button
        onClick={() => setExpanded(v => !v)}
        className="w-full flex items-center justify-center gap-1 py-2.5 border-t border-white/60 text-xs font-bold text-muted-foreground hover:bg-white/45 transition-colors"
      >
        {expanded ? <><ChevronUp className="w-3.5 h-3.5" />收起成员</>
          : <><ChevronDown className="w-3.5 h-3.5" />管理成员</>}
      </button>

      {/* expanded member management */}
      {expanded && (
        <div className="border-t border-white/60 p-4 space-y-3 bg-white/28">
          {pb.members.length === 0 ? (
            <p className="text-xs text-muted-foreground text-center py-2">暂无成员，添加 Key 后将按轮询分发请求</p>
          ) : (
            <ul className="space-y-2">
              {pb.members.map((mk) => (
                <li key={mk} className="flex items-center gap-2 text-xs">
                  <code className="flex-1 font-mono text-muted-foreground bg-muted/50 px-2 py-1.5 rounded truncate">{maskKey(mk)}</code>
                  <button onClick={() => handleRemoveMember(mk)} className="p-1 text-muted-foreground hover:text-red-400 transition-colors shrink-0">
                    <Trash2 className="w-3.5 h-3.5" />
                  </button>
                </li>
              ))}
            </ul>
          )}

          {/* add member */}
          <div className="space-y-1.5">
            <div className="flex gap-1.5">
              <input
                type="text"
                value={addKey}
                onChange={e => { setAddKey(e.target.value); setAddStatus("idle"); }}
                placeholder="输入要加入的 API Key"
                className="flex-1 px-2.5 py-2 rounded-lg border border-input bg-background text-xs text-foreground placeholder:text-muted-foreground focus:outline-none focus:ring-2 focus:ring-primary/30"
                onKeyDown={e => e.key === "Enter" && handleAddMember()}
              />
              <button
                onClick={handleAddMember}
                disabled={!addKey.trim() || addStatus === "loading"}
                className="px-3 py-2 rounded-lg bg-primary text-primary-foreground text-xs font-medium hover:bg-primary/90 disabled:opacity-50 flex items-center gap-1"
              >
                <Plus className="w-3.5 h-3.5" />
                添加
              </button>
            </div>
            {addStatus === "error" && <p className="text-xs text-red-400">{addMsg}</p>}
            {addStatus === "ok" && <p className="text-xs text-green-400">✓ 已添加</p>}
          </div>
        </div>
      )}
    </div>
  );
}

/* ── Quota Redeem Modal ── */
function QuotaRedeemModal({
  item,
  ownerKey,
  onClose,
  onSuccess,
}: {
  item: BackpackItem;
  ownerKey: string;
  onClose: () => void;
  onSuccess: () => void;
}) {
  const [targetKey, setTargetKey] = useState("");
  const [status, setStatus] = useState<"idle" | "loading" | "ok" | "error">("idle");
  const [msg, setMsg] = useState("");
  const amount = (item.metadata?.quota_amount as number) ?? null;

  const doRedeem = async () => {
    if (!targetKey.trim()) return;
    setStatus("loading");
    try {
      const res = await fetch("/key/quota-redeem", {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({ password: ownerKey, item_id: item.id, target_key: targetKey.trim() }),
      });
      if (res.ok) {
        setStatus("ok");
        onSuccess();
      } else {
        const d = await res.json().catch(() => ({}));
        setStatus("error");
        setMsg(d.detail || "充值失败");
      }
    } catch {
      setStatus("error");
      setMsg("网络错误，请重试");
    }
  };

  return (
    <div className="fixed inset-0 z-50 flex items-center justify-center bg-black/50 backdrop-blur-sm p-4">
      <div className="bg-card border border-border rounded-2xl shadow-2xl w-full max-w-sm p-6 relative">
        <button onClick={onClose} className="absolute right-4 top-4 text-muted-foreground hover:text-foreground">
          <X className="w-4 h-4" />
        </button>
        <div className="flex flex-col items-center gap-2 mb-5">
          <CreditCard className="w-10 h-10 text-primary" />
          <h2 className="text-lg font-bold text-foreground">使用额度道具</h2>
          <p className="text-sm font-semibold text-primary text-center">{item.prize_name}</p>
          {amount && (
            <p className="text-xs text-muted-foreground text-center">
              向指定 Key 充值 <span className="font-bold text-primary">{amount} 次</span>用量上限
            </p>
          )}
        </div>

        {status === "ok" ? (
          <div className="flex flex-col items-center gap-3">
            <div className="flex items-center gap-2 text-green-500 font-medium">
              <Check className="w-5 h-5" />{amount ? `${amount} 次` : ""}额度已充值！
            </div>
            <button onClick={onClose} className="mt-2 px-6 py-2 rounded-lg bg-primary text-primary-foreground text-sm font-medium hover:bg-primary/90">
              完成
            </button>
          </div>
        ) : (
          <>
            <input
              type="text"
              value={targetKey}
              onChange={e => { setTargetKey(e.target.value); setStatus("idle"); }}
              placeholder="粘贴要充值的 API Key"
              className="w-full px-3 py-2.5 rounded-lg border border-input bg-background text-xs text-foreground placeholder:text-muted-foreground focus:outline-none focus:ring-2 focus:ring-primary/30 font-mono mb-3"
              autoFocus
              onKeyDown={e => e.key === "Enter" && doRedeem()}
            />
            {status === "error" && <p className="text-xs text-red-400 mb-2 text-center">{msg}</p>}
            <div className="flex gap-2">
              <button onClick={onClose} className="flex-1 py-2.5 rounded-lg border border-border text-sm text-muted-foreground hover:bg-muted/30">
                取消
              </button>
              <button
                onClick={doRedeem}
                disabled={!targetKey.trim() || status === "loading"}
                className="flex-1 py-2.5 rounded-lg bg-primary text-primary-foreground text-sm font-semibold hover:bg-primary/90 disabled:opacity-50 flex items-center justify-center gap-1.5"
              >
                <CreditCard className="w-4 h-4" />
                {status === "loading" ? "充值中…" : "立即充值"}
              </button>
            </div>
          </>
        )}
      </div>
    </div>
  );
}

/* ── 从密码导入 弹窗 ── */
function ImportFromPasswordModal({
  dcToken,
  userTag,
  onClose,
  onSuccess,
}: {
  dcToken: string;
  userTag: string | null;
  onClose: () => void;
  onSuccess: () => void;
}) {
  const [password, setPassword] = useState("");
  const [status, setStatus] = useState<"idle" | "loading" | "ok" | "error">("idle");
  const [msg, setMsg] = useState("");
  const [result, setResult] = useState<{ imported_items: number; imported_points: number; new_total_points: number } | null>(null);

  const doImport = async () => {
    if (!password.trim()) return;
    setStatus("loading");
    try {
      const res = await fetch("/key/dc-import-from-password", {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({ discord_token: dcToken, password: password.trim() }),
      });
      const d = await res.json();
      if (res.ok) {
        setResult(d);
        setStatus("ok");
        onSuccess();
      } else {
        setStatus("error");
        setMsg(d.detail || "导入失败");
      }
    } catch {
      setStatus("error");
      setMsg("网络错误，请重试");
    }
  };

  return (
    <div className="fixed inset-0 z-50 flex items-center justify-center bg-black/50 backdrop-blur-sm p-4">
      <div className="bg-card border border-border rounded-2xl shadow-2xl w-full max-w-sm p-6 relative">
        <button onClick={onClose} className="absolute right-4 top-4 text-muted-foreground hover:text-foreground">
          <X className="w-4 h-4" />
        </button>
        <div className="flex items-center gap-2 mb-1">
          <Download className="w-5 h-5 text-indigo-500" />
          <h2 className="text-base font-bold text-foreground">从密码账号导入</h2>
        </div>
        <p className="text-xs text-muted-foreground mb-1">
          将旧密码账号的背包物品和圣人点数合并到您的 Discord 账号
        </p>
        {userTag && (
          <p className="text-xs text-indigo-500 mb-4">目标账号：{userTag}</p>
        )}

        {status === "ok" && result ? (
          <div className="space-y-3 text-center">
            <div className="text-3xl">✅</div>
            <p className="text-sm font-bold text-foreground">导入成功！</p>
            <div className="text-xs text-muted-foreground space-y-1 text-left p-3 rounded-lg bg-muted/40 border border-border">
              <p>物品：<span className="font-semibold text-foreground">{result.imported_items}</span> 件已迁移</p>
              <p>点数：迁移 <span className="font-semibold text-amber-500">{result.imported_points}</span> 个</p>
              <p>当前圣人点数：<span className="font-black text-amber-500">{result.new_total_points}</span></p>
            </div>
            <button
              onClick={onClose}
              className="w-full py-2.5 rounded-lg bg-indigo-600 text-white text-sm font-semibold hover:bg-indigo-700"
            >
              完成
            </button>
          </div>
        ) : (
          <div className="space-y-3">
            <input
              type="text"
              value={password}
              onChange={e => { setPassword(e.target.value); setStatus("idle"); }}
              placeholder="输入旧账号密码"
              className="w-full px-3 py-2.5 rounded-lg border border-input bg-background text-sm text-foreground placeholder:text-muted-foreground focus:outline-none focus:ring-2 focus:ring-indigo-400/30"
              onKeyDown={e => e.key === "Enter" && doImport()}
              autoFocus
            />
            {status === "error" && <p className="text-xs text-red-400 text-center">{msg}</p>}
            <p className="text-xs text-muted-foreground">旧账号的物品和圣人点数将合并到 Discord 账号，旧账号点数清零（物品转移）。</p>
            <div className="flex gap-2">
              <button onClick={onClose} className="flex-1 py-2.5 rounded-lg border border-border text-sm text-muted-foreground hover:bg-muted/30">
                取消
              </button>
              <button
                onClick={doImport}
                disabled={!password.trim() || status === "loading"}
                className="flex-1 py-2.5 rounded-lg bg-indigo-600 text-white text-sm font-semibold hover:bg-indigo-700 disabled:opacity-50 flex items-center justify-center gap-1.5"
              >
                <Download className="w-4 h-4" />
                {status === "loading" ? "导入中…" : "确认导入"}
              </button>
            </div>
          </div>
        )}
      </div>
    </div>
  );
}

/* ── Main page ── */
export default function Backpack() {
  const [activatingItem, setActivatingItem] = useState<BackpackItem | null>(null);
  const [redeemingItem, setRedeemingItem] = useState<BackpackItem | null>(null);
  const [showImport, setShowImport] = useState(false);
  const qc = useQueryClient();

  const { dcToken, userTag, isLoggedIn: dcLoggedIn, login: dcLogin, logout: dcLogout } = useDiscordAuth("backpack");

  // DC 背包查询
  const { data, isLoading, isError, error, refetch } = useQuery<BackpackData & { owner_key: string }>({
    queryKey: ["dc-backpack", dcToken],
    queryFn: async () => {
      const res = await fetch(`/key/dc-backpack?discord_token=${encodeURIComponent(dcToken!)}`);
      if (!res.ok) {
        const d = await res.json().catch(() => ({}));
        throw new Error(d.detail || "查询失败");
      }
      return res.json();
    },
    enabled: !!dcToken,
    staleTime: 10_000,
    retry: false,
  });

  const ownerKey = data?.owner_key ?? "";

  const unusedPokeballs = data?.items.filter(i => !i.used && /宝可梦/.test(i.prize_name)) ?? [];
  const unusedQuotaItems = data?.items.filter(i => !i.used && isQuota(i.prize_name)) ?? [];
  const usedItems = data?.items.filter(i => i.used) ?? [];
  const activePokeballs = data?.pokeballs ?? [];

  return (
    <div className="premium-page-shell mx-auto max-w-5xl space-y-6">
      <section className="premium-hero-panel rounded-[2rem] p-5 sm:p-7">
        <div className="relative z-10 grid gap-6 lg:grid-cols-[1fr_320px] lg:items-center">
          <div>
            <div className="mb-4 inline-flex items-center gap-2 rounded-full border border-white/70 bg-white/60 px-3 py-1.5 text-xs font-black uppercase tracking-[0.2em] text-orange-700 shadow-sm backdrop-blur">
              <Sparkles className="h-3.5 w-3.5" />
              Discord Backpack Vault
            </div>
            <h1 className="text-3xl font-black tracking-tight text-foreground sm:text-5xl">
              我的<span className="citrus-text">背包</span>
            </h1>
            <p className="mt-3 max-w-2xl text-sm leading-6 text-muted-foreground sm:text-base">
              管理橘子机奖品、额度道具与宝可梦球聚合 Key。所有物品与 Discord 账号绑定，支持导入旧账号资产。
            </p>
            <div className="mt-5 flex flex-wrap gap-2">
              <span className="inline-flex items-center gap-2 rounded-2xl bg-white/58 px-3 py-2 text-xs font-bold text-foreground ring-1 ring-white/65">
                <Archive className="h-4 w-4 text-orange-600" />
                Prize Vault
              </span>
              <span className="inline-flex items-center gap-2 rounded-2xl bg-white/58 px-3 py-2 text-xs font-bold text-foreground ring-1 ring-white/65">
                <Gem className="h-4 w-4 text-cyan-700" />
                Quota Items
              </span>
              <span className="inline-flex items-center gap-2 rounded-2xl bg-white/58 px-3 py-2 text-xs font-bold text-foreground ring-1 ring-white/65">
                <Wand2 className="h-4 w-4 text-purple-600" />
                Pokéball Keys
              </span>
            </div>
          </div>
          <div className="rounded-[1.75rem] border border-white/70 bg-white/56 p-4 shadow-xl shadow-orange-950/5 backdrop-blur-xl">
            <div className="flex items-center gap-3">
              <div className="backpack-orb grid h-14 w-14 place-items-center rounded-2xl">
                <Package className="h-6 w-6 text-white" />
              </div>
              <div>
                <p className="text-xs font-black uppercase tracking-[0.22em] text-muted-foreground">Inventory</p>
                <p className="mt-1 text-lg font-black text-foreground">
                  {data ? `${unusedPokeballs.length + unusedQuotaItems.length + activePokeballs.length} active` : "Secure"}
                </p>
              </div>
            </div>
            <p className="mt-4 text-xs leading-5 text-muted-foreground">
              宝可梦球可聚合多个成员 Key；额度道具可直接为指定 API Key 充值上限。
            </p>
          </div>
        </div>
      </section>

      {/* DC 登录状态栏 */}
      <div className="premium-surface rounded-[1.75rem] p-4 flex items-center gap-2 flex-wrap">
        {dcLoggedIn ? (
          <>
            <div className="flex items-center gap-2 px-3 py-2 rounded-2xl text-xs font-black border border-indigo-200 bg-indigo-50 text-indigo-700">
              <ShieldCheck className="w-3.5 h-3.5" />
              <span>Discord：{userTag}</span>
            </div>
            <button
              onClick={dcLogout}
              className="flex items-center gap-1 px-3 py-2 rounded-2xl text-xs font-bold border border-white/70 bg-white/60 text-muted-foreground hover:bg-white"
            >
              <LogOut className="w-3 h-3" />
              退出
            </button>
            <button
              onClick={() => setShowImport(true)}
              className="flex items-center gap-1 px-3 py-2 rounded-2xl text-xs font-bold border border-indigo-200 bg-indigo-50 text-indigo-600 hover:bg-indigo-100"
            >
              <Download className="w-3 h-3" />
              从密码导入
            </button>
          </>
        ) : (
          <button
            onClick={dcLogin}
            className="flex items-center gap-1.5 px-4 py-3 rounded-2xl text-sm font-black bg-indigo-600 text-white shadow-lg shadow-indigo-500/20 hover:bg-indigo-700"
          >
            <LogIn className="w-3.5 h-3.5" />
            Discord 登录查看背包
          </button>
        )}
      </div>

      {/* 未登录提示 */}
      {!dcLoggedIn && (
        <div className="premium-surface flex flex-col items-center justify-center py-16 gap-4 text-center rounded-[2rem]">
          <div className="backpack-orb grid h-16 w-16 place-items-center rounded-3xl">
            <Package className="w-8 h-8 text-white" />
          </div>
          <p className="text-sm font-bold text-foreground">请先用 Discord 登录，才能查看背包内容</p>
          <p className="max-w-sm text-xs text-muted-foreground">登录后可以查看奖品、激活宝可梦球、充值额度，以及从旧密码账号导入资产。</p>
          <button
            onClick={dcLogin}
            className="flex items-center gap-1.5 px-5 py-3 rounded-2xl text-sm font-black bg-indigo-600 text-white shadow-lg shadow-indigo-500/20 hover:bg-indigo-700"
          >
            <LogIn className="w-4 h-4" />
            Discord 登录
          </button>
        </div>
      )}

      {/* 弹窗 */}
      {showImport && dcToken && (
        <ImportFromPasswordModal
          dcToken={dcToken}
          userTag={userTag}
          onClose={() => setShowImport(false)}
          onSuccess={() => { setShowImport(false); refetch(); qc.invalidateQueries({ queryKey: ["dc-backpack"] }); }}
        />
      )}
      {activatingItem && (
        <ActivateModal
          item={activatingItem}
          ownerKey={ownerKey}
          onClose={() => setActivatingItem(null)}
          onSuccess={() => { refetch(); qc.invalidateQueries({ queryKey: ["backpack", "dc-backpack"] }); }}
        />
      )}
      {redeemingItem && (
        <QuotaRedeemModal
          item={redeemingItem}
          ownerKey={ownerKey}
          onClose={() => setRedeemingItem(null)}
          onSuccess={() => { setRedeemingItem(null); refetch(); qc.invalidateQueries({ queryKey: ["backpack", "dc-backpack"] }); }}
        />
      )}

      {/* content */}
      {isLoading && (
        <div className="flex items-center justify-center py-12 text-muted-foreground">
          <Zap className="w-5 h-5 animate-pulse mr-2" />查询中…
        </div>
      )}

      {isError && (
        <div className="p-4 rounded-lg border border-red-500/20 bg-red-500/5 text-sm text-red-400 text-center">
          {(error as Error)?.message || "查询失败，请退出后重新登录"}
        </div>
      )}

      {data && (
        <>
          {/* unused pokeball items */}
          {unusedPokeballs.length > 0 && (
            <section className="space-y-3">
              <h2 className="text-sm font-semibold text-foreground flex items-center gap-2">
                <span className="text-base">🎁</span>未使用道具
              </h2>
              {unusedPokeballs.map(item => (
                <div key={item.id} className="backpack-item-card flex items-center gap-3 p-4 rounded-[1.5rem]">
                  <div className="backpack-orb grid h-12 w-12 place-items-center rounded-2xl text-2xl shrink-0">🔮</div>
                  <div className="flex-1 min-w-0">
                    <p className="text-sm font-medium text-foreground">{item.prize_name}</p>
                    <p className="text-xs text-muted-foreground">容量 {(item.metadata.capacity as number) ?? "?"} · {new Date(item.created_at).toLocaleDateString("zh-CN")}</p>
                  </div>
                  <button
                    onClick={() => setActivatingItem(item)}
                    className="shrink-0 px-4 py-2 rounded-2xl bg-gradient-to-r from-orange-400 to-orange-600 text-white text-xs font-black shadow-lg shadow-orange-500/20 hover:-translate-y-0.5 transition"
                  >
                    激活
                  </button>
                </div>
              ))}
            </section>
          )}

          {/* unused quota items */}
          {unusedQuotaItems.length > 0 && (
            <section className="space-y-3">
              <h2 className="text-sm font-semibold text-foreground flex items-center gap-2">
                <CreditCard className="w-4 h-4 text-blue-400" />未使用额度
              </h2>
              {unusedQuotaItems.map(item => {
                const amount = item.metadata?.quota_amount as number | undefined;
                return (
                  <div key={item.id} className="backpack-item-card flex items-center gap-3 p-4 rounded-[1.5rem]">
                    <div className="grid h-12 w-12 shrink-0 place-items-center rounded-2xl bg-cyan-100 text-cyan-700 ring-1 ring-cyan-200">
                      <CreditCard className="w-6 h-6" />
                    </div>
                    <div className="flex-1 min-w-0">
                      <p className="text-sm font-medium text-foreground">{item.prize_name}</p>
                      <p className="text-xs text-muted-foreground">
                        {amount ? `${amount} 次用量` : "额度道具"} · {new Date(item.created_at).toLocaleDateString("zh-CN")}
                      </p>
                    </div>
                    <button
                      onClick={() => setRedeemingItem(item)}
                      className="shrink-0 px-4 py-2 rounded-2xl bg-cyan-600 text-white text-xs font-black shadow-lg shadow-cyan-500/20 hover:bg-cyan-700 transition"
                    >
                      使用
                    </button>
                  </div>
                );
              })}
            </section>
          )}

          {/* active pokeballs */}
          {activePokeballs.length > 0 && (
            <section className="space-y-3">
              <h2 className="text-sm font-semibold text-foreground flex items-center gap-2">
                <span className="text-base">🔮</span>我的宝可梦球
                <span className="text-xs text-muted-foreground font-normal">· 请求将在成员 Key 间轮询分发</span>
              </h2>
              {activePokeballs.map(pb => (
                <PokeballCard key={pb.id} pb={pb} ownerKey={ownerKey} onRefresh={refetch} />
              ))}
            </section>
          )}

          {/* other used items */}
          {usedItems.filter(i => !/宝可梦/.test(i.prize_name)).length > 0 && (
            <section className="space-y-2">
              <h2 className="text-sm font-semibold text-muted-foreground">其他奖品记录</h2>
              {usedItems.filter(i => !/宝可梦/.test(i.prize_name)).map(item => (
                <div key={item.id} className="backpack-item-card flex items-center gap-3 p-4 rounded-[1.5rem] opacity-70">
                  {isQuota(item.prize_name)
                    ? <CreditCard className="w-5 h-5 text-blue-400 shrink-0" />
                    : <div className="text-lg">🎫</div>
                  }
                  <div className="flex-1 min-w-0">
                    <p className="text-sm text-foreground">{item.prize_name}</p>
                    <p className="text-xs text-muted-foreground">已使用 · {new Date(item.created_at).toLocaleDateString("zh-CN")}</p>
                  </div>
                  <span className="text-xs text-muted-foreground px-2 py-0.5 rounded-full border border-border">已用</span>
                </div>
              ))}
            </section>
          )}

          {unusedPokeballs.length === 0 && unusedQuotaItems.length === 0 && activePokeballs.length === 0 && usedItems.length === 0 && (
            <div className="premium-surface text-center py-14 text-muted-foreground rounded-[2rem]">
              <div className="backpack-orb mx-auto mb-4 grid h-14 w-14 place-items-center rounded-3xl">
                <Package className="w-7 h-7 text-white" />
              </div>
              <p className="text-sm font-black text-foreground">背包空空如也</p>
              <p className="text-xs mt-1">去橘子机抽一个宝可梦球吧！</p>
            </div>
          )}
        </>
      )}
    </div>
  );
}
