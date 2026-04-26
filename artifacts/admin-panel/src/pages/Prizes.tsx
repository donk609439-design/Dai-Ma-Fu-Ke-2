import { useState } from "react";
import { useQuery, useMutation, useQueryClient } from "@tanstack/react-query";
import { adminFetch } from "@/lib/admin-auth";
import { useToast } from "@/hooks/use-toast";
import { Plus, Pencil, Trash2, ToggleLeft, ToggleRight, Gift } from "lucide-react";

interface Prize {
  id: number;
  name: string;
  quantity: number;
  weight: number;
  is_active: boolean;
  created_at: string;
}

const SEGMENT_COLORS = [
  "#ef4444","#f97316","#eab308","#22c55e",
  "#06b6d4","#6366f1","#8b5cf6","#ec4899",
];

const emptyForm = { name: "", quantity: "-1", weight: "10" };

function PrizeModal({
  prize,
  totalWeight,
  onClose,
  onSave,
  saving,
}: {
  prize: Partial<Prize> | null;
  totalWeight: number;
  onClose: () => void;
  onSave: (data: { name: string; quantity: number; weight: number }) => void;
  saving: boolean;
}) {
  const [form, setForm] = useState({
    name: prize?.name ?? "",
    quantity: prize?.quantity !== undefined ? String(prize.quantity) : "-1",
    weight: prize?.weight !== undefined ? String(prize.weight) : "10",
  });

  const w = Math.max(1, parseInt(form.weight) || 1);
  const base = prize?.id ? totalWeight : totalWeight + w;
  const pct = base > 0 ? ((w / base) * 100).toFixed(1) : "0.0";

  const submit = (e: React.FormEvent) => {
    e.preventDefault();
    onSave({ name: form.name.trim(), quantity: parseInt(form.quantity), weight: w });
  };

  return (
    <div className="fixed inset-0 bg-black/60 flex items-center justify-center z-50">
      <div className="bg-card border border-border rounded-xl p-6 w-full max-w-md shadow-2xl">
        <h2 className="text-lg font-bold mb-5">{prize?.id ? "编辑奖品" : "新增奖品"}</h2>
        <form onSubmit={submit} className="space-y-4">
          <div>
            <label className="block text-sm font-medium text-muted-foreground mb-1">奖品名称</label>
            <input
              className="w-full bg-background border border-border rounded-lg px-3 py-2 text-sm focus:outline-none focus:ring-2 focus:ring-primary/50"
              value={form.name}
              onChange={e => setForm(f => ({ ...f, name: e.target.value }))}
              placeholder="例：API 月卡"
              required
            />
          </div>
          <div>
            <label className="block text-sm font-medium text-muted-foreground mb-1">
              库存数量 <span className="text-xs opacity-60">（-1 = 无限）</span>
            </label>
            <input
              type="number"
              className="w-full bg-background border border-border rounded-lg px-3 py-2 text-sm focus:outline-none focus:ring-2 focus:ring-primary/50"
              value={form.quantity}
              onChange={e => setForm(f => ({ ...f, quantity: e.target.value }))}
              min={-1}
            />
          </div>
          <div>
            <label className="block text-sm font-medium text-muted-foreground mb-1">
              概率权重 <span className="text-xs opacity-60">（数值越大中奖率越高）</span>
            </label>
            <input
              type="number"
              className="w-full bg-background border border-border rounded-lg px-3 py-2 text-sm focus:outline-none focus:ring-2 focus:ring-primary/50"
              value={form.weight}
              onChange={e => setForm(f => ({ ...f, weight: e.target.value }))}
              min={1}
            />
            <p className="text-xs text-muted-foreground mt-1">
              预计中奖率约 <span className="text-primary font-semibold">{pct}%</span>
            </p>
          </div>
          <div className="flex gap-3 pt-2">
            <button
              type="button"
              onClick={onClose}
              className="flex-1 py-2 rounded-lg border border-border text-sm font-medium hover:bg-muted/30 transition-colors"
            >
              取消
            </button>
            <button
              type="submit"
              disabled={saving}
              className="flex-1 py-2 rounded-lg bg-primary text-primary-foreground text-sm font-medium hover:bg-primary/90 transition-colors disabled:opacity-50"
            >
              {saving ? "保存中…" : "保存"}
            </button>
          </div>
        </form>
      </div>
    </div>
  );
}

const emptyAward = { dc_identifier: "", prize_name: "", custom: "" };

function AwardPanel({ prizes }: { prizes: Prize[] }) {
  const { toast } = useToast();
  const [form, setForm] = useState(emptyAward);
  const [useCustom, setUseCustom] = useState(false);
  const [broadcastAll, setBroadcastAll] = useState(false);
  const [confirmOpen, setConfirmOpen] = useState(false);

  const getPrizeName = () => (useCustom ? form.custom.trim() : form.prize_name);

  const awardMutation = useMutation({
    mutationFn: async () => {
      const prize_name = getPrizeName();
      if (!prize_name) throw new Error("请填写奖品名称");
      if (broadcastAll) {
        const res = await adminFetch("/admin/award-prize-all", {
          method: "POST",
          headers: { "Content-Type": "application/json" },
          body: JSON.stringify({ prize_name }),
        });
        if (!res.ok) {
          const err = await res.json().catch(() => ({}));
          throw new Error(err.detail || "群发失败");
        }
        return res.json();
      } else {
        const dc_identifier = form.dc_identifier.trim();
        if (!dc_identifier) throw new Error("请填写 DC 账号");
        const res = await adminFetch("/admin/award-prize", {
          method: "POST",
          headers: { "Content-Type": "application/json" },
          body: JSON.stringify({ dc_identifier, prize_name }),
        });
        if (!res.ok) {
          const err = await res.json().catch(() => ({}));
          throw new Error(err.detail || "发奖失败");
        }
        return res.json();
      }
    },
    onSuccess: (data) => {
      if (broadcastAll) {
        toast({ title: "群发成功", description: `已向全体 ${data.awarded_count} 名用户发放「${data.prize_name}」` });
      } else {
        const display = data.dc_tag || data.owner_key;
        toast({ title: "发奖成功", description: `已向 ${display} 发放「${data.prize_name}」` });
      }
      setForm(emptyAward);
      setConfirmOpen(false);
    },
    onError: (e: Error) => {
      setConfirmOpen(false);
      toast({ title: broadcastAll ? "群发失败" : "发奖失败", description: e.message, variant: "destructive" });
    },
  });

  const handleSubmit = () => {
    if (broadcastAll) {
      setConfirmOpen(true);
    } else {
      awardMutation.mutate();
    }
  };

  return (
    <div className="bg-card border border-border rounded-xl p-5">
      {/* 二次确认对话框 */}
      {confirmOpen && (
        <div className="fixed inset-0 bg-black/60 flex items-center justify-center z-50">
          <div className="bg-card border border-border rounded-xl p-6 w-full max-w-sm shadow-2xl">
            <h3 className="text-base font-bold mb-2">确认群发</h3>
            <p className="text-sm text-muted-foreground mb-4">
              将向<span className="text-foreground font-semibold">所有已注册用户</span>发放「{getPrizeName()}」，此操作不可撤销。
            </p>
            <div className="flex gap-2">
              <button
                onClick={() => setConfirmOpen(false)}
                className="flex-1 py-2 rounded-lg border border-border text-sm hover:bg-muted/40 transition-colors"
              >
                取消
              </button>
              <button
                onClick={() => awardMutation.mutate()}
                disabled={awardMutation.isPending}
                className="flex-1 py-2 rounded-lg bg-rose-600 text-white text-sm font-medium hover:bg-rose-700 transition-colors disabled:opacity-50"
              >
                {awardMutation.isPending ? "发放中…" : "确认群发"}
              </button>
            </div>
          </div>
        </div>
      )}

      <div className="flex items-center justify-between mb-4">
        <div className="flex items-center gap-2">
          <Gift className="w-4 h-4 text-primary" />
          <h2 className="text-sm font-semibold">手动发奖</h2>
        </div>
        <button
          type="button"
          onClick={() => { setBroadcastAll(v => !v); setForm(emptyAward); }}
          className={`text-xs px-2.5 py-1 rounded-full border transition-colors ${
            broadcastAll
              ? "bg-rose-500/15 border-rose-500/50 text-rose-400 font-semibold"
              : "border-border text-muted-foreground hover:text-foreground"
          }`}
        >
          {broadcastAll ? "给所有人发奖 ✓" : "给所有人发奖"}
        </button>
      </div>

      <div className="grid grid-cols-1 sm:grid-cols-3 gap-3">
        {!broadcastAll && (
          <div>
            <label className="block text-xs font-medium text-muted-foreground mb-1">
              DC 账号 <span className="text-[10px] opacity-60">（用户名 或 Discord 用户ID）</span>
            </label>
            <input
              className="w-full bg-background border border-border rounded-lg px-3 py-2 text-sm focus:outline-none focus:ring-2 focus:ring-primary/50"
              placeholder="例：tomzhanggg 或 1234567890"
              value={form.dc_identifier}
              onChange={e => setForm(f => ({ ...f, dc_identifier: e.target.value }))}
            />
          </div>
        )}
        <div className={broadcastAll ? "sm:col-span-2" : ""}>
          <div className="flex items-center justify-between mb-1">
            <label className="text-xs font-medium text-muted-foreground">奖品</label>
            <button
              type="button"
              onClick={() => { setUseCustom(v => !v); setForm(f => ({ ...f, prize_name: "", custom: "" })); }}
              className="text-xs text-primary hover:underline"
            >
              {useCustom ? "从列表选择" : "自定义名称"}
            </button>
          </div>
          {useCustom ? (
            <input
              className="w-full bg-background border border-border rounded-lg px-3 py-2 text-sm focus:outline-none focus:ring-2 focus:ring-primary/50"
              placeholder="自定义奖品名称"
              value={form.custom}
              onChange={e => setForm(f => ({ ...f, custom: e.target.value }))}
            />
          ) : (
            <select
              className="w-full bg-background border border-border rounded-lg px-3 py-2 text-sm focus:outline-none focus:ring-2 focus:ring-primary/50"
              value={form.prize_name}
              onChange={e => setForm(f => ({ ...f, prize_name: e.target.value }))}
            >
              <option value="">选择奖品…</option>
              {prizes.map(p => (
                <option key={p.id} value={p.name}>{p.name}{!p.is_active ? "（已禁用）" : ""}</option>
              ))}
            </select>
          )}
        </div>
        <div className="flex items-end">
          <button
            onClick={handleSubmit}
            disabled={awardMutation.isPending}
            className={`w-full py-2 rounded-lg text-sm font-medium transition-colors disabled:opacity-50 ${
              broadcastAll
                ? "bg-rose-600 text-white hover:bg-rose-700"
                : "bg-primary text-primary-foreground hover:bg-primary/90"
            }`}
          >
            {awardMutation.isPending ? "发放中…" : broadcastAll ? "群发给所有人" : "发放奖品"}
          </button>
        </div>
      </div>
    </div>
  );
}

export default function Prizes() {
  const qc = useQueryClient();
  const { toast } = useToast();
  const [modal, setModal] = useState<"add" | Prize | null>(null);
  const [deletingId, setDeletingId] = useState<number | null>(null);

  const { data: prizes = [], isLoading } = useQuery<Prize[]>({
    queryKey: ["prizes"],
    queryFn: async () => {
      const res = await adminFetch("/admin/prizes");
      if (!res.ok) throw new Error("获取失败");
      return res.json();
    },
  });

  const totalWeight = prizes.filter(p => p.is_active).reduce((s, p) => s + p.weight, 0);

  const createMutation = useMutation({
    mutationFn: async (data: object) => {
      const res = await adminFetch("/admin/prizes", {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify(data),
      });
      if (!res.ok) throw new Error((await res.json()).detail || "创建失败");
      return res.json();
    },
    onSuccess: () => { qc.invalidateQueries({ queryKey: ["prizes"] }); setModal(null); toast({ title: "奖品已添加" }); },
    onError: (e: Error) => toast({ title: "添加失败", description: e.message, variant: "destructive" }),
  });

  const updateMutation = useMutation({
    mutationFn: async ({ id, ...data }: { id: number } & object) => {
      const res = await adminFetch(`/admin/prizes/${id}`, {
        method: "PUT",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify(data),
      });
      if (!res.ok) throw new Error((await res.json()).detail || "更新失败");
      return res.json();
    },
    onSuccess: () => { qc.invalidateQueries({ queryKey: ["prizes"] }); setModal(null); toast({ title: "已更新" }); },
    onError: (e: Error) => toast({ title: "更新失败", description: e.message, variant: "destructive" }),
  });

  const deleteMutation = useMutation({
    mutationFn: async (id: number) => {
      const res = await adminFetch(`/admin/prizes/${id}`, { method: "DELETE" });
      if (!res.ok) throw new Error("删除失败");
    },
    onSuccess: () => { qc.invalidateQueries({ queryKey: ["prizes"] }); setDeletingId(null); toast({ title: "已删除" }); },
    onError: (e: Error) => toast({ title: "删除失败", description: e.message, variant: "destructive" }),
  });

  const toggleMutation = useMutation({
    mutationFn: async ({ id, is_active }: { id: number; is_active: boolean }) => {
      const res = await adminFetch(`/admin/prizes/${id}`, {
        method: "PUT",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({ is_active }),
      });
      if (!res.ok) throw new Error("切换失败");
      return { id, is_active };
    },
    // 乐观更新：立即翻转状态，失败时回滚
    onMutate: async ({ id, is_active }) => {
      await qc.cancelQueries({ queryKey: ["prizes"] });
      const prev = qc.getQueryData<Prize[]>(["prizes"]);
      qc.setQueryData<Prize[]>(["prizes"], old =>
        old?.map(p => p.id === id ? { ...p, is_active } : p)
      );
      return { prev };
    },
    onError: (e: Error, _v, ctx) => {
      if (ctx?.prev) qc.setQueryData(["prizes"], ctx.prev);
      toast({ title: "操作失败", description: e.message, variant: "destructive" });
    },
    onSettled: () => qc.invalidateQueries({ queryKey: ["prizes"] }),
  });

  const handleSave = (data: { name: string; quantity: number; weight: number }) => {
    if (modal === "add") {
      createMutation.mutate(data);
    } else if (modal && typeof modal === "object") {
      updateMutation.mutate({ id: modal.id, ...data });
    }
  };

  const saving = createMutation.isPending || updateMutation.isPending;

  return (
    <div className="space-y-6">
      <div className="flex items-center justify-between">
        <div>
          <h1 className="text-2xl font-bold">奖品管理</h1>
          <p className="text-sm text-muted-foreground mt-1">管理橘子机抽奖轮盘上的奖品</p>
        </div>
        <button
          onClick={() => setModal("add")}
          className="flex items-center gap-2 px-4 py-2 bg-primary text-primary-foreground rounded-lg text-sm font-medium hover:bg-primary/90 transition-colors"
        >
          <Plus className="w-4 h-4" /> 新增奖品
        </button>
      </div>

      {/* Stats bar */}
      <div className="grid grid-cols-3 gap-4">
        {[
          { label: "奖品总数", value: prizes.length },
          { label: "启用中", value: prizes.filter(p => p.is_active).length },
          { label: "总权重", value: totalWeight },
        ].map(({ label, value }) => (
          <div key={label} className="bg-card border border-border rounded-xl p-4 text-center">
            <p className="text-2xl font-bold text-primary">{value}</p>
            <p className="text-xs text-muted-foreground mt-1">{label}</p>
          </div>
        ))}
      </div>

      {/* Probability visualization */}
      {prizes.filter(p => p.is_active).length > 0 && (
        <div className="bg-card border border-border rounded-xl p-4">
          <p className="text-sm font-medium mb-3 text-muted-foreground">概率分布预览</p>
          <div className="flex rounded-lg overflow-hidden h-6">
            {prizes.filter(p => p.is_active).map((p, i) => (
              <div
                key={p.id}
                style={{ width: `${(p.weight / totalWeight) * 100}%`, background: SEGMENT_COLORS[i % SEGMENT_COLORS.length] }}
                title={`${p.name}: ${((p.weight / totalWeight) * 100).toFixed(1)}%`}
                className="transition-all"
              />
            ))}
          </div>
          <div className="flex flex-wrap gap-x-4 gap-y-1 mt-2">
            {prizes.filter(p => p.is_active).map((p, i) => (
              <span key={p.id} className="flex items-center gap-1.5 text-xs text-muted-foreground">
                <span className="w-2.5 h-2.5 rounded-full inline-block" style={{ background: SEGMENT_COLORS[i % SEGMENT_COLORS.length] }} />
                {p.name} {((p.weight / totalWeight) * 100).toFixed(1)}%
              </span>
            ))}
          </div>
        </div>
      )}

      {/* Table */}
      <div className="bg-card border border-border rounded-xl overflow-hidden">
        {isLoading ? (
          <div className="p-10 text-center text-muted-foreground text-sm">加载中…</div>
        ) : prizes.length === 0 ? (
          <div className="p-10 text-center text-muted-foreground text-sm">暂无奖品，点击「新增奖品」开始配置</div>
        ) : (
          <table className="w-full text-sm">
            <thead>
              <tr className="border-b border-border bg-muted/20">
                <th className="text-left px-4 py-3 font-medium text-muted-foreground">奖品名称</th>
                <th className="text-center px-4 py-3 font-medium text-muted-foreground">库存</th>
                <th className="text-center px-4 py-3 font-medium text-muted-foreground">权重</th>
                <th className="text-center px-4 py-3 font-medium text-muted-foreground">中奖率</th>
                <th className="text-center px-4 py-3 font-medium text-muted-foreground">状态</th>
                <th className="text-center px-4 py-3 font-medium text-muted-foreground">操作</th>
              </tr>
            </thead>
            <tbody className="divide-y divide-border">
              {prizes.map((prize, i) => {
                const pct = totalWeight > 0 ? ((prize.weight / totalWeight) * 100).toFixed(1) : "0.0";
                return (
                  <tr key={prize.id} className="hover:bg-muted/10 transition-colors">
                    <td className="px-4 py-3">
                      <div className="flex items-center gap-2">
                        <span
                          className="w-3 h-3 rounded-full shrink-0"
                          style={{ background: prize.is_active ? SEGMENT_COLORS[i % SEGMENT_COLORS.length] : "#6b7280" }}
                        />
                        <span className={prize.is_active ? "" : "text-muted-foreground line-through"}>
                          {prize.name}
                        </span>
                      </div>
                    </td>
                    <td className="px-4 py-3 text-center text-muted-foreground">
                      {prize.quantity === -1 ? <span className="text-xs bg-muted/40 px-2 py-0.5 rounded">无限</span> : prize.quantity}
                    </td>
                    <td className="px-4 py-3 text-center font-mono text-xs">{prize.weight}</td>
                    <td className="px-4 py-3 text-center">
                      <span className={`text-xs font-semibold ${prize.is_active ? "text-primary" : "text-muted-foreground"}`}>
                        {prize.is_active ? `${pct}%` : "—"}
                      </span>
                    </td>
                    <td className="px-4 py-3 text-center">
                      <button
                        onClick={() => toggleMutation.mutate({ id: prize.id, is_active: !prize.is_active })}
                        className="transition-colors"
                        title={prize.is_active ? "点击禁用" : "点击启用"}
                      >
                        {prize.is_active
                          ? <ToggleRight className="w-6 h-6 text-primary" />
                          : <ToggleLeft className="w-6 h-6 text-muted-foreground" />}
                      </button>
                    </td>
                    <td className="px-4 py-3">
                      <div className="flex items-center justify-center gap-2">
                        <button
                          onClick={() => setModal(prize)}
                          className="p-1.5 rounded hover:bg-muted/40 text-muted-foreground hover:text-foreground transition-colors"
                          title="编辑"
                        >
                          <Pencil className="w-3.5 h-3.5" />
                        </button>
                        <button
                          onClick={() => setDeletingId(prize.id)}
                          className="p-1.5 rounded hover:bg-red-500/10 text-muted-foreground hover:text-red-400 transition-colors"
                          title="删除"
                        >
                          <Trash2 className="w-3.5 h-3.5" />
                        </button>
                      </div>
                    </td>
                  </tr>
                );
              })}
            </tbody>
          </table>
        )}
      </div>

      {/* Manual Award */}
      <AwardPanel prizes={prizes} />

      {/* Add/Edit Modal */}
      {modal !== null && (
        <PrizeModal
          prize={modal === "add" ? {} : modal}
          totalWeight={totalWeight}
          onClose={() => setModal(null)}
          onSave={handleSave}
          saving={saving}
        />
      )}

      {/* Delete Confirm */}
      {deletingId !== null && (
        <div className="fixed inset-0 bg-black/60 flex items-center justify-center z-50">
          <div className="bg-card border border-border rounded-xl p-6 w-80 shadow-2xl">
            <h2 className="text-lg font-bold mb-2">确认删除</h2>
            <p className="text-sm text-muted-foreground mb-5">删除后奖品将从轮盘移除，此操作不可恢复。</p>
            <div className="flex gap-3">
              <button onClick={() => setDeletingId(null)} className="flex-1 py-2 rounded-lg border border-border text-sm font-medium hover:bg-muted/30">取消</button>
              <button
                onClick={() => deleteMutation.mutate(deletingId!)}
                disabled={deleteMutation.isPending}
                className="flex-1 py-2 rounded-lg bg-red-500 hover:bg-red-600 text-white text-sm font-medium disabled:opacity-50"
              >
                {deleteMutation.isPending ? "删除中…" : "确认删除"}
              </button>
            </div>
          </div>
        </div>
      )}
    </div>
  );
}
