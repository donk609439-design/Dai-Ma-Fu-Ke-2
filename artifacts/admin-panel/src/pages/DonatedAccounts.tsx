import { useState } from "react";
import { useQuery, useQueryClient } from "@tanstack/react-query";
import { getAdminKey } from "@/lib/admin-auth";
import { useToast } from "@/hooks/use-toast";
import {
  ShieldCheck, ChevronDown, ChevronUp, Check, X, Clock, Copy, CheckCircle2,
} from "lucide-react";
import { cn } from "@/lib/utils";

interface DonatedEntry {
  id: number;
  jb_email: string;
  jb_password: string;
  dc_tag: string;
  submitted_at: string | null;
  reviewed_at: string | null;
  admin_used: boolean;
  admin_used_at: string | null;
}

interface DonatedAccountsData {
  pending: DonatedEntry[];
  approved: DonatedEntry[];
}

function fmt(iso: string | null) {
  if (!iso) return "—";
  return new Date(iso).toLocaleString("zh-CN", { hour12: false });
}

function CopyButton({ text }: { text: string }) {
  const [copied, setCopied] = useState(false);
  return (
    <button
      onClick={() => {
        navigator.clipboard.writeText(text).then(() => {
          setCopied(true);
          setTimeout(() => setCopied(false), 2000);
        });
      }}
      className="p-1 rounded text-muted-foreground hover:text-foreground transition-colors"
    >
      {copied
        ? <Check className="w-3.5 h-3.5 text-green-500" />
        : <Copy className="w-3.5 h-3.5" />}
    </button>
  );
}

function Section({
  label,
  count,
  defaultOpen,
  children,
}: {
  label: string;
  count: number;
  defaultOpen?: boolean;
  children: React.ReactNode;
}) {
  const [open, setOpen] = useState(defaultOpen ?? false);
  return (
    <div className="rounded-xl border overflow-hidden">
      <button
        onClick={() => setOpen((v) => !v)}
        className="w-full flex items-center justify-between px-4 py-3 bg-muted/40 hover:bg-muted/60 transition-colors text-left"
      >
        <span className="text-sm font-semibold">
          {label}
          <span className="ml-2 text-xs font-normal text-muted-foreground">({count})</span>
        </span>
        {open ? <ChevronUp className="w-4 h-4 text-muted-foreground" /> : <ChevronDown className="w-4 h-4 text-muted-foreground" />}
      </button>
      {open && <div className="divide-y">{children}</div>}
    </div>
  );
}

function EntryRow({
  entry,
  showActions,
  showUsedButton,
  onApprove,
  onReject,
  onMarkUsed,
  approving,
  rejecting,
  markingUsed,
}: {
  entry: DonatedEntry;
  showActions: boolean;
  showUsedButton?: boolean;
  onApprove?: () => void;
  onReject?: () => void;
  onMarkUsed?: () => void;
  approving?: boolean;
  rejecting?: boolean;
  markingUsed?: boolean;
}) {
  return (
    <div className={cn(
      "px-4 py-3 flex items-center gap-3 text-sm transition-colors",
      entry.admin_used && "bg-muted/20",
    )}>
      <div className="flex-1 min-w-0 space-y-0.5">
        <div className="flex items-center gap-1.5 flex-wrap">
          <span className={cn(
            "font-mono text-xs bg-muted px-1.5 py-0.5 rounded",
            entry.admin_used && "opacity-50",
          )}>{entry.jb_email}</span>
          <CopyButton text={entry.jb_email} />
          <span className="text-muted-foreground">/</span>
          <span className={cn(
            "font-mono text-xs bg-muted px-1.5 py-0.5 rounded",
            entry.admin_used && "opacity-50",
          )}>{entry.jb_password}</span>
          <CopyButton text={entry.jb_password} />
          {entry.admin_used && (
            <span className="flex items-center gap-1 text-[10px] font-medium text-green-600 dark:text-green-400 bg-green-100 dark:bg-green-900/30 px-1.5 py-0.5 rounded-full">
              <CheckCircle2 className="w-3 h-3" />
              已使用
            </span>
          )}
        </div>
        <div className="flex items-center gap-3 text-xs text-muted-foreground flex-wrap">
          <span>DC: {entry.dc_tag || "—"}</span>
          <span className="flex items-center gap-1">
            <Clock className="w-3 h-3" />
            {fmt(entry.submitted_at)}
          </span>
          {entry.reviewed_at && (
            <span>审核: {fmt(entry.reviewed_at)}</span>
          )}
          {entry.admin_used && entry.admin_used_at && (
            <span className="text-green-600 dark:text-green-400">
              使用于: {fmt(entry.admin_used_at)}
            </span>
          )}
        </div>
      </div>
      <div className="flex items-center gap-2 shrink-0">
        {showUsedButton && !entry.admin_used && (
          <button
            onClick={onMarkUsed}
            disabled={markingUsed}
            className={cn(
              "flex items-center gap-1 px-2.5 py-1.5 rounded-lg text-xs font-medium transition-colors",
              "bg-blue-100 text-blue-700 hover:bg-blue-200 dark:bg-blue-900/30 dark:text-blue-400 dark:hover:bg-blue-900/50",
              "disabled:opacity-50",
            )}
            title="标记此邮密已被你使用"
          >
            <CheckCircle2 className="w-3.5 h-3.5" />
            {markingUsed ? "标记中…" : "已使用"}
          </button>
        )}
        {showActions && (
          <>
            <button
              onClick={onApprove}
              disabled={approving || rejecting}
              className={cn(
                "flex items-center gap-1 px-2.5 py-1.5 rounded-lg text-xs font-medium transition-colors",
                "bg-green-100 text-green-700 hover:bg-green-200 dark:bg-green-900/30 dark:text-green-400 dark:hover:bg-green-900/50",
                "disabled:opacity-50",
              )}
            >
              <Check className="w-3.5 h-3.5" />
              {approving ? "处理中…" : "通过"}
            </button>
            <button
              onClick={onReject}
              disabled={approving || rejecting}
              className={cn(
                "flex items-center gap-1 px-2.5 py-1.5 rounded-lg text-xs font-medium transition-colors",
                "bg-red-100 text-red-700 hover:bg-red-200 dark:bg-red-900/30 dark:text-red-400 dark:hover:bg-red-900/50",
                "disabled:opacity-50",
              )}
            >
              <X className="w-3.5 h-3.5" />
              {rejecting ? "处理中…" : "拒绝"}
            </button>
          </>
        )}
      </div>
    </div>
  );
}

export default function DonatedAccounts() {
  const { toast } = useToast();
  const qc = useQueryClient();
  const adminKey = getAdminKey();

  const [loadingId, setLoadingId] = useState<{ id: number; action: "approve" | "reject" | "mark-used" } | null>(null);

  const { data, isLoading, isError } = useQuery<DonatedAccountsData>({
    queryKey: ["donate-accounts"],
    queryFn: async () => {
      const res = await fetch("/admin/donate-accounts", {
        headers: { "X-Admin-Key": adminKey ?? "" },
      });
      if (!res.ok) throw new Error("获取失败");
      return res.json();
    },
    refetchInterval: 30_000,
  });

  async function handleApprove(id: number) {
    setLoadingId({ id, action: "approve" });
    try {
      const res = await fetch(`/admin/donate-accounts/${id}/approve`, {
        method: "POST",
        headers: { "X-Admin-Key": adminKey ?? "" },
      });
      const body = await res.json();
      if (!res.ok) throw new Error(body.detail || "操作失败");
      toast({ title: "审核通过", description: body.message });
      qc.invalidateQueries({ queryKey: ["donate-accounts"] });
    } catch (err: any) {
      toast({ title: "操作失败", description: err.message, variant: "destructive" });
    } finally {
      setLoadingId(null);
    }
  }

  async function handleReject(id: number) {
    setLoadingId({ id, action: "reject" });
    try {
      const res = await fetch(`/admin/donate-accounts/${id}/reject`, {
        method: "DELETE",
        headers: { "X-Admin-Key": adminKey ?? "" },
      });
      if (!res.ok) {
        const body = await res.json().catch(() => ({}));
        throw new Error(body.detail || "操作失败");
      }
      toast({ title: "已拒绝", description: "记录已删除" });
      qc.invalidateQueries({ queryKey: ["donate-accounts"] });
    } catch (err: any) {
      toast({ title: "操作失败", description: err.message, variant: "destructive" });
    } finally {
      setLoadingId(null);
    }
  }

  async function handleMarkUsed(id: number) {
    setLoadingId({ id, action: "mark-used" });
    try {
      const res = await fetch(`/admin/donate-accounts/${id}/mark-used`, {
        method: "POST",
        headers: { "X-Admin-Key": adminKey ?? "" },
      });
      const body = await res.json().catch(() => ({}));
      if (!res.ok) throw new Error(body.detail || "操作失败");
      toast({
        title: "已标记为已使用",
        description: body.already_marked ? "该记录之前已被标记过" : "标记成功",
      });
      qc.invalidateQueries({ queryKey: ["donate-accounts"] });
    } catch (err: any) {
      toast({ title: "标记失败", description: err.message, variant: "destructive" });
    } finally {
      setLoadingId(null);
    }
  }

  const usedCount = data?.approved.filter(e => e.admin_used).length ?? 0;

  return (
    <div className="space-y-6">
      <div className="flex items-center gap-3">
        <div className="flex items-center justify-center w-10 h-10 rounded-xl bg-emerald-100 dark:bg-emerald-900/30">
          <ShieldCheck className="w-5 h-5 text-emerald-600 dark:text-emerald-400" />
        </div>
        <div>
          <h1 className="text-lg font-semibold">后备隐藏能源</h1>
          <p className="text-sm text-muted-foreground">用户捐献的 R一串账号邮密，审核通过自动奖励 10 圣人点数</p>
        </div>
      </div>

      {isLoading && (
        <div className="text-sm text-muted-foreground py-8 text-center">加载中…</div>
      )}
      {isError && (
        <div className="text-sm text-destructive py-8 text-center">加载失败，请刷新页面</div>
      )}

      {data && (
        <div className="space-y-3">
          <Section label="待验证" count={data.pending.length}>
            {data.pending.length === 0 ? (
              <div className="px-4 py-6 text-sm text-muted-foreground text-center">暂无待审核记录</div>
            ) : (
              data.pending.map((entry) => (
                <EntryRow
                  key={entry.id}
                  entry={entry}
                  showActions
                  onApprove={() => handleApprove(entry.id)}
                  onReject={() => handleReject(entry.id)}
                  approving={loadingId?.id === entry.id && loadingId.action === "approve"}
                  rejecting={loadingId?.id === entry.id && loadingId.action === "reject"}
                />
              ))
            )}
          </Section>

          <Section
            label={`已验证${usedCount > 0 ? `（已使用 ${usedCount}/${data.approved.length}）` : ""}`}
            count={data.approved.length}
          >
            {data.approved.length === 0 ? (
              <div className="px-4 py-6 text-sm text-muted-foreground text-center">暂无已审核记录</div>
            ) : (
              data.approved.map((entry) => (
                <EntryRow
                  key={entry.id}
                  entry={entry}
                  showActions={false}
                  showUsedButton
                  onMarkUsed={() => handleMarkUsed(entry.id)}
                  markingUsed={loadingId?.id === entry.id && loadingId.action === "mark-used"}
                />
              ))
            )}
          </Section>
        </div>
      )}
    </div>
  );
}
