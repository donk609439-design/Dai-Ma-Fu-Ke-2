import { type ReactNode, useState, useRef, useCallback, useEffect } from "react";
import { useQuery } from "@tanstack/react-query";
import {
  X,
  Gift,
  Check,
  Sparkles,
  KeyRound,
  LogIn,
  LogOut,
  Trophy,
  ShieldCheck,
  Star,
  ArrowRight,
  Loader2,
} from "lucide-react";
import { useDiscordAuth } from "@/hooks/useDiscordAuth";

interface Prize {
  id: number;
  name: string;
  quantity: number;
  weight: number;
}

const FALLBACK_SEGMENTS = [
  { prize: "敬请期待", bg: "#f7f2ea" },
  { prize: "敬请期待", bg: "#e8ded1" },
  { prize: "敬请期待", bg: "#d8c7b5" },
  { prize: "敬请期待", bg: "#f2eadf" },
  { prize: "敬请期待", bg: "#cfc1b0" },
  { prize: "敬请期待", bg: "#eee4d8" },
  { prize: "敬请期待", bg: "#ddcfbf" },
  { prize: "敬请期待", bg: "#f5efe6" },
];

const SEG_COLORS = [
  "#f7f2ea",
  "#e8ded1",
  "#d8c7b5",
  "#f2eadf",
  "#cfc1b0",
  "#eee4d8",
  "#ddcfbf",
  "#f5efe6",
];

function hexToRgb(hex: string) {
  const normalized = hex.replace("#", "");
  const value = parseInt(normalized, 16);
  return {
    r: (value >> 16) & 255,
    g: (value >> 8) & 255,
    b: value & 255,
  };
}

function mixHex(hex: string, target: "white" | "black", amount: number) {
  const rgb = hexToRgb(hex);
  const to = target === "white" ? 255 : 0;
  const mixed = {
    r: Math.round(rgb.r + (to - rgb.r) * amount),
    g: Math.round(rgb.g + (to - rgb.g) * amount),
    b: Math.round(rgb.b + (to - rgb.b) * amount),
  };
  return `rgb(${mixed.r}, ${mixed.g}, ${mixed.b})`;
}

const CX = 160;
const CY = 160;
const R = 148;

function toRad(deg: number) {
  return (deg * Math.PI) / 180;
}

function sectorPath(startDeg: number, endDeg: number) {
  const sx = CX + R * Math.cos(toRad(startDeg));
  const sy = CY + R * Math.sin(toRad(startDeg));
  const ex = CX + R * Math.cos(toRad(endDeg));
  const ey = CY + R * Math.sin(toRad(endDeg));
  return `M ${CX} ${CY} L ${sx} ${sy} A ${R} ${R} 0 ${endDeg - startDeg > 180 ? 1 : 0} 1 ${ex} ${ey} Z`;
}

function weightedRandom(segments: { weight: number }[]): number {
  const total = segments.reduce((s, p) => s + p.weight, 0);
  let r = Math.random() * total;
  for (let i = 0; i < segments.length; i++) {
    r -= segments[i].weight;
    if (r <= 0) return i;
  }
  return segments.length - 1;
}

function isPokeball(name: string) {
  return /宝可梦/.test(name);
}

/* ── Audio helpers ── */
function getAudioCtx(): AudioContext {
  return new (window.AudioContext || (window as unknown as { webkitAudioContext: typeof AudioContext }).webkitAudioContext)();
}

function playTick(ctx: AudioContext, when: number, vol = 0.08) {
  const osc = ctx.createOscillator();
  const g = ctx.createGain();
  osc.type = "sine";
  osc.frequency.setValueAtTime(420, when);
  osc.frequency.exponentialRampToValueAtTime(180, when + 0.06);
  g.gain.setValueAtTime(vol, when);
  g.gain.exponentialRampToValueAtTime(0.0001, when + 0.08);
  osc.connect(g).connect(ctx.destination);
  osc.start(when);
  osc.stop(when + 0.09);
}

function playSpinSound(duration: number): () => void {
  try {
    const ctx = getAudioCtx();
    for (let i = 0; i < 80; i++) {
      const t = i / 80;
      const time = (duration * (1 - Math.exp(-4 * t))) / (1 - Math.exp(-4));
      playTick(ctx, ctx.currentTime + time, 0.07 * (1 - (time / duration) * 0.5));
    }
    return () => {
      try {
        ctx.close();
      } catch {
        /* ignore */
      }
    };
  } catch {
    return () => {};
  }
}

function playWinSound() {
  try {
    const ctx = getAudioCtx();
    [523.25, 659.25, 783.99, 1046.5].forEach((freq, i) => {
      const osc = ctx.createOscillator();
      const g = ctx.createGain();
      osc.type = "sine";
      osc.frequency.value = freq;
      const start = ctx.currentTime + i * 0.12;
      g.gain.setValueAtTime(0, start);
      g.gain.linearRampToValueAtTime(0.13, start + 0.04);
      g.gain.exponentialRampToValueAtTime(0.0001, start + 0.5);
      osc.connect(g).connect(ctx.destination);
      osc.start(start);
      osc.stop(start + 0.55);
    });
    setTimeout(() => {
      try {
        ctx.close();
      } catch {
        /* ignore */
      }
    }, 1500);
  } catch {
    /* 音频不可用时静默忽略，不影响弹窗逻辑 */
  }
}

function playNoWinSound() {
  try {
    const ctx = getAudioCtx();
    [330, 247].forEach((freq, i) => {
      const osc = ctx.createOscillator();
      const g = ctx.createGain();
      osc.type = "sine";
      osc.frequency.value = freq;
      const start = ctx.currentTime + i * 0.18;
      g.gain.setValueAtTime(0, start);
      g.gain.linearRampToValueAtTime(0.1, start + 0.03);
      g.gain.exponentialRampToValueAtTime(0.0001, start + 0.4);
      osc.connect(g).connect(ctx.destination);
      osc.start(start);
      osc.stop(start + 0.45);
    });
    setTimeout(() => {
      try {
        ctx.close();
      } catch {
        /* ignore */
      }
    }, 800);
  } catch {
    /* 音频不可用时静默忽略 */
  }
}

function isQuota(name: string) {
  return /额度/.test(name);
}

function parseQuotaAmount(name: string): number | null {
  const m = name.match(/(\d+)/);
  return m ? parseInt(m[1], 10) : null;
}

function prizeEmoji(name: string) {
  if (isPokeball(name)) return "⚡";
  if (isQuota(name)) return "💎";
  if (/谢谢|未中|再来/.test(name)) return "🍃";
  return "🍊";
}

function shortenPrize(name: string) {
  return name.length > 7 ? `${name.slice(0, 6)}…` : name;
}

function getApiErrorMessage(data: any, fallback: string) {
  return data?.detail || data?.error?.message || data?.message || fallback;
}

function ModalShell({
  children,
  onClose,
  maxWidth = "max-w-sm",
}: {
  children: ReactNode;
  onClose: () => void;
  maxWidth?: string;
}) {
  return (
    <div className="fixed inset-0 z-50 flex items-center justify-center bg-stone-950/55 p-4 backdrop-blur-md">
      <div className={`relative w-full ${maxWidth} overflow-hidden rounded-[2rem] border border-white/70 bg-white/82 p-5 shadow-2xl shadow-orange-950/20 backdrop-blur-2xl`}>
        <div className="pointer-events-none absolute -right-16 -top-16 h-40 w-40 rounded-full bg-orange-300/30 blur-2xl" />
        <div className="pointer-events-none absolute -bottom-20 -left-16 h-44 w-44 rounded-full bg-cyan-300/20 blur-2xl" />
        <button
          onClick={onClose}
          className="absolute right-4 top-4 z-30 rounded-2xl bg-white/70 p-2 text-stone-500 shadow-sm ring-1 ring-orange-900/10 transition hover:bg-white hover:text-stone-800"
          aria-label="关闭"
          type="button"
        >
          <X className="h-4 w-4" />
        </button>
        <div className="relative z-10">{children}</div>
      </div>
    </div>
  );
}

/* ── Claim modal（仅 Discord 模式）── */
function ClaimModal({
  prize,
  spinToken,
  onClose,
  onSuccess,
  dcToken,
  userTag,
}: {
  prize: string;
  spinToken: string;
  onClose: () => void;
  onSuccess: () => void;
  dcToken: string;
  userTag?: string | null;
}) {
  const [status, setStatus] = useState<"idle" | "loading" | "ok" | "error">("idle");
  const [msg, setMsg] = useState("");

  const doClaim = async () => {
    setStatus("loading");
    try {
      const res = await fetch("/key/dc-claim-prize", {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({ discord_token: dcToken, spin_token: spinToken }),
      });
      if (res.ok) {
        setStatus("ok");
        setMsg("奖品已存入您的 Discord 背包！");
        onSuccess();
      } else {
        const d = await res.json().catch(() => ({}));
        setStatus("error");
        setMsg(getApiErrorMessage(d, "领取失败"));
      }
    } catch {
      setStatus("error");
      setMsg("网络错误，请重试");
    }
  };

  return (
    <ModalShell onClose={onClose}>
      <div className="flex flex-col items-center text-center">
        <div className="mb-3 grid h-16 w-16 place-items-center rounded-[1.4rem] bg-gradient-to-br from-amber-300 to-orange-500 text-3xl shadow-xl shadow-orange-500/25">
          🎊
        </div>
        <p className="text-xs font-black uppercase tracking-[0.24em] text-orange-600">Prize Unlocked</p>
        <h2 className="mt-2 text-xl font-black text-stone-900">恭喜获得</h2>
        <p className="mt-2 rounded-2xl bg-orange-50 px-4 py-2 text-base font-black text-orange-700 ring-1 ring-orange-200/70">
          {prize}
        </p>
        <p className="mt-3 text-xs leading-5 text-indigo-600">将领取到您的 Discord 背包（{userTag}）</p>

        {status === "ok" ? (
          <div className="mt-5 w-full space-y-4">
            <div className="flex items-center justify-center gap-2 rounded-2xl bg-cyan-50 px-4 py-3 text-sm font-bold text-cyan-700 ring-1 ring-cyan-200">
              <Check className="h-5 w-5" />
              {msg}
            </div>
            <p className="text-xs text-muted-foreground">前往「我的背包」用 Discord 登录查看并激活</p>
            <button
              onClick={onClose}
              className="w-full rounded-2xl bg-gradient-to-r from-orange-400 to-orange-600 px-6 py-3 text-sm font-black text-white shadow-lg shadow-orange-500/20 transition hover:-translate-y-0.5"
            >
              关闭
            </button>
          </div>
        ) : (
          <div className="mt-5 w-full">
            {status === "error" && <p className="mb-3 text-center text-xs font-semibold text-red-500">{msg}</p>}
            <div className="grid grid-cols-2 gap-2">
              <button
                onClick={onClose}
                className="rounded-2xl border border-stone-200 bg-white/72 py-3 text-sm font-bold text-stone-600 transition hover:bg-white"
              >
                跳过
              </button>
              <button
                onClick={doClaim}
                disabled={status === "loading"}
                className="flex items-center justify-center gap-1.5 rounded-2xl bg-indigo-600 py-3 text-sm font-black text-white shadow-lg shadow-indigo-500/20 transition hover:bg-indigo-700 disabled:opacity-50"
              >
                {status === "loading" ? <Loader2 className="h-4 w-4 animate-spin" /> : <Gift className="h-4 w-4" />}
                {status === "loading" ? "领取中…" : "领取"}
              </button>
            </div>
          </div>
        )}
      </div>
    </ModalShell>
  );
}

/* ── 首次捐献宝可梦球奖励弹窗 ── */
function PokeballAwardModal({ itemId, onClose }: { itemId: number; onClose: () => void }) {
  return (
    <div className="fixed inset-0 z-[60] flex items-center justify-center bg-stone-950/60 p-4 backdrop-blur-md">
      <div className="relative w-full max-w-sm overflow-hidden rounded-[2rem] border border-white/70 bg-white/86 p-6 text-center shadow-2xl shadow-orange-950/20 backdrop-blur-2xl">
        <div className="pointer-events-none absolute inset-0 opacity-60 [background:radial-gradient(circle_at_50%_0%,rgba(251,191,36,.35),transparent_55%),radial-gradient(circle_at_85%_85%,rgba(99,102,241,.20),transparent_48%)]" />
        <div className="relative flex flex-col items-center gap-3">
          <div className="animate-bounce text-6xl">🎁</div>
          <div>
            <p className="text-xs font-black uppercase tracking-[0.24em] text-indigo-500">首次捐献礼</p>
            <h2 className="mt-1 text-2xl font-black text-stone-950">恭喜获得</h2>
            <p className="mt-2 text-base font-black text-orange-700">宝可梦球【容量10000】</p>
          </div>

          <div className="w-full rounded-3xl border border-orange-200 bg-orange-50/80 px-4 py-3">
            <p className="text-xs font-semibold text-stone-600">道具已存入您的 Discord 背包</p>
            <p className="mt-1 text-xs text-stone-400">道具编号 #{itemId}</p>
          </div>

          <p className="text-xs leading-5 text-stone-500">前往「我的背包」用 Discord 登录，激活后即可获得专属聚合 Key</p>

          <button
            onClick={onClose}
            className="w-full rounded-2xl bg-gradient-to-r from-amber-400 to-orange-500 py-3 text-sm font-black text-white shadow-lg shadow-orange-500/20 transition hover:-translate-y-0.5"
          >
            好的，知道了！
          </button>
        </div>
      </div>
    </div>
  );
}

/* ── 我要当圣人 弹窗（仅 Discord 模式）── */
function SaintModal({
  onClose,
  onDone,
  dcToken,
  userTag,
  onLogin,
}: {
  onClose: () => void;
  onDone: (points: number) => void;
  dcToken?: string | null;
  userTag?: string | null;
  onLogin: () => void;
}) {
  const [apiKey, setApiKey] = useState("");
  const [status, setStatus] = useState<"idle" | "loading" | "ok" | "error">("idle");
  const [msg, setMsg] = useState("");
  const [newPoints, setNewPoints] = useState(0);
  const [pokeballAward, setPokeballAward] = useState<{ itemId: number } | null>(null);

  const doDonate = async () => {
    if (!dcToken) return;
    if (!apiKey.trim()) {
      setStatus("error");
      setMsg("请填入 Key");
      return;
    }
    setStatus("loading");
    try {
      const res = await fetch("/key/dc-saint-donate", {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({ discord_token: dcToken, api_key: apiKey.trim() }),
      });
      const d = await res.json();
      if (res.ok) {
        setNewPoints(d.points);
        setStatus("ok");
        if (d.pokeball_awarded && d.pokeball_item_id) {
          setPokeballAward({ itemId: d.pokeball_item_id });
        }
      } else {
        setStatus("error");
        setMsg(getApiErrorMessage(d, "捐献失败"));
      }
    } catch {
      setStatus("error");
      setMsg("网络错误");
    }
  };

  return (
    <>
      <ModalShell onClose={onClose}>
        <div>
          <div className="mb-4 flex items-center gap-3">
            <div className="grid h-12 w-12 place-items-center rounded-2xl bg-gradient-to-br from-amber-300 to-orange-500 text-white shadow-lg shadow-orange-500/20">
              <Sparkles className="h-5 w-5" />
            </div>
            <div>
              <p className="text-xs font-black uppercase tracking-[0.22em] text-orange-600">Saint Program</p>
              <h2 className="text-xl font-black text-stone-950">我要当圣人</h2>
            </div>
          </div>
          <p className="mb-4 rounded-2xl bg-orange-50/80 px-3 py-2 text-xs font-semibold leading-5 text-stone-600 ring-1 ring-orange-200/60">
            捐献一个 JB Key → 获得 1 圣人点数 → 可抽 1 次奖
          </p>

          {!dcToken ? (
            <div className="space-y-3 text-center">
              <p className="text-sm text-stone-600">需要先用 Discord 登录才能参与圣人活动</p>
              <button
                onClick={() => {
                  onClose();
                  onLogin();
                }}
                className="flex w-full items-center justify-center gap-2 rounded-2xl bg-indigo-600 py-3 text-sm font-black text-white shadow-lg shadow-indigo-500/20 transition hover:bg-indigo-700"
              >
                <LogIn className="h-4 w-4" />
                Discord 登录
              </button>
              <button
                onClick={onClose}
                className="w-full rounded-2xl border border-stone-200 bg-white/70 py-3 text-sm font-bold text-stone-500 transition hover:bg-white"
              >
                取消
              </button>
            </div>
          ) : status === "ok" ? (
            <div className="space-y-4 text-center">
              <div className="text-5xl">🎖️</div>
              <p className="text-lg font-black text-orange-700">捐献成功！</p>
              <p className="text-sm text-stone-600">
                您当前共有 <span className="text-2xl font-black text-orange-600">{newPoints}</span> 个圣人点数
              </p>
              {pokeballAward && (
                <div className="flex items-center gap-3 rounded-2xl border border-orange-200 bg-gradient-to-r from-orange-50 to-amber-50 px-3 py-3">
                  <span className="text-2xl">🎁</span>
                  <p className="text-left text-xs font-bold leading-5 text-orange-800">
                    首次捐献奖励已发放！<br />
                    <span className="font-medium text-orange-600">查看下方弹窗了解详情</span>
                  </p>
                </div>
              )}
              <button
                onClick={() => {
                  onDone(newPoints);
                  onClose();
                }}
                className="w-full rounded-2xl bg-gradient-to-r from-orange-400 to-orange-600 py-3 font-black text-white shadow-lg shadow-orange-500/20 transition hover:-translate-y-0.5"
              >
                好的，去抽奖！
              </button>
            </div>
          ) : (
            <div className="space-y-3">
              <p className="flex items-center gap-1 rounded-2xl bg-indigo-50 px-3 py-2 text-xs font-bold text-indigo-600 ring-1 ring-indigo-100">
                <ShieldCheck className="h-3.5 w-3.5" />
                Discord：{userTag}
              </p>
              <div>
                <label className="mb-1.5 block text-xs font-black uppercase tracking-[0.16em] text-stone-500">要捐献的 JB Key</label>
                <input
                  type="text"
                  value={apiKey}
                  onChange={(e) => {
                    setApiKey(e.target.value);
                    setStatus("idle");
                  }}
                  placeholder="粘贴 Key（捐献后自动从数据库删除）"
                  className="w-full rounded-2xl border border-orange-200 bg-white/78 px-3 py-3 font-mono text-sm shadow-inner outline-none transition focus:border-orange-400 focus:ring-4 focus:ring-orange-200/60"
                  onKeyDown={(e) => e.key === "Enter" && doDonate()}
                />
              </div>
              {status === "error" && <p className="text-center text-xs font-semibold text-red-500">{msg}</p>}
              <div className="grid grid-cols-2 gap-2 pt-1">
                <button
                  onClick={onClose}
                  className="rounded-2xl border border-stone-200 bg-white/70 py-3 text-sm font-bold text-stone-600 transition hover:bg-white"
                >
                  取消
                </button>
                <button
                  onClick={doDonate}
                  disabled={status === "loading"}
                  className="flex items-center justify-center gap-1.5 rounded-2xl bg-orange-500 py-3 text-sm font-black text-white shadow-lg shadow-orange-500/20 transition hover:bg-orange-600 disabled:opacity-50"
                >
                  {status === "loading" ? <Loader2 className="h-4 w-4 animate-spin" /> : <KeyRound className="h-4 w-4" />}
                  {status === "loading" ? "处理中…" : "捐献 Key"}
                </button>
              </div>
            </div>
          )}
        </div>
      </ModalShell>

      {pokeballAward && <PokeballAwardModal itemId={pokeballAward.itemId} onClose={() => setPokeballAward(null)} />}
    </>
  );
}

/* ── 圣人点数排行榜 弹窗 ── */
interface LeaderEntry {
  rank: number;
  name: string;
  total_earned: number;
}

function LeaderboardModal({ onClose }: { onClose: () => void }) {
  const [entries, setEntries] = useState<LeaderEntry[]>([]);
  const [loading, setLoading] = useState(true);

  useEffect(() => {
    fetch("/key/saint-leaderboard")
      .then((r) => r.json())
      .then((d) => {
        setEntries(d.entries ?? []);
        setLoading(false);
      })
      .catch(() => setLoading(false));
  }, []);

  const medals = ["🥇", "🥈", "🥉"];

  return (
    <ModalShell onClose={onClose} maxWidth="max-w-md">
      <div className="flex max-h-[78vh] flex-col">
        <div className="mb-4 flex items-center gap-3 pr-10">
          <div className="grid h-12 w-12 place-items-center rounded-2xl bg-gradient-to-br from-amber-300 to-orange-500 text-white shadow-lg shadow-orange-500/20">
            <Trophy className="h-5 w-5" />
          </div>
          <div>
            <p className="text-xs font-black uppercase tracking-[0.22em] text-orange-600">Leaderboard</p>
            <h2 className="text-xl font-black text-stone-950">圣人点数排行榜</h2>
          </div>
        </div>
        <p className="mb-3 rounded-2xl bg-orange-50/70 px-3 py-2 text-xs text-stone-500 ring-1 ring-orange-100">按累计获得点数排名（不计消耗）</p>

        <div className="min-h-0 flex-1 overflow-y-auto pr-1">
          {loading ? (
            <div className="py-12 text-center text-sm text-stone-400">加载中…</div>
          ) : entries.length === 0 ? (
            <div className="py-12 text-center text-sm text-stone-400">暂无数据</div>
          ) : (
            <ol className="space-y-2">
              {entries.map((e) => (
                <li
                  key={e.rank}
                  className="flex items-center gap-3 rounded-2xl border border-white/70 bg-white/58 px-3 py-3 shadow-sm backdrop-blur"
                >
                  <span className="grid h-9 w-9 shrink-0 place-items-center rounded-xl bg-orange-50 text-center text-lg font-black text-orange-700 ring-1 ring-orange-100">
                    {e.rank <= 3 ? medals[e.rank - 1] : e.rank}
                  </span>
                  <span className="min-w-0 flex-1 truncate text-sm font-bold text-stone-700">{e.name}</span>
                  <span className="shrink-0 rounded-full bg-orange-100 px-2.5 py-1 text-xs font-black text-orange-700">
                    {e.total_earned} pt
                  </span>
                </li>
              ))}
            </ol>
          )}
        </div>
      </div>
    </ModalShell>
  );
}

export default function Lottery() {
  const [rotation, setRotation] = useState(0);
  const [spinning, setSpinning] = useState(false);
  const [transitionOn, setTransitionOn] = useState(false);
  const [result, setResult] = useState<{ prize: string; win: boolean } | null>(null);
  const [showClaim, setShowClaim] = useState(false);
  const [claimed, setClaimed] = useState(false);
  const [spinToken, setSpinToken] = useState("");
  const [showSaint, setShowSaint] = useState(false);
  const [showLeaderboard, setShowLeaderboard] = useState(false);
  const [saintPoints, setSaintPoints] = useState<number | null>(null);
  const [saintAuthed, setSaintAuthed] = useState<boolean | null>(null);
  const stopSpinSoundRef = useRef<(() => void) | null>(null);

  const { dcToken, userTag, isLoggedIn: dcLoggedIn, login: dcLogin, logout: dcLogout } = useDiscordAuth("lottery");

  const { data: prizes } = useQuery<Prize[]>({
    queryKey: ["public-prizes"],
    queryFn: async () => {
      const res = await fetch("/key/prizes");
      if (!res.ok) return [];
      return res.json();
    },
    staleTime: 30_000,
  });

  // 刷新圣人点数（仅 DC 模式）
  // authed=false 表示 token 在后端已失效（如服务器重启），与 0 点区分
  useEffect(() => {
    if (!dcToken) {
      setSaintPoints(null);
      setSaintAuthed(null);
      return;
    }
    const ctrl = new AbortController();
    fetch(`/key/dc-saint-points?discord_token=${encodeURIComponent(dcToken)}`, { signal: ctrl.signal })
      .then((r) => r.json())
      .then((d) => {
        if (ctrl.signal.aborted) return;
        setSaintPoints(d.points ?? 0);
        setSaintAuthed(d.authed !== false);
      })
      .catch(() => {});
    return () => ctrl.abort();
  }, [dcToken]);

  const segments =
    prizes && prizes.length > 0
      ? prizes.map((p, i) => ({ prize: p.name, bg: SEG_COLORS[i % SEG_COLORS.length], weight: p.weight }))
      : FALLBACK_SEGMENTS.map((s) => ({ ...s, weight: 10 }));

  const N = segments.length;
  const SEG_ANGLE = 360 / N;

  const doSpin = useCallback(async () => {
    if (spinning) return;

    // 未登录
    if (!dcToken) {
      setShowSaint(true);
      return;
    }

    // 已有 dcToken 但后端不认（服务器重启/会话过期）→ 提示重新登录
    if (saintAuthed === false) {
      dcLogin();
      return;
    }

    // 等待点数加载完毕
    if (saintPoints === null) return;

    // 必须有圣人点数
    if (saintPoints < 1) {
      setShowSaint(true);
      return;
    }

    // 调用后端：扣圣人点数 + 服务端抽奖
    let backendPrize = "";
    try {
      const res = await fetch("/key/dc-saint-spin", {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({ discord_token: dcToken }),
      });
      if (!res.ok) {
        const d = await res.json().catch(() => ({}));
        const message = getApiErrorMessage(d, `抽奖失败（HTTP ${res.status}）`);
        if (res.status === 401) {
          alert(`${message}，请重新登录 Discord`);
          dcLogin();
        } else if (res.status === 402) {
          // 点数不足 → 引导捐 Key
          alert(message || "圣人点数不足");
          setShowSaint(true);
        } else {
          // 503=暂无奖品，409=库存并发不足，或其他错误：展示后端真实原因
          alert(message || "抽奖失败，请稍后重试");
        }
        return;
      }
      const d = await res.json();
      setSaintPoints(d.points);
      backendPrize = d.prize ?? "";
      setSpinToken(d.spin_token ?? "");
    } catch {
      alert("网络错误，请重试");
      return;
    }

    // 根据后端返回的奖品名找段落索引
    const idx = segments.findIndex((s) => s.prize === backendPrize);
    const target = idx >= 0 ? idx : weightedRandom(segments);

    setResult(null);
    setShowClaim(false);
    setClaimed(false);
    setSpinning(true);
    setTransitionOn(true);

    const stopFn = playSpinSound(3.2);
    stopSpinSoundRef.current = stopFn;

    const adjRaw = ((-(target + 0.5) * SEG_ANGLE) % 360 + 360) % 360;
    const curMod = ((rotation % 360) + 360) % 360;
    const diff = (adjRaw - curMod + 360) % 360;
    const newRot = rotation + 5 * 360 + (diff === 0 ? 360 : diff);
    setRotation(newRot);

    const finalPrize = backendPrize || segments[target].prize;
    setTimeout(() => {
      stopSpinSoundRef.current?.();
      stopSpinSoundRef.current = null;
      setSpinning(false);
      setTransitionOn(false);
      const won = finalPrize !== "谢谢参与";
      setResult({ prize: finalPrize, win: won });
      if (won) {
        playWinSound();
        if (isPokeball(finalPrize) || isQuota(finalPrize)) {
          setTimeout(() => setShowClaim(true), 500);
        }
      } else {
        playNoWinSound();
      }
    }, 3300);
  }, [spinning, rotation, segments, SEG_ANGLE, saintPoints, saintAuthed, dcToken, dcLogin]);

  const sessionExpired = !!dcToken && saintAuthed === false;
  const noPoints = !dcToken || (!sessionExpired && (saintPoints ?? 0) < 1);
  const spinLabel = spinning ? "旋 转 中…" : sessionExpired ? "重 新 登 录" : noPoints ? "去 当 圣 人" : "抽　　奖";
  const spinHint = result
    ? result.win
      ? "奖品已锁定，记得及时领取可入库奖励"
      : "本次未中奖，补充圣人点数后可以继续挑战"
    : spinning
      ? "能量加速中，大奖正在靠近指针"
      : !dcToken
        ? "Discord 登录后捐献 Key 获得圣人点数，方可抽奖"
        : sessionExpired
          ? "Discord 会话已过期 · 点击按钮重新登录"
          : (saintPoints ?? 0) < 1
            ? "圣人点数不足 · 点击捐献 Key 获得抽奖次数"
            : "圣人点数就绪，按下按钮开启好运";

  return (
    <>
      {showClaim && result && dcToken && (
        <ClaimModal
          prize={result.prize}
          spinToken={spinToken}
          onClose={() => setShowClaim(false)}
          onSuccess={() => setClaimed(true)}
          dcToken={dcToken}
          userTag={userTag}
        />
      )}

      {showSaint && (
        <SaintModal
          onClose={() => setShowSaint(false)}
          onDone={(pts) => setSaintPoints(pts)}
          dcToken={dcToken}
          userTag={userTag}
          onLogin={dcLogin}
        />
      )}

      {showLeaderboard && <LeaderboardModal onClose={() => setShowLeaderboard(false)} />}

      <div className="lottery-stage fixed inset-0 overflow-x-hidden overflow-y-auto">
        <div className="lottery-grid" />
        <div className="lottery-aurora lottery-aurora-one" />
        <div className="lottery-aurora lottery-aurora-two" />
        <div className="lottery-aurora lottery-aurora-three" />

        <div className="relative z-10 flex min-h-screen flex-col px-4 py-20 sm:px-6 lg:px-10">
          <div className="mx-auto grid w-full max-w-7xl flex-1 items-center gap-6 lg:grid-cols-[320px_minmax(420px,1fr)_320px]">
            {/* Left control deck */}
            <aside className="order-2 space-y-3 lg:order-1">
              <div className="rounded-[2rem] border border-white/70 bg-white/54 p-4 shadow-2xl shadow-stone-950/8 backdrop-blur-2xl">
                <div className="mb-4 flex items-center gap-3">
                  <div className="grid h-11 w-11 place-items-center rounded-2xl border border-stone-200 bg-white/72 text-stone-700 shadow-sm">
                    <Sparkles className="h-5 w-5" />
                  </div>
                  <div>
                    <p className="text-xs font-semibold uppercase tracking-[0.24em] text-stone-400">Control</p>
                    <h2 className="text-lg font-black text-stone-950">圣人控制台</h2>
                  </div>
                </div>

                <div className="grid gap-2">
                  <button
                    onClick={() => setShowSaint(true)}
                    className="group flex items-center justify-between rounded-2xl border border-stone-200/80 bg-white/58 px-3.5 py-3 text-left shadow-sm transition hover:-translate-y-0.5 hover:bg-white/78 hover:shadow-lg hover:shadow-stone-950/5"
                  >
                    <span className="flex items-center gap-2 text-sm font-black text-stone-800">
                      <Sparkles className="h-4 w-4 text-stone-500" />
                      我要当圣人
                    </span>
                    <ArrowRight className="h-4 w-4 text-stone-400 transition group-hover:translate-x-0.5" />
                  </button>

                  <button
                    onClick={() => setShowLeaderboard(true)}
                    className="group flex items-center justify-between rounded-2xl border border-stone-200/80 bg-white/44 px-3.5 py-3 text-left shadow-sm transition hover:-translate-y-0.5 hover:bg-white/72 hover:shadow-lg hover:shadow-stone-950/5"
                  >
                    <span className="flex items-center gap-2 text-sm font-black text-stone-800">
                      <Trophy className="h-4 w-4 text-stone-500" />
                      圣人排行榜
                    </span>
                    <ArrowRight className="h-4 w-4 text-stone-400 transition group-hover:translate-x-0.5" />
                  </button>

                  {dcLoggedIn ? (
                    <button
                      onClick={dcLogout}
                      className="flex items-center justify-between rounded-2xl border border-stone-200/80 bg-white/48 px-3.5 py-3 text-left shadow-sm transition hover:bg-white/72"
                      title="点击退出 Discord 登录"
                    >
                      <span className="min-w-0 flex items-center gap-2 text-sm font-black text-stone-700">
                        <LogOut className="h-4 w-4 shrink-0" />
                        <span className="truncate">{userTag}</span>
                      </span>
                      <span className="text-[10px] font-black uppercase tracking-wider text-stone-400">Logout</span>
                    </button>
                  ) : (
                    <button
                      onClick={dcLogin}
                      className="flex items-center justify-center gap-2 rounded-2xl border border-stone-800 bg-stone-900 px-3.5 py-3 text-sm font-black text-white shadow-lg shadow-stone-950/15 transition hover:bg-stone-800"
                    >
                      <LogIn className="h-4 w-4" />
                      Discord 登录
                    </button>
                  )}
                </div>
              </div>

              <div className="rounded-[2rem] border border-white/70 bg-white/42 p-4 shadow-xl shadow-stone-950/5 backdrop-blur-2xl">
                <p className="mb-3 text-xs font-semibold uppercase tracking-[0.24em] text-stone-400">Wallet</p>
                {dcToken && saintAuthed === false ? (
                  <button
                    className="flex w-full items-center gap-2 rounded-2xl border border-rose-200 bg-rose-50 px-3 py-3 text-left text-sm font-bold text-rose-700"
                    onClick={dcLogin}
                    title="点击重新登录 Discord"
                  >
                    <span>⚠️</span>
                    会话过期，点此重登
                  </button>
                ) : dcToken && saintPoints !== null ? (
                  <div className="rounded-2xl border border-stone-200/80 bg-white/54 px-4 py-4">
                    <p className="text-xs font-bold text-stone-500">圣人点数</p>
                    <p className="mt-1 text-4xl font-black tracking-tight text-stone-950">{saintPoints}</p>
                    <p className="mt-2 text-xs leading-5 text-stone-500">每 1 点可启动一次橘子机</p>
                  </div>
                ) : (
                  <div className="rounded-2xl border border-stone-200 bg-white/60 px-4 py-4 text-sm font-semibold text-stone-500">
                    登录后显示点数
                  </div>
                )}
              </div>
            </aside>

            {/* Center stage */}
            <main className="order-1 flex flex-col items-center lg:order-2">
              <div className="mb-6 text-center">
                <div className="mx-auto mb-4 inline-flex items-center gap-2 rounded-full border border-stone-200/80 bg-white/70 px-4 py-2 text-[11px] font-semibold uppercase tracking-[0.28em] text-stone-500 shadow-sm backdrop-blur">
                  <Star className="h-3.5 w-3.5 text-stone-400" />
                  Private Draw
                </div>
                <h1 className="text-5xl font-black tracking-[-0.08em] text-stone-950 sm:text-6xl lg:text-7xl">
                  橘子<span className="citrus-text">机</span>
                </h1>
                <p className="mt-4 text-sm font-medium tracking-wide text-stone-500">去繁从简的圣人点数抽奖台</p>
              </div>

              <div className="lottery-wheel-frame relative grid h-[min(86vw,520px)] w-[min(86vw,520px)] place-items-center rounded-full">
                <div className="lottery-wheel-halo" />

                <div
                  className="relative z-10 h-[min(72vw,420px)] w-[min(72vw,420px)]"
                  style={{
                    transform: `rotate(${rotation}deg)`,
                    transition: transitionOn ? "transform 3.2s cubic-bezier(0.05, 0.9, 0.1, 1)" : "none",
                    filter: "drop-shadow(0 30px 58px rgba(28,25,23,0.16))",
                  }}
                >
                  <svg viewBox="0 0 320 320" className="h-full w-full overflow-visible">
                    <defs>
                      <filter id="lotteryWheelShadow" x="-28%" y="-28%" width="156%" height="156%">
                        <feDropShadow dx="0" dy="22" stdDeviation="18" floodColor="#1c1917" floodOpacity="0.16" />
                      </filter>
                      <radialGradient id="minimalRim" cx="36%" cy="24%">
                        <stop offset="0%" stopColor="#ffffff" />
                        <stop offset="48%" stopColor="#eee7dd" />
                        <stop offset="100%" stopColor="#b8aa99" />
                      </radialGradient>
                      <radialGradient id="hubGradient" cx="35%" cy="28%">
                        <stop offset="0%" stopColor="#ffffff" />
                        <stop offset="62%" stopColor="#eee6dc" />
                        <stop offset="100%" stopColor="#a99a88" />
                      </radialGradient>
                      {segments.map((seg, i) => (
                        <linearGradient key={`grad-${i}`} id={`segmentGradient-${i}`} x1="0" x2="1" y1="0" y2="1">
                          <stop offset="0%" stopColor={mixHex(seg.bg, "white", 0.42)} />
                          <stop offset="100%" stopColor={mixHex(seg.bg, "black", 0.06)} />
                        </linearGradient>
                      ))}
                    </defs>

                    <g filter="url(#lotteryWheelShadow)">
                      <circle cx={CX} cy={CY} r={R + 19} fill="url(#minimalRim)" />
                      <circle cx={CX} cy={CY} r={R + 13} fill="#faf7f2" />
                      <circle cx={CX} cy={CY} r={R + 4} fill="#d8cbbb" opacity="0.42" />

                      {segments.map((seg, i) => {
                        const startDeg = -90 + i * SEG_ANGLE;
                        const endDeg = -90 + (i + 1) * SEG_ANGLE;
                        const midDeg = startDeg + SEG_ANGLE / 2;
                        const labelR = R * 0.68;
                        const labelX = CX + labelR * Math.cos(toRad(midDeg));
                        const labelY = CY + labelR * Math.sin(toRad(midDeg));
                        const labelRotation = midDeg + 90;
                        return (
                          <g key={`${seg.prize}-${i}`}>
                            <path
                              d={sectorPath(startDeg + 0.25, endDeg - 0.25)}
                              fill={`url(#segmentGradient-${i})`}
                              stroke="rgba(120,113,108,0.18)"
                              strokeWidth="0.8"
                            />
                            <g transform={`translate(${labelX} ${labelY}) rotate(${labelRotation})`}>
                              <text
                                y="1"
                                textAnchor="middle"
                                dominantBaseline="middle"
                                fontSize={8.4}
                                fill="#57534e"
                                fontWeight="700"
                                style={{ userSelect: "none", letterSpacing: "0.03em" }}
                              >
                                {shortenPrize(seg.prize)}
                              </text>
                            </g>
                          </g>
                        );
                      })}

                      <circle cx={CX} cy={CY} r={R + 2} fill="none" stroke="rgba(255,255,255,0.9)" strokeWidth="2.2" />
                      <circle cx={CX} cy={CY} r={R - 36} fill="none" stroke="rgba(87,83,78,0.11)" strokeWidth="1" />
                      <circle cx={CX} cy={CY} r={R - 68} fill="none" stroke="rgba(87,83,78,0.08)" strokeWidth="1" />

                      <circle cx={CX} cy={CY} r={39} fill="rgba(255,255,255,0.84)" />
                      <circle cx={CX} cy={CY} r={31} fill="url(#hubGradient)" stroke="rgba(87,83,78,0.16)" strokeWidth="1" />
                      <circle cx={CX} cy={CY} r={10} fill="#292524" />
                      <circle cx={CX} cy={CY} r={4} fill="#f8f3ec" />
                    </g>
                  </svg>
                </div>

                <div className="lottery-pointer absolute left-1/2 top-5 z-20 -translate-x-1/2">
                  <div className="h-0 w-0 border-l-[14px] border-r-[14px] border-t-[38px] border-l-transparent border-r-transparent border-t-stone-900 drop-shadow-[0_12px_18px_rgba(28,25,23,0.24)]" />
                  <div className="absolute left-1/2 top-[-10px] h-5 w-5 -translate-x-1/2 rounded-full border border-stone-200 bg-white shadow-md" />
                </div>
              </div>

              <div className="mt-5 flex min-h-16 w-full max-w-2xl items-center justify-center">
                {result ? (
                  <div className="rounded-[1.75rem] border border-white/70 bg-white/62 px-5 py-3 text-center shadow-xl shadow-orange-950/8 backdrop-blur-xl">
                    <p className={`text-xs font-black uppercase tracking-[0.2em] ${result.win ? "text-cyan-700" : "text-orange-700"}`}>
                      {result.win ? "Prize Result" : "Try Again"}
                    </p>
                    <div className="mt-1 flex flex-wrap items-center justify-center gap-2">
                      <p className={`text-2xl font-black tracking-wide ${result.win ? "text-cyan-800" : "text-orange-800"}`}>
                        {prizeEmoji(result.prize)} {result.prize}
                      </p>
                      {result.win && (isPokeball(result.prize) || isQuota(result.prize)) && !claimed && (
                        <button
                          onClick={() => setShowClaim(true)}
                          className="rounded-xl border border-orange-300 bg-orange-100 px-3 py-1.5 text-xs font-black text-orange-700 transition hover:bg-orange-200"
                        >
                          领取
                        </button>
                      )}
                      {result.win && (isPokeball(result.prize) || isQuota(result.prize)) && claimed && (
                        <span className="flex items-center gap-1 rounded-xl bg-cyan-100 px-3 py-1.5 text-xs font-black text-cyan-700">
                          <Check className="h-3.5 w-3.5" />
                          已领取
                        </span>
                      )}
                    </div>
                  </div>
                ) : (
                  <p className="rounded-full border border-white/70 bg-white/52 px-4 py-2 text-center text-sm font-bold text-stone-600 shadow-sm backdrop-blur">
                    {spinning ? "正在抽奖…" : spinHint}
                  </p>
                )}
              </div>

              <button
                onClick={doSpin}
                disabled={spinning}
                className="lottery-spin-button mt-2 select-none rounded-[1.65rem] px-12 py-4 text-2xl font-black tracking-[0.24em] text-white transition disabled:cursor-not-allowed sm:px-16"
                data-state={spinning ? "spinning" : sessionExpired ? "expired" : noPoints ? "empty" : "ready"}
              >
                {spinLabel}
              </button>
            </main>

            {/* Right prize deck */}
            <aside className="order-3 space-y-3">
              <div className="rounded-[2rem] border border-white/70 bg-white/44 p-4 shadow-2xl shadow-stone-950/8 backdrop-blur-2xl">
                <div className="mb-4 flex items-center justify-between">
                  <div>
                    <p className="text-xs font-semibold uppercase tracking-[0.24em] text-stone-400">Prize Pool</p>
                    <h2 className="text-lg font-black text-stone-950">奖品池</h2>
                  </div>
                  <span className="rounded-full border border-stone-200 bg-white/54 px-2.5 py-1 text-xs font-black text-stone-500">
                    {segments.length} 项
                  </span>
                </div>
                <div className="max-h-[360px] space-y-2 overflow-y-auto pr-1">
                  {segments.map((seg, i) => (
                    <div
                      key={`${seg.prize}-deck-${i}`}
                      className="flex items-center gap-3 rounded-2xl border border-stone-200/70 bg-white/42 px-3 py-2.5 shadow-sm backdrop-blur"
                    >
                      <span
                        className="grid h-9 w-9 shrink-0 place-items-center rounded-xl border border-stone-200 text-[10px] font-black text-stone-500 shadow-sm"
                        style={{ background: seg.bg }}
                      >
                        {i + 1}
                      </span>
                      <div className="min-w-0 flex-1">
                        <p className="truncate text-sm font-black text-stone-800">{seg.prize}</p>
                      </div>
                    </div>
                  ))}
                </div>
              </div>

              <div className="rounded-[2rem] border border-white/70 bg-white/38 p-4 shadow-xl shadow-stone-950/5 backdrop-blur-2xl">
                <p className="mb-2 text-xs font-semibold uppercase tracking-[0.24em] text-stone-400">Tips</p>
                <div className="space-y-2 text-xs font-semibold leading-5 text-stone-500">
                  <p className="rounded-2xl bg-white/52 px-3 py-2 ring-1 ring-white/70">捐献 Key 可获得圣人点数。</p>
                  <p className="rounded-2xl bg-white/52 px-3 py-2 ring-1 ring-white/70">宝可梦球与额度类奖品可领取到 Discord 背包。</p>
                  <p className="rounded-2xl bg-white/52 px-3 py-2 ring-1 ring-white/70">转盘结果由服务端抽取，前端只负责展示动画。</p>
                </div>
              </div>
            </aside>
          </div>
        </div>
      </div>
    </>
  );
}
