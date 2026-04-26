import { useState, useRef, useCallback, useEffect } from "react";
import { useQuery } from "@tanstack/react-query";
import { X, Gift, Check, Sparkles, KeyRound, LogIn, LogOut, Trophy } from "lucide-react";
import { useDiscordAuth } from "@/hooks/useDiscordAuth";

interface Prize {
  id: number;
  name: string;
  quantity: number;
  weight: number;
}

const FALLBACK_SEGMENTS = [
  { prize: "敬请期待", bg: "#ef4444" },
  { prize: "敬请期待", bg: "#f97316" },
  { prize: "敬请期待", bg: "#eab308" },
  { prize: "敬请期待", bg: "#22c55e" },
  { prize: "敬请期待", bg: "#06b6d4" },
  { prize: "敬请期待", bg: "#6366f1" },
  { prize: "敬请期待", bg: "#8b5cf6" },
  { prize: "敬请期待", bg: "#ec4899" },
];

const SEG_COLORS = [
  "#ef4444","#f97316","#eab308","#22c55e",
  "#06b6d4","#6366f1","#8b5cf6","#ec4899",
];

const CX = 160, CY = 160, R = 148;

function toRad(deg: number) { return (deg * Math.PI) / 180; }

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
  osc.start(when); osc.stop(when + 0.09);
}
function playSpinSound(duration: number): () => void {
  try {
    const ctx = getAudioCtx();
    for (let i = 0; i < 80; i++) {
      const t = i / 80;
      const time = duration * (1 - Math.exp(-4 * t)) / (1 - Math.exp(-4));
      playTick(ctx, ctx.currentTime + time, 0.07 * (1 - (time / duration) * 0.5));
    }
    return () => { try { ctx.close(); } catch { /* ignore */ } };
  } catch {
    return () => {}; // 音频不可用时返回空函数
  }
}
function playWinSound() {
  try {
    const ctx = getAudioCtx();
    [523.25, 659.25, 783.99, 1046.5].forEach((freq, i) => {
      const osc = ctx.createOscillator(); const g = ctx.createGain();
      osc.type = "sine"; osc.frequency.value = freq;
      const start = ctx.currentTime + i * 0.12;
      g.gain.setValueAtTime(0, start); g.gain.linearRampToValueAtTime(0.13, start + 0.04);
      g.gain.exponentialRampToValueAtTime(0.0001, start + 0.5);
      osc.connect(g).connect(ctx.destination); osc.start(start); osc.stop(start + 0.55);
    });
    setTimeout(() => { try { ctx.close(); } catch { /* ignore */ } }, 1500);
  } catch { /* 音频不可用时静默忽略，不影响弹窗逻辑 */ }
}
function playNoWinSound() {
  try {
    const ctx = getAudioCtx();
    [330, 247].forEach((freq, i) => {
      const osc = ctx.createOscillator(); const g = ctx.createGain();
      osc.type = "sine"; osc.frequency.value = freq;
      const start = ctx.currentTime + i * 0.18;
      g.gain.setValueAtTime(0, start); g.gain.linearRampToValueAtTime(0.1, start + 0.03);
      g.gain.exponentialRampToValueAtTime(0.0001, start + 0.4);
      osc.connect(g).connect(ctx.destination); osc.start(start); osc.stop(start + 0.45);
    });
    setTimeout(() => { try { ctx.close(); } catch { /* ignore */ } }, 800);
  } catch { /* 音频不可用时静默忽略 */ }
}

function isQuota(name: string) { return /额度/.test(name); }
function parseQuotaAmount(name: string): number | null {
  const m = name.match(/(\d+)/);
  return m ? parseInt(m[1], 10) : null;
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
        setMsg(d.detail || "领取失败");
      }
    } catch {
      setStatus("error");
      setMsg("网络错误，请重试");
    }
  };

  return (
    <div className="fixed inset-0 z-50 flex items-center justify-center bg-black/40 backdrop-blur-sm">
      <div className="bg-white rounded-2xl shadow-2xl p-6 w-full max-w-sm mx-4 relative">
        <button onClick={onClose} className="absolute top-3 right-3 p-1 text-gray-400 hover:text-gray-600">
          <X className="w-5 h-5" />
        </button>
        <div className="flex flex-col items-center gap-3 mb-5">
          <div className="text-4xl">🎊</div>
          <h2 className="text-lg font-bold text-gray-800">恭喜获得</h2>
          <p className="text-base font-semibold text-amber-700 text-center">{prize}</p>
          <p className="text-xs text-indigo-600 text-center">将领取到您的 Discord 背包（{userTag}）</p>
        </div>

        {status === "ok" ? (
          <div className="flex flex-col items-center gap-3">
            <div className="flex items-center gap-2 text-green-600 font-medium">
              <Check className="w-5 h-5" /> {msg}
            </div>
            <p className="text-xs text-gray-400">前往「我的背包」用 Discord 登录查看并激活</p>
            <button onClick={onClose} className="mt-2 px-6 py-2 rounded-lg bg-amber-500 text-white text-sm font-medium hover:bg-amber-600">
              关闭
            </button>
          </div>
        ) : (
          <>
            {status === "error" && <p className="text-xs text-red-500 mb-2 text-center">{msg}</p>}
            <div className="flex gap-2">
              <button onClick={onClose} className="flex-1 py-2.5 rounded-lg border border-gray-200 text-sm text-gray-600 hover:bg-gray-50">
                跳过
              </button>
              <button
                onClick={doClaim}
                disabled={status === "loading"}
                className="flex-1 py-2.5 rounded-lg bg-indigo-600 text-white text-sm font-semibold hover:bg-indigo-700 disabled:opacity-50 flex items-center justify-center gap-1.5"
              >
                <Gift className="w-4 h-4" />
                {status === "loading" ? "领取中…" : "领取到 DC 背包"}
              </button>
            </div>
          </>
        )}
      </div>
    </div>
  );
}

/* ── 首次捐献宝可梦球奖励弹窗 ── */
function PokeballAwardModal({ itemId, onClose }: { itemId: number; onClose: () => void }) {
  return (
    <div className="fixed inset-0 z-[60] flex items-center justify-center bg-black/60 backdrop-blur-sm p-4">
      <div className="bg-white rounded-2xl shadow-2xl w-full max-w-sm p-6 relative overflow-hidden">
        {/* 彩带背景 */}
        <div className="absolute inset-0 pointer-events-none opacity-10"
          style={{ background: "radial-gradient(circle at 50% 0%, #fbbf24 0%, transparent 70%), radial-gradient(circle at 80% 80%, #818cf8 0%, transparent 60%)" }} />

        <div className="relative flex flex-col items-center gap-3 text-center">
          <div className="text-5xl animate-bounce">🎁</div>
          <div className="space-y-0.5">
            <p className="text-xs font-semibold text-indigo-500 tracking-wide uppercase">首次捐献礼</p>
            <h2 className="text-xl font-black text-gray-900">恭喜获得</h2>
            <p className="text-base font-bold text-amber-700">宝可梦球【容量10000】</p>
          </div>

          <div className="w-full rounded-xl bg-amber-50 border border-amber-200 px-4 py-3 space-y-1">
            <p className="text-xs text-gray-600">道具已存入您的 Discord 背包</p>
            <p className="text-xs text-gray-400">道具编号 #{itemId}</p>
          </div>

          <p className="text-xs text-gray-500">前往「我的背包」用 Discord 登录，激活后即可获得专属聚合 Key</p>

          <button
            onClick={onClose}
            className="w-full py-2.5 rounded-xl bg-gradient-to-r from-amber-400 to-orange-400 text-white font-bold text-sm hover:from-amber-500 hover:to-orange-500 shadow-md"
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
    if (!apiKey.trim()) { setStatus("error"); setMsg("请填入 Key"); return; }
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
        setMsg(d.detail || "捐献失败");
      }
    } catch {
      setStatus("error"); setMsg("网络错误");
    }
  };

  return (
    <>
      <div className="fixed inset-0 z-50 flex items-center justify-center bg-black/50 backdrop-blur-sm p-4">
        <div className="bg-white rounded-2xl shadow-2xl w-full max-w-sm p-6 relative">
          <button onClick={onClose} className="absolute right-4 top-4 text-gray-400 hover:text-gray-600">
            <X className="w-4 h-4" />
          </button>

          <div className="flex items-center gap-2 mb-1">
            <span className="text-2xl">✨</span>
            <h2 className="text-lg font-black text-amber-800">我要当圣人</h2>
          </div>
          <p className="text-xs text-gray-500 mb-3">捐献一个 JB Key → 获得 1 圣人点数 → 可抽 1 次奖</p>

          {!dcToken ? (
            <div className="space-y-3 text-center">
              <p className="text-sm text-gray-600">需要先用 Discord 登录才能参与圣人活动</p>
              <button
                onClick={() => { onClose(); onLogin(); }}
                className="w-full py-2.5 rounded-xl bg-indigo-600 text-white text-sm font-bold hover:bg-indigo-700 flex items-center justify-center gap-2"
              >
                <LogIn className="w-4 h-4" />
                Discord 登录
              </button>
              <button onClick={onClose} className="w-full py-2 rounded-xl border border-gray-200 text-sm text-gray-500 hover:bg-gray-50">
                取消
              </button>
            </div>
          ) : status === "ok" ? (
            <div className="space-y-3 text-center">
              <div className="text-4xl">🎖️</div>
              <p className="text-base font-bold text-amber-700">捐献成功！</p>
              <p className="text-sm text-gray-600">您当前共有 <span className="font-black text-amber-600 text-lg">{newPoints}</span> 个圣人点数</p>
              {pokeballAward && (
                <div className="rounded-xl bg-gradient-to-r from-amber-50 to-orange-50 border border-amber-200 px-3 py-2 flex items-center gap-2">
                  <span className="text-xl">🎁</span>
                  <p className="text-xs text-amber-800 font-semibold text-left">首次捐献奖励已发放！<br/>
                    <span className="font-normal text-amber-600">查看下方弹窗了解详情</span>
                  </p>
                </div>
              )}
              <button
                onClick={() => { onDone(newPoints); onClose(); }}
                className="w-full py-2.5 rounded-xl bg-amber-500 text-white font-bold hover:bg-amber-600"
              >
                好的，去抽奖！
              </button>
            </div>
          ) : (
            <div className="space-y-3">
              <p className="text-xs text-indigo-600 flex items-center gap-1">
                <span className="font-semibold">Discord：</span>{userTag}
              </p>
              <div>
                <label className="text-xs font-semibold text-gray-500 mb-1 block">要捐献的 JB Key</label>
                <input
                  type="text"
                  value={apiKey}
                  onChange={e => { setApiKey(e.target.value); setStatus("idle"); }}
                  placeholder="粘贴 Key（捐献后自动从数据库删除）"
                  className="w-full px-3 py-2.5 rounded-lg border border-gray-200 text-sm focus:outline-none focus:ring-2 focus:ring-amber-300 font-mono"
                  onKeyDown={e => e.key === "Enter" && doDonate()}
                />
              </div>
              {status === "error" && <p className="text-xs text-red-500 text-center">{msg}</p>}
              <div className="flex gap-2 pt-1">
                <button onClick={onClose} className="flex-1 py-2.5 rounded-xl border border-gray-200 text-sm text-gray-600 hover:bg-gray-50">
                  取消
                </button>
                <button
                  onClick={doDonate}
                  disabled={status === "loading"}
                  className="flex-1 py-2.5 rounded-xl bg-amber-500 text-white text-sm font-bold hover:bg-amber-600 disabled:opacity-50 flex items-center justify-center gap-1.5"
                >
                  <KeyRound className="w-4 h-4" />
                  {status === "loading" ? "处理中…" : "捐献 Key"}
                </button>
              </div>
            </div>
          )}
        </div>
      </div>

      {/* 宝可梦球奖励弹窗（叠在圣人弹窗上方） */}
      {pokeballAward && (
        <PokeballAwardModal
          itemId={pokeballAward.itemId}
          onClose={() => setPokeballAward(null)}
        />
      )}
    </>
  );
}

/* ── 圣人点数排行榜 弹窗 ── */
interface LeaderEntry { rank: number; name: string; total_earned: number; }

function LeaderboardModal({ onClose }: { onClose: () => void }) {
  const [entries, setEntries] = useState<LeaderEntry[]>([]);
  const [loading, setLoading] = useState(true);

  useEffect(() => {
    fetch("/key/saint-leaderboard")
      .then(r => r.json())
      .then(d => { setEntries(d.entries ?? []); setLoading(false); })
      .catch(() => setLoading(false));
  }, []);

  const medals = ["🥇", "🥈", "🥉"];

  return (
    <div className="fixed inset-0 z-50 flex items-center justify-center bg-black/40 backdrop-blur-sm p-4">
      <div className="bg-white rounded-2xl shadow-2xl w-full max-w-sm mx-auto relative flex flex-col"
           style={{ maxHeight: "80vh" }}>
        <div className="flex items-center justify-between px-5 pt-5 pb-3 border-b border-amber-100 flex-shrink-0">
          <div className="flex items-center gap-2">
            <Trophy className="w-5 h-5 text-amber-500" />
            <h2 className="text-base font-black text-amber-900">圣人点数排行榜</h2>
          </div>
          <button onClick={onClose} className="p-1 text-gray-400 hover:text-gray-600">
            <X className="w-4 h-4" />
          </button>
        </div>
        <p className="text-xs text-gray-400 px-5 py-1.5 flex-shrink-0">按累计获得点数排名（不计消耗）</p>

        <div className="overflow-y-auto flex-1 px-4 pb-4">
          {loading ? (
            <div className="py-10 text-center text-sm text-gray-400">加载中…</div>
          ) : entries.length === 0 ? (
            <div className="py-10 text-center text-sm text-gray-400">暂无数据</div>
          ) : (
            <ol className="space-y-2 mt-2">
              {entries.map((e) => (
                <li
                  key={e.rank}
                  className="flex items-center gap-3 px-3 py-2 rounded-xl border"
                  style={{
                    background: e.rank === 1 ? "linear-gradient(90deg,#fef9c3,#fef3c7)" : e.rank === 2 ? "linear-gradient(90deg,#f3f4f6,#e5e7eb)" : e.rank === 3 ? "linear-gradient(90deg,#fff7ed,#fde8c7)" : "transparent",
                    borderColor: e.rank <= 3 ? "#fcd34d" : "#f3f4f6",
                  }}
                >
                  <span className="text-lg w-6 text-center flex-shrink-0">
                    {e.rank <= 3 ? medals[e.rank - 1] : `${e.rank}`}
                  </span>
                  <span className="flex-1 text-xs font-medium text-gray-700">{e.name}</span>
                  <span className="text-sm font-black text-amber-600 flex-shrink-0">{e.total_earned} pt</span>
                </li>
              ))}
            </ol>
          )}
        </div>
      </div>
    </div>
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
    if (!dcToken) { setSaintPoints(null); setSaintAuthed(null); return; }
    const ctrl = new AbortController();
    fetch(`/key/dc-saint-points?discord_token=${encodeURIComponent(dcToken)}`, { signal: ctrl.signal })
      .then(r => r.json())
      .then(d => {
        if (ctrl.signal.aborted) return;
        setSaintPoints(d.points ?? 0);
        setSaintAuthed(d.authed !== false);
      })
      .catch(() => {});
    return () => ctrl.abort();
  }, [dcToken]);

  const segments = prizes && prizes.length > 0
    ? prizes.map((p, i) => ({ prize: p.name, bg: SEG_COLORS[i % SEG_COLORS.length], weight: p.weight }))
    : FALLBACK_SEGMENTS.map(s => ({ ...s, weight: 10 }));

  const N = segments.length;
  const SEG_ANGLE = 360 / N;

  const doSpin = useCallback(async () => {
    if (spinning) return;

    // 未登录
    if (!dcToken) { setShowSaint(true); return; }

    // 已有 dcToken 但后端不认（服务器重启/会话过期）→ 提示重新登录
    if (saintAuthed === false) { dcLogin(); return; }

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
        const d = await res.json();
        if (res.status === 402) {
          // 点数不足 → 引导捐 Key
          alert(d.detail || "圣人点数不足");
          setShowSaint(true);
        } else {
          // 503=暂无奖品，或其他错误 → 只提示，不弹捐Key框
          alert(d.detail || "抽奖失败，请稍后重试");
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
    const idx = segments.findIndex(s => s.prize === backendPrize);
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

      {showLeaderboard && (
        <LeaderboardModal onClose={() => setShowLeaderboard(false)} />
      )}

      {/* 我要当圣人 悬浮按钮 & DC 登录 & 点数显示 */}
      <div className="fixed left-4 z-40 flex flex-col items-start gap-2" style={{ top: 64 }}>
        <button
          onClick={() => setShowSaint(true)}
          className="flex items-center gap-1.5 px-3 py-1.5 rounded-xl text-xs font-bold shadow-md border transition-colors"
          style={{
            background: "linear-gradient(135deg, #fef3c7, #fde68a)",
            borderColor: "#f59e0b",
            color: "#92400e",
          }}
        >
          <Sparkles className="w-3.5 h-3.5 text-amber-500" />
          我要当圣人
        </button>

        <button
          onClick={() => setShowLeaderboard(true)}
          className="flex items-center gap-1.5 px-3 py-1.5 rounded-xl text-xs font-bold shadow-md border transition-colors"
          style={{
            background: "linear-gradient(135deg, #ecfdf5, #d1fae5)",
            borderColor: "#34d399",
            color: "#065f46",
          }}
        >
          <Trophy className="w-3.5 h-3.5 text-emerald-500" />
          排行榜
        </button>

        {/* DC 登录状态 */}
        {dcLoggedIn ? (
          <button
            onClick={dcLogout}
            className="flex items-center gap-1 px-3 py-1.5 rounded-xl text-xs font-semibold shadow-md border transition-colors"
            style={{ background: "rgba(99,102,241,0.12)", borderColor: "#818cf8", color: "#4338ca" }}
            title="点击退出 Discord 登录"
          >
            <LogOut className="w-3 h-3" />
            {userTag}
          </button>
        ) : (
          <button
            onClick={dcLogin}
            className="flex items-center gap-1.5 px-3 py-1.5 rounded-xl text-xs font-bold shadow-md border transition-colors"
            style={{ background: "rgba(99,102,241,0.1)", borderColor: "#818cf8", color: "#4338ca" }}
          >
            <LogIn className="w-3.5 h-3.5" />
            Discord 登录
          </button>
        )}

        {/* 圣人点数显示 */}
        {dcToken && saintAuthed === false ? (
          <div
            className="flex items-center gap-1 px-3 py-1 rounded-xl text-xs font-semibold shadow border cursor-pointer"
            style={{ background: "rgba(255,255,255,0.85)", borderColor: "#fda4af", color: "#be123c" }}
            onClick={dcLogin}
            title="点击重新登录 Discord"
          >
            <span>⚠️</span>
            <span>会话过期，点此重登</span>
          </div>
        ) : dcToken && saintPoints !== null ? (
          <div
            className="flex items-center gap-1 px-3 py-1 rounded-xl text-xs font-semibold shadow border"
            style={{ background: "rgba(255,255,255,0.85)", borderColor: "#fcd34d", color: "#78350f" }}
          >
            <span>✨</span>
            <span>圣人点数 <span className="font-black text-amber-600">{saintPoints}</span></span>
          </div>
        ) : null}
      </div>

      <div
        className="fixed inset-0 flex flex-col items-center justify-center gap-6"
        style={{ background: "linear-gradient(135deg, #fff7ed 0%, #fef3c7 55%, #fde68a 100%)" }}
      >
        <h1 className="text-3xl font-black tracking-widest" style={{ color: "#92400e", textShadow: "0 2px 0 rgba(0,0,0,0.08)" }}>
          橘子机
        </h1>

        <div className="relative flex items-center justify-center" style={{ width: 360, height: 360 }}>
          {/* Soft rainbow glow behind the wheel */}
          <div className="absolute rounded-full" style={{
            width: 340, height: 340,
            background: "conic-gradient(from -90deg, hsl(0,80%,80%), hsl(60,80%,80%), hsl(120,80%,80%), hsl(180,80%,80%), hsl(240,80%,80%), hsl(300,80%,80%), hsl(360,80%,80%))",
            filter: "blur(22px)", opacity: 0.4,
          }} />

          {/* Rotating wheel container (both gradient + SVG rotate together) */}
          <div style={{
            position: "relative", width: 320, height: 320, zIndex: 1,
            transform: `rotate(${rotation}deg)`,
            transition: transitionOn ? "transform 3.2s cubic-bezier(0.05, 0.9, 0.1, 1)" : "none",
            filter: "drop-shadow(0 6px 24px rgba(0,0,0,0.15))",
          }}>
            {/* Rainbow conic-gradient wheel background */}
            <div style={{
              position: "absolute", inset: 0, borderRadius: "50%",
              background: "conic-gradient(from -90deg, hsl(0,60%,84%), hsl(51,65%,84%), hsl(102,60%,84%), hsl(153,60%,84%), hsl(204,65%,84%), hsl(255,60%,84%), hsl(306,60%,84%), hsl(357,60%,84%))",
              border: "4.5px solid white",
              boxSizing: "border-box",
            }} />

            {/* SVG overlay: white dividers + emoji labels + center hub */}
            <svg viewBox="0 0 320 320" style={{ position: "absolute", inset: 0, width: "100%", height: "100%" }}>
              {/* White radial divider lines */}
              {segments.map((_, i) => {
                const angleDeg = -90 + i * SEG_ANGLE;
                const x2 = CX + R * Math.cos(toRad(angleDeg));
                const y2 = CY + R * Math.sin(toRad(angleDeg));
                return <line key={i} x1={CX} y1={CY} x2={x2} y2={y2} stroke="white" strokeWidth="3" strokeLinecap="round" />;
              })}

              {/* Prize label in each segment */}
              {segments.map((seg, i) => {
                const midDeg = -90 + (i + 0.5) * SEG_ANGLE;
                const textR = R * 0.62;
                const tx = CX + textR * Math.cos(toRad(midDeg));
                const ty = CY + textR * Math.sin(toRad(midDeg));
                const label = seg.prize.length > 5 ? seg.prize.slice(0, 4) + "…" : seg.prize;
                return (
                  <g key={i}>
                    <text x={tx} y={ty - 8} textAnchor="middle" dominantBaseline="middle"
                      fontSize={20} style={{ userSelect: "none" }}>🍊</text>
                    <text x={tx} y={ty + 14} textAnchor="middle" dominantBaseline="middle"
                      fontSize={9} fill="rgba(0,0,0,0.55)" fontWeight="600" style={{ userSelect: "none" }}>
                      {label}
                    </text>
                  </g>
                );
              })}

              {/* Outer circle border */}
              <circle cx={CX} cy={CY} r={R} fill="none" stroke="white" strokeWidth="5" />

              {/* Center hub: white > amber > white dot */}
              <circle cx={CX} cy={CY} r={28} fill="white" />
              <circle cx={CX} cy={CY} r={28} fill="none" stroke="#f59e0b" strokeWidth="5" />
              <circle cx={CX} cy={CY} r={16} fill="#f59e0b" />
              <circle cx={CX} cy={CY} r={8}  fill="white" />
              <circle cx={CX} cy={CY} r={3}  fill="#f59e0b" />
            </svg>
          </div>

          {/* Red triangle pointer (non-rotating) */}
          <div className="absolute z-10" style={{
            top: 6, left: "50%", transform: "translateX(-50%)",
            width: 0, height: 0,
            borderLeft: "14px solid transparent", borderRight: "14px solid transparent",
            borderTop: "32px solid #dc2626",
            filter: "drop-shadow(0 3px 5px rgba(0,0,0,0.35))",
          }} />
        </div>

        <div className="h-10 flex items-center justify-center">
          {result ? (
            <div className="flex items-center gap-2">
              <p className="text-xl font-black tracking-wide animate-bounce" style={{ color: result.win ? "#065f46" : "#78350f" }}>
                🍊&nbsp;{result.prize}
              </p>
              {result.win && (isPokeball(result.prize) || isQuota(result.prize)) && !claimed && (
                <button
                  onClick={() => setShowClaim(true)}
                  className="text-xs px-2 py-1 rounded-md bg-amber-100 text-amber-700 border border-amber-300 hover:bg-amber-200 font-medium"
                >
                  领取
                </button>
              )}
              {result.win && (isPokeball(result.prize) || isQuota(result.prize)) && claimed && (
                <span className="text-xs text-green-600 font-medium flex items-center gap-1">
                  <Check className="w-3 h-3" />已领取
                </span>
              )}
            </div>
          ) : spinning ? (
            <p className="text-sm text-amber-700 opacity-60 font-medium">正在抽奖…</p>
          ) : !dcToken ? (
            <p className="text-xs text-amber-700/70 font-medium">↖ Discord 登录后捐献 Key 获得圣人点数，方可抽奖</p>
          ) : saintAuthed === false ? (
            <p className="text-xs text-rose-500 font-medium">Discord 会话已过期 · 点击按钮重新登录</p>
          ) : (saintPoints ?? 0) < 1 ? (
            <p className="text-xs text-amber-700/70 font-medium">圣人点数不足 · 点击左上角捐献 Key</p>
          ) : null}
        </div>

        {(() => {
          const sessionExpired = !!dcToken && saintAuthed === false;
          const noPoints = !dcToken || (!sessionExpired && (saintPoints ?? 0) < 1);
          return (
            <button
              onClick={doSpin}
              disabled={spinning}
              className="px-16 py-4 text-white font-black text-2xl rounded-2xl select-none tracking-widest transition-all duration-75"
              style={{
                background: spinning
                  ? "#9ca3af"
                  : sessionExpired
                  ? "linear-gradient(180deg, #f43f5e 0%, #e11d48 100%)"
                  : noPoints
                  ? "linear-gradient(180deg, #fbbf24 0%, #f59e0b 100%)"
                  : "linear-gradient(180deg, #f87171 0%, #ef4444 100%)",
                boxShadow: spinning
                  ? "0 2px 0 #6b7280"
                  : sessionExpired
                  ? "0 6px 0 #9f1239"
                  : noPoints
                  ? "0 6px 0 #d97706"
                  : "0 6px 0 #991b1b",
                transform: spinning ? "translateY(4px)" : "translateY(0)",
                cursor: spinning ? "not-allowed" : "pointer",
                letterSpacing: "0.2em",
              }}
            >
              {spinning ? "旋 转 中…" : sessionExpired ? "重 新 登 录" : noPoints ? "去 当 圣 人" : "抽　　奖"}
            </button>
          );
        })()}
      </div>
    </>
  );
}
