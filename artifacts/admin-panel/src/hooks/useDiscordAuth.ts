import { useState, useEffect, useCallback } from "react";

const SESSION_KEY = "dc_auth";
// 与后端保持一致：7 天有效期
const SESSION_TTL_MS = 7 * 24 * 60 * 60 * 1000;

interface DcSession {
  token: string;
  userTag: string;
  ts: number;
}

function loadSession(): DcSession | null {
  try {
    const raw = localStorage.getItem(SESSION_KEY);
    if (!raw) return null;
    const s: DcSession = JSON.parse(raw);
    if (Date.now() - s.ts > SESSION_TTL_MS) {
      localStorage.removeItem(SESSION_KEY);
      return null;
    }
    return s;
  } catch {
    return null;
  }
}

function saveSession(token: string, userTag: string) {
  const s: DcSession = { token, userTag, ts: Date.now() };
  localStorage.setItem(SESSION_KEY, JSON.stringify(s));
}

export function useDiscordAuth(
  redirectPage: "lottery" | "backpack" | "donate" | "activate" | "my-cf-pool",
) {
  const [session, setSession] = useState<DcSession | null>(() => loadSession());

  useEffect(() => {
    const params = new URLSearchParams(window.location.search);
    const token = params.get("discord_token");
    const tag = params.get("tag") || "";
    const err = params.get("discord_error");

    if (token) {
      saveSession(token, decodeURIComponent(tag));
      setSession({ token, userTag: decodeURIComponent(tag), ts: Date.now() });
      // 清掉 URL 参数，避免 token 泄漏到分享链接
      const clean = window.location.pathname + window.location.hash;
      window.history.replaceState({}, "", clean);
    } else if (err) {
      const clean = window.location.pathname + window.location.hash;
      window.history.replaceState({}, "", clean);
    }
  }, []);

  const login = useCallback(() => {
    window.location.href = `/key/discord-auth?mode=pack&redirect_to=${redirectPage}`;
  }, [redirectPage]);

  const logout = useCallback(() => {
    localStorage.removeItem(SESSION_KEY);
    setSession(null);
  }, []);

  return {
    dcToken: session?.token ?? null,
    userTag: session?.userTag ?? null,
    isLoggedIn: !!session,
    login,
    logout,
  };
}
