import { useEffect, useState } from "react";

const STORAGE_KEY = "jb_admin_key";
const API_BASE_KEY = "jb_api_base";
const ROLE_KEY = "jb_admin_role";
const ROLE_EVT = "jb-admin-role-change";

/** 调用者身份：'admin' = 完整管理员；'low_admin' = 次级管理员（用户面板） */
export type AdminRole = "admin" | "low_admin";
/** 含未确认状态：仅有 key 但尚未通过 /admin/status 校验时返回 'unknown' */
export type AdminRoleResolved = AdminRole | "unknown";

export function getAdminKey(): string {
  return localStorage.getItem(STORAGE_KEY) ?? "";
}

export function setAdminKey(key: string): void {
  localStorage.setItem(STORAGE_KEY, key);
  // key 变更时清除旧 role，强制重新校验身份
  localStorage.removeItem(ROLE_KEY);
  if (typeof window !== "undefined") window.dispatchEvent(new Event(ROLE_EVT));
}

export function clearAdminKey(): void {
  localStorage.removeItem(STORAGE_KEY);
  localStorage.removeItem(ROLE_KEY);
  if (typeof window !== "undefined") window.dispatchEvent(new Event(ROLE_EVT));
}

/** 同步读取 role；未确认时返回 'unknown'，避免 UI 先闪一下 admin */
export function getAdminRole(): AdminRoleResolved {
  const v = localStorage.getItem(ROLE_KEY);
  if (v === "low_admin") return "low_admin";
  if (v === "admin") return "admin";
  return "unknown";
}

export function setAdminRole(role: AdminRole): void {
  localStorage.setItem(ROLE_KEY, role);
  if (typeof window !== "undefined") window.dispatchEvent(new Event(ROLE_EVT));
}

/** 是否完整管理员（仅在角色已确认为 admin 时为 true） */
export function isFullAdmin(): boolean {
  return !!getAdminKey() && getAdminRole() === "admin";
}

/** React 钩子：响应式订阅 role 变更（同窗口 + 跨标签页 storage 事件） */
export function useAdminRole(): AdminRoleResolved {
  const [v, setV] = useState<AdminRoleResolved>(() => getAdminRole());
  useEffect(() => {
    const onChange = () => setV(getAdminRole());
    window.addEventListener(ROLE_EVT, onChange);
    window.addEventListener("storage", onChange);
    return () => {
      window.removeEventListener(ROLE_EVT, onChange);
      window.removeEventListener("storage", onChange);
    };
  }, []);
  return v;
}

/**
 * Returns the API base URL.
 * Priority: localStorage override → window.location.origin (same-domain routing)
 *
 * In dev: Vite proxy intercepts /admin/* at the same origin (localhost:PORT)
 * In production: Replit path-based router sends /admin/* to the API Server
 */
export function getApiBase(): string {
  return localStorage.getItem(API_BASE_KEY)?.replace(/\/$/, "") ?? window.location.origin;
}

export function setApiBase(url: string): void {
  if (url) {
    localStorage.setItem(API_BASE_KEY, url.replace(/\/$/, ""));
  } else {
    localStorage.removeItem(API_BASE_KEY);
  }
}

/** fetch wrapper that auto-injects X-Admin-Key header and resolves absolute URL */
export function adminFetch(input: RequestInfo | URL, init: RequestInit = {}): Promise<Response> {
  const key = getAdminKey();
  const headers = new Headers(init.headers);
  if (key) headers.set("X-Admin-Key", key);

  // Convert relative paths to absolute using API base URL
  let url: RequestInfo | URL = input;
  if (typeof input === "string" && input.startsWith("/")) {
    url = getApiBase() + input;
  }

  return fetch(url, { ...init, headers });
}
