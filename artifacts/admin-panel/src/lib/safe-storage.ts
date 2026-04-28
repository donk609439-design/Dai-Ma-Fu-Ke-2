export type StorageArea = "local" | "session";

function getStorage(area: StorageArea): Storage | null {
  if (typeof window === "undefined") return null;

  try {
    return area === "local" ? window.localStorage : window.sessionStorage;
  } catch {
    return null;
  }
}

export function safeGetStorageItem(key: string, area: StorageArea = "local"): string | null {
  try {
    return getStorage(area)?.getItem(key) ?? null;
  } catch {
    return null;
  }
}

export function safeSetStorageItem(
  key: string,
  value: string,
  area: StorageArea = "local",
): boolean {
  try {
    getStorage(area)?.setItem(key, value);
    return true;
  } catch {
    return false;
  }
}

export function safeRemoveStorageItem(key: string, area: StorageArea = "local"): void {
  try {
    getStorage(area)?.removeItem(key);
  } catch {
    // ignore storage errors; callers should never white-screen because cleanup failed
  }
}

export function safeClearAppStorage(): void {
  const keys = [
    "jb_admin_key",
    "jb_admin_role",
    "jb_api_base",
    "dc_auth",
    "sr_dc_auth",
    "partner_push_log_v1",
  ];

  for (const key of keys) {
    safeRemoveStorageItem(key, "local");
    safeRemoveStorageItem(key, "session");
  }
}
