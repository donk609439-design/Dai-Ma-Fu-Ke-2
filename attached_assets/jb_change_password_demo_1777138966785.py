#!/usr/bin/env python3
"""Standalone demo for JetBrains Account password change without browser.

The script logs into JetBrains Account with the current password, opens the
change-password page to obtain the one-time `_st` token, and submits the form
directly over HTTP.
"""

from __future__ import annotations

import argparse
import base64
import csv
import getpass
import hashlib
import json
import os
from pathlib import Path
import re
import secrets
import ssl
import sys
import time
import uuid
from dataclasses import dataclass
from typing import Any, Dict, Optional, Tuple
from urllib.parse import parse_qs, urlencode, urljoin, urlparse

import httpx


ACCOUNT_BASE = "https://account.jetbrains.com"
DEFAULT_CLIENT_BUILD = "253.29346.143"
DEFAULT_CLIENT_PRODUCT = "WS"


class PasswordChangeError(RuntimeError):
    pass


@dataclass
class OAuthFlow:
    state: str
    redirect_uri: str
    code_verifier: str
    code_challenge: str


@dataclass
class PasswordChangeJob:
    email: str
    password: str
    new_password: str


def b64url_sha256(text: str) -> str:
    digest = hashlib.sha256(text.encode("utf-8")).digest()
    return base64.urlsafe_b64encode(digest).decode("utf-8").rstrip("=")


def print_step(index: int, total: int, text: str) -> None:
    print(f"[{index}/{total}] {text}...")


def print_ok(text: str) -> None:
    print(f"\u2713 {text}")


def print_warn(text: str) -> None:
    print(f"! {text}")


def _resolve_new_password(current_password: str, explicit_new_password: str, append_chars: str) -> str:
    if explicit_new_password:
        return explicit_new_password
    if append_chars:
        return f"{current_password}{append_chars}"
    return ""
    append_chars: str = ""


class PasswordChangeDemo:
    def __init__(self, args: argparse.Namespace):
        self.args = args
        verify_setting: bool | str | ssl.SSLContext = True
        ca_bundle = (args.ca_bundle or os.getenv("JBAI_CA_BUNDLE") or "").strip()
        if args.insecure:
            verify_setting = False
        elif ca_bundle:
            verify_setting = ca_bundle
        elif os.name == "nt":
            verify_setting = ssl.create_default_context()

        self.client = httpx.Client(
            timeout=args.timeout,
            follow_redirects=False,
            verify=verify_setting,
            headers={"User-Agent": args.user_agent},
        )
        self.flow = self._new_oauth_flow(args.redirect_uri)

    def close(self) -> None:
        self.client.close()

    @staticmethod
    def _is_transient_network_error(exc: Exception) -> bool:
        text = str(exc).lower()
        hints = [
            "winerror 10054",
            "winerror 10053",
            "connection reset",
            "connection aborted",
            "connection refused",
            "read timeout",
            "timed out",
            "temporarily unavailable",
        ]
        return any(h in text for h in hints)

    def _request_with_retry(self, method: str, url: str, **kwargs) -> httpx.Response:
        last_exc: Optional[Exception] = None
        attempts = max(1, int(self.args.retries))

        for idx in range(1, attempts + 1):
            try:
                return self.client.request(method, url, **kwargs)
            except httpx.TransportError as exc:
                last_exc = exc
                if idx >= attempts or not self._is_transient_network_error(exc):
                    raise
                wait_seconds = float(self.args.retry_delay) * idx
                print_warn(
                    f"网络暂时异常: {method} {urlparse(url).path}，"
                    f"将在 {wait_seconds:.1f}s 后重试 ({idx}/{attempts - 1})"
                )
                time.sleep(wait_seconds)

        if last_exc:
            raise last_exc
        raise PasswordChangeError("重试流程出现异常状态")

    @staticmethod
    def _new_oauth_flow(redirect_uri: str) -> OAuthFlow:
        code_verifier = secrets.token_urlsafe(64)
        return OAuthFlow(
            state=str(uuid.uuid4()),
            redirect_uri=redirect_uri,
            code_verifier=code_verifier,
            code_challenge=b64url_sha256(code_verifier),
        )

    def _get_account_cookie(self, name: str) -> str:
        for cookie in self.client.cookies.jar:
            if cookie.name == name and "account.jetbrains.com" in (cookie.domain or ""):
                return cookie.value
        return self.client.cookies.get(name) or ""

    def _build_client_info(self) -> str:
        payload = {
            "build": (self.args.client_build or DEFAULT_CLIENT_BUILD).strip() or DEFAULT_CLIENT_BUILD,
            "product": (self.args.client_product or DEFAULT_CLIENT_PRODUCT).strip() or DEFAULT_CLIENT_PRODUCT,
        }
        raw = json.dumps(payload, separators=(",", ":"), ensure_ascii=True).encode("utf-8")
        return base64.urlsafe_b64encode(raw).decode("utf-8").rstrip("=")

    def _build_account_api_headers(self, auth_session_id: str) -> Dict[str, str]:
        xsrf_token = self._get_account_cookie("_st-JBA")
        headers: Dict[str, str] = {
            "Accept": "application/json, text/plain, */*",
            "Content-Type": "application/json",
            "Accept-Language": "zh-CN,zh;q=0.9,en;q=0.8",
            "Origin": ACCOUNT_BASE,
            "Referer": f"{ACCOUNT_BASE}/login?authSessionId={auth_session_id}&reauthenticate=true",
            "X-Requested-With": "XMLHttpRequest",
            "sec-fetch-site": "same-origin",
            "sec-fetch-mode": "cors",
            "sec-fetch-dest": "empty",
            "sec-ch-ua": '"Chromium";v="124", "Not-A.Brand";v="99"',
            "sec-ch-ua-mobile": "?0",
            "sec-ch-ua-platform": '"Windows"',
        }
        if xsrf_token:
            headers["x-xsrf-token"] = xsrf_token
        return headers

    def _build_login_url(self) -> str:
        oauth_query = urlencode(
            {
                "client_id": "ide",
                "scope": "openid offline_access r_ide_auth",
                "state": self.flow.state,
                "code_challenge_method": "S256",
                "code_challenge": self.flow.code_challenge,
                "redirect_uri": self.flow.redirect_uri,
                "response_type": "code",
                "request_credentials": "required",
                "client_info": self._build_client_info(),
            }
        )
        return f"{ACCOUNT_BASE}/oauth/login?{oauth_query}"

    def _follow_redirects_until(self, start_url: str, max_hops: int) -> Tuple[httpx.Response, str, int]:
        current = start_url
        hops = 0

        while True:
            resp = self._request_with_retry("GET", current)
            if 300 <= resp.status_code < 400:
                location = resp.headers.get("location")
                if not location:
                    raise PasswordChangeError("重定向响应缺少 Location 头")
                current = urljoin(str(resp.request.url), location)
                hops += 1
                if hops > max_hops:
                    raise PasswordChangeError("登录跳转次数过多")
                continue

            return resp, str(resp.request.url), hops

    def _consume_return_to(self, return_to: str) -> None:
        if not return_to:
            return

        parsed = urlparse(return_to)
        account_netloc = urlparse(ACCOUNT_BASE).netloc
        if parsed.scheme in {"http", "https"} and parsed.netloc and parsed.netloc != account_netloc:
            return
        if parsed.scheme and parsed.netloc == "localhost":
            return

        target = urljoin(ACCOUNT_BASE, return_to)
        self._request_with_retry("GET", target)

    def step_login(self, email: str, password: str) -> None:
        print_step(1, 2, "JBA 登录")

        login_url = self._build_login_url()
        resp, final_url, _ = self._follow_redirects_until(login_url, max_hops=10)
        if resp.status_code != 200:
            raise PasswordChangeError(f"无法打开登录页，HTTP {resp.status_code}")

        parsed = urlparse(final_url)
        auth_session_ids = parse_qs(parsed.query).get("authSessionId", [])
        if not auth_session_ids:
            raise PasswordChangeError("登录 URL 中未找到 authSessionId")
        auth_session_id = auth_session_ids[0]

        session_api = f"{ACCOUNT_BASE}/api/auth/sessions/{auth_session_id}"
        session_get = self._request_with_retry("GET", session_api)
        if session_get.status_code != 200:
            raise PasswordChangeError(f"无法加载认证会话，HTTP {session_get.status_code}")

        api_headers = self._build_account_api_headers(auth_session_id)

        email_resp = self._request_with_retry(
            "POST",
            session_api + "/email/login",
            json={"email": email},
            headers=api_headers,
        )
        if email_resp.status_code != 200:
            snippet = (email_resp.text or "")[:240]
            raise PasswordChangeError(f"提交邮箱失败，HTTP {email_resp.status_code}。response={snippet}")

        pwd_resp = self._request_with_retry(
            "POST",
            session_api + "/password",
            json={"email": email, "password": password},
            headers=api_headers,
        )
        if pwd_resp.status_code != 200:
            snippet = (pwd_resp.text or "")[:240]
            raise PasswordChangeError(f"提交密码失败，HTTP {pwd_resp.status_code}。response={snippet}")

        pwd_json = pwd_resp.json()
        state = (pwd_json.get("state") or "").strip()
        if state != "REDIRECT_TO_RETURN_URL":
            raise PasswordChangeError(f"登录状态异常: {state}")

        return_to = (pwd_json.get("returnTo") or "").strip()
        if not return_to:
            raise PasswordChangeError("登录响应缺少 returnTo")

        self._consume_return_to(return_to)
        print_ok("登录成功")
        print(f"  returnTo: {return_to}")

    def step_change_password(self, current_password: str, new_password: str) -> None:
        print_step(2, 2, "提交改密表单")

        page = self._request_with_retry("GET", f"{ACCOUNT_BASE}/change-password")
        if page.status_code != 200:
            raise PasswordChangeError(f"无法打开改密页面，HTTP {page.status_code}")

        match = re.search(r'action="/change-password\?_st=([^"]+)"', page.text or "", flags=re.IGNORECASE)
        if not match:
            raise PasswordChangeError("未能从改密页面提取 _st token")
        st_token = match.group(1)

        headers = {
            "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.7",
            "Content-Type": "application/x-www-form-urlencoded; charset=UTF-8",
            "Origin": ACCOUNT_BASE,
            "Referer": f"{ACCOUNT_BASE}/change-password",
        }

        resp = self._request_with_retry(
            "POST",
            f"{ACCOUNT_BASE}/change-password?_st={st_token}",
            data={
                "old_password": current_password,
                "password": new_password,
                "pass2": new_password,
            },
            headers=headers,
        )

        if resp.status_code == 302 and resp.headers.get("location") == "/":
            print_ok("密码修改成功")
            return

        snippet = (resp.text or "")[:400].replace("\n", " ")
        raise PasswordChangeError(
            f"改密失败，HTTP {resp.status_code}，location={resp.headers.get('location', '')}，body={snippet}"
        )

    def change_password(self, email: str, current_password: str, new_password: str) -> None:
        self.step_login(email, current_password)
        self.step_change_password(current_password, new_password)


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="JetBrains Account password change demo")
    parser.add_argument("--email", default="", help="JetBrains Account email")
    parser.add_argument("--password", default="", help="Current password")
    parser.add_argument("--new-password", default="", help="New password")
    parser.add_argument("--append-chars", default="", help="Append characters to current password to form the new password")
    parser.add_argument("--csv", default="", help="CSV file for batch mode")
    parser.add_argument("--redirect-uri", default="http://localhost:62345", help="OAuth redirect URI")
    parser.add_argument("--timeout", type=float, default=30.0, help="HTTP timeout in seconds")
    parser.add_argument("--retries", type=int, default=3, help="Retry count for transient network errors")
    parser.add_argument("--retry-delay", type=float, default=0.8, help="Base retry delay in seconds")
    parser.add_argument("--user-agent", default="Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/133.0.0.0 Safari/537.36", help="User-Agent header")
    parser.add_argument("--ca-bundle", default="", help="Custom CA bundle path")
    parser.add_argument("--insecure", action="store_true", help="Disable TLS verification")
    parser.add_argument("--client-build", default=DEFAULT_CLIENT_BUILD, help="Client build used for login bootstrap")
    parser.add_argument("--client-product", default=DEFAULT_CLIENT_PRODUCT, help="Client product used for login bootstrap")
    parser.add_argument("--output", default="", help="Optional output JSON file")
    return parser


def _prompt_if_missing(args: argparse.Namespace) -> Tuple[str, str, str]:
    email = (args.email or "").strip() or input("JetBrains Account 邮箱: ").strip()
    password = (args.password or "").strip() or getpass.getpass("当前密码: ")

    new_password = _resolve_new_password(password, (args.new_password or "").strip(), (args.append_chars or "").strip())
    if not new_password:
        append_chars = (args.append_chars or "").strip()
        if append_chars:
            new_password = f"{password}{append_chars}"
        else:
            new_password = getpass.getpass("新密码: ")
            confirm = getpass.getpass("再次输入新密码: ")
            if new_password != confirm:
                raise PasswordChangeError("两次输入的新密码不一致")

    return email, password, new_password


def _normalize_csv_key(key: str) -> str:
    return str(key or "").strip().lower().replace(" ", "_")


def _resolve_row_value(row: Dict[str, str], *names: str) -> str:
    normalized = {_normalize_csv_key(key): value for key, value in row.items()}
    for name in names:
        value = str(normalized.get(name, "") or "").strip()
        if value:
            return value
    return ""


def load_jobs_from_csv(csv_path: str, default_new_password: str, default_append_chars: str) -> list[PasswordChangeJob]:
    file_path = Path(csv_path)
    if not file_path.exists() or not file_path.is_file():
        raise PasswordChangeError(f"CSV 文件不存在: {csv_path}")

    jobs: list[PasswordChangeJob] = []
    with file_path.open("r", encoding="utf-8-sig", newline="") as handle:
        reader = csv.DictReader(handle)
        if not reader.fieldnames:
            raise PasswordChangeError("CSV 缺少表头")

        for idx, row in enumerate(reader, start=2):
            email = _resolve_row_value(row, "email", "mail", "account")
            password = _resolve_row_value(row, "password", "current_password", "old_password")
            new_password = _resolve_row_value(row, "new_password", "newpass", "target_password")
            append_chars = _resolve_row_value(row, "append_chars", "append", "suffix")
            if not new_password:
                new_password = _resolve_new_password(password, default_new_password.strip(), append_chars or default_append_chars.strip())

            if not email or not password or not new_password:
                raise PasswordChangeError(
                    f"CSV 第 {idx} 行缺少必填字段，至少需要 email、password，且 new_password 需要在 CSV 或 --new-password / --append-chars 中提供"
                )

            jobs.append(PasswordChangeJob(email=email, password=password, new_password=new_password, append_chars=append_chars))

    if not jobs:
        raise PasswordChangeError("CSV 中没有可执行的账号")

    return jobs


def run_single_job(args: argparse.Namespace) -> Dict[str, Any]:
    email, password, new_password = _prompt_if_missing(args)
    if not email:
        raise PasswordChangeError("邮箱不能为空")
    if not password:
        raise PasswordChangeError("当前密码不能为空")
    if not new_password:
        raise PasswordChangeError("新密码不能为空")

    demo = PasswordChangeDemo(args)
    try:
        demo.change_password(email, password, new_password)
        return {"success": True, "email": email, "changed": True}
    finally:
        demo.close()


def run_batch_jobs(args: argparse.Namespace) -> Dict[str, Any]:
    default_new_password = _resolve_new_password("", (args.new_password or "").strip(), "")
    jobs = load_jobs_from_csv(args.csv, default_new_password, (args.append_chars or "").strip())

    results: list[Dict[str, Any]] = []
    total = len(jobs)
    for index, job in enumerate(jobs, start=1):
        print_step(index, total, f"批量处理 {job.email}")
        demo = PasswordChangeDemo(args)
        try:
            demo.change_password(job.email, job.password, job.new_password)
            results.append({"email": job.email, "success": True, "changed": True})
        except Exception as exc:
            results.append({"email": job.email, "success": False, "error": str(exc)})
            print_warn(f"{job.email} 失败: {exc}")
        finally:
            demo.close()

    summary = {
        "success": all(item.get("success") for item in results),
        "mode": "batch",
        "total": total,
        "changed_count": sum(1 for item in results if item.get("success")),
        "failed_count": sum(1 for item in results if not item.get("success")),
        "results": results,
    }
    return summary


def main(argv: list[str]) -> int:
    parser = build_parser()
    args = parser.parse_args(argv)

    try:
        if args.csv:
            result = run_batch_jobs(args)
        else:
            result = run_single_job(args)

        if args.output:
            Path(args.output).write_text(json.dumps(result, ensure_ascii=False, indent=2), encoding="utf-8")

        if not result.get("success", False):
            return 1
        return 0
    except Exception as exc:
        if args.output:
            Path(args.output).write_text(
                json.dumps({"success": False, "error": str(exc)}, ensure_ascii=False, indent=2),
                encoding="utf-8",
            )
        print(f"ERROR: {exc}", file=sys.stderr)
        return 1


if __name__ == "__main__":
    raise SystemExit(main(sys.argv[1:]))
