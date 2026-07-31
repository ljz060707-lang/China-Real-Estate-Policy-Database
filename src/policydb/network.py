from __future__ import annotations

import ipaddress
import json
import os
import socket
import ssl
import subprocess
from dataclasses import dataclass, field
from datetime import UTC, datetime
from typing import Any
from urllib.parse import urljoin, urlsplit

import httpx
import polars as pl

from policydb.crawl.dedup import content_sha256
from policydb.settings import Settings

REDIRECT_CODES = {301, 302, 303, 307, 308}
NETWORK_STATUSES = {
    "direct_ok",
    "proxy_ok",
    "direct_required",
    "tls_incompatible",
    "tun_intercepted",
    "dns_failed",
    "timeout",
    "http_fallback_ok",
    "curl_fallback_ok",
    "blocked",
    "unknown",
}


def _redacted_proxy_environment() -> dict[str, str | bool]:
    result: dict[str, str | bool] = {}
    for key in ("HTTP_PROXY", "HTTPS_PROXY", "ALL_PROXY", "NO_PROXY"):
        value = os.getenv(key) or os.getenv(key.lower())
        if not value:
            result[key] = False
        elif key == "NO_PROXY":
            result[key] = value
        else:
            result[key] = True
    return result


def _redacted_proxy_endpoint(value: str) -> dict[str, Any]:
    parsed = urlsplit(value)
    return {
        "configured": bool(value),
        "scheme": parsed.scheme or None,
        "host": parsed.hostname,
        "port": parsed.port,
        "credentials_present": bool(parsed.username or parsed.password),
    }


def _is_tun_fake_ip(address: str) -> bool:
    try:
        return ipaddress.ip_address(address) in ipaddress.ip_network("198.18.0.0/15")
    except ValueError:
        return False


def _same_government_domain(first: str, second: str, aliases: set[str]) -> bool:
    left = (urlsplit(first).hostname or "").lower()
    right = (urlsplit(second).hostname or "").lower()
    allowed = {item.lower() for item in aliases}
    if right in allowed:
        return True
    if left == right:
        return True
    left_parts = left.split(".")
    right_parts = right.split(".")
    return (
        len(left_parts) >= 3
        and len(right_parts) >= 3
        and left_parts[-3:] == right_parts[-3:]
        and right.endswith(".gov.cn")
    )


def _tls_like(error: Exception) -> bool:
    lowered = str(error).lower()
    return any(token in lowered for token in ("ssl", "tls", "unexpected_eof", "eof while reading"))


def _curl_schannel_get(
    url: str, original_url: str, aliases: set[str]
) -> DirectResponse | None:
    if os.name != "nt":
        return None
    host = (urlsplit(url).hostname or "").lower()
    if not (host == "gov.cn" or host.endswith(".gov.cn") or host in aliases):
        return None
    marker = b"\n__CRPD_CURL_META__"
    try:
        completed = subprocess.run(
            [
                "curl.exe",
                "--noproxy",
                "*",
                "--location",
                "--max-redirs",
                "10",
                "--max-time",
                "30",
                "--silent",
                "--show-error",
                "--write-out",
                "\n__CRPD_CURL_META__%{http_code}|%{url_effective}",
                url,
            ],
            check=False,
            capture_output=True,
            timeout=35,
        )
    except (OSError, subprocess.SubprocessError):
        return None
    if completed.returncode != 0 or marker not in completed.stdout:
        return None
    body, metadata = completed.stdout.rsplit(marker, 1)
    status_text, _, final_url = metadata.decode("utf-8", errors="replace").partition("|")
    if not status_text.isdigit() or not final_url:
        return None
    if not _same_government_domain(original_url, final_url, aliases):
        return None
    return DirectResponse(
        requested_url=original_url,
        final_url=final_url,
        status_code=int(status_text),
        headers={},
        content=body,
        redirect_chain=[
            {
                "url": url,
                "status_code": int(status_text),
                "location": final_url,
                "via": "curl_schannel",
            }
        ],
        network_route="curl_fallback_ok",
        protocol=urlsplit(final_url).scheme,
        resolved_addresses=GovernmentDirectClient.resolve_host(final_url),
        fallback_used="curl_schannel",
    )


@dataclass(slots=True)
class DirectResponse:
    requested_url: str
    final_url: str
    status_code: int
    headers: dict[str, str]
    content: bytes
    redirect_chain: list[dict[str, Any]] = field(default_factory=list)
    network_route: str = "direct_ok"
    protocol: str = "https"
    resolved_addresses: list[str] = field(default_factory=list)
    fallback_used: str | None = None


class GovernmentDirectClient:
    """HTTP client for official sites that never inherits proxy environment variables."""

    def __init__(
        self,
        *,
        timeout: float = 30.0,
        connect_timeout: float = 10.0,
        user_agent: str = "Mozilla/5.0 (compatible; CRPDResearchBot/2.0)",
        max_redirects: int = 10,
        allowed_aliases: set[str] | None = None,
        transport: httpx.BaseTransport | None = None,
    ) -> None:
        self.max_redirects = max_redirects
        self.allowed_aliases = allowed_aliases or set()
        self.client = httpx.Client(
            trust_env=False,
            follow_redirects=False,
            verify=True,
            headers={"User-Agent": user_agent},
            timeout=httpx.Timeout(timeout, connect=connect_timeout),
            transport=transport,
        )

    @staticmethod
    def resolve_host(url: str) -> list[str]:
        host = urlsplit(url).hostname
        if not host:
            return []
        try:
            return sorted(
                {item[4][0] for item in socket.getaddrinfo(host, None)}
            )
        except socket.gaierror:
            return []

    def get(self, url: str, *, headers: dict[str, str] | None = None) -> DirectResponse:
        original = url
        current = url
        chain: list[dict[str, Any]] = []
        seen: set[str] = set()
        response: httpx.Response | None = None
        for _ in range(self.max_redirects + 1):
            if current in seen:
                raise httpx.TooManyRedirects("redirect loop detected")
            seen.add(current)
            try:
                response = self.client.get(current, headers=headers)
            except Exception as exc:
                if _tls_like(exc):
                    fallback = _curl_schannel_get(
                        current,
                        original,
                        {item.lower() for item in self.allowed_aliases},
                    )
                    if fallback is not None:
                        fallback.redirect_chain = [*chain, *fallback.redirect_chain]
                        return fallback
                exc.redirect_chain = list(chain)
                exc.network_route = "direct"
                exc.requested_protocol = urlsplit(original).scheme
                exc.failed_url = current
                raise
            chain.append(
                {
                    "url": current,
                    "status_code": response.status_code,
                    "location": response.headers.get("location"),
                }
            )
            if response.status_code not in REDIRECT_CODES:
                break
            location = response.headers.get("location")
            if not location:
                break
            target = urljoin(current, location)
            if not _same_government_domain(original, target, self.allowed_aliases):
                raise httpx.HTTPError(
                    f"redirect target is outside verified government domain: {target}"
                )
            current = target
        else:
            raise httpx.TooManyRedirects("redirect limit exceeded")
        assert response is not None
        return DirectResponse(
            requested_url=original,
            final_url=str(response.url),
            status_code=response.status_code,
            headers=dict(response.headers),
            content=response.content,
            redirect_chain=chain,
            network_route="direct_ok",
            protocol=urlsplit(str(response.url)).scheme,
            resolved_addresses=self.resolve_host(str(response.url)),
        )

    def close(self) -> None:
        self.client.close()


class AIProxyClient:
    """Dedicated AI client. It deliberately may inherit the user's proxy settings."""

    def __init__(
        self,
        *,
        timeout: float = 60.0,
        transport: httpx.BaseTransport | None = None,
    ) -> None:
        explicit_proxy = os.getenv("CRPD_AI_PROXY_URL") or os.getenv("CRPD_PROXY_URL")
        self.client = httpx.Client(
            trust_env=not bool(explicit_proxy),
            follow_redirects=True,
            verify=True,
            timeout=timeout,
            transport=transport,
            **({"proxy": explicit_proxy} if explicit_proxy and transport is None else {}),
        )

    def close(self) -> None:
        self.client.close()


def _attempt_httpx(url: str, *, trust_env: bool) -> dict[str, Any]:
    started = datetime.now(UTC)
    try:
        with httpx.Client(
            trust_env=trust_env,
            follow_redirects=False,
            verify=True,
            timeout=httpx.Timeout(12.0, connect=6.0),
        ) as client:
            response = client.get(url)
        return {
            "ok": True,
            "status_code": response.status_code,
            "final_url": str(response.url),
            "location": response.headers.get("location"),
            "elapsed_ms": int(
                (datetime.now(UTC) - started).total_seconds() * 1000
            ),
        }
    except Exception as exc:
        lowered = str(exc).lower()
        status = (
            "tls_incompatible"
            if isinstance(exc, (ssl.SSLError, httpx.ConnectError))
            and ("ssl" in lowered or "tls" in lowered or "eof" in lowered)
            else "dns_failed"
            if "name" in lowered or "dns" in lowered
            else "timeout"
            if isinstance(exc, (httpx.ConnectTimeout, httpx.ReadTimeout))
            else "unknown"
        )
        return {
            "ok": False,
            "error_type": type(exc).__name__,
            "error": str(exc)[:500],
            "network_status": status,
        }


def _curl_direct(url: str) -> dict[str, Any]:
    executable = "curl.exe" if os.name == "nt" else "curl"
    try:
        completed = subprocess.run(
            [
                executable,
                "--noproxy",
                "*",
                "--max-time",
                "15",
                "--silent",
                "--show-error",
                "--head",
                "--output",
                os.devnull,
                "--write-out",
                "%{http_code}|%{url_effective}",
                url,
            ],
            check=False,
            capture_output=True,
            text=True,
            timeout=20,
        )
        value = completed.stdout.strip()
        status, _, final_url = value.partition("|")
        return {
            "ok": completed.returncode == 0,
            "return_code": completed.returncode,
            "status_code": int(status) if status.isdigit() else None,
            "final_url": final_url or None,
            "stderr": completed.stderr.strip()[:500],
        }
    except (OSError, subprocess.SubprocessError) as exc:
        return {"ok": False, "error_type": type(exc).__name__, "error": str(exc)}


def _curl_proxy(url: str, proxy_url: str) -> dict[str, Any]:
    executable = "curl.exe" if os.name == "nt" else "curl"
    try:
        completed = subprocess.run(
            [
                executable,
                "--proxy",
                proxy_url,
                "--max-time",
                "15",
                "--silent",
                "--show-error",
                "--head",
                "--output",
                os.devnull,
                "--write-out",
                "%{http_code}|%{url_effective}",
                url,
            ],
            check=False,
            capture_output=True,
            text=True,
            timeout=20,
        )
        value = completed.stdout.strip()
        status, _, final_url = value.partition("|")
        return {
            "ok": completed.returncode == 0 and status.isdigit() and int(status) > 0,
            "return_code": completed.returncode,
            "status_code": int(status) if status.isdigit() else None,
            "final_url": final_url or None,
            "stderr": completed.stderr.strip()[:500],
        }
    except (OSError, subprocess.SubprocessError) as exc:
        return {"ok": False, "error_type": type(exc).__name__, "error": str(exc)}


def probe_proxy(
    *, url: str = "https://github.com", proxy_url: str | None = None
) -> dict[str, Any]:
    """Detect whether a proxy endpoint accepts HTTP CONNECT and/or SOCKS5H."""
    value = proxy_url or os.getenv("CRPD_PROXY_URL") or "http://127.0.0.1:7897"
    parsed = urlsplit(value if "://" in value else f"http://{value}")
    if not parsed.hostname or not parsed.port:
        raise ValueError("proxy URL must contain host and port")
    authority = f"{parsed.hostname}:{parsed.port}"
    attempts = {
        "http": _curl_proxy(url, f"http://{authority}"),
        "socks5h": _curl_proxy(url, f"socks5h://{authority}"),
    }
    working = [name for name, result in attempts.items() if result.get("ok")]
    return {
        "created_at": datetime.now(UTC).isoformat(),
        "target_url": url,
        "proxy": _redacted_proxy_endpoint(value),
        "protocol": "mixed" if len(working) > 1 else (working[0] if working else "unavailable"),
        "attempts": attempts,
    }


def probe_direct(*, url: str) -> dict[str, Any]:
    """Compare Python trust_env=False and curl --noproxy, with TUN Fake-IP detection."""
    dns = GovernmentDirectClient.resolve_host(url)
    python_result = _attempt_httpx(url, trust_env=False)
    curl_result = _curl_direct(url)
    fake_ip = any(_is_tun_fake_ip(address) for address in dns)
    ok = bool(python_result.get("ok") or curl_result.get("ok"))
    status = "direct_ok" if ok else "tun_intercepted" if fake_ip else str(
        python_result.get("network_status") or "blocked"
    )
    return {
        "created_at": datetime.now(UTC).isoformat(),
        "url": url,
        "dns_addresses": dns,
        "tun_fake_ip_detected": fake_ip,
        "python_direct": python_result,
        "curl_no_proxy": curl_result,
        "network_status": status,
    }


def compare_routes(
    *, url: str, proxy_url: str | None = None
) -> dict[str, Any]:
    direct = probe_direct(url=url)
    proxy = probe_proxy(url=url, proxy_url=proxy_url)
    proxy_ok = any(item.get("ok") for item in proxy["attempts"].values())
    if direct["network_status"] == "direct_ok":
        selected = "direct"
    elif proxy_ok:
        selected = "proxy"
    else:
        selected = "blocked"
    return {
        "created_at": datetime.now(UTC).isoformat(),
        "url": url,
        "selected_route": selected,
        "direct": direct,
        "proxy": proxy,
        "recommendation": (
            "Add DOMAIN-SUFFIX,gov.cn,DIRECT before generic proxy rules and disable Fake-IP for gov.cn."
            if direct["tun_fake_ip_detected"]
            else "Keep government fetches direct and AI/search traffic on the dedicated proxy."
        ),
    }


def audit_source_routes(
    *,
    city: str | None = None,
    enabled_only: bool = True,
    limit: int | None = None,
    settings: Settings | None = None,
) -> dict[str, Any]:
    """Probe registry entries without mutating health or enablement state."""
    from policydb.crawl.registry import load_registry

    settings = settings or Settings.discover()
    city_id: str | None = None
    if city:
        from policydb.scope import load_cities_105

        matches = load_cities_105(settings).filter(
            (pl.col("city_name") == city)
            | (pl.col("city_name_short") == city)
            | (pl.col("city_id") == city)
        )
        if matches.height != 1:
            raise ValueError(f"city must uniquely match the 105-city list: {city}")
        city_id = str(matches[0, "city_id"])
    sources = [
        source
        for source in load_registry(settings)
        if (not enabled_only or source.crawl_enabled)
        and (not city_id or city_id in source.city_ids)
    ]
    if limit is not None:
        sources = sources[:limit]
    rows: list[dict[str, Any]] = []
    for source in sources:
        entry = next(
            (
                item
                for item in [*source.list_page_urls, source.homepage_url]
                if item
            ),
            None,
        )
        result = probe_direct(url=entry) if entry else {"network_status": "missing_entry"}
        rows.append(
            {
                "source_id": source.source_id,
                "source_name": source.source_name,
                "entry_url": entry,
                "network_status": result["network_status"],
                "tun_fake_ip_detected": bool(result.get("tun_fake_ip_detected")),
                "evidence": result,
            }
        )
    counts: dict[str, int] = {}
    for row in rows:
        key = str(row["network_status"])
        counts[key] = counts.get(key, 0) + 1
    report = {
        "created_at": datetime.now(UTC).isoformat(),
        "city": city,
        "enabled_only": enabled_only,
        "checked": len(rows),
        "status_counts": counts,
        "sources": rows,
    }
    output = settings.outputs / "acceptance" / "network_source_audit.json"
    output.parent.mkdir(parents=True, exist_ok=True)
    temp = output.with_suffix(".json.tmp")
    temp.write_text(json.dumps(report, ensure_ascii=False, indent=2), encoding="utf-8")
    os.replace(temp, output)
    report["output"] = str(output)
    return report


def diagnose_network(
    *,
    url: str,
    city: str | None = None,
    settings: Settings | None = None,
) -> dict[str, Any]:
    settings = settings or Settings.discover()
    parsed = urlsplit(url)
    dns = GovernmentDirectClient.resolve_host(url)
    default_result = _attempt_httpx(url, trust_env=True)
    direct_result = _attempt_httpx(url, trust_env=False)
    curl_result = _curl_direct(url)
    fake_ip = any(_is_tun_fake_ip(address) for address in dns)
    if direct_result.get("ok"):
        classification = "direct_ok"
    elif not default_result.get("ok") and curl_result.get("ok"):
        classification = "direct_required"
    elif fake_ip:
        classification = "tun_intercepted"
    elif direct_result.get("network_status") == "tls_incompatible":
        classification = "tun_intercepted" if default_result.get("ok") else "tls_incompatible"
    elif not dns:
        classification = "dns_failed"
    else:
        classification = str(direct_result.get("network_status") or "unknown")
    report = {
        "created_at": datetime.now(UTC).isoformat(),
        "city": city,
        "url": url,
        "host": parsed.hostname,
        "scheme": parsed.scheme,
        "proxy_environment": _redacted_proxy_environment(),
        "dns_addresses": dns,
        "tun_fake_ip_detected": fake_ip,
        "default_request": default_result,
        "direct_request": direct_result,
        "curl_no_proxy": curl_result,
        "network_status": classification,
        "recommendation": (
            "在代理客户端中将 DOMAIN-SUFFIX,gov.cn,DIRECT 置于通用代理规则之前。"
            if classification in {"direct_required", "tun_intercepted"}
            else "保持政府抓取直连；AI客户端继续使用独立代理会话。"
        ),
    }
    output = settings.outputs / "acceptance"
    output.mkdir(parents=True, exist_ok=True)
    path = output / (
        f"network_diagnostics_{'nanjing' if city == '南京市' else 'custom'}.json"
    )
    temp = path.with_suffix(path.suffix + ".tmp")
    temp.write_text(json.dumps(report, ensure_ascii=False, indent=2), encoding="utf-8")
    os.replace(temp, path)
    report["output"] = str(path)
    return report


def response_fingerprint(response: DirectResponse) -> dict[str, Any]:
    return {
        "status_code": response.status_code,
        "final_url": response.final_url,
        "content_sha256": content_sha256(response.content),
        "redirect_chain": response.redirect_chain,
        "network_route": response.network_route,
    }
