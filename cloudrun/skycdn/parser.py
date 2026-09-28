import datetime as dt
import json
from pathlib import Path
from urllib.parse import urlsplit

MODEL = json.loads((Path(__file__).with_name("skycdn_model.json")).read_text())

OTT_SERVICES = {
    "nowtv_linear", "nowtv_vod",
    "skygo_linear", "skygo_vod",
}
SOIP_SERVICES = {
    "soip_linear", "soip_vod", "soip_ads", "soip_npvr", "soip_ivod",
}
LEGACY_SERVICES = {
    "hip_linear", "hip_vod",
    "verdi_linear", "verdi_vod",
    "vod-stb",
}


def parse_fields(line: str) -> dict[str, str]:
    # Equivalent to Java line.trim().split("\\s+") for SkyCDN.
    tokens = line.strip().split()
    return {
        key: tokens[index] if index < len(tokens) else ""
        for key, index in MODEL.items()
    }


def _host_from_url(url: str) -> str:
    if not url:
        return ""
    try:
        return (urlsplit(url).hostname or "").lower()
    except Exception:
        return ""


def _contains(value: str, needle: str) -> bool:
    return needle in (value or "").lower()


def resolve_service(row: dict[str, str]) -> str:
    # Intentionally mirrors the current Java ServiceResolver ordering/naming.
    url = row.get("csUri", "")
    host = (row.get("requestedHost", "") or "").lower()
    if not host or host == "-":
        host = _host_from_url(url)
    path = (url or "").lower()

    ads = _contains(host, "ads") or _contains(path, "ads")
    live = (
        _contains(host, "lin") or _contains(host, "live")
        or _contains(path, "lin") or _contains(path, "live")
        or _contains(path, "/service/") or _contains(path, "/channel")
    )
    vod = _contains(host, "vod") or _contains(path, ".nff") or _contains(path, "vod")
    ivod = _contains(host, "ivod") or _contains(path, "ivod")
    npvr = _contains(host, "npvr") or _contains(path, "npvr")

    skygo = (
        _contains(host, "skygo") or _contains(path, "/100e/") or _contains(path, "qgo")
        or _contains(host, "skyq") or _contains(path, "skyq")
    )
    nowtv = (
        _contains(host, "cssott02.com") or _contains(host, "cdn13.skycdp.com")
        or _contains(path, "cssott02.com") or _contains(path, "cdn13.skycdp.com")
        or _contains(path, "/016a/") or _contains(path, "now")
    )
    vodstb = (
        _contains(host, "stb") or _contains(path, "stb")
        or _contains(host, "vod-stb") or _contains(host, "pdl")
        or _contains(path, "nff")
    )
    soip = (
        _contains(host, "cdn03.skycdp.com") or _contains(path, "cdn03.skycdp.com")
        or _contains(host, "c02.skycdp.com") or _contains(path, "c02.skycdp.com")
    )
    hip = _contains(host, "hip") or _contains(path, "hip")
    verdi = _contains(host, "qpuck") or _contains(path, "qpuck")

    if skygo:
        if live:
            return "skygo_linear"
        if vod:
            return "skygo_vod"

    if nowtv:
        if live:
            return "nowtv_linear"
        if vod:
            return "nowtv_vod"

    if vodstb:
        return "vod_stb"

    if hip:
        if live:
            return "hip_linear"
        if vod:
            return "hip_vod"

    if verdi:
        if live:
            return "verdi_linear"
        if vod:
            return "verdi_vod"

    if soip:
        if live:
            return "soip_linear"
        if ivod:
            return "soip_ivod"
        if vod:
            return "soip_vod"
        if ads:
            return "soip_ads"
        if npvr:
            return "npvr"

    return "nomatch"


def _to_int(value: str):
    try:
        return int(value)
    except Exception:
        return None


def _to_float(value: str):
    try:
        return float(value)
    except Exception:
        return None


def _parse_instant(value: str) -> dt.datetime:
    if not value:
        raise ValueError("empty datetime")
    parsed = dt.datetime.fromisoformat(value.replace("Z", "+00:00"))
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=dt.timezone.utc)
    return parsed.astimezone(dt.timezone.utc)


def _request_path(url: str):
    try:
        return urlsplit(url).path
    except Exception:
        return url


def map_to_bq_row(row: dict[str, str], object_name: str, date_insert: str) -> dict:
    instant = _parse_instant(row.get("datetime", ""))
    response_size = _to_float(row.get("responseSize", ""))
    if response_size is None:
        response_size = _to_float(row.get("csBytes", ""))

    return {
        "request_id": None,
        "date_start": instant.date().isoformat(),
        "request_time": instant.strftime("%Y-%m-%d %H:%M:%S.%f")[:-3],
        "client_ip": row.get("csIp"),
        "http_status_code": _to_int(row.get("pssc", "")),
        "request_host": row.get("requestedHost"),
        "request_path": _request_path(row.get("csUri", "")),
        "response_content_type": row.get("psct"),
        "user_agent": row.get("csUserAgent"),
        "total_bytes": response_size,
        "error_code": None,
        "turnaround_time": _to_float(row.get("requestTime", "")),
        "transfer_time": _to_float(row.get("ttfb", "")),
        "custom_field_unprocessed": None,
        "cache_status": None,
        "request_end_time": None,
        "asnum": None,
        "breadcrumbs": None,
        "protocol_type": row.get("csProtocol"),
        "request_method": row.get("httpMethod"),
        "cookie": None,
        "referer": None,
        "edge_ip": row.get("hii"),
        "xmt": row.get("xmt"),
        "cmcd": row.get("cmcd"),
        "crc": row.get("crc"),
        "phr": row.get("phr"),
        "file_name": object_name,
        "date_insert": date_insert,
    }


def destination_for(service: str) -> tuple[str, str] | None:
    if service in LEGACY_SERVICES:
        group = "legacy"
    elif service in SOIP_SERVICES:
        group = "soip"
    elif service in OTT_SERVICES:
        group = "ott"
    else:
        return None

    return f"it_skycdn_{group}_logs", service.replace("-", "_")
