#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""从多个优选订阅源收集 IP，去重清洗后生成 ADD.txt / result.csv。"""

from __future__ import annotations

import argparse
import base64
import csv
import json
import re
import socket
import ssl
import sys
import urllib.error
import urllib.parse
import urllib.request
from concurrent.futures import ThreadPoolExecutor, as_completed
from pathlib import Path


USER_AGENT = "v2rayN/edgetunnel (https://github.com/cmliu/edgetunnel)"
UUID_MARK = "00000000-0000-4000-8000-000000000000"
HOST_MARK = "example.com"


NOISE_PATTERNS = [
    r"(?i)t\.me/[^\s]+",
    r"(?i)telegram",
    r"(?i)官方群组",
    r"(?i)官方频道",
    r"(?i)官方更新",
    r"(?i)官方发布",
    r"(?i)订阅免费谨防受骗",
    r"(?i)免费共享",
    r"(?i)加入我的频道",
    r"(?i)解锁更多优选节点",
    r"(?i)天诚",
    r"(?i)🐲|™️|🌐",
    r"(?i)群组@[\w_]+",
]


COUNTRY_HINTS = {
    "HKG": "香港",
    "HK": "香港",
    "NRT": "日本",
    "KIX": "日本",
    "JP": "日本",
    "TPE": "台湾",
    "TW": "台湾",
    "SIN": "新加坡",
    "SG": "新加坡",
    "ICN": "韩国",
    "KR": "韩国",
    "LAX": "美国",
    "SJC": "美国",
    "SEA": "美国",
    "DFW": "美国",
    "ORD": "美国",
    "IAD": "美国",
    "JFK": "美国",
    "EWR": "美国",
    "ATL": "美国",
    "MIA": "美国",
    "US": "美国",
    "FI": "芬兰",
    "DE": "德国",
    "TR": "土耳其",
    "UK": "英国",
    "GB": "英国",
    "FR": "法国",
    "NL": "荷兰",
    "TW": "台湾",
    "CA": "加拿大",
    "AU": "澳大利亚",
}


GEOIP_URL = "https://ipwho.is/{ip}"
IPV4_ONLY_RE = re.compile(r"^(\\d{1,3}(?:\\.\\d{1,3}){3}):(\\d+)$")
GEOIP_COUNTRY_MAP = {
    "HK": "香港",
    "TW": "台湾",
    "JP": "日本",
    "KR": "韩国",
    "SG": "新加坡",
    "TH": "泰国",
    "MY": "马来西亚",
    "PH": "菲律宾",
    "ID": "印度尼西亚",
    "VN": "越南",
    "IN": "印度",
    "AU": "澳大利亚",
    "NZ": "新西兰",
    "US": "美国",
    "CA": "加拿大",
    "MX": "墨西哥",
    "BR": "巴西",
    "CL": "智利",
    "GB": "英国",
    "FR": "法国",
    "DE": "德国",
    "NL": "荷兰",
    "ES": "西班牙",
    "IT": "意大利",
    "CH": "瑞士",
    "AT": "奥地利",
    "SE": "瑞典",
    "DK": "丹麦",
    "PL": "波兰",
    "CZ": "捷克",
    "TR": "土耳其",
    "AE": "阿联酋",
    "QA": "卡塔尔",
    "ZA": "南非",
    "FI": "芬兰",
}


def fetch_text(url: str) -> str:
    req = urllib.request.Request(url, headers={"User-Agent": USER_AGENT})
    with urllib.request.urlopen(req, timeout=30) as resp:
        return resp.read().decode("utf-8", errors="replace")


def decode_subscription(text: str) -> str:
    clean = "".join(text.split())
    if not clean:
        return ""
    try:
        return base64.b64decode(clean + "=" * (-len(clean) % 4)).decode("utf-8", errors="replace")
    except Exception:
        return text


def source_url(host: str) -> str:
    host = host.strip()
    if not host:
        raise ValueError("空订阅源")
    if not re.match(r"^https?://", host, re.I):
        host = "https://" + host
    host = host.rstrip("/")
    return f"{host}/sub?host=example.com&uuid={UUID_MARK}"


def extract_addr_and_remark(line: str) -> tuple[str, str] | None:
    if UUID_MARK not in line or HOST_MARK not in line:
        return None
    m = re.search(r"://[^@]+@([^?]+)", line)
    if not m:
        return None
    addr = m.group(1).strip()
    remark = ""
    h = re.search(r"#(.+)$", line)
    if h:
        remark = urllib.parse.unquote(h.group(1)).strip()
    return addr, remark


def clean_remark(remark: str) -> str:
    text = remark.strip()
    for pattern in NOISE_PATTERNS:
        text = re.sub(pattern, "", text)
    text = re.sub(r"\s+", " ", text).strip(" -_|,，。.;；")
    return text


def normalize_location(remark: str) -> str:
    if not remark:
        return "优选"
    upper = remark.upper()
    for key, value in COUNTRY_HINTS.items():
        if key in upper:
            return value
    # 直接包含中文地区关键字时保留
    for word in ["香港", "日本", "台湾", "新加坡", "韩国", "美国", "德国", "芬兰", "土耳其", "英国", "法国", "荷兰", "加拿大", "澳大利亚"]:
        if word in remark:
            return word
    return remark[:20] if remark else "优选"


def extract_explicit_country(remark: str) -> str:
    if not remark:
        return ""
    upper = remark.upper()
    for key, value in COUNTRY_HINTS.items():
        if key in upper:
            return value
    for word in ["香港", "日本", "台湾", "新加坡", "韩国", "美国", "德国", "芬兰", "土耳其", "英国", "法国", "荷兰", "加拿大", "澳大利亚"]:
        if word in remark:
            return word
    return ""


def geoip_country(ip: str, cache: dict[str, str]) -> str:
    if ip in cache:
        return cache[ip]
    try:
        req = urllib.request.Request(GEOIP_URL.format(ip=ip), headers={"User-Agent": USER_AGENT})
        with urllib.request.urlopen(req, timeout=20) as resp:
            data = json.loads(resp.read().decode("utf-8", errors="replace"))
        code = str(data.get("country_code", "")).strip().upper()
        country = GEOIP_COUNTRY_MAP.get(code, "")
        cache[ip] = country
        return country
    except Exception:
        cache[ip] = ""
        return ""


def score_remark(remark: str) -> int:
    score = 0
    if re.search(r"\d+(\.\d+)?\s*MB/s", remark, re.I):
        score += 50
    if any(x in remark for x in ["香港", "日本", "台湾", "新加坡", "韩国", "美国"]):
        score += 30
    if re.search(r"\b(HKG|NRT|TPE|SIN|ICN|LAX|SJC|SEA|DFW|ORD|US|JP|HK|SG|KR)\b", remark, re.I):
        score += 20
    if "优选" in remark:
        score += 10
    return score


def build_output_remark(remark: str, addr: str, geo_cache: dict[str, str]) -> str:
    clean = clean_remark(remark)
    location = extract_explicit_country(clean)
    if not location:
        m = IPV4_ONLY_RE.match(addr)
        if not m:
            return ""
        ip = m.group(1)
        location = geoip_country(ip, geo_cache)
        if not location:
            return ""
    speed = ""
    m = re.search(r"(\d+(?:\.\d+)?)\s*MB/s", clean, re.I)
    if m:
        speed = f"{float(m.group(1)):g}MB/s"
    return f"{location}{speed}" if speed else location


def check_cf_connectivity(addr: str, timeout: float = 1.5) -> bool:
    """
    测试目标节点连通性：
    向目标节点的 IP 和端口建立 TCP 连接。
    若端口能在超时时间内成功建立三次握手，说明节点在线、网络畅通，返回 True；
    若连接超时、被拒绝或无法路由，说明为死节点，返回 False。
    """
    try:
        if ":" not in addr:
            return False
        ip, port_str = addr.rsplit(":", 1)
        port = int(port_str)

        with socket.create_connection((ip, port), timeout=timeout) as sock:
            return True
    except Exception:
        return False


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--sources", required=True, help="逗号分隔的订阅源域名")
    parser.add_argument("--output", required=True, help="输出 ADD.txt")
    parser.add_argument("--merged-output", required=True, help="输出 result.csv")
    parser.add_argument("--top", type=int, default=120, help="输出前 N 个")
    parser.add_argument("--per-file", type=int, default=30, help="每个文件保存的 IP 数量（默认 30）")
    args = parser.parse_args()

    output_path = Path(args.output)
    merged_output = Path(args.merged_output)
    hosts = [x.strip() for x in args.sources.split(",") if x.strip()]
    if not hosts:
        raise SystemExit("没有订阅源")

    merged_rows: list[dict[str, str]] = []
    dedup: dict[str, dict[str, str | int]] = {}
    geo_cache: dict[str, str] = {}

    for host in hosts:
        url = source_url(host)
        try:
            raw = fetch_text(url)
            decoded = decode_subscription(raw)
        except urllib.error.URLError as exc:
            print(f"[WARN] 拉取失败 {host}: {exc}", file=sys.stderr)
            continue

        for line in decoded.splitlines():
            line = line.strip()
            if not line:
                continue
            parsed = extract_addr_and_remark(line)
            if not parsed:
                continue
            addr, remark = parsed
            clean = clean_remark(remark)
            final_remark = build_output_remark(clean, addr, geo_cache)
            if not final_remark:
                continue
            score = score_remark(clean)
            merged_rows.append(
                {
                    "source": host,
                    "addr": addr,
                    "raw_remark": remark,
                    "clean_remark": clean,
                    "final_remark": final_remark,
                    "score": str(score),
                }
            )

            old = dedup.get(addr)
            candidate = {
                "source": host,
                "addr": addr,
                "raw_remark": remark,
                "clean_remark": clean,
                "final_remark": final_remark,
                "score": score,
            }
            if old is None or int(candidate["score"]) > int(old["score"]):
                dedup[addr] = candidate

    ranked = sorted(dedup.values(), key=lambda x: int(x["score"]), reverse=True)

    # ── 连通性探测过滤（30 线程并发，剔除死节点）──
    candidate_items = ranked[: args.top]
    alive_items = []
    print(f"[INFO] 正在对 {len(candidate_items)} 个候选节点进行 Cloudflare 真实连通性探测...", file=sys.stderr)

    with ThreadPoolExecutor(max_workers=30) as executor:
        future_to_item = {executor.submit(check_cf_connectivity, item["addr"]): item for item in candidate_items}
        for future in as_completed(future_to_item):
            item = future_to_item[future]
            try:
                if future.result():
                    alive_items.append(item)
            except Exception:
                pass

    # 保持原评分排序
    alive_items.sort(key=lambda x: int(x["score"]), reverse=True)
    dead_count = len(candidate_items) - len(alive_items)
    print(f"[INFO] 探测完成: 候选 {len(candidate_items)} 个，存活 {len(alive_items)} 个，淘汰死节点 {dead_count} 个", file=sys.stderr)

    lines = [f"{item['addr']}#{item['final_remark']}" for item in alive_items]

    # 1. 写入全量存活节点列表
    output_path.parent.mkdir(parents=True, exist_ok=True)
    output_path.write_text("\n".join(lines) + ("\n" if lines else ""), encoding="utf-8")

    # 2. 按 --per-file 指定数量拆分文件（如 per_file=30 时输出 ADD_1.txt, ADD_2.txt...）
    if args.per_file > 0 and lines:
        chunk_size = args.per_file
        total_chunks = (len(lines) + chunk_size - 1) // chunk_size
        stem = output_path.stem
        suffix = output_path.suffix
        out_dir = output_path.parent

        for idx in range(total_chunks):
            chunk = lines[idx * chunk_size : (idx + 1) * chunk_size]
            sub_file = out_dir / f"{stem}_{idx + 1}{suffix}"
            sub_file.write_text("\n".join(chunk) + "\n", encoding="utf-8")
            print(f"[INFO] 生成拆分文件 {sub_file.name}: 包含 {len(chunk)} 个节点", file=sys.stderr)

    merged_output.parent.mkdir(parents=True, exist_ok=True)
    with merged_output.open("w", encoding="utf-8-sig", newline="") as f:
        fieldnames = ["source", "addr", "raw_remark", "clean_remark", "final_remark", "score"]
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(merged_rows)

    if not lines:
        raise SystemExit("没有生成任何 IP，请检查订阅源")


if __name__ == "__main__":
    main()
