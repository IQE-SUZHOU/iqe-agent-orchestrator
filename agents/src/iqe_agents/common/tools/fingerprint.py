"""失败签名指纹：Monitor 与 Triage 共用同一算法，保证签名可对账。

格式：<component>:<errorcode>:<fp12>
"""

from __future__ import annotations

import hashlib
import re

COMPONENT_PATTERNS = [
    (
        "network",
        r"(network unreachable|connection refused|Could not resolve|dns|timed out|TLS|proxy)",
    ),
    ("apt-repo", r"(apt(-get)?|Release file|Packages index|E:Failed to fetch|dpkg)"),
    ("image-build", r"(image build|initramfs|mkimage|squashfs|rootfs|tar: )"),
    ("install", r"(install failed|postinst|dpkg --configure|dependency)"),
    ("test", r"(AssertionError|pytest|FAILED |assert |test_.*failed|断言失败)"),
    (
        "driver",
        r"(driver|kmod|module load|dmesg|GPU|amdgpu|kernel panic|oops|BUG:)",
    ),
    ("toolchain", r"(gcc|clang|ld: |undefined reference|compilation failed|编译失败|make: \*\*\*)"),
    ("timeout", r"(TIMEOUT|超时|exceeded.*time|deadline)"),
    ("ci-infra", r"(jenkins|agent disconnected|no space left|disk quota|workspace|executor died)"),
]
ERROR_CODE_PATTERNS = [
    ("offline", r"(network unreachable|connection refused|Could not resolve|dns|timed out)"),
    ("repo-404", r"(404|Release file|Failed to fetch|Packages index)"),
    ("assert-fail", r"(AssertionError|assert|断言失败|FAILED )"),
    ("compile-error", r"(error:|编译失败|undefined reference|compilation failed)"),
    ("timeout", r"(TIMEOUT|超时|timed out|deadline)"),
    ("crash", r"(kernel panic|Oops|BUG:|core dumped|segfault|SIGSEGV)"),
    ("infra-abort", r"(ABORTED|agent disconnected|no space left|executor died|jenkins)"),
    ("unknown", r"."),
]


def normalize(text: str) -> str:
    text = re.sub(r"\d+", "#", text or "")
    text = re.sub(r"\s+", " ", text).strip().lower()
    return text[:2000]


def component_of(text: str) -> str:
    low = (text or "").lower()
    for name, pat in COMPONENT_PATTERNS:
        if re.search(pat, low):
            return name
    return "unknown"


def error_code_of(text: str) -> str:
    low = (text or "").lower()
    for name, pat in ERROR_CODE_PATTERNS:
        if re.search(pat, low):
            return name
    return "unknown"


def signature_of(log_excerpt: str, *, job: str | None = None) -> str:
    comp = component_of(log_excerpt)
    code = error_code_of(log_excerpt)
    fp = hashlib.sha256(normalize(log_excerpt).encode("utf-8")).hexdigest()[:12]
    return f"{comp}:{code}:{fp}"


def signature_family(sig: str) -> str:
    """去掉易变指纹后的签名族，用于跨 build 合并与已知噪声匹配。"""
    parts = (sig or "").split(":")
    return ":".join(parts[:2]) if len(parts) >= 2 else sig or "unknown:unknown"
