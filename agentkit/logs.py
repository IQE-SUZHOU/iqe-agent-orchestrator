"""结构化 JSON 日志 + 进程内计数器（/metrics）。"""
from __future__ import annotations

import json
import logging
import sys
import time
from collections import defaultdict

from .config import cfg


class JsonFormatter(logging.Formatter):
    def format(self, record: logging.LogRecord) -> str:
        payload = {
            "ts": time.strftime("%Y-%m-%dT%H:%M:%S", time.gmtime()) + f".{int(record.msecs):03d}Z",
            "level": record.levelname,
            "svc": cfg.svc,
            "event": record.getMessage(),
        }
        ctx = getattr(record, "ctx", None)
        if ctx:
            payload.update(ctx)
        if record.exc_info:
            payload["err"] = self.formatException(record.exc_info)[:800]
        return json.dumps(payload, ensure_ascii=False)


def setup_logging(name: str = "agentkit") -> None:
    handler = logging.StreamHandler(sys.stdout)
    handler.setFormatter(JsonFormatter())
    root = logging.getLogger()
    root.handlers = [handler]
    root.setLevel(getattr(logging, cfg.log_level.upper(), logging.INFO))
    logging.getLogger("uvicorn.access").setLevel(logging.WARNING)


def log_with(logger: logging.Logger, level: str, event: str, **ctx) -> None:
    logger.log(getattr(logging, level.upper(), logging.INFO), event, extra={"ctx": ctx})


class Counters:
    def __init__(self) -> None:
        self.c: dict[tuple[str, tuple], float] = defaultdict(float)
        self.hist: dict[str, list[float]] = defaultdict(list)

    def inc(self, name: str, **labels) -> None:
        self.c[(name, tuple(sorted(labels.items())))] += 1

    def observe(self, name: str, value: float) -> None:
        self.hist[name].append(value)
        if len(self.hist[name]) > 2000:
            self.hist[name] = self.hist[name][-2000:]

    def render(self) -> str:
        out = []
        for (name, labels), val in sorted(self.c.items()):
            lbl = ",".join(f'{k}="{v}"' for k, v in labels)
            out.append(f"{name}{{{lbl}}}{val}" if lbl else f"{name}{val}")
        for name, vals in self.hist.items():
            if vals:
                out.append(f"{name}_count{len(vals)}")
                out.append(f"{name}_max{max(vals):.3f}")
                out.append(f"{name}_avg{sum(vals) / len(vals):.3f}")
        return "\n".join(out) + "\n"


counters = Counters()
