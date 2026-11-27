#!/usr/bin/env python3
"""复用集群现有 RabbitMQ：为 CI Triage 演示建独立 vhost `/ci` + 每个单元独立账号。

价值：星型拓扑「只有编排层能写 ci.cmd」不是代码约定，而是 broker 权限强制——
Agent 账号对 ci.cmd 没有 write 权限，也读不到别的 Agent 的队列。
"""

from __future__ import annotations

import argparse
import json
import os
import sys
import urllib.parse

import requests

# dry-run / 摘要里一律用它替口令：这个工具的输出经常被贴进终端记录和 CI 日志。
_REDACTED = "<redacted>"

STAGES = [
    "monitor",
    "triage",
    "reviewer",
    "remediation",
    "reproduction",
    "regression",
    "ticketing",
    "reporting",
]
QUEUES = ["q.agent." + s for s in STAGES] + [
    "q.orch.results",
    "q.orch.events",
    "q.dlq",
    "q.audit.results",
    "q.audit.events",
]
EXCHANGES = ["ci.cmd", "ci.result", "ci.event", "ci.dlx"]


def put(sess: requests.Session, path: str, body: dict) -> tuple[int, str]:
    r = sess.put(path, json=body, timeout=20)
    return r.status_code, (r.text[:300] if r.text else "")


def main() -> int:
    ap = argparse.ArgumentParser()
    # RabbitMQ svc 是 ClusterIP（没有 NodePort）：本机跑时先 port-forward，例如
    #   kubectl -n infra-database port-forward svc/rabbitmq 15672:15672
    ap.add_argument("--mgmt", default="http://127.0.0.1:15672")
    ap.add_argument("--admin-user", default=os.environ.get("RABBITMQ_ADMIN_USER", "admin"))
    # 口令只来自环境变量（deploy/secrets.env 提供），命令行可临时覆盖；仓库里不留明文
    ap.add_argument("--admin-pass", default=os.environ.get("RABBITMQ_ADMIN_PASS", ""))
    ap.add_argument("--vhost", default="ci")
    ap.add_argument("--app-pass", default=os.environ.get("CI_APP_PASS", ""))
    ap.add_argument("--dry-run", action="store_true")
    ap.add_argument("--verify", action="store_true")
    a = ap.parse_args()

    if not a.admin_pass or not a.app_pass:
        print(
            "缺少口令：请设 RABBITMQ_ADMIN_PASS / CI_APP_PASS（或 cp deploy/secrets.env.example "
            "deploy/secrets.env），也可用 --admin-pass / --app-pass 传入",
            file=sys.stderr,
        )
        return 2

    sess = requests.Session()
    sess.auth = (a.admin_user, a.admin_pass)
    base = a.mgmt.rstrip("/")
    v = a.vhost
    vq = urllib.parse.quote(v, safe="")  # vhost 名 "ci" → "ci"；"/ci" → "%2Fci"

    if a.verify:
        for name in ["ci_orch"] + [f"ci_agent_{s}" for s in STAGES]:
            r = sess.get(f"{base}/api/permissions/{vq}/{name}", timeout=20)
            print(name, r.status_code, r.text[:220])
        return 0

    steps = [("vhost", f"/api/vhosts/{v}", {})]
    orch_configure = r"^(ci\.(cmd|result|event|dlx)|q\.(orch|dlq|audit)[.a-z_]*|q\.agent\.[a-z]+)$"
    steps.append(("user", "/api/users/ci_orch", {"password": a.app_pass, "tags": "policy"}))
    steps.append(
        (
            "perm",
            f"/api/permissions/{vq}/ci_orch",
            # read 里包含 q.agent.*：RabbitMQ 的 queue.bind 需要对队列的 read 权限
            # （编排层负责把 agent 队列绑到 ci.cmd，因此必须能 read 这些队列）。
            # 星型拓扑的强约束在 write 上：只有 ci_orch 能写 ci.cmd。
            # 注意：RabbitMQ 把 queue.bind 记为「对队列的 write」，
            # 所以编排层（唯一做拓扑声明的单元）需要对队列的 write。
            # 星型拓扑的强约束仍然成立：只有 ci_orch 对 exchange ci.cmd 有 write 权限，
            # Agent 账号 write 只有 ci.result，物理上无法命令别的 Agent。
            {
                "configure": orch_configure,
                "write": r"^(ci\.(cmd|event|dlx)|q\.(agent\.[a-z]+|orch\.(results|events)|dlq|audit\.[a-z]+))$",
                # read 里含 exchange：RabbitMQ 把 queue.bind 判为「对队列 write + 对 exchange read」
                "read": r"^(ci\.(cmd|result|event|dlx)|q\.(agent\.[a-z]+|orch\.(results|events)|dlq|audit\.[a-z]+))$",
            },
        )
    )
    for s in STAGES:
        u = f"ci_agent_{s}"
        steps.append(("user", f"/api/users/{u}", {"password": a.app_pass, "tags": ""}))
        steps.append(
            (
                "perm",
                f"/api/permissions/{vq}/{u}",
                {
                    "configure": rf"^(q\.agent\.{s}|ci\.result)$",
                    "write": r"^ci\.result$",
                    "read": rf"^q\.agent\.{s}$",
                },
            )
        )

    for kind, path, body in steps:
        if a.dry_run:
            # 口令一律不落终端：dry-run 只展示账号结构
            shown = {**body, "password": _REDACTED} if "password" in body else body
            print(f"[dry-run] PUT {path} {json.dumps(shown, ensure_ascii=False)}")
            continue
        code, text = put(sess, base + path, body)
        flag = "OK" if code in (201, 204) else "FAIL"
        print(f"{flag:4} {kind:5} {path} -> {code} {text}")
        if flag == "FAIL":
            return 1

    if not a.dry_run:
        # 只打印结构，不打印口令：口令在 deploy/secrets.env，K8s Secret 由 deploy/deploy.sh 生成。
        print("\nAMQP URLs（口令取自环境变量 CI_APP_PASS，不在此回显）：")
        host = "rabbitmq.infra-database.svc.cluster.local"
        print(f"  AMQP_ORCH=amqp://ci_orch:${{CI_APP_PASS}}@{host}:5672/{v}")
        for s in STAGES:
            print(f"  AMQP_{s.upper()}=amqp://ci_agent_{s}:${{CI_APP_PASS}}@{host}:5672/{v}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
