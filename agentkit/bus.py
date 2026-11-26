"""总线封装（RabbitMQ / aio_pika）：拓扑声明、发布（publisher confirm）、消费。

拓扑与 design/02 一致：exchange ci.cmd|ci.result|ci.event|ci.dlx；
队列 q.agent.<stage> / q.orch.results / q.orch.events / q.audit.* / q.dlq。
"""
from __future__ import annotations

import asyncio
import logging
from dataclasses import dataclass

import aio_pika
from aio_pika import DeliveryMode, ExchangeType, Message

from .config import cfg
from .envelope import dump

log = logging.getLogger("agentkit.bus")

CMD_EXCHANGE = "ci.cmd"
RESULT_EXCHANGE = "ci.result"
EVENT_EXCHANGE = "ci.event"
DLX_EXCHANGE = "ci.dlx"

#: stage -> (queue, binding key, prefetch)
AGENT_QUEUES: dict[str, tuple[str, str, int]] = {
    "monitor": ("q.agent.monitor", "ci.cmd.monitor.scan", 1),
    # prefetch = 该 Agent 的并发度：aio-pika 会为每条消息起一个 handler 协程，
    # prefetch>1 时一个慢任务（例如 reproduction 的 45s 剧本）不会把队列里其他 case 卡住。
    "triage": ("q.agent.triage", "ci.cmd.triage.run", 4),
    "reproduction": ("q.agent.reproduction", "ci.cmd.reproduction.run", 4),
    "ticket": ("q.agent.ticket", "ci.cmd.ticket.run", 2),
    "review": ("q.agent.review", "ci.cmd.review.run", 4),
    "reporting": ("q.agent.reporting", "ci.cmd.reporting.#", 2),
}

ORCH_RESULT_QUEUE = "q.orch.results"
ORCH_EVENT_QUEUE = "q.orch.events"
DLQ_QUEUE = "q.dlq"
AUDIT_RESULT_QUEUE = "q.audit.results"
AUDIT_EVENT_QUEUE = "q.audit.events"


@dataclass
class Bus:
    conn: aio_pika.abc.AbstractRobustConnection
    channel: aio_pika.abc.AbstractChannel
    exchanges: dict[str, aio_pika.abc.AbstractExchange]

    async def publish(self, exchange: str, routing_key: str, body: dict, *,
                      message_id: str | None = None, correlation_id: str | None = None,
                      headers: dict | None = None, priority: int | None = None) -> None:
        msg = Message(
            dump(body),
            delivery_mode=DeliveryMode.PERSISTENT,
            message_id=message_id,
            correlation_id=correlation_id,
            priority=priority,
            headers=headers or {},
            content_type="application/json",
        )
        # 懒声明：每个单元只对它真正发布的 exchange 需要 configure 权限
        exch = self.exchanges.get(exchange)
        if exch is None:
            exch = await self.channel.declare_exchange(exchange, ExchangeType.TOPIC, durable=True)
            self.exchanges[exchange] = exch
        # publisher confirm：aio_pika 的 publish 会等待 confirm（channel 已开 publisher_confirms）
        await exch.publish(msg, routing_key=routing_key)

    async def publish_result(self, command: dict, body: dict) -> None:
        await self.publish(RESULT_EXCHANGE, f"ci.result.{command['stage']}", body,
                           message_id=body["msg_id"], correlation_id=body["correlation_id"],
                           headers={"idempotency_key": body["idempotency_key"],
                                    "case_id": body.get("case_id") or "",
                                    "attempt": body.get("attempt", 1)})

    async def close(self) -> None:
        try:
            await self.conn.close()
        except Exception:
            pass


def _queue_args(queue: str) -> dict:
    args: dict = {"x-message-ttl": 3_600_000}
    if queue.startswith("q.agent."):
        stage = queue.split(".")[-1]
        args["x-dead-letter-exchange"] = DLX_EXCHANGE
        args["x-dead-letter-routing-key"] = f"ci.dlq.{stage}"
    if queue.startswith("q.audit."):
        args["x-message-ttl"] = 7 * 86_400_000 if "results" in queue else 30 * 86_400_000
        args["x-max-length"] = 50_000
    return args


async def connect(*, declare: bool = False, prefetch: int = 1) -> Bus:
    conn = await aio_pika.connect_robust(cfg.amqp_url, heartbeat=60,
                                         connection_name=f"{cfg.svc}-{cfg.mock_scenario}")
    channel = await conn.channel(publisher_confirms=True)   # aio-pika 10：publish 会等待 confirm
    await channel.set_qos(prefetch_count=prefetch)
    exchanges: dict = {}
    if declare:
        # 只有编排层有权限声明全套拓扑（见 deploy/setup_broker.py 的权限矩阵）
        await declare_topology(channel)
        for name in (CMD_EXCHANGE, RESULT_EXCHANGE, EVENT_EXCHANGE, DLX_EXCHANGE):
            exchanges[name] = await channel.declare_exchange(name, ExchangeType.TOPIC, durable=True)
    return Bus(conn=conn, channel=channel, exchanges=exchanges)


async def declare_topology(channel: aio_pika.abc.AbstractChannel) -> None:
    for name, kind in ((CMD_EXCHANGE, ExchangeType.TOPIC), (RESULT_EXCHANGE, ExchangeType.TOPIC),
                       (EVENT_EXCHANGE, ExchangeType.TOPIC), (DLX_EXCHANGE, ExchangeType.TOPIC)):
        await channel.declare_exchange(name, kind, durable=True)

    queues: dict[str, list[str]] = {}
    for stage, (queue, bind, _pf) in AGENT_QUEUES.items():
        queues[queue] = [bind]
    queues[ORCH_RESULT_QUEUE] = ["ci.result.*"]
    queues[ORCH_EVENT_QUEUE] = ["ci.event.#"]
    queues[DLQ_QUEUE] = ["ci.dlq.#"]
    queues[AUDIT_RESULT_QUEUE] = ["ci.result.*"]
    queues[AUDIT_EVENT_QUEUE] = ["ci.event.#"]

    def target_exchange(queue: str) -> str:
        if queue.startswith("q.agent."):
            return CMD_EXCHANGE
        if queue == DLQ_QUEUE:
            return DLX_EXCHANGE
        if queue.endswith(("results", "audit.results")):
            return RESULT_EXCHANGE
        return EVENT_EXCHANGE

    declared: dict[str, aio_pika.abc.AbstractExchange] = {}
    for queue, keys in queues.items():
        q = await channel.declare_queue(queue, durable=True, arguments=_queue_args(queue))
        target = target_exchange(queue)
        exchange = declared.setdefault(
            target, await channel.declare_exchange(target, ExchangeType.TOPIC, durable=True))
        for key in keys:
            await q.bind(exchange, routing_key=key)
    log.info("topology declared: %d queues", len(queues))


async def consume_agent(bus: Bus, stage: str, handler) -> aio_pika.abc.AbstractConsumer:
    queue_name, _bind, prefetch = AGENT_QUEUES[stage]
    await bus.channel.set_qos(prefetch_count=prefetch)
    queue = await bus.channel.declare_queue(queue_name, durable=True, arguments=_queue_args(queue_name),
                                            passive=True)
    return await queue.consume(handler, no_ack=False)


async def consume_results(bus: Bus, handler) -> aio_pika.abc.AbstractConsumer:
    queue = await bus.channel.declare_queue(ORCH_RESULT_QUEUE, durable=True,
                                            arguments={"x-message-ttl": 3_600_000}, passive=True)
    return await queue.consume(handler, no_ack=False)


async def nack_to_dlq(message: aio_pika.abc.AbstractIncomingMessage) -> None:
    await message.nack(requeue=False)


async def wait_ready(timeout: float = 30) -> bool:
    deadline = asyncio.get_event_loop().time() + timeout
    while asyncio.get_event_loop().time() < deadline:
        try:
            conn = await aio_pika.connect_robust(cfg.amqp_url, timeout=3)
            await conn.close()
            return True
        except Exception as exc:
            log.warning("broker not ready: %s", exc)
            await asyncio.sleep(2)
    return False
