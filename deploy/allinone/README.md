# ci-triage 全自包含演示镜像（编排层 + RabbitMQ + Postgres + 8 Agent）

一个镜像里装齐整条流水线所需的全部中间件与进程，`docker run` 一条命令即可演示
「12 阶段 / 两条分支 / 三道闸门」。**这是演示件，不是生产部署件**：
生产口径仍是根目录 `Dockerfile`（无状态进程）+ 集群 `infra-database` 的 PG / RabbitMQ。

## 1. 镜像内容

| 组件 | 说明 |
|---|---|
| Postgres 15 | 容器内 `127.0.0.1:5432`，库 `ci_triage`，启动时自动建库 + 迁移 |
| RabbitMQ 3.10 + management | `127.0.0.1:5672`，vhost `/ci`，管理台 `:15672`（guest/guest） |
| 编排层 | `iqe_orchestrator.main`，API/UI `:8080`，启动时声明 4 exchange + 13 队列 |
| 8 个 Agent | `iqe_agents.<stage>.main`，健康检查 `:8101..8108` |

RabbitMQ 账号沿用星型权限模型：`ci_orch` + `ci_agent_<stage>` 共 9 个，
口令统一由 `CI_DEMO_PASS` 派生（默认 `ci-demo-pass`），Agent 账号对 `ci.cmd` 无写权限。

## 2. 构建 / 推送

```bash
# 构建 + 推 Harbor（默认 iqe-agent-orchestrator/ci-triage-allinone）
bash deploy/build_allinone.sh

# 只构建、指定 tag
PUSH=0 TAG=local-dev bash deploy/build_allinone.sh
```

镜像 tag 默认 `日期-时分-gitsha`，同时打一个 `latest`。

## 3. 运行与验证

```bash
docker run -d --name ci-allinone \
  -p 18080:8080 -p 15672:15672 \
  172.31.0.3:7777/iqe-agent-orchestrator/ci-triage-allinone:latest

# 就绪（9 个进程 + 迁移全部完成）
curl -s localhost:18080/readyz

# 跑一次完整外部分支（网关 1 → 4b → 5b → 6c → 7b → 8b → 9）
curl -s -XPOST localhost:18080/api/scan \
  -H 'content-type: application/json' \
  -d '{"scenario":"new_failure","wait":true}' | head -c 400

# 观察链路与闸门结论
curl -s localhost:18080/api/cases | head -c 400
open http://localhost:18080/          # Case 列表
open http://localhost:18080/ui/plans  # Workflow 注册表 + 已封存计划
open http://localhost:18080/ui/health # Stage 执行情况 + DLQ
```

改口令（PG 与 MQ 同源）：

```bash
docker run -d --name ci-allinone -e CI_DEMO_PASS='my-demo-pass' -p 18080:8080 <image>
```

## 4. 日志与排障

```bash
docker exec ci-allinone tail -n 80 /var/log/ci-triage/orchestrator.log
docker exec ci-allinone ls /var/log/ci-triage          # setup_broker.log / agent-<stage>.log
docker exec ci-allinone rabbitmqctl list_queues -p ci name messages consumers
docker exec ci-allinone psql -U postgres -d ci_triage -c 'select status,count(*) from cases group by 1'
```

启动顺序由 `entrypoint.sh` 保证：PG → RabbitMQ → vhost/账号 → 编排层 `/readyz`
→ 8 个 Agent。任何一步失败会打印相关日志尾部并以非 0 退出（容器不会「假活」）。

## 5. 已知取舍

* 容器内进程以 root 启动（`pg_ctlcluster` / `rabbitmq-server` 需要切换系统用户），
  python 进程降权到 UID 10001。**不要**把这个镜像塞进 `runAsNonRoot: true` 的生产命名空间。
* 数据在容器内（无 volume），`docker rm` 即清空；需要留存请挂 `/var/lib/postgresql` 与
  `/var/lib/rabbitmq`。
* 演示口令是明文默认值，只用于本地/评审环境。
* 镜像体积约 700MB–1GB（erlang + postgres + python 依赖）。

## 6. k8s 清单

`deploy/k8s/30-allinone.yaml` 是配套的 Deployment/Service 参考清单，
**本轮未 apply**：集群里已有 `infra-database` 的 RabbitMQ，两套 broker 同时存在会让
`q.agent.*` 出现两个消费者（旧栈抢消息）。要演练时先缩掉现有 Deployment，再单独 apply。