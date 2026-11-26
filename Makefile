# 工作区入口：两个自包含子项目 orchestrator/ 与 agents/。
# venv 默认取仓库内 .venv，没有则用 ../.venv（本演示机的现有环境）。
PY    ?= $(shell test -x .venv/bin/python && echo .venv/bin/python || echo ../.venv/bin/python)
NS     = ci-triage
PKGS   = orchestrator agents

.PHONY: help install install-dev install-hooks lint format typecheck test test-unit test-contract test-integration selfcheck smoke ui-check run-local deploy sync-common clean

help:
	@echo "make install          安装两个子项目（editable）"
	@echo "make install-dev      上面 + 开发工具 + pre-commit 钩子"
	@echo "make lint             ruff check（--fix）"
	@echo "make format           isort + black"
	@echo "make typecheck        mypy（两个子项目）"
	@echo "make test             单元 + 契约（离线，秒级）"
	@echo "make test-integration 端到端（需要编排层 + 6 个 Agent 在跑）"
	@echo "make selfcheck        六个剧本 + 计划契约离线自检（部署前门禁）"
	@echo "make run-local        本机起 7 个进程（连集群 MQ/DB）"
	@echo "make sync-common      把共享层从 orchestrator 同步到 agents（两份必须字节一致）"
	@echo "make deploy           构建镜像 → 推 Harbor → 部署 → rollout"

install:
	$(PY) -m pip install -e ./orchestrator -e ./agents

install-dev: install
	$(PY) -m pip install -e ".[dev]"
	$(PY) -m pre-commit install

install-hooks:
	$(PY) -m pre-commit install

lint:
	$(PY) -m ruff check --fix orchestrator/src agents/src tests

format:
	$(PY) -m isort orchestrator/src agents/src tests
	$(PY) -m black orchestrator/src agents/src tests

typecheck:
	$(PY) -m mypy

test:
	$(PY) -m pytest -m "unit or contract"

test-unit:
	$(PY) -m pytest -m unit

test-contract:
	$(PY) -m pytest -m contract

test-integration:
	$(PY) -m pytest -m integration

selfcheck:
	$(PY) -m iqe_agents.tools.selfcheck

smoke:
	$(PY) -m iqe_orchestrator.tools.graph_smoke

ui-check:
	$(PY) -m iqe_orchestrator.tools.ui_render_check

run-local:
	bash scripts/run_local.sh

sync-common:
	bash scripts/sync_common.sh

deploy:
	bash deploy/deploy.sh

clean:
	rm -rf .pytest_cache .mypy_cache .ruff_cache .coverage htmlcov build dist \
	  orchestrator/src/*.egg-info agents/src/*.egg-info
	find . -name __pycache__ -not -path './.git/*' -prune -exec rm -rf {} +
