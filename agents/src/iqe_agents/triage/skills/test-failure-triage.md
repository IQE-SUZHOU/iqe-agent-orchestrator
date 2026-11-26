# SKILL: test-failure-triage（测试类失败分诊）

适用签名族：`test:*`（test:assert-fail）。典型 job：`os-staging-test` 的 userspace-smoke / 用例集阶段。

## 识别特征
- 日志出现 `AssertionError`、`FAILED tests/...::<case>`、`断言失败`，并有用例统计（例如「1 个用例失败，11 个用例通过」）。
- 失败集中在单个或少数用例，且用例之外的阶段（构建、安装、引导）全部正常。
- 断言信息给出「期望值 / 实际值」对照，说明被测接口返回了错误码或不一致数据。
- 同一测试文件在多个 job / 多台机器上重复出现同一家族（签名族相同、指纹不同）→ 属于测试面问题或已知不稳定用例。
- 运行时间正常（未出现排队或 deadline），排除资源等待造成的假失败。

## 排除项
- 排除 **driver 类**：没有 `Kernel panic` / `Oops` / `BUG:` / `GPU page fault`，用例是被判定失败而不是被中断。
- 排除 **environment 类**：没有 `apt` / `Packages index` / `404` / `network unreachable`，依赖与环境准备阶段完成。
- 排除 **ci-infra 类**：没有 `executor died` / `agent disconnected` / `no space left on device`；测试阶段真实执行过。
- 排除 **timeout 类**：没有 `TIMEOUT` / `deadline`，用例在自身时限内正常结束。
- 注意：用例连续多轮同家族失败时不要逐条建单，先按签名族合并。

## 需要补的证据
1. 失败用例的完整断言上下文（前 25 行与后 25 行 console）与 JUnit 摘要。
2. 该用例最近 10 次运行的通过/失败序列，以及是否已被标记为 flaky。
3. 复现矩阵：同一用例在 default / low-priv 两类配置下的结果，用于判定 `flaky`。
4. 用例依赖的制品与镜像版本（确认不是被测版本变更引入）。
5. 已有工单查重结果（同签名族是否已存在工单），避免重复开单。
