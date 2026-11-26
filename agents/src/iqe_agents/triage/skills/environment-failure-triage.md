# SKILL: environment-failure-triage（环境与 CI 基础设施类失败分诊）

适用签名族：`apt-repo:*` / `network:*` / `install:*` / `image-build:*` / `toolchain:*` / `timeout:*`（→ environment）、`ci-infra:*`（→ infrastructure）。典型 job：`os-staging-image`、`os-staging-test` 的准备阶段。

## 识别特征
- 失败发生在「进入用例之前」：`apt-get`/`dpkg` 拉包、`Release file`/`Packages index` 校验、`E:Failed to fetch`、`404 Not Found`、`network unreachable`、`Could not resolve`。
- 镜像组装环节的相邻字样：`image build`、`mkimage`、`initramfs`、`squashfs`、`rootfs`、`tar: `，且伴随 `error:` / `编译失败`。
- 工具链编译环节：`error:`、`undefined reference`、`compile-error`、`构建脚本退出码 1`。
- 整段静默后超时：`Timeout`、`构建超过 N 分钟被中止`，且没有用例断言字样。
- CI 基础设施：`jenkins`、`executor died`、`agent disconnected`、`no space left on device`、`disk quota`、`workspace`，或结果被标记为 `ABORTED`。
- 同族日志在多台构建机、多个 job 上同时出现——环境问题/基建问题的典型特征（单点问题不会同时扩散）。

## 排除项
- 排除 **driver 类**：没有 `Kernel panic` / `Oops` / `BUG:` / `SIGSEGV` / `GPU page fault`，也没有 `[amdgpu]` 之类的模块符号。
- 排除 **test 类**：没有 `AssertionError` / `FAILED <case>` / `断言失败` / 「N 失败 M 通过」的用例统计。
- 排除「真实缺陷」型环境问题：仓库 404 若只发生一次且随后自愈，多半是镜像源抖动（按已知噪声处理），不要开成缺陷单。
- `ci-infra` 与 `apt-repo` 需区分：前者是执行机/控制器问题（不该占用重跑机位），后者是被测环境依赖问题（可安排受控重跑）。
- 超时类先确认是不是被基建中止（`ABORTED`）：基建中止导致的超时不是产品缺陷。

## 需要补的证据
1. 失败段完整 console（含 `apt`/构建工具的原始报错行与退出码）与构建机当时的网络/磁盘状态快照。
2. 仓库侧证据：目标 `Release file` 的 HTTP 状态、镜像源时间戳、同一时刻其他构建机的对照结果。
3. 基建侧证据：Jenkins 控制器日志片段、构建机磁盘占用、执行机存活记录（用于确认 `ci-infra` 归属）。
4. 已知问题库命中情况：命中的族、噪声分、上次人工确认时间（决定是否抑制）。
5. 一次同参受控重跑记录（仅 environment 类；`ci-infra` 类改走基建巡检，不占用重跑机位）。
