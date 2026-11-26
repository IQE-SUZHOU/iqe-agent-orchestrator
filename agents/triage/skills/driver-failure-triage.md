# SKILL: driver-failure-triage（驱动类失败分诊）

适用签名族：`driver:*`（driver:crash / driver:unknown）。典型 job：`os-staging-test`、`os-staging-image` 的 kernel-mode 阶段。

## 识别特征
- 日志出现 `amdgpu` / `GPU page fault` / `vmid` / `ring gfx_0` 等设备侧字样，且带 `Kernel panic`、`Oops`、`BUG:`、`soft lockup` 内核字样。
- 失败发生在 `kmod-load` 之后、用例执行中途：前段模块加载正常，后段用例「未跑完」而不是「跑完但断言失败」。
- 同一构建机上多次失败且伴随「构建机被看护程序重启」——设备被复位是驱动类失败的高置信信号。
- 内核 Call Trace / RIP 指向驱动模块符号（`[amdgpu]`），而不是测试框架栈。
- 时间线特征：故障发生在 resume / reset / 电源状态切换之后数秒内。

## 排除项
- 排除 **test 类**：没有出现 `AssertionError`、`FAILED <case>`、`断言失败`，也没有「N 失败 M 通过」的用例统计。
- 排除 **environment 类**：没有 `apt` / `Release file` / `404` / `network unreachable` / `Could not resolve` 等仓库与网络字样。
- 排除 **ci-infra 类**：没有 `jenkins executor died` / `agent disconnected` / `no space left on device`；构建确实进入了测试阶段。
- 排除 **toolchain 类**：没有 `error:` / `undefined reference` / `编译失败`，说明不是上游代码编译问题。
- 注意：若同一日志既有 GPU 故障又有断言失败，优先按驱动类处理（断言失败可能是 GPU 复位的次生现象）。

## 需要补的证据
1. 完整失败段日志（`kmod-load` 到构建结束的全量 console）与 `dmesg` 尾部原文，用于确认错误簇首行。
2. 该构建机过去 10 次同签名族的结果序列（判定是回归还是偶发）。
3. 模块版本与变更记录：`amdgpu-dkms` 版本号、本次纳入的 commit 列表、疑似改动文件。
4. 受控重跑记录（同参数、同机位）：至少 2 次，用于给出 `reproducible`。
5. 换机对照：至少一台不同构建机的同参结果，用于排除单机硬件问题。
