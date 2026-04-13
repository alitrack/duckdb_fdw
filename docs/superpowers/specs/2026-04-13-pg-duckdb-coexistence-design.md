# duckdb_fdw 与 pg_duckdb 共存设计

## 背景

`duckdb_fdw` 当前通过本地 `libduckdb.so` 动态链接 DuckDB 运行时，并在 PostgreSQL backend 进程内直接调用 DuckDB C API。`pg_duckdb` 也属于会把 DuckDB 运行时带入 PostgreSQL backend 的扩展类别。两者一旦进入同一个 backend，真正的风险不在于 SQL 级功能冲突，而在于：

- 运行时动态库来源不同
- DuckDB 版本精确值不同
- C/C++ ABI 或全局状态不一致
- load 顺序改变实际符号绑定结果
- 用户把“看起来能跑”误解成“官方支持共存”

本设计的目标不是立即承诺“任何情况下都能共存”，而是先把支持边界、阻断逻辑、诊断体验和演进路径定义清楚，避免未定义行为被包装成默认能力。

## 设计结论

本次设计采用以下立场：

- 长期公开支持策略采用严格版“同版本且可证明兼容时才可共存”模型。
- 只有在运行期能证明兼容时，`duckdb_fdw` 才允许与 `pg_duckdb` 在同一 backend 中共存。
- 无法证明安全时，公开支持路径一律拒绝执行。
- 保留一个显式的 unsupported override，供实验用途放行，但该模式不进入公开支持矩阵。
- 当前实现优先只改 `duckdb_fdw`，但允许未来定义一个可选协作接口，供 `pg_duckdb` 暴露更强的运行时指纹。

## 目标

- 明确 `duckdb_fdw` 与 `pg_duckdb` 的公开支持边界。
- 为“未安装 `pg_duckdb`”、“已安装但未在当前 backend 加载”、“当前 backend 已加载但可证明兼容”、“当前 backend 已加载但无法证明兼容”几类场景定义统一行为。
- 提供安装期软预检和运行期硬阻断，减少误导性成功。
- 给用户提供足够的诊断信息，解释为什么被允许或为什么被拒绝。
- 预留未来双边协作增强路径，但不要求首版依赖 `pg_duckdb` 改动。

## 非目标

- 不承诺 `duckdb_fdw` 与 `pg_duckdb` 在所有版本组合下都可以共存。
- 不把“版本字符串看起来接近”视为公开支持条件。
- 不在本阶段要求 `pg_duckdb` 必须实现配套接口。
- 不把真正的 `pg_duckdb` 集成测试直接并入默认 `installcheck`。
- 不把 unsupported override 设计成默认推荐路径。

## 支持边界

### 公开支持

- 未安装 `pg_duckdb`：支持。
- 已安装 `pg_duckdb`，但当前 backend 未加载：支持。
- 当前 backend 已加载 `pg_duckdb`：
  - 首版实现中默认不提供公开支持成功路径。
  - 版本不一致或运行时来源明确冲突时拒绝执行。
  - 无法证明兼容时，按严格策略拒绝执行。
  - 只有在未来版本引入可验证的协作 fingerprint，且能证明兼容时，才进入公开支持路径。

### Unsupported Override

- 提供显式 GUC 开关放行非支持场景。
- 仅供实验用途。
- 默认关闭。
- 开启后仍然必须输出清晰 `WARNING`。
- 文档中明确声明该模式不属于官方支持合同。

### 首版支持矩阵

| 场景 | 首版默认行为 | 是否属于公开支持 |
|---|---|---|
| 未安装 `pg_duckdb` | 放行 | 是 |
| 已安装 `pg_duckdb`，但当前 backend 未加载 | 放行 | 是 |
| 当前 backend 已加载 `pg_duckdb`，且版本/来源明确冲突 | 阻断 | 是，作为受支持的拒绝行为 |
| 当前 backend 已加载 `pg_duckdb`，版本看似一致但无法证明兼容 | 阻断 | 是，作为受支持的拒绝行为 |
| 当前 backend 已加载 `pg_duckdb`，用户显式开启 override | 放行并告警 | 否 |
| 当前 backend 已加载 `pg_duckdb`，且 future cooperative fingerprint 证明兼容 | 预留未来放行路径 | 否，非首版交付 |

## 兼容性判定模型

兼容性模型分成安装期和运行期两层，但真正决定放行/阻断的只有运行期。

### 安装期

安装期只做软预检，不做最终兼容性裁决。

行为：

- 如果未发现 `pg_duckdb`，不额外制造噪音。
- 如果发现数据库中已安装 `pg_duckdb`，或实例中可发现 `pg_duckdb` 可用，输出 `NOTICE/WARNING`：
  - 当前仓库采用严格共存策略
  - 安装顺序不决定兼容性
  - 最终判定以后端运行期强校验为准
  - 输出 `duckdb_fdw` 侧可见的 DuckDB 版本信息

安装期候选信号来源：

- 当前数据库中的 `pg_extension`
- 实例级的 `pg_available_extensions`

这些信号只用于提醒，不用于最终运行时裁决。

### 运行期状态机

运行期判定结果统一收敛为以下状态：

- `NoPeerLoaded`
  - 当前 backend 未加载 `pg_duckdb`
  - 直接放行
- `PeerLoadedNeedValidation`
  - 当前 backend 已加载 `pg_duckdb`
  - 进入强校验
- `CompatibleProven`
  - 已证明兼容
  - 允许执行
- `CompatibleUnproven`
  - 版本看起来一致，但无法证明两边绑定的是同一运行时实体或同一共享库来源
  - 默认拒绝执行
- `Incompatible`
  - 版本不一致、来源不一致、指纹冲突，或其他明确冲突
  - 直接拒绝执行

核心原则：

> 不能证明安全，不等于可以尝试；在公开支持路径里，不能证明安全就视为不安全。

对首版范围的进一步约束：

- 首版的 `CompatibleProven` 仅作为状态机保留态，不要求交付 peer-loaded 下的公开成功路径。
- 首版一旦判断“当前 backend 已加载 `pg_duckdb`”，默认结果只会落在：
  - `Incompatible`
  - `CompatibleUnproven`
  - 或 unsupported override 放行
- 进入 `CompatibleProven` 的公开成功路径明确推迟到 future cooperative fingerprint 阶段。

## 运行时指纹模型

运行时指纹分三层，逐层增强证明力。

### 第 1 层：版本指纹

最低层、必须有。

至少包含：

- `duckdb_library_version()` 返回值
- `duckdb_fdw` 编译目标 DuckDB 版本
- 如果未来可获取，则加入 `pg_duckdb` 暴露的 DuckDB 精确版本

判定规则：

- 版本字符串不精确一致：`Incompatible`
- 版本字符串一致：仅说明“可能兼容”，不能直接放行

### 第 2 层：加载来源指纹

当前单边方案的核心层。

目标是判断 `duckdb_fdw` 当前调用到的 DuckDB 符号来自哪个共享对象，以及该来源是否能与 `pg_duckdb` 的来源信息相匹配。

至少尝试采集：

- `duckdb_library_version` 符号所在共享对象路径
- `duckdb_fdw` 模块自身路径
- `libduckdb.so` 的实际 resolved path
- 必要时的来源摘要，例如 inode、device、size、mtime 或 hash

判定规则：

- 来源明确冲突：`Incompatible`
- 来源可比较且一致：提升可信度，但仍取决于是否能关联到 peer 信息
- 来源缺失、无法比较或证据不足：`CompatibleUnproven`

### 第 3 层：协作运行时指纹

这是未来增强层，也是进入 `CompatibleProven` 的主要路径。

如果未来 `pg_duckdb` 愿意暴露轻量协作接口，建议至少返回：

- provider 名称
- DuckDB 精确版本
- runtime source id 或 canonical library path
- build fingerprint
- 可选的 ABI tag / commit hash

`duckdb_fdw` 生成相同结构的 fingerprint。只有在以下条件都成立时，才进入 `CompatibleProven`：

- DuckDB 版本精确一致
- runtime source 一致
- fingerprint 协议版本兼容

## 探测信号与执行挂点

### 探测信号优先级

运行期优先级从高到低：

1. 可选协作接口返回的 fingerprint
2. 当前 backend 是否已加载 `pg_duckdb`
3. `duckdb_fdw` 自身 runtime fingerprint
4. 无法证明时落入 `CompatibleUnproven`

### 首版 primary detection path

首版明确采用 Linux-first 策略。

运行期判断“当前 backend 是否已加载 `pg_duckdb`”的主路径：

- 使用动态加载器枚举当前进程已加载共享对象
- 在已加载模块列表中匹配 `pg_duckdb` 扩展模块名或其 canonical path
- 一旦匹配到 peer 模块，进入强校验

安装期预检不依赖这个机制，只做 catalog 级提示。

如果首版在 Linux 上都无法可靠完成 peer-loaded 检测，则 Phase 1 不进入实现，而应先做设计探针收口该前置条件。

### 执行挂点

真正的 runtime guard 必须集中在一个统一守卫中，例如 `duckdb_fdw_check_runtime_compatibility()`，并在所有会触发 DuckDB API 的入口前调用。

最低接线点：

- `connection.c` 中的 `duckdb_get_connection()`
- `duckdb_fdw.c` 中的 `duckdb_fdw_version()`
- 其他任何未来直接触发 DuckDB C API、但不经过 `duckdb_get_connection()` 的 helper 路径

这样可以避免判定逻辑散落在多条执行路径里。

## 用户体验与错误策略

### 安装期提示

安装期只输出 `NOTICE/WARNING`，不阻断：

- 未发现 `pg_duckdb`：静默或最小提示
- 发现 `pg_duckdb`：
  - 说明当前采用严格共存策略
  - 说明安装顺序不决定最终结果
  - 说明最终结果以运行期强校验为准

### 运行期阻断

运行期一旦拒绝执行，错误对象至少包含：

- 当前 backend 已加载 `pg_duckdb`
- 当前判定结果：`CompatibleUnproven` 或 `Incompatible`
- `duckdb_fdw` 侧可见的版本和来源信息
- 如果拿得到，则附带 peer 侧信息
- 下一步建议：
  - 改到未加载 `pg_duckdb` 的 session 再执行
  - 对齐 DuckDB 精确版本
  - 仅实验用途下显式开启 unsupported override

### Unsupported Override

建议通过 GUC 暴露，而不是通过 SQL 函数参数。

推荐命名：

- `duckdb_fdw.allow_unsupported_pg_duckdb_coexistence`

要求：

- 默认值 `off`
- 优先按 session 级使用
- 即使 override 生效，也必须输出 `WARNING`
- 明确声明当前处于 unsupported mode

## 架构分层

建议新增一个独立模块，例如 `runtime_guard.c/.h` 或 `coexistence.c/.h`。

职责边界：

- 采集本侧 runtime fingerprint
- 判断当前 backend 是否已加载 `pg_duckdb`
- 执行状态机判定
- 统一输出允许/拒绝/告警结果

不建议把这部分逻辑直接塞进 `connection.c` 或 `duckdb_fdw.c`，否则后续会快速演变成条件分支堆积。

## 文件触点

首版实现建议主要触达以下文件：

- `duckdb_fdw.c`
  - `_PG_init()` 注册 GUC
  - `duckdb_fdw_version()` 接入 runtime guard
  - 诊断 SQL 函数入口实现
- `connection.c`
  - `duckdb_get_connection()` 前增加统一 guard
- `duckdb_fdw.h`
  - 增加兼容性状态与函数声明
- `Makefile`
  - 增加新模块编译单元
- `duckdb_fdw--2.0.0.sql` 或新增升级脚本
  - 暴露预检/诊断 SQL 接口
- `README.md`
  - 文档化严格共存策略、runtime guard 和 unsupported override
- 新增诊断型测试或专门集成验证脚本

首版平台范围：

- 仅承诺 Linux-first 规划与实现
- 其他平台暂不进入首版支持矩阵，只保留接口与文档占位

## 分阶段落地计划

### Phase 1：守卫骨架

- 新增 runtime guard 模块
- 建立状态机与 runtime fingerprint 基础结构
- 在 `duckdb_get_connection()` 和 `duckdb_fdw_version()` 前接入 guard
- 注册 override GUC
- 首版先支持：
  - `NoPeerLoaded` 直接通过
  - peer 已加载但无法证明兼容时阻断
  - override 可放行

退出标准：

- 能稳定识别 `NoPeerLoaded`
- 能识别“当前 backend 已加载 `pg_duckdb`”并进入强校验
- peer-loaded 且未证实时一致阻断
- override 可按 session 级放行，并输出 `WARNING`

### Phase 2：诊断与安装期体验

- 增加预检函数
- 增加 runtime fingerprint / compatibility status 诊断 SQL 函数
- 完善 README 与错误文案
- 让用户可以自助理解为何被拦截

退出标准：

- 安装期预检能输出正确提醒但不误阻断
- 用户可通过诊断接口看到当前判定状态
- 错误对象能说明阻断原因、已知 fingerprint 和下一步建议

### Phase 3：协作接口预留

- 定义未来 `pg_duckdb` 可选返回的 fingerprint contract
- 在 `duckdb_fdw` 中预留协议槽位
- 不要求首版必须实现双边协作

退出标准：

- future cooperative fingerprint contract 被写清
- `duckdb_fdw` 的判定模型可无歧义地接入该 contract

## 验证策略

不建议第一阶段就把真正的 `pg_duckdb` 集成环境塞进默认 `installcheck`。更合适的验证分层是：

- 单元/诊断测试
  - 无 peer 时返回 `NoPeerLoaded`
  - peer 已加载但信息不足时返回 `CompatibleUnproven`
  - override 打开后输出正确 warning
- 专门的集成 lane
  - 安装期软预检行为
  - 运行期硬阻断行为
  - 若未来有协作接口，再覆盖 `CompatibleProven`

## 风险与权衡

- 风险：首版单边探测在很多场景只能得到 `CompatibleUnproven`
  - 取舍：宁可保守阻断，也不把未定义行为纳入公开支持
- 风险：用户会觉得“版本一样为什么还不让跑”
  - 取舍：文档和错误文案必须明确“版本一致只是必要条件，不是充分条件”
- 风险：unsupported override 被误用为长期配置
  - 取舍：默认关闭、命名显式高风险、文档中不把它当推荐路径

## 开放问题

- `duckdb_library_version` 等符号来源路径在 Linux 上是否足够稳定，需要哪些 fallback 信息
- `pg_duckdb` 若未来提供协作接口，最稳妥的暴露方式是 SQL、共享内存还是 C symbol

## 推荐结论

推荐按“分层兼容协议”落地：

- 当前版本先用 `duckdb_fdw` 单边守卫收紧公开支持边界
- 公开支持遵循严格版“同版本且可证明兼容才放行”
- 无法证明兼容时默认拒绝执行
- 同时为未来 `pg_duckdb` 可选协作接口预留增强路径

这条路线比直接承诺“版本一致基本可共存”更稳，也更适合作为一个可长期维护的扩展合同。
