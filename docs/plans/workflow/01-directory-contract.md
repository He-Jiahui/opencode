# 01 — 工程目录契约与写入权属

> 解决:引擎与 agent 互相覆盖文件(P1-1)、同一 workflow 双目录分叉(P1-2)、
> 非原子写与索引失同步(P3-1)。
> 主要改动面:`workflow.ts` 的落盘辅助函数与所有 `writeFileEnsured/appendFileEnsured` 调用点、
> 工程目录创建逻辑、file write 工具的权限钩子。

## 1. 核心原则

工程目录不是共享白板,而是一个有产权登记的园区。每个路径属于且仅属于下列三区之一:

| 区 | 写入者 | 读者 | 特性 |
|----|--------|------|------|
| **投影区(views)** | 仅引擎 | agent、人类 | 从 DB 单向生成,可随时整体重建,文件头带 generated 标记 |
| **产物区(work)** | 仅 agent(经工具) | 引擎、其他 agent、人类 | 引擎永不覆盖、永不删除;folder reset 之类的操作不得触及 |
| **日志区(journal)** | 仅引擎 | 诊断工具、`workflow doctor` | 仅追加 JSONL,崩溃恢复与审计的依据 |

## 2. 目标目录布局

```
.opencode/workflows/
  wfl_<id>/                        # ← 目录名只用 workflow ID(见 §3)
    manifest.json                  # 权属声明 + 人类可读标题 + schema 版本(引擎写,原子)
    workflow.xml                   # 当前生效的图(投影;真相在 DB)
    workflow-state.json            # portable state(投影;原子写,见 05)
    graph/
      rev-001.xml                  # 每次 update_xml 成功后追加一个修订(只增不删)
      rev-002.xml
    views/                         # ─── 投影区:引擎单向生成,agent 只读 ───
      index.md
      organization.md              # 员工名册(由 DB members 生成,取代 agent 手写)
      progress.md                  # 里程碑状态表(由 DB 生成,消灭"叙述与表格矛盾")
      delivery-summary.md
    journal/                       # ─── 日志区:仅追加 ───
      commands.jsonl               # 每条控制面命令:提交/应用/拒绝(见 02)
      messages.jsonl               # 每条协作消息的投递轨迹(见 03)
      events.jsonl                 # 状态转换、图修订、folder 操作
    work/                          # ─── 产物区:agent 所有,引擎只读 ───
      staff/<member-id>.md         # 员工个人记忆(现 reference/staff/*)
      milestones/<milestone-id>/
        plan.md                    # 部门 PM 的执行计划
        review.md                  # reviewer 结论
        ...                        # executor 自由落盘的中间产物
      shared/                      # 跨角色共享的工作文档(需求分解、charter 等)
    archive/                       # ─── 投影区:会话归档 ───
      session_<id>.md
      session_<id>-summary.md
    inbox/                         # 协作消息的文件视图(投影,见 03)
      consultations/
      interventions/
      standups/
```

与现状的关键差异:

1. 投影/产物/日志物理分目录,一眼可见权属;
2. `reference/` 拆分:staff 记忆与里程碑产物归 `work/`(agent 所有),
   索引与会话归档归 `views/`、`archive/`(引擎所有);
3. `requirements/`、`audit-*/` 这类"里程碑目录"统一收进 `work/milestones/<id>/`,
   避免里程碑 ID 与保留目录名冲突;
4. 新增 `manifest.json` 与 `journal/`。

### 迁移

旧布局(ZirconEngine 现存目录)由 `workflow doctor --migrate`(见 05)一次性搬迁:
`reference/staff/* → work/staff/`,里程碑目录 → `work/milestones/`,其余顶层 md → `views/`
并重新生成。不做在线兼容,portable state 版本号升到 v2 时同步切换。

## 3. 目录命名:消灭双目录分叉

**现状**:目录名 = 时间戳 + 用户原始请求文本(中文、空格、可能被改写),
ZirconEngine 出现 `…帮你review…` 与 `…帮我review…` 两个目录指向同一 `wfl_` ID。

**方案**:

- 目录名一律 `wfl_<id>`,不含任何用户文本。人类可读标题写进 `manifest.json`
  (`title` 字段)与 `views/index.md` 首行;
- 引擎启动/恢复时,同一 workflow ID 只认 manifest 中 `workflowID` 匹配的目录;
  发现多个候选目录时拒绝启动并要求 `workflow doctor` 仲裁,而不是静默新建;
- 兼容:TUI/列表展示用 manifest 的 title,不损失可读性。中文路径带来的
  shell 转义问题(空格、宽字符)一并消失。

## 4. manifest.json

```jsonc
{
  "schema": 2,
  "workflowID": "wfl_…",
  "projectID": "prj_…",
  "title": "review zircon_runtime 代码与计划",
  "created": 1783119931751,
  "ownership": {
    "views/**": "engine",
    "archive/**": "engine",
    "inbox/**": "engine",
    "journal/**": "engine-append",
    "graph/**": "engine-append",
    "work/**": "agent",
    "manifest.json": "engine",
    "workflow.xml": "engine",
    "workflow-state.json": "engine"
  }
}
```

- `ownership` 是引擎与工具共同执行的**唯一权威**;新增路径必须先在此登记;
- 引擎侧:所有落盘统一收口到一个 `WorkflowFs` 模块(见 §5),写前断言
  目标路径的 owner 是 `engine`/`engine-append`,否则抛错(这是防御自己人的护栏——
  今天 workflow.ts 里散落的 `writeFileEnsured` 调用正是事故来源);
- agent 侧:file write / edit 工具在工程目录内执行前查询 manifest,
  对 `engine*` 区的写入直接拒绝并返回指引:
  *"该文件由 workflow 引擎生成,请改用 workflow 命令(如 status_update / update_xml)"*。
  这样 agent 想"更新自己的状态"时会被自然引导到命令通道,而不是手改 `organization.md`。

## 5. WorkflowFs:统一落盘模块

把 workflow.ts 中的 `writeFileEnsured / appendFileEnsured / writeFileEnsuredIfMissing`
收敛为一个带权属校验的模块,并落实三条写入纪律:

1. **原子写**:所有整文件写入走 `write temp → fsync → rename`,同目录临时文件
   (`.tmp-<random>`),杜绝半截 JSON/markdown;
2. **仅追加**:journal 区只允许 append,单行完整 JSON,行内含单调递增 `seq`,
   崩溃后按 seq 截断尾部残行即可恢复;
3. **索引即投影**:`views/index.md`、`inbox/*/index.md`、`archive` 索引一律
   **从 DB/目录扫描整体重建**,禁止 append 维护(现 `standups/index.md` 的
   append 模式在"写文件成功、追加索引前崩溃"时必然失同步)。

单写者约定:每个 workflow 的全部引擎落盘在该 workflow 的调度 fiber 内串行执行
(Effect 层用每 workflow 一把互斥锁),消除引擎自身的并发覆盖;agent 产物区
天然按 `work/staff/<member-id>/`、`work/milestones/<id>/` 分片,不同 agent
不会写同一文件——部门 PM 为里程碑写 `plan.md` 时该里程碑尚未派发,executor
接手后 plan.md 转为只读输入,各自产物写各自的文件。

## 6. 废除 folder reset,改为图修订历史

**现状**:图变更会触发目录重置,"保留 `reference/staff/` 与 `requirements/`、
清掉顶层文档"——主 PM 的 `main-plan.md` 因此丢失,只能靠员工记忆文件重建。

**方案**:

- 彻底删除 reset 逻辑。`update_xml` 成功后:
  1. `graph/rev-NNN.xml` 追加新修订(附 `journal/events.jsonl` 一条
     `graph.revised` 事件,记录 diff 摘要与发起人);
  2. `workflow.xml` 原子覆盖为最新修订;
  3. 投影区整体重建(这是投影区存在的意义——重建无损);
  4. **产物区一个字节都不动**。被删除里程碑的 `work/milestones/<id>/`
     保留原地,仅在 views 索引里标记 `(已从图中移除)`。
- 由此 `main-plan.md` 应从投影区移入 `work/staff/<main-pm>/main-plan.md`
  (它是主 PM 的产物而非引擎投影),永不再被引擎触碰。

## 7. 验收标准

- [x] 引擎任何路径写入前均通过 WorkflowFs 权属断言;对产物区的引擎写入在
      单测中直接失败;
- [x] agent 在工程目录内对投影区文件的 write/edit 被工具层拒绝且给出替代命令提示;
- [x] `update_xml` 十次连续图重构后,`work/` 区内容与重构前逐字节一致;
- [x] kill -9 引擎进程于任意落盘时刻,重启后 `workflow doctor` 校验:无半截文件、
      journal 可截断恢复、索引可重建;
- [x] 新建 workflow 的目录名为 `wfl_<id>`;同 ID 双目录场景被拒绝启动并提示仲裁。

## 产出记录和时间

- 2026-07-04 08:16:40 +08:00 — 状态:已完成止血子任务。新建 workflow 目录从“时间戳 + 请求文本”改为 `.opencode/workflows/<workflowID>`,不再把用户提示词写进路径;旧式时间戳/标题目录在读取时会迁移到 ID 路径,并同步重写 workflow/test、milestone plan/review、intervention 文档路径引用。完整 manifest/WorkflowFs/doctor 仲裁仍按本篇后续验收继续推进。
- 2026-07-05 00:02:44 +08:00 — 状态:部分完成 manifest 子任务。新建/更新 workflow 状态时会同步原子写 `.opencode/workflows/<workflowID>/manifest.json`,记录 schema、workflowID、projectID、标题、创建时间与目录权属声明;重复目录识别优先读取 manifest,即使 fork 目录缺失 `workflow-state.json` 也能被识别为同一 workflow 分叉。完整 WorkflowFs 权属强制、agent 写投影区拒绝与 v2 布局迁移仍按本篇后续验收继续推进。
- 2026-07-05 00:08:32 +08:00 — 状态:部分完成图修订历史子任务。`saveDefinition` 现在会把每个不同的 workflow XML 修订原子写入 `graph/rev-NNN.xml`,重复保存相同 XML 不追加新版本,并在 `journal/events.jsonl` 记录 `graph.revised` 事件。完整 folder reset 废除、产物区逐字节保护与投影区重建仍按本篇后续验收继续推进。
- 2026-07-05 00:15:59 +08:00 — 状态:已完成 agent 写入护栏止血。`write`、`edit`、`apply_patch` 工具现在会读取 `.opencode/workflows/<id>/manifest.json` 的 ownership,拒绝 agent 直接写 `engine`/`engine-append` 区(如 `progress.md`、`journal/**`),并提示改用 workflow 工具;`work/**` 仍允许 agent 写入。完整 WorkflowFs 引擎侧统一收口、旧布局迁移与投影区整体重建仍按本篇后续验收继续推进。
- 2026-07-05 00:45:38 +08:00 — 状态:已完成引擎写入并发止血。workflow 引擎内部的整文件写入和 append 写入现在按目标文件路径串行化;整文件写入走同目录临时文件、fsync、rename,append journal 同样通过同一路径队列,避免 Windows 下并发投影/状态/journal 写入出现 `EPERM rename`、半截文件或同一文件并发覆盖。完整 WorkflowFs 模块化收口和 per-workflow 互斥仍按本篇后续验收继续推进。
- 2026-07-05 01:22:54 +08:00 — 状态:已完成旧布局迁移止血。`workflow doctor --migrate` 现在会把旧式时间戳/提示词 workflow 目录迁到规范 `.opencode/workflows/<workflowID>` 路径,同步重写 workflow、milestone plan/review、intervention 路径引用,写回新的 `workflow-state.json`,并将旧目录改名为 `.orphaned-<timestamp>` 留档,避免同一 workflowID 的旧目录继续参与活跃分叉识别。完整 WorkflowFs 模块化收口和投影区整体重建仍按本篇后续验收继续推进。
- 2026-07-05 04:44:40 +08:00 — 状态:已完成临时写入残留修复止血。`workflow doctor --fix` 现在会清理 workflow 根目录下由原子写失败或崩溃留下的 `.tmp-*` 临时文件;只读 doctor 继续报告 `temporary_file`,显式 fix 后残留会被删除且不再作为 warning 返回。这补齐了“崩溃后无半截临时文件残留”的一部分目录契约验收。完整 WorkflowFs 模块化收口与任意时刻 kill 注入测试仍按本篇后续验收继续推进。
- 2026-07-05 04:51:14 +08:00 — 状态:已完成 journal 尾部残行修复止血。`workflow doctor --fix` 现在会对 `commands.jsonl`、`messages.jsonl`、`events.jsonl` 进行安全尾部修复:只有无效 JSON 行位于文件末尾且其后没有有效审计行时才截断;如果坏行位于中间则继续报告 `invalid_journal_json` 并拒绝自动修复,避免误删后续有效记录。这补齐了“journal 可截断恢复”的基础实现。完整 kill 注入与索引可重建验收仍按本篇后续任务继续。
- 2026-07-05 05:31:37 +08:00 — 状态:已完成 `work/` 产物区保护回归。新增 `preserves workflow work artifacts across repeated XML graph rebuilds`,在 `work/staff/**` 与 `work/milestones/**/artifacts/**` 写入文本和二进制产物后连续执行 10 次不同 `updateXml`,断言 `work/` 快照逐字节一致,覆盖图重构只追加 `graph/` 与重建投影、不移动或覆盖 agent 产物的目录契约。
- 2026-07-05 06:18:18 +08:00 — 状态:已完成 agent 写投影区拒绝验收。补强 `write`、`edit`、`apply_patch` 三类工具回归,不仅断言 `progress.md=engine` 时保持原文件不变,也断言错误提示包含 `Use the workflow tool`,给 agent 明确替代路径;`work/**=agent` 仍可写。已通过: `bun test test/tool/write.test.ts test/tool/edit.test.ts test/tool/apply_patch.test.ts --test-name-pattern "workflow engine-owned" --timeout 30000`;`bun typecheck`。
- 2026-07-05 08:36:24 +08:00 — 状态:已完成引擎写产物区拒绝验收。workflow 引擎内部 `writeFileEnsured`/`appendFileEnsured` 写入前会读取 workflow `manifest.json` ownership,只允许写 `engine`/`engine-append` 路径;当 XML 里程碑 ID 试图让预创建计划落到 `work/plan.md` 这类 agent 产物区时会直接失败且不产生文件。已通过: `bun test test/workflow/company-flow.test.ts --test-name-pattern "rejects engine writes into workflow agent-owned work artifacts|preserves workflow work artifacts across repeated XML graph rebuilds" --timeout 90000`;`bun typecheck`。
- 2026-07-05 08:41:20 +08:00 — 状态:已完成双目录启动仲裁验收。`workflow.start` 已使用 `.opencode/workflows/<workflowID>` 规范目录且不含提示词;`workflow.resume`/control resume 前新增只读 doctor gate,发现同一 workflowID 双目录、非规范路径或损坏 manifest/state 等 P0 目录问题时会直接 block,提示先执行 `workflow doctor --fix/--migrate`,不会继续启动调度。已通过: `bun test test/workflow/company-flow.test.ts --test-name-pattern "doctor reports duplicate local workflow directories|doctor fix keeps the highest journal high-water workflow directory" --timeout 90000`;`bun typecheck`。
- 2026-07-05 16:24:15 +08:00 — 状态:已完成新建 workflow manifest 可见性修复。`workflow.start` 在 DB 行可见前先写入 `workflow.xml`、`manifest.json` 与 `main-plan.md`;启动恢复扫描会跳过 5 秒内刚创建且尚未完全可见的新 workflow,避免 doctor 在 manifest 写入竞态窗口把新建 workflow 误判为 `missing_manifest` 并 block。
- 2026-07-05 17:22:03 +08:00 — 状态:继续补齐 kill 恢复相关目录契约。新增 doctor 综合修复回归,同一目录同时覆盖临时文件残留、journal 尾部半截 JSON、seq 空洞/缺失、双目录仲裁与 orphan active milestone;`doctor --fix` 后这些可恢复问题归零,并修正 `.orphaned-*` 留档目录不再被 workflow state 扫描重新导入。完整任意时刻 kill 注入和索引重建验收仍未最终关闭。
- 2026-07-05 22:14:58 +08:00 — 状态:已完成 20 点持久化故障注入核心回归。workflow 写文件层新增测试专用故障点,覆盖整文件原子写、append journal 与 command journal 落账前后中断;命令持久化失败不再被永久记录为 rejected,失败会释放 inflight 并允许同 command id 重试;`workflow doctor --fix` 现在可重写 workflow-state.json 的 workflow/milestone 状态不一致。真实外部 kill -9 金丝雀与索引重建验收仍未最终关闭。
- 2026-07-06 00:30:51 +08:00 — 状态:已完成索引/投影重建验收。新建 workflow 现在会立即生成 reference/progress/organization/delivery 等本地投影;`workflow doctor` 会以 warning 报告缺失或空的投影文件(`missing_projection`/`empty_projection`),`doctor --fix` 会从 DB/快照重建 `index.md`、`progress.md`、`organization.md`、`reference/index.md`、`reference/requester.md`、consultation/intervention/standup 索引与 staff memory。已通过投影重建回归、doctor 组合回归与 `bun typecheck`。真实外部 kill -9 金丝雀仍未最终关闭。
- 2026-07-06 00:48:55 +08:00 — 状态:已完成真实外部 kill 金丝雀。新增子进程 helper 在 workflow 写入 `write:after-write` 故障点写出 marker 后阻塞,父测试进程用 `SIGKILL` 杀掉该子进程,随后新子进程用同一工程目录和 sqlite DB 执行 `workflow doctor --fix` 与同 command id 重试,断言恢复后无临时文件/journal/投影/state 可恢复问题且新 XML 里程碑可见。已通过: `bun test test/workflow/company-flow.test.ts --test-name-pattern "external process kill" --timeout 120000`;并复跑 20 点进程内注入与投影重建组合。多写入点外部 kill 压测仍未展开,但真实 OS 级进程终止 canary 已闭环。
- 2026-07-06 01:46:52 +08:00 — 状态:已完成 20 点真实外部 kill 验收。外部子进程 canary 现在按 20 个 workflow 写入故障点逐点启动独立 workflow、在指定落盘点写 marker 后阻塞、由父测试进程 `SIGKILL` 强杀,再用新子进程执行 `workflow doctor --fix` 与同 command id retry;每个点均断言 doctor 无临时文件、journal、投影和 state 可恢复残留,且 retry 后 `kill-canary-c` 可见。已通过: `bun test test/workflow/company-flow.test.ts --test-name-pattern "external process kill" --timeout 660000`。
