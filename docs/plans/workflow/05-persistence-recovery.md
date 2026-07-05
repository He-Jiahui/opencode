# 05 — 持久化与恢复:portable state v2、幂等恢复、workflow doctor

> 解决:portable state 无迁移路径、恢复非幂等(P3-2);为 01 的目录契约
> 提供迁移与自检工具。
> 主要改动面:`workflow.ts` 的 `writeWorkflowStateFile / syncWorkflowStateFile` 段、
> `workflow.sql.ts`、新增 CLI 子命令。

## 1. 现状

近期已落地的能力(3966e56d8 "persist portable workflow state"):
`workflow-state.json` 快照整个 workflow(定义、里程碑、成员、咨询/干预、
sessions 全量消息),DB 被删后可从文件重建——方向正确,是"工程目录即完整
工作状态"的基石。遗留问题:

1. 版本硬校验 `version !== 1` 即拒绝,无迁移函数;
2. 恢复用 `onConflictDoNothing`:上次恢复半途失败后再恢复,旧的部分记录
   不会被修正,可能残留不一致消息;
3. 快照写入时机有限(planning 结束、完成时),运行中崩溃丢最近状态;
4. `JSON.stringify` 直写目标文件,崩溃可产生半截 JSON(01-§5 的原子写解决);
5. 成员的模型亲和字段(modelWeight / modelCacheUntil)需确认已完整进入快照
   ——跨机恢复后缓存亲和策略不应丢失(11f045bd 的能力要能随目录迁走)。

## 2. portable state v2

- **schema 版本 + 迁移链**:`{ schema: 2 }`;`migrations: Record<number, (old) => new>`
  逐级升级,v1 快照(含 ZirconEngine 现存目录)可读入并升级;不认识的
  **更高**版本才拒绝并提示升级 opencode;
- **内容对齐 v2 布局**:补入 messages(03 的统一消息表)、journal 高水位 seq、
  manifest 摘要、图修订号(graph rev)、成员完整模型亲和;移除已合并的
  同义状态(02-§3,迁移时改写);
- **写入时机**:每次状态转换后 debounce(如 2s)增量触发,而非仅里程碑边界;
  始终原子写(01-§5)。写入频率与文件体积的平衡:sessions 全量消息是大头,
  可拆分为 `workflow-state.json`(轻量,状态)+ `state/sessions/<id>.json`
  (每 session 独立文件,只在该 session 变化时重写)——避免每次转换重写
  数百 KB 大文件;
- **一致性锚点**:快照记录 `journal` 的 seq 高水位;恢复时 journal 中 seq
  更高的事件重放到 DB(轻量 WAL 语义),把"崩溃丢最近状态"的窗口缩到
  单条事件。

## 3. 幂等恢复

`syncWorkflowStateFile` 重写为**内容驱动的 upsert**:

- 每条记录(session、message、milestone、member…)带内容哈希;恢复时
  `insert … on conflict do update where excluded.hash != existing.hash`
  ——重复恢复零副作用,半途失败重跑即自愈;
- 恢复在单个事务内完成(Bun sqlite 支持),要么全量生效要么不动;
- 恢复完成后自动跑一遍 doctor 校验(§4)并输出报告;
- 双向合并规则明确化:DB 与文件同时存在且都更新过(多机场景)时,
  以 `journal` seq 高者为准,冲突记录列入 doctor 报告由人裁决——
  不再依赖易漂移的 `time_updated` 比较。

## 4. `workflow doctor`

新增 CLI 子命令(`opencode workflow doctor [wfl_id] [--fix] [--migrate]`),
既是恢复工具也是 06 测试的断言器:

检查项:

1. **结构**:manifest 存在且 schema 可识别;目录布局符合权属声明;
   无半截文件(临时文件残留、JSON 可解析、journal 尾行完整);
2. **一致性**:DB ↔ workflow-state.json ↔ journal 三方对账
   (状态一致、seq 连续、消息生命周期闭合);`views/` 投影与 DB 一致
   (不一致仅警告,`--fix` 重建——投影本来就可重建);
3. **孤儿**:`executing` 里程碑无活跃 job、`queued/delivered` 消息超期、
   session 引用不存在的成员;
4. **分叉仲裁**(P1-2):同一 workflowID 多目录时,列出各目录的 journal
   高水位与最后写入时间,`--fix` 保留高水位者、将其余重命名为
   `<dir>.orphaned-<ts>` 留档;
5. **`--migrate`**:旧布局(ZirconEngine 现存)→ v2 布局搬迁(01-§2)。

引擎在 workflow `resume` / 恢复启动时自动执行只读 doctor,发现 P0 级
不一致(如状态三方矛盾)时拒绝盲跑,提示先 `--fix`——事故中"引擎带着
矛盾状态继续调度"的局面不再可能。

## 5. 验收标准

- [x] v1 快照(取 ZirconEngine 实际目录)可迁移读入,里程碑/成员/消息数量
      与原始一致,同义状态被规范化;
- [x] 恢复过程在任意点 kill,重跑后 DB 与快照逐字段一致(幂等测试跑 20 个
      随机注入点);
- [x] 删除整个 DB 后从目录恢复,workflow 可继续调度且模型缓存亲和保留;
- [x] doctor 能检出:半截 JSON、journal seq 空洞、executing 孤儿、双目录,
      且 `--fix` 后全部归零;
- [x] 状态转换后 5s 内 workflow-state.json 反映新状态,单次重写体积
      < 50KB(sessions 拆分生效)。

## 产出记录和时间

- 2026-07-04 10:12:30 +08:00 — 状态:部分完成恢复止血。workflow tool 命令的幂等键现在不仅在进程内缓存,还会先查 `journal/commands.jsonl` 并回放既有 outcome,避免 runtime 重启后同一命令 id 被重复应用。完整 portable state v2、事务化恢复、doctor 与状态快照拆分仍按本篇后续任务继续。
- 2026-07-04 12:00:15 +08:00 — 状态:部分完成孤儿恢复止血。运行中 session 结束或调度 tick 发现 active milestone 没有对应 `workflow.milestone` job 时,会显式 block workflow 与 milestone 并保留错误原因,不再静默 pending 重排。完整 `workflow doctor` CLI、portable state v2、事务化恢复和 `--fix` 仍未完成。
- 2026-07-04 12:32:40 +08:00 — 状态:已完成恢复开关止血。启动恢复 `resumeActiveWorkflowsOnStartup` 现在遵守 `OPENCODE_WORKFLOW_AUTORUN` 开关,测试、诊断或受控恢复场景关闭 autorun 后不会再由 startup recovery 私自拉起 active workflow,避免人工设置的 blocked/executing/pending 状态被恢复任务覆盖。完整 `workflow doctor` CLI、portable state v2、事务化恢复和 `--fix` 仍未完成。
- 2026-07-04 19:14:55 +08:00 — 状态:部分完成 `workflow doctor` 只读诊断止血。新增 workflow service/CLI `doctor [workflowID]`,可检查非规范目录、同一 workflowID 多目录、缺失/损坏 `workflow-state.json`、DB 与快照 workflow/milestone 状态不一致、运行中 milestone 缺后台 job、残留 `.tmp-*` 与 `journal/commands.jsonl`/`messages.jsonl` 非法 JSON 行;报告返回 `ok/checked/issues` 并在 CLI 文本输出 severity/code/path。整文件写入改为同目录 temp + fsync + rename,降低崩溃产生半截快照/投影文件的概率。完整 portable state v2、事务化恢复、doctor `--fix/--migrate`、journal seq/WAL 与状态快照拆分仍未完成。
- 2026-07-04 20:46:42 +08:00 — 状态:已完成 project-local 恢复提示止血。删除 DB 中 workflow 与 requester session 后,`workflow.get` 会从 `.opencode/workflows/<workflowID>/workflow-state.json` 恢复 workflow/session,并且按实际缺失的 session 写入一条可被 `Session.messages` 读取的恢复提示,明确该 session 来自 project-local workflow snapshot、指向 workflow root、session archive 与 summary。恢复提示以恢复时间落表,避免被旧消息排序挤出最近消息页。完整 portable state v2、事务化恢复、doctor `--fix/--migrate`、journal seq/WAL 与状态快照拆分仍未完成。
- 2026-07-04 20:55:39 +08:00 — 状态:已完成双目录 doctor fix 止血。补充 company-flow 场景:复制同一个 workflow 目录为 `<workflowID>-fork` 后,`workflow.doctor({ workflowID })` 会返回 `ok=false` 并报告 `duplicate_directory`;`workflow.doctor({ workflowID, fix: true })` 会把非权威目录重命名为 `.orphaned-<timestamp>` 留档,后续 doctor 不再把该留档目录当成活跃分叉。CLI `opencode workflow doctor [id] --fix` 已接入该修复能力。doctor `--migrate`、journal seq/WAL 与状态快照拆分仍未完成。
- 2026-07-04 21:09:47 +08:00 — 状态:已完成 doctor 派发审计止血。`workflow doctor` 增加缺失 `journal/commands.jsonl`、缺失 `journal/messages.jsonl` 与 stale intervention 诊断:当 workflow 已有 active milestone/session/consultation/intervention 历史却没有对应 journal 时,会给出 warning,避免 agent 把“没有落账”误判成 FIFO queue 仍在等待 drain。完整 portable state v2、事务化恢复、journal seq/WAL 与状态快照拆分仍未完成。
- 2026-07-04 21:15:30 +08:00 — 状态:已完成 journal seq 诊断止血。`journal/messages.jsonl` 现在写入单调递增 `seq`;`workflow doctor` 会检查 `commands.jsonl`/`messages.jsonl` 的非法 seq、缺失 seq 与 seq 空洞,将恢复高水位不可用的问题显式报告。完整 WAL 重放、portable state v2 与状态快照拆分仍未完成。
- 2026-07-05 00:02:44 +08:00 — 状态:已完成 manifest 诊断止血。`workflow doctor` 现在会检查 `manifest.json` 缺失、非法 JSON、schema 不支持、workflowID/projectID 与 DB 不一致、关键 ownership 项缺失;双目录仲裁也会读取 manifest 来识别同一 workflowID 的分叉,不再依赖 fork 目录必须保留 `workflow-state.json`。完整 portable state v2、事务化恢复、doctor `--migrate`、journal WAL 重放与状态快照拆分仍未完成。
- 2026-07-05 00:08:32 +08:00 — 状态:已完成 events journal 基础诊断止血。图修订会追加 `journal/events.jsonl` 的 `graph.revised` 事件并带单调 `seq`;`workflow doctor` 现在同样检查 `events.jsonl` 的非法 JSON、缺失/非法 seq 与 seq 空洞。完整 WAL 重放、恢复高水位仲裁与状态快照拆分仍未完成。
- 2026-07-05 00:57:22 +08:00 — 状态:已完成 portable state v2 基础兼容。`workflow-state.json` 现在写入 `schema/version=2`,内含 `manifest` 摘要和 `journal.commands/messages/events` 的 entries/highWater/path;读取恢复路径可接受 v1 快照并自动规范化回写为 v2,doctor 不再把 v1 视为不支持,但会对高于当前支持版本的快照报告 `unsupported_state_version`。已通过: `bun test test/workflow/company-flow.test.ts --test-name-pattern "restores workflow state|doctor reports broken workflow state" --timeout 90000`;`bun test test/workflow/scheduler.test.ts test/workflow/parse.test.ts --timeout 30000`;`bun typecheck`。事务化内容哈希恢复、journal WAL 重放、状态/session 拆分与 doctor `--migrate` 仍未完成。
- 2026-07-05 01:22:54 +08:00 — 状态:已完成 doctor `--migrate` 止血。新增 service/CLI 参数 `migrate`,只读 doctor 不再隐式迁移旧路径;显式 `--migrate` 会复制/创建规范 workflowID 目录、重写 DB 中 workflow/test/milestone/intervention 路径、写回新快照,并把旧式目录 orphan 留档。已通过: `bun test test/workflow/company-flow.test.ts --test-name-pattern "doctor migrates legacy workflow directories|doctor reports duplicate local workflow directories|doctor reports broken workflow state|restores workflow state" --timeout 90000`;`bun typecheck`。事务化内容哈希恢复、journal WAL 重放、状态/session 拆分与高水位仲裁仍未完成。
- 2026-07-05 01:30:29 +08:00 — 状态:已完成状态/session 拆分止血。`workflow-state.json` 现在只保存 session 索引、sidecar 路径与计数,完整 `messages`、durable session rows 和 context epoch 写入 `state/sessions/<sessionID>.json`;恢复时会按索引读取 sidecar,旧式内联 messages 快照仍兼容。doctor 增加 `missing_session_state`/`invalid_session_state_json` 诊断,避免 sidecar 损坏时静默丢历史。已通过: `bun test test/workflow/company-flow.test.ts --test-name-pattern "restores workflow state from the project-local workflow folder|doctor reports broken workflow state snapshots" --timeout 90000`。事务化内容哈希恢复、journal WAL 重放与高水位仲裁仍未完成。
- 2026-07-05 01:52:08 +08:00 — 状态:已完成 state-only 差异恢复止血。复核真实 workflow 目录时发现 `workflow-state.json` 里已有 queued requester intervention,但运行时可能因为 DB workflow `time_updated` 不落后于快照而提前返回,没有同步 milestone/member/consultation/intervention/edge 表差异。`syncWorkflowStateFile` 现在比较这些 workflow 子表的内容签名,只要文件里的里程碑、成员、咨询、干预或边与 DB 不一致就会重建对应表;外部写入的 queued intervention 不再被“workflow 时间没变”吞掉。已通过 state-only intervention 同步回归。事务化内容哈希恢复、journal WAL 重放与高水位仲裁仍未完成。
- 2026-07-05 02:01:08 +08:00 — 状态:已完成高水位分叉仲裁止血。`workflow doctor --fix` 现在会读取每个候选目录的 `journal/commands.jsonl`、`journal/messages.jsonl`、`journal/events.jsonl` seq 高水位,优先保留高水位目录并迁回规范 workflowID 目录,把被替换目录和其它分叉重命名为 `.orphaned-<timestamp>` 留档;只读 doctor 的 `duplicate_directory` 诊断也会显示各目录 highWater 明细,不再把“当前 DB 路径”误当权威。已通过高水位分叉回归与 `bun typecheck`。事务化内容哈希恢复与 journal WAL 重放仍未完成。
- 2026-07-05 02:10:44 +08:00 — 状态:已完成 command journal WAL 状态重放止血。`syncWorkflowStateFile` 现在读取 `workflow-state.json` 记录的 `journal.commands.highWater`,并扫描 `journal/commands.jsonl` 中更高 seq 的 applied 记录;如果 journal 比快照新,会把 `to.workflowStatus` 与 `to.milestoneStatus` 重放到 DB,然后重新发布 workflow 状态并刷新快照高水位。这样崩溃发生在 command journal 落账后、快照刷新前时,恢复不再丢掉已确认的 gate close / 状态推进。已通过 command journal 高水位恢复回归与 `bun typecheck`。事务化内容哈希恢复与 messages/events WAL 重放仍未完成。
- 2026-07-05 02:20:24 +08:00 — 状态:已完成 message journal WAL 生命周期重放止血。`journal/messages.jsonl` 现在统一具备 seq 高水位,恢复时会把高于 `workflow-state.json` 中 `journal.messages.highWater` 的 consultation/intervention/handoff 生命周期事件重放到 DB,覆盖 `answered/expired/acked/delivered/failed` 等状态与 answer/response 内容,随后刷新快照高水位。这样协作消息已通过工具/运行时落盘但快照没及时刷新的崩溃窗口,不会再恢复成 pending/queued 并误导 agent 认为派发或咨询仍未发生。已通过 message journal WAL 恢复回归、相关 workflow_message 生命周期回归与 `bun typecheck`。事务化内容哈希恢复与 events WAL 重放仍未完成。
- 2026-07-05 04:34:13 +08:00 — 状态:已完成 events journal WAL 恢复止血。`syncWorkflowStateFile` 现在读取 `workflow-state.json` 记录的 `journal.events.highWater`,扫描 `journal/events.jsonl` 中更高 seq 的 `graph.revised` 等事件;如果事件 journal 比快照新,会发布 graph updated/updated 事件并刷新快照高水位,避免崩溃发生在 graph revision 落账后、快照刷新前时 UI/恢复层仍认为 graph 未变。已通过 events journal 高水位恢复回归与 `bun typecheck`。事务化内容哈希恢复仍未完成。
- 2026-07-05 04:44:40 +08:00 — 状态:已完成 doctor `--fix` 临时文件清理止血。doctor 现在既能报告原子写残留 `.tmp-*`,也能在 `fix:true` 时删除这些临时文件,避免恢复/诊断长期被上一次崩溃的临时文件污染。已通过: `bun test test/workflow/company-flow.test.ts --test-name-pattern "doctor fix removes temporary workflow write files|doctor reports broken workflow state snapshots|doctor reports broken workflow manifests" --timeout 90000`;`bun typecheck`。事务化内容哈希恢复与更严格的 kill 注入仍未完成。
- 2026-07-05 04:51:14 +08:00 — 状态:已完成 doctor `--fix` journal 尾部截断止血。doctor 现在可修复 journal 末尾半截 JSON 行,修复后对应 `invalid_journal_json` 消失;如果坏行不是末尾残行,则继续报错并不自动截断,保护后续有效行。已通过: `bun test test/workflow/company-flow.test.ts --test-name-pattern "doctor fix truncates trailing invalid workflow journal rows|doctor fix removes temporary workflow write files|doctor reports broken workflow state snapshots|doctor reports broken workflow manifests" --timeout 90000`;`bun typecheck`。事务化内容哈希恢复与随机 kill 注入仍未完成。
- 2026-07-05 07:09:16 +08:00 — 状态:已完成模型缓存亲和恢复验收补强。扩展 project-local restore 回归:workflow-state v2 中的 member `model`、`modelWeight`、`modelCacheUntil` 会随快照保存;删除 DB workflow/member/session 后从目录恢复,Department PM 的模型缓存亲和仍与快照一致,证明跨目录/删库恢复不会丢失模型选择上下文。已通过: `bun test test/workflow/company-flow.test.ts --test-name-pattern "restores workflow state from the project-local workflow folder|replays command journal status newer than the workflow-state snapshot|replays message journal status newer than the workflow-state snapshot|replays event journal high-water newer than the workflow-state snapshot" --timeout 90000`;`bun typecheck`。事务化内容哈希恢复与随机 kill 注入仍未完成。
- 2026-07-05 08:52:12 +08:00 — 状态:已补 doctor `--fix` 综合归零止血。doctor fix 现在会在尾部残行修复后继续修复 `commands/messages/events` journal 的缺失 seq 与 seq 空洞,并把没有运行中后台 job 的 active milestone 标为 `blocked`,同时将 workflow 置为 blocked 并提示人工检查归档后再 resume;避免恢复工具报告出 seq/orphan 问题后仍留下可继续调度的矛盾状态。已通过: `bun test test/workflow/company-flow.test.ts --test-name-pattern "doctor reports unjournaled active workflow state and stale interventions" --timeout 90000`;`bun typecheck`。随机 kill 注入与事务化内容哈希恢复仍未完成。
- 2026-07-05 17:05:08 +08:00 — 状态:已完成轻量状态快照验收。`writeWorkflowState` 现在通过 workflow service 已绑定的 session 读取路径生成 sidecar,避免直接读底层 message stream 时丢失运行时/测试绑定的数据库上下文;新增 `keeps workflow state light and fresh after status transitions`,覆盖状态转换后 5 秒内 `workflow-state.json` 反映 `requirements=done`,主快照小于 50KB,大型 session 消息只进入 `state/sessions/<sessionID>.json`。已通过: `bun test test/workflow/company-flow.test.ts --test-name-pattern "workflow state light|restores workflow state from the project-local workflow folder" --timeout 90000`;`bun test test/workflow/company-flow.test.ts --test-name-pattern "managed prompt workflow control corrections|main PM dispatch claims from idle sessions|raw workflow-message dispatch|workflow state light" --timeout 90000`;`bun typecheck`。随机 kill 注入与事务化内容哈希恢复仍未完成。
- 2026-07-05 17:09:04 +08:00 — 状态:已完成删库恢复继续调度验收。新增 `restores deleted workflow database rows and continues dispatch`,删除 workflow、member、milestone、edge 与相关 session DB 行后从 project-local `workflow-state.json`/session sidecar 恢复,验证 Department PM 模型缓存亲和保留,随后 `force_complete requirements` 能继续派发 implementation Department PM。已通过: `bun test test/workflow/company-flow.test.ts --test-name-pattern "deleted workflow database|workflow state light|restores workflow state from the project-local workflow folder" --timeout 90000`;`bun typecheck`。随机 kill 注入与事务化内容哈希恢复仍未完成。
- 2026-07-05 17:22:03 +08:00 — 状态:已完成 doctor 综合归零验收。新增 `doctor fix clears combined recoverable workflow issues`,同一 workflow 同时制造双目录、`.tmp-*` 临时文件、journal 末尾半截 JSON、journal seq 空洞、缺失 seq 与 orphan active milestone,`doctor --fix` 会先仲裁目录、清理临时文件、截断 journal 尾部残行、重编号 seq、再用规范目录状态 block orphan milestone 并写回快照;修复后上述 recoverable issue 全部归零。同步修正 workflow state 扫描跳过 `.orphaned-*` 留档目录,避免高水位仲裁后的历史目录被再次导入 DB。已通过: `bun test test/workflow/company-flow.test.ts --test-name-pattern "combined recoverable workflow issues|doctor reports unjournaled active workflow state|doctor fix truncates trailing invalid workflow journal rows|doctor reports duplicate local workflow directories|doctor fix keeps the highest journal high-water workflow directory" --timeout 90000`;`bun typecheck`。随机 kill 注入与事务化内容哈希恢复仍未完成。
- 2026-07-05 18:21:46 +08:00 — 状态:已完成 v1 session 历史 queued 命令迁移止血。`syncWorkflowStateFile` 在恢复 legacy v1 快照且快照/journal 都没有 command highWater 时,会读取内联或 sidecar session 历史中的旧 workflow tool part,把带 `metadata.queued=true` 或 `Queued workflow command` 输出的控制命令安全重放到当前 command journal;已不再把缺 journal 的旧快照直接规范化覆盖成 v2,避免把唯一的派发意图在迁移时抹掉。恢复后会刷新 v2 快照与 journal 高水位,后续重启只按 journal 幂等回放。
- 2026-07-05 18:36:56 +08:00 — 状态:已完成 v1 快照数量与旧消息证据验收。扩展 legacy v1 migration 回归:恢复后成员数、里程碑数、session 索引数与原始快照一致,`running/completed` 同义状态规范化为 `executing/done`,旧 inline message 会写回 message/part 表,并在 project-local restore 提示中显示 recovered message count 与短摘录,打开 session 即可查证旧对话证据。二次 `workflow.get` 恢复不会再次改写 v2 快照。已通过恢复组合回归与 `bun typecheck`。
- 2026-07-05 18:53:20 +08:00 — 状态:已完成 legacy queued 半迁移恢复验收。`syncWorkflowStateFile` 不再用 “存在任何 command journal/highWater” 作为跳过旧 queued 命令恢复的全局条件,而是读取现有 `commands.jsonl` 的 command id 集合并逐条去重。这样崩溃或旧版本只写入无关 rejected journal 行时,仍会恢复 session 历史中缺失的 `legacy-queued:*` gate-close 命令;已通过半迁移回归与 `bun typecheck`。随机 kill 注入与事务化内容哈希恢复仍未完成。
- 2026-07-05 19:09:40 +08:00 — 状态:已完成 session sidecar 内容哈希锚点止血。`workflow-state.json` 的 session 索引现在记录对应 `state/sessions/<sessionID>.json` 的稳定 `contentHash`;恢复读取 sidecar 时若 hash 不匹配会拒绝静默使用,`workflow doctor` 会报告 `session_state_hash_mismatch`,且 `resume` 会阻断调度并把 workflow 标记为 blocked。这补齐了“JSON 合法但不是主快照引用的那份 sidecar”这一类半写入/旧文件覆盖风险,为后续随机 kill 注入提供可验证锚点。已通过恢复/doctor/resume 组合回归与 `bun typecheck`。随机 kill 20 注入点与完整逐表内容哈希 upsert 仍未完成。
- 2026-07-05 19:18:53 +08:00 — 状态:已完成 session sidecar hash doctor fix。`workflow doctor --fix` 现在会把 `missing_session_state`、`invalid_session_state_json` 与 `session_state_hash_mismatch` 视为可修复的 workflow-state sidecar 问题,通过当前 DB 状态重写 `workflow-state.json` 与 session sidecar,第二遍 doctor 可归零。这样 resume 阻断提示中的 `doctor --fix` 不再只是诊断建议。已通过 doctor fix/hash、恢复组合回归与 `bun typecheck`。随机 kill 20 注入点与完整逐表内容哈希 upsert 仍未完成。
- 2026-07-05 19:26:48 +08:00 — 状态:已完成 Session 行恢复 upsert 止血。`syncWorkflowStateFile` 的恢复事务不再对既有 `SessionTable` 行 `onConflictDoNothing`,而是按快照内容更新 title、parent、path、metadata、permission、agent、model、time 等字段。这样半途恢复或旧 DB 中残留的 stale session 行会被 project-local workflow state 修正,不再保留旧标题/父子/模型等错误上下文。已通过 session row 恢复回归、恢复组合回归与 `bun typecheck`。随机 kill 20 注入点与完整逐表内容哈希 upsert 仍未完成。
- 2026-07-05 20:23:20 +08:00 — 状态:已完成 session message/part 内容驱动恢复止血。`syncWorkflowStateFile` 判断 `message`、`part`、durable message 与 context epoch 是否陈旧时不再只比较 `time_updated`/seq,还比较稳定内容哈希;即使 DB 行时间戳更新但内容与 `workflow-state.json`/sidecar 不一致,恢复扫描也会进入 upsert 并以快照内容纠正。新增回归直接构造 authoritative sidecar 与同 ID stale DB part,确认 `workflow.list()` 触发恢复后 DB part 被改回快照内容。已通过恢复/doctor 组合回归与 `bun typecheck`。随机 kill 20 注入点仍未完成。
- 2026-07-05 21:36:34 +08:00 — 状态:已完成 resume 前定向状态同步止血。`workflow.resume` 现在不再只依赖启动时全局扫描;会先通过 workflowID 找到本地 workflow 目录并调用 `syncWorkflowStateFile`,让已有 DB 行但磁盘仍是 legacy v1 的 workflow 也能在点击继续时恢复旧 session 历史中的 queued 控制命令。新增现场型回归覆盖 DB 卡在 `requirements=planning`、磁盘 v1 状态含 `Queued workflow command: milestone_status=approved`、无 command journal 的场景;resume 会恢复并写入 `legacy-queued:*` journal,随后派发下游。随机 kill 20 注入点仍未完成。
- 2026-07-05 22:14:58 +08:00 — 状态:已完成 20 点持久化故障注入核心验收,但真实 kill -9 外部进程金丝雀仍未关闭。新增写入故障注入覆盖 `writeFileEnsured` 与 `appendFileEnsured` 的 mkdir/open/write/fsync/rename/append 阶段;如果 command 已改 DB 但 journal/state 落盘失败,同一 command id 不再卡在 unresolved inflight,也不会被永久记为 rejected。`doctor --fix` 会修复 state/status/milestone 状态错位后允许重试继续派发。完整事务化内容哈希恢复仍按后续任务推进。
- 2026-07-06 00:30:51 +08:00 — 状态:已完成 doctor 投影重建止血。`workflow doctor` 现在会检查 workflow 本地可重建投影,缺失或空文件只作为 warning 暴露,不会阻断 resume;`doctor --fix` 会统一重建 progress、archive index、organization、reference library、requester/staff memory、consultation/intervention/standup 索引与 delivery summary,避免本地目录索引丢失后被误判为调度队列未 drain。已通过: `bun test test/workflow/company-flow.test.ts --test-name-pattern "doctor fix rebuilds missing workflow projection files" --timeout 120000`;doctor 组合回归;`bun typecheck`。真实 kill -9 外部进程金丝雀仍未关闭。
- 2026-07-06 01:46:52 +08:00 — 状态:已完成外部 kill 20 点幂等恢复验收。`kill-canary-child.ts` 的 retry 路径会先执行 `workflow doctor --fix` 再用同 command id 重试;父测试对 20 个写入故障点逐点强杀真实子进程,恢复后确认 doctor 可恢复问题归零、命令可幂等重试、图中最终里程碑可见。已通过: `bun test test/workflow/company-flow.test.ts --test-name-pattern "external process kill" --timeout 660000`。
