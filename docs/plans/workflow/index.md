# Workflow(公司化协作)完善计划 — 索引

> 位置:`docs/plans/workflow/`
> 范围:`packages/opencode/src/workflow/`(schema.ts / parse.ts / scheduler.ts / command.ts / workflow.sql.ts / workflow.ts)
> 依据:2026-07-04 对 ZirconEngine 实际 workflow 运行目录
> (`.opencode/workflows/20260704_064850_329_帮我review当前zircon_runtime的代码 …`)的事故复盘 + 源码审查。

## 1. 愿景

workflow 模式的目标形态是一个**虚拟公司**:

- **requester**(需求方)提出战略需求,可随时干预;
- **main_pm**(主 PM)拆解主要任务、规划里程碑图、监督全局;
- **department_pm**(部门 PM)把里程碑细化为可执行需求与验收标准;
- **executor**(执行者)实施;**reviewer** 功能复审;**tester** 按验收标准测试并把结果反馈给 PM;
- **expert**(技术顾问)在架构/风险决策时被咨询。

每个身份可有多个 agent 实例(现有 staffing 上限 12,`schema.ts:35`)。agent 之间通过
workflow 命令交流、更新自身状态、共同设计流水线与调度;所有工作状态落盘在一个
本地工程目录中,可随时恢复、可被人类阅读。

这套骨架**已经存在**(角色、staffing、模型白名单、咨询/干预/站会、portable state),
本计划解决的是"跑起来会卡死、会互相踩文件"的可靠性问题,以及协作原语不完整的问题。

## 2. 现状诊断(按严重度排序)

| # | 问题 | 证据 | 后果 |
|---|------|------|------|
| P0-1 | **控制面命令排队后永不应用**。命令经 `WorkflowToolCommandEvent` 总线异步发布(`command.ts:58`),agent 只得到 "Queued workflow command",没有应用结果、没有失败原因 | ZirconEngine 运行中 `requirements` 里程碑卡在 `planning`:主 PM 与部门 PM 共发出 7+ 次 `milestone_status done/approved`、多次 `resume`,全部 queued、无一生效,13 个下游里程碑全部饿死 50+ 分钟 | 整个公司停摆,agent 被迫用文件传话绕过引擎 |
| P0-2 | **通信面消息 queued 后不投递**。干预/咨询写入 DB 与 `interventions/*.md` 后停留在 `queued`,无投递循环、无超时 | 主 PM→部门 PM 的 interrupt、requester→主 PM 的 "忽视planning状态,直接继续到执行者" 均无人收到 | 升级通道也失效,死锁无法人工解开 |
| P1-1 | **引擎与 agent 对工程目录的写入权属不清**。引擎 `writeFileEnsured` 直接整文件覆盖(workflow.ts 写入辅助函数),图变更时的 folder reset 会清掉顶层文档 | 主 PM 的 `main-plan.md` 被 reset 冲掉后被迫手工重建;`progress.md` 写着 "requirements approved" 而里程碑表写着 `planning`(自相矛盾) | agent 产出物丢失;人类/agent 读到过期或矛盾的状态 |
| P1-2 | **同一 workflow 出现两个落盘目录**(`…帮你review…` 与 `…帮我review…`,同一 `wfl_` ID)。目录名取自用户中文原文(含空格),重命名/重建产生分叉 | 两个目录、11 分钟间隔、内容部分重叠 | 状态源头不唯一,恢复时可能选错 |
| P1-3 | **失败依赖导致永久饿死**。`readyMilestones` 仅认 `approved/done/completed/skipped` 为满足(`scheduler.ts:8-11`),依赖处于 `rejected/failed` 时下游永远 pending,且无人被通知 | 代码审查可直接推演 | 静默停摆 |
| P2-1 | **`update_xml` 提交时不校验**。畸形 XML(如里程碑正文里的字面 `<slug>` 占位符)提交被接受,解析期才报 "unsupported workflow element" | 主 PM 第一次图重构因此失败,靠自查猜出原因 | 试错成本高,且失败后无回滚保证 |
| P2-2 | **状态机无显式转换表**。里程碑状态多达 14 个且语义重叠(`done`/`completed`、`running`/`executing`,`schema.ts:95-110`);非法转换没有明确错误 | `planning → done` 是否合法无从得知,这正是 P0-1 卡死的温床 | 行为不可预测、不可解释 |
| P2-3 | **无超时/看门狗**。咨询 `pending`、干预 `queued`、里程碑停留同一状态,均可无限期持续,无升级机制 | ZirconEngine 运行 50+ 分钟无任何自动告警 | 只能靠人盯 |
| P3-1 | **落盘写入非原子、索引靠 append**。`writeFile` 直写目标路径;`standups/index.md` 等靠 `appendFile`,崩溃后索引与文件不一致 | 代码审查 | 崩溃后目录部分损坏 |
| P3-2 | **portable state 恢复非幂等**。`workflow-state.json` 仅 `version===1` 硬校验;恢复用 `onConflictDoNothing`,半途失败再恢复会残留旧消息 | workflow.ts 状态同步段 | 跨机迁移/灾后恢复不可靠 |

## 3. 设计总纲(三条铁律)

后续所有子计划共同遵守:

1. **DB 是唯一真相源;工程目录里凡是引擎生成的文件都是"投影",随时可以整体重建。**
   agent 与人类改变状态只能走命令,不能改投影文件;引擎重建投影不会碰 agent 产物区。
2. **每个路径有且只有一个写入者。** 目录契约把工程目录划为 引擎投影区 / agent 产物区 /
   仅追加日志区,引擎写文件前校验权属,agent 侧由工具拦截对投影区的写入。
3. **每条命令/消息都有确定的终态。** 命令同步返回 `applied` 或结构化拒绝原因;
   消息从 `queued` 起必然在有限时间内到达 `delivered/expired/failed`,超时自动升级。

## 4. 子计划索引

| 文档 | 主题 | 解决的问题 |
|------|------|-----------|
| [01-directory-contract.md](01-directory-contract.md) | 工程目录契约与写入权属 | P1-1, P1-2, P3-1 |
| [02-control-plane.md](02-control-plane.md) | 控制面:命令总线、显式状态机、看门狗 | P0-1, P2-1, P2-2, P2-3 |
| [03-collaboration-protocol.md](03-collaboration-protocol.md) | 协作面:信箱、咨询、干预、站会、状态更新 | P0-2, P2-3 |
| [04-scheduling-pipeline.md](04-scheduling-pipeline.md) | 调度与流水线模式、图修订、失败策略 | P1-3 |
| [05-persistence-recovery.md](05-persistence-recovery.md) | 持久化、portable state、恢复与自检 | P3-2 |
| [06-testing-rollout.md](06-testing-rollout.md) | 验收测试与分阶段落地 | 全部 |

## 5. 路线图

- **Phase 0(止血,先于一切)**:控制面命令改同步应用 + 显式状态转换表 + `update_xml`
  提交时校验。目标:ZirconEngine 卡死场景在回归测试中可复现且被修复
  (见 06 的 T-0 用例)。
- **Phase 1(目录契约)**:目录改用 `wfl_` ID 命名、manifest 权属声明、原子写、
  废除 folder reset(改为图修订历史)。
- **Phase 2(协作协议)**:统一消息原语与投递循环、超时升级、`status_update`/`standup`
  命令,agent 不再需要手写 organization/progress。
- **Phase 3(调度增强)**:失败依赖策略、`<pipeline>` 组、requester 强制推进命令、
  图修订 reconciliation。
- **Phase 4(恢复与自检)**:portable state v2(迁移、幂等恢复)、`workflow doctor`。

各 Phase 的验收标准见 [06-testing-rollout.md](06-testing-rollout.md)。

## 产出记录和时间

- 2026-07-04 08:16:40 +08:00 — 状态:Phase 0 止血推进中。已落地命令确认回传、GlobalBus 桥接、命令错误显式返回、里程碑旧 job 取消、调度 PM 容量修正、workflow 目录 ID 化等子任务。尚未完成完整同步 RPC 入口、结构化 rejection.code、显式转换表、journal、权限矩阵、看门狗、目录契约 v2 与 doctor。
- 2026-07-05 06:57:06 +08:00 — 状态:Phase 0/3 派发止血继续推进。复核 agent 附件后确认 raw `<opencode-workflow-message>` 仍不是派发通道;真实派发以 workflow control/tool/XML scheduler 与 command journal/result 为准。已补并行 13 audit track gate close 后全部绑定 Department PM session 的回归,并将 milestone session 引用落账从重型文档刷新热路径中拆出。完整 Phase 4 恢复仲裁、统一 message 表和剩余 UI/全量金丝雀仍按分篇继续推进。
- 2026-07-05 16:24:15 +08:00 — 状态:Phase 0/1/3 派发链路继续加固。已修复新建 workflow manifest/doctor 竞态与 `update_xml` 后误走 resume recovery 的派发回归;当前结论仍是 raw `<opencode-workflow-message>` 不能派发 session,所有真实派发必须落到 workflow tool/control/XML scheduler 与 command journal/result。剩余 Phase 4 恢复仲裁、统一 message 表、UI 金丝雀与完整 commit/push 仍继续。
- 2026-07-05 16:54:53 +08:00 — 状态:Phase 0 派发声明兜底继续加固。主 PM session idle 时如果只声称 “queued/dispatch/assignment” 而没有真实 workflow control/update,现在会被自动纠偏到控制面;纠偏成功会落 `journal/commands.jsonl` 并继续派发,纠偏失败才 block,避免 session 自己结束后没有任何派发事件。剩余 Phase 4 恢复/自检与完整提交推送继续推进。
- 2026-07-05 17:05:08 +08:00 — 状态:Phase 4 轻量快照验收推进。修正 workflow state sidecar 读取路径,确保状态转换后 5 秒内主快照刷新且保持小于 50KB,完整 session 消息进入 `state/sessions/<sessionID>.json`,恢复证据不再因底层消息读取上下文丢失而空洞。剩余随机 kill 注入、事务化内容哈希恢复与完整提交推送继续推进。
- 2026-07-05 17:09:04 +08:00 — 状态:Phase 4 删库恢复继续调度验收推进。已补整套 workflow DB 行与 session 行删除后的目录恢复回归,确认恢复后模型缓存亲和仍在,并能继续通过控制命令派发下游 session。剩余随机 kill 注入、事务化内容哈希恢复与完整提交推送继续推进。
- 2026-07-05 17:22:03 +08:00 — 状态:Phase 4 doctor 综合归零验收推进。已补同一 workflow 多类可恢复目录/日志/孤儿问题的 `doctor --fix` 一次性修复回归,并修正 orphaned 留档目录不再被恢复扫描重新导入。剩余随机 kill 注入、事务化内容哈希恢复与完整提交推送继续推进。
- 2026-07-05 17:35:35 +08:00 — 状态:Phase 0 派发推测继续加固。根据最新附件复核 Department PM “队列不 drain 后改用 workflow-message 派发”的路径,补齐 idle 钩子回归:部门 PM 结束时若只有 queued/stalled/first-wave 伪派发声明,会被纠偏到真实 `plan_complete` 控制面并重新调度,不会创建伪 executor consultation。剩余 Phase 4 恢复/自检、统一 message 表与完整提交推送继续。
- 2026-07-05 18:21:46 +08:00 — 状态:Phase 0/4 派发恢复继续加固。根据附件和真实 workflow 目录复核,旧 v1 “queued 但不 drain” 不是仍有 FIFO 队列等待消费,而是旧工具调用只留在 session 历史、没有 command journal;现在恢复流程会从旧 session tool part 中重放仍相关的 queued 控制命令,写入 journal 后再调度,避免 requirements planning gate 因迁移缺口永久挡住下游。剩余随机 kill 注入、事务化内容哈希恢复、统一 message 表与完整提交推送继续。
- 2026-07-05 18:36:56 +08:00 — 状态:Phase 4 v1 迁移验收推进。补强 legacy v1 快照恢复:成员、里程碑、session 索引数量保持一致,同义状态规范化,旧 inline message 既恢复到底层 message/part 表,也在 session 恢复提示中给出消息数量和短摘录,方便从 UI/session 直接查证。剩余随机 kill 注入、事务化内容哈希恢复、统一 message 表与完整提交推送继续。
- 2026-07-05 18:53:20 +08:00 — 状态:Phase 0/4 派发恢复继续加固。根据 agent 对 stale queue/FIFO 的新推测复核,确认还有一个真实半迁移缺口:无关 rejected command journal 会让旧 v1 queued gate-close 恢复提前退出。现在恢复按 command id 去重并继续重放缺失的相关 queued `milestone_status`,可把 requirements gate 从旧 session 历史恢复到真实 command journal 并继续派发。剩余随机 kill 注入、事务化内容哈希恢复、统一 message 表与完整提交推送继续。
- 2026-07-05 19:09:40 +08:00 — 状态:Phase 4 内容一致性继续加固。session sidecar 已有稳定内容哈希锚点,doctor 能识别合法 JSON 但内容与 `workflow-state.json` 索引不一致的损坏,恢复路径也不会静默吞入不匹配 sidecar。剩余随机 kill 注入、逐表内容哈希 upsert、统一 message 表与完整提交推送继续。
- 2026-07-05 19:18:53 +08:00 — 状态:Phase 4 doctor fix 闭环继续加固。session sidecar hash mismatch 不只会阻断 resume,也能由 `workflow doctor --fix` 基于当前 DB 状态重建快照和 sidecar 后归零,避免用户被提示修复却无法恢复。剩余随机 kill 注入、逐表内容哈希 upsert、统一 message 表与完整提交推送继续。
- 2026-07-05 19:26:48 +08:00 — 状态:Phase 4 内容驱动恢复继续加固。恢复事务现在会 upsert 既有 Session 行,让 project-local 快照能纠正半恢复遗留的 stale session title/metadata/model/path 等核心字段。剩余随机 kill 注入、更细粒度逐表内容哈希 upsert、统一 message 表与完整提交推送继续。
- 2026-07-05 19:45:31 +08:00 — 状态:Phase 0 派发旁路继续加固。根据 agent “raw workflow-message 是派发通道、直接给 executor assignment” 的推测再查,修复 requester raw XML 旁路:requester→executor/reviewer/tester/department_pm 的 assignment 不再作为咨询投递,而是记录伪派发 reject 并转入 requester direct-execution override,由 command journal + scheduler 创建真实 milestone session。剩余随机 kill 注入、统一 message 表、UI 金丝雀与完整提交推送继续。
- 2026-07-05 20:23:20 +08:00 — 状态:Phase 4 内容驱动恢复继续加固。恢复扫描现在对 session message/part、durable message 与 context epoch 追加稳定内容哈希比较,不会再因为 DB 行时间戳较新就跳过内容错误的半恢复残留;新增 stale part 回归证明 project-local sidecar 可覆盖同 ID 错误 DB 行。剩余随机 kill 注入、统一 message 表、UI 金丝雀与完整提交推送继续。
- 2026-07-05 21:02:55 +08:00 — 状态:Phase 0 派发控制继续加固。根据最新 agent 附件复核,确认 raw workflow-message 仍不是派发通道;新增确定性 fallback:PM/requester/部门 PM 已完成 planning gate 却只声称 queued/stalled/first-wave dispatch 时,运行时直接落真实 `force_complete`/`plan_complete` 并调度。同步修复 main PM member 未写回 `workflow.pm_session_id` 导致 idle hook 漏处理的问题。剩余随机 kill 注入、统一 message 表、UI 金丝雀与完整提交推送继续。
- 2026-07-05 21:36:34 +08:00 — 状态:Phase 0/4 派发恢复继续加固。根据 agent 对 stale FIFO/owner acknowledgement 的推测复核真实 v1 workflow,确认旧工具调用确实只以 `Queued workflow command` 留在 session 历史且没有 command journal;新增 resume 前定向磁盘同步,让用户点击继续时也能恢复该 workflow 的 legacy queued `milestone_status` 并派发下游,不再要求重启服务或等待 file watcher。剩余随机 kill 注入、统一 message 表、UI 金丝雀与完整提交推送继续。
- 2026-07-05 22:14:58 +08:00 — 状态:Phase 4 派发/持久化恢复继续加固。根据 agent 对“queue stalled / command 不 drain”的推测继续下钻,修复 command 持久化失败后 inflight 卡死或永久 rejected 的恢复缺口;新增 20 点写入故障注入回归,证明 doctor 修复后同 command id 可重试并继续派发。剩余真实外部 kill -9 金丝雀、统一 message 表、UI 金丝雀与完整提交推送继续。
- 2026-07-05 22:45:12 +08:00 — 状态:Phase 2 协作协议继续推进。新增兼容式 `workflow_message` 表,把 consultation、intervention、handoff 的 send/deliver/ack/answer/expire/escalate 生命周期同步为同一索引,并在 state restore 时从旧表重建,为后续读取侧迁移和消息图谱统一打基础。剩余读取侧完全切换、standup 机制化、真实外部 kill -9 金丝雀、UI 金丝雀与完整提交推送继续。
- 2026-07-05 23:36:14 +08:00 — 状态:Phase 2 standup/派发监督继续加固。根据 agent 对 “control plane queue 不 drain 后 raw workflow-message 直接派发 executor” 的推测复核,确认真实派发回归已覆盖并通过;本轮修复的是 standup 控制应用的自取消竞态,避免监督 session 输出 block 后被当前 standup job 自己取消而静默失效。剩余读取侧完全切换、真实外部 kill -9 金丝雀、UI 金丝雀与完整提交推送继续。
- 2026-07-05 23:49:02 +08:00 — 状态:Phase 2 消息状态一致性继续加固。根据 runtime delivery 回归复核,修复前台 `workflow_message send` 用 queued 结果覆盖后台 delivered 的竞态,避免协作面统一消息表与 graph 状态不一致后被误判为“派发没有发生”;同时 standup ack 保持 `kind=standup`。剩余读取侧完全切换、真实外部 kill -9 金丝雀、UI 金丝雀与完整提交推送继续。
- 2026-07-06 00:00:15 +08:00 — 状态:Phase 2 统一消息读取侧继续加固。`workflow_message` 工具的 inbox/answer/ack 已优先使用统一 `workflow_message` 表,旧 consultation/intervention 表降为补漏兼容;这样恢复/迁移只重建统一表时,目标 session 仍能看到并闭合协作消息。剩余真实外部 kill -9 金丝雀、UI 金丝雀与完整提交推送继续。
- 2026-07-06 00:14:30 +08:00 — 状态:Phase 0/2 派发与协作边界继续加固。根据最新 agent 推测复查后确认:真实控制命令会同步命中 runtime dispatcher,main PM `milestone_status=approval` 或 stalled queue 文案可落 `force_complete` 并调度;新增修复保证 managed session 自动纠偏成功后不会再把原始 raw `<opencode-workflow-message>` executor assignment 投递成 consultation,防止“真实 milestone 派发”和“伪直达 executor”并存。剩余真实外部 kill -9 金丝雀、UI 金丝雀与完整提交推送继续。
- 2026-07-06 00:30:51 +08:00 — 状态:Phase 4 目录投影恢复继续加固。新建 workflow 会预生成完整本地参考/进度/组织投影;doctor 现在能报告缺失/空投影并在 `--fix` 中重建,让本地索引丢失不再表现为“workflow 状态已经写了但 UI/目录没有跟上”。剩余真实外部 kill -9 金丝雀、UI 金丝雀与完整提交推送继续。
- 2026-07-06 01:19:11 +08:00 — 状态:Phase 0/3 派发容量继续加固。根据 agent 对“不 drain 后 raw workflow-message 派发 first wave”的推测复核,真实派发仍以控制面/journal/scheduler 为准;本轮修复 PM 规划派发被 executor 容量误限的问题,使 Department PM 能继续接收 ready 轨道,executor 忙时由 milestone 等待空位而非 block workflow。剩余真实外部 kill -9 多点压测、UI 金丝雀与完整提交推送继续。
- 2026-07-06 02:04:45 +08:00 — 状态:Workflow 计划验收已闭合。真实外部 kill -9 多点压测扩展为 20 个写入故障点并通过;派发容量/伪派发纠偏组合复跑通过;SDK 重新生成、`packages/opencode`/`packages/app`/`packages/desktop` 类型检查均通过。后续仅剩按本次目标提交并推送。
