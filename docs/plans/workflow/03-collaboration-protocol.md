# 03 — 协作面:统一消息原语、信箱投递、状态更新与站会

> 解决:干预/咨询 queued 后无人投递(P0-2)、无超时升级(P2-3),
> 并补齐"agent 通过 opencode 的方法互相协作"所缺的原语。
> 主要改动面:`schema.ts`(consultation/intervention 合并)、`workflow.ts` 消息处理段、
> agent 工具与系统提示词。

## 1. 病根与目标

现状有**三套**互不相通的通信机制:

1. 咨询 `WorkflowConsultation`(schema.ts:143)——agent↔agent 问答;
2. 干预 `WorkflowIntervention`(schema.ts:166)——requester/PM → 角色的指令;
3. 会话内 XML 标签(`<opencode-workflow-message>` 等)——由引擎从消息文本中解析。

三者各有自己的状态字段、落盘路径和(理论上的)投递逻辑,而事故中的表现完全一致:
**写入 DB、生成 `interventions/*.md`、状态停在 `queued`,从未到达目标 session**。
主 PM 给部门 PM 的催办、requester 给主 PM 的"忽视 planning 直接推进",都躺在
文件里无人知晓。

目标:一个消息原语、一个投递循环、一个可验证的生命周期。

## 2. 统一原语:WorkflowMessage

```ts
export const WorkflowMessageKind = Schema.Literals([
  "consult",        // 期望回答(原 consultation);answer 回填后通知发起者
  "direct",         // 指令/告知,期望 ack(原 intervention)
  "handoff",        // 里程碑交接:附产物路径清单(部门PM→executor,executor→reviewer…)
  "report",         // 结果上报:tester→PM 的测试结论、executor→PM 的完成汇报
  "standup",        // 状态摘要:成员→main_pm,或引擎看门狗生成(02-§7)
  "broadcast",      // main_pm/requester → 全员或某角色全体
])

export const WorkflowMessage = Schema.Struct({
  id: Schema.String,
  workflowID: WorkflowID,
  kind: WorkflowMessageKind,
  from: Schema.Struct({ sessionID: SessionID, role: WorkflowRole, memberID: Schema.optional(Schema.String) }),
  // 目标三选一:具体成员 / 某角色(引擎按负载挑成员)/ 角色全体(broadcast)
  to: Schema.Struct({
    memberID: Schema.optional(Schema.String),
    role: Schema.optional(WorkflowRole),
    all: Schema.optional(Schema.Boolean),
  }),
  milestoneID: Schema.optional(WorkflowMilestoneID),
  timing: WorkflowCommunicationTiming,     // after-task | interrupt | temporary-interrupt(沿用)
  body: Schema.String,
  attachments: Schema.optional(Schema.Array(Schema.String)),  // work/ 区相对路径
  replyTo: Schema.optional(Schema.String), // 回复串联
  deadline: Schema.optional(Schema.Number),// 超时时刻(见 §4)
  status: Schema.Literals(["queued", "delivered", "acked", "answered", "expired", "failed"]),
  time: Schema.Struct({ created: Schema.Number, delivered: Schema.optional(Schema.Number), closed: Schema.optional(Schema.Number) }),
})
```

现有 `WorkflowConsultation` / `WorkflowIntervention` 表迁移为 `WorkflowMessageTable`
的两个 kind;HTTP `intervene` 路由与 graph 结构保持兼容(graph 的
consultations/interventions 字段由 message 表按 kind 投影生成)。

## 3. 投递循环:消息必达或必超时

生命周期(每一步写 `journal/messages.jsonl`):

```
queued → delivered → acked / answered
   │         └─(deadline 前无 ack/answer)→ expired → 升级
   └─(目标成员不存在/已 paused 且无候补)→ failed → 通知发起者
```

投递机制,按 timing 分派,由 02-§7 的调度 tick 驱动(**这是现状缺失的那台发动机**):

- **interrupt**:目标 session 若正在运行,注入中断(复用 session abort +
  恢复上下文注入);若空闲,立即以新 turn 唤起,消息作为该 turn 的首条输入;
- **temporary-interrupt**:目标当前 turn 结束后、下一步开始前注入;
- **after-task**:目标当前里程碑阶段完成后注入;若目标空闲则等同立即投递。

投递的物理形式 = 在目标 session 注入一条结构化用户消息(带 `<workflow-inbox>`
包裹与消息 id),并在系统提示中约定:收到后必须调用 `workflow_message` 工具
`ack`(direct/handoff)或 `answer`(consult)。工具调用即闭环,引擎据此推进状态
——**判定送达的标准是对方 ack,而不是引擎自认为发出去了**。

文件视图:`inbox/` 下按成员生成投影(`inbox/<member-id>/index.md` + 消息文件),
供人类围观与 agent 冷启动时补课;真相在 DB,文件可重建(遵守 01 权属)。

## 4. 超时与升级链

每个 kind 有默认 deadline(可按消息覆盖):consult 30min、direct/handoff 15min、
report/standup 无(单向)。到期未闭环:

1. 状态置 `expired`,journal 记账;
2. 引擎生成一条升级消息:consult 超时 → 通知发起者"未获回答,可改派他人或升级
   expert";direct 超时 → 通知发起者的上级(executor 的上级是 department_pm,
   department_pm 的上级是 main_pm,main_pm 的上级是 requester);
3. 升级消息本身也有 deadline,最高升到 requester 后转为 workflow 级 `blocked`
   + TUI 醒目提示——**宁可显式阻塞等人,不可静默停摆**(事故的 50 分钟静默
   正是要消灭的对象)。

## 5. agent 状态自更新:status_update

用户目标之一:"agent 能更新自己状态"。现状 agent 只能手写 `organization.md`
(与引擎投影冲突,01 已禁止)。新增轻量命令(走 02 的 dispatchCommand):

```ts
{ action: "status_update", memberID, state: {
    availability: "idle" | "working" | "blocked_waiting",
    currentFocus: string,          // 一句话:在做什么
    blockers: string[],            // 卡在哪(空为无)
    progressNote: string,          // 本 turn 增量
}}
```

- 写入 member 表新字段,`views/organization.md` 与 graph 即时反映;
- 系统提示要求各角色在每个 turn 结束前调用一次(轻量、无副作用、可幂等);
- 看门狗(02-§7)把 `blocked_waiting` 超阈值的成员列入 standup 提醒。

## 6. 站会(standup)机制化

现状 `standups/` 仅有主 PM 手写的监督笔记。机制化为:

- **定时站会**:workflow config 可设周期(默认:无,由 PM 按需);触发时引擎向
  全体 active 成员发 `standup` 请求消息,成员以 `status_update` + 简短文字回应;
  引擎汇总生成 `views/standup_<n>.md` 投影并作为一条 `report` 投递给 main_pm;
- **事件站会**:看门狗停滞检测、孤儿检测(02-§7)自动生成;
- main_pm 读站会后的动作(催办、重派、block、update_xml)全部走既有命令,
  形成"检测 → 汇报 → 决策 → 命令"的闭环。

## 7. 员工记忆与交接(固化事故中自发形成的最佳实践)

事故中真正幸存的协作通道是 `reference/staff/*.md`:两位 PM 靠各自的记忆文件
完成了跨 turn、跨重置的交接。这个模式值得固化而不是替代:

- `work/staff/<member-id>.md` 为成员私有产物(01 权属),引擎永不触碰;
- 系统提示模板统一约定其结构:profile / 当前职责 / 交接摘要 / 决策日志;
- **handoff 消息强制附件**:里程碑阶段移交(plan→execute、execute→review、
  review→test)时,发起方必须在 handoff 消息的 attachments 中列出产物路径
  (plan.md、findings、review.md…),接收方冷启动提示词自动注入这些路径
  ——正式把"文件传话"升级为"消息索引文件"的双层协作:消息负责时序与通知,
  文件负责大体量内容。

## 8. 工具面(agent 可见的 API)

统一为两个工具,降低模型的选择负担:

- `workflow_command`:02 的控制面(状态查询/转换/update_xml/staffing/force_*);
- `workflow_message`:本篇的协作面(send / ack / answer / inbox 查询)。

会话内 XML 标签(`<opencode-workflow-consult>` 等)保留为**降级通道**(模型忘记
调工具时引擎仍能从文本解析),但解析成功后同样进入 message 表走统一生命周期,
并在下一 turn 提醒 agent 改用工具。

## 9. 验收标准

- [x] 复现测试 T-1(见 06):requester 发 "忽视 planning 直接推进" 的 direct 消息,
      main_pm 在其下一 turn 收到并 ack,journal 完整记录
      queued→delivered→acked 时间线;
- [x] consult 无人回答 30min 后发起者收到 expired 升级;升级链最终可达
      requester 并转 workflow blocked;
- [x] 三种 timing 的注入时机各有集成测试(interrupt 打断运行中 session、
      temporary-interrupt 在 turn 间隙、after-task 在阶段完成后);
- [x] `status_update` 后 graph 与 `views/organization.md` 五秒内反映新状态;
- [x] handoff 消息缺 attachments 时被拒绝(precondition_failed)。

## 产出记录和时间

- 2026-07-04 08:57:53 +08:00 — 状态:部分完成止血子任务。Requester session 中直接输入 `<opencode-workflow-message ...>` 时,不再一律包装成 main PM strategic intervention;运行时会先识别 workflow communication XML,直接路由到目标员工 session,记录 consultation 黄色边、归档 session、刷新 reference/archive/organization。该能力仍属于协作面,不是 milestone 派发面。
- 2026-07-04 08:57:53 +08:00 — 状态:未完成。统一 `workflow_message` 工具、消息生命周期表、ack/answer、超时升级、status_update、standup 机制化与 handoff attachments 仍按本篇后续任务继续。
- 2026-07-04 11:08:36 +08:00 — 状态:已完成 `status_update` 子任务。新增 workflow tool action=`status_update`,workflow employee session 可上报 `availability`/`currentFocus`/`blockers`/`progressNote`;成员表、`workflow-state.json`、`organization.md`、`progress.md` 与员工公司快照提示都会反映该状态。通用员工上下文已要求每个 workflow turn 开始/结束使用该工具,避免只在自然语言中声明阻塞或空闲。统一 `workflow_message` 工具、消息生命周期表、ack/answer、超时升级、standup 机制化与 handoff attachments 仍未完成。
- 2026-07-04 11:27:15 +08:00 — 状态:已完成 `workflow_message` 首个工具面子任务。新增内置 `workflow_message` 工具并注册到工具列表,支持 employee session 查询 inbox、用 `answer` 闭合 consultation、用 `ack`/`answer` 闭合 requester intervention,并写入 `journal/messages.jsonl`。员工上下文提示已要求每个 workflow turn 开始查询 inbox,且不得在工具确认前宣称咨询/干预已闭环。完整统一 message 表、send、queued→delivered→acked 全生命周期、超时升级、standup 机制化与 handoff attachments 仍未完成。
- 2026-07-04 11:41:01 +08:00 — 状态:已完成协作面误派发保护。`<opencode-workflow-message>` 保持为咨询/通知通道,但观察器会拦截 PM 角色把该通道写成 “Your assignment / first wave dispatch / 直接继续到执行者” 的情况;拦截后先自动追问该 session 输出真实 workflow 控制结果,仍未修正则 block 并记录明确原因。这样黄色咨询边不会再被误解为 milestone job 派发。
- 2026-07-04 12:57:52 +08:00 — 状态:已完成 `workflow_message send` 止血子任务。`workflow_message` 工具新增 `send`,可由 workflow 成员向具体 session 或角色发送 tracked consultation/intervention,写入现有 consultation/intervention 表与 `journal/messages.jsonl`,目标 session 的 `inbox` 能立即看到并继续用 `answer`/`ack` 闭环。完整统一 message 表、deadline/expired 升级、三种 timing 的真实注入与 handoff attachments 强校验仍未完成。
- 2026-07-04 13:07:20 +08:00 — 状态:已完成 `workflow_message send` 误用保护。`send` 保留为协作消息入口,但如果 requester/main PM/department PM 试图向部门 PM、执行者、reviewer 或 tester 发送带 assignment/dispatch/wave/直接派发语义的内容,工具会拒绝落库并返回明确说明:message 不能派发 milestone、不能绑定 session、不能解锁依赖;真实派发必须走 workflow control plane。完整 deadline/expired 升级、三种 timing 的真实注入与 handoff attachments 强校验仍未完成。
- 2026-07-04 13:30:23 +08:00 — 状态:已完成 intervention 真实投递桥接。复核 agent “raw `<opencode-workflow-message>` 是派发通道” 的推测后,确认协作消息不能替代 milestone 控制面;同时修复 `workflow_message send kind=intervention` 只写表不唤醒目标 session 的缺口:工具现在通过 workflow runtime dispatcher 复用 `intervene`/`deliverIntervention`,有运行时时会创建后台 `workflow.intervention` 投递任务并记录真实来源 session,无运行时时明确 `updated=false`。consultation 仍保持 inbox/answer 路径;完整统一 message 表、deadline/expired 升级与 handoff attachments 强校验仍未完成。
- 2026-07-04 13:36:02 +08:00 — 状态:已完成 intervention ack 闭环语义修正。`delivered` 现在只表示 runtime 已把干预送到目标 session;目标 `workflow_message inbox` 会继续显示 delivered intervention,直到目标 session 用 `ack`/`answer` 明确闭环为 `acked`。这避免“送达=已处理”的误判,也让 T-1 的 queued→delivered→acked 生命周期有可测试状态。完整 deadline/expired 升级、三种 timing 的精确注入与 handoff attachments 强校验仍未完成。
- 2026-07-04 14:03:22 +08:00 — 状态:已完成协作消息超时升级止血。`graph`、`workflow status` 与 `resume` 会扫描 30 分钟未闭环的 consultation/intervention:pending/queued/delivered/blocked 消息,把原消息标记 `expired`,写入 `journal/messages.jsonl`,并创建一条返回发起方的 requester/main_pm intervention 升级通知。这样咨询无人回答不再永久挂起,发起者会在 inbox 看到可重试/改派/阻塞的明确任务。完整三种 timing 精确注入与 handoff attachments 强校验仍未完成。
- 2026-07-04 15:13:13 +08:00 — 状态:已完成 handoff attachments 强校验止血。`workflow_message send kind=handoff` 现在是显式协作消息类型,必须携带非空 `attachments`,且附件必须是 workflow 相对路径、不能是绝对路径或 `..` 逃逸;运行时 dispatcher 也做同样防御。合法 handoff 复用 intervention 投递生命周期唤醒目标 session,消息正文与 `journal/messages.jsonl` 都记录附件清单。完整三种 timing 精确注入仍未完成。
- 2026-07-04 15:35:46 +08:00 — 状态:已完成派发误判二次止血。复核 agent “我已经用 raw `<opencode-workflow-message>` 直接派发第一波执行者” 的推测后,确认该通道只能做咨询/通知;调度观察器的纠偏现在不再把普通 `## Handoff Summary` 当成成功修复,必须看到真实 `workflow update/control` 或合法咨询后才继续,否则 block 并写明需要控制面派发。已通过 raw dispatch 误用回归。
- 2026-07-04 21:24:52 +08:00 — 状态:已完成 requester 干预控制化止血。复核真实 workflow 目录发现 requester 的“忽视 planning 直接继续到执行者”停留在 `queued/temporary-interrupt` intervention,没有转成 milestone 状态命令。现在这类 requester 明确流程指令会在 `intervene` 入口直接转换为控制面 `force_complete`,并把 intervention 标记为 `acked` 或 `failed` 附带响应,避免协作消息挂起导致下游审计/执行 milestone 永远不派发。
- 2026-07-05 02:20:24 +08:00 — 状态:已完成协作消息恢复止血。复核 agent “raw `<opencode-workflow-message>` 是 proper dispatch channel” 的推测后再次确认:message 通道只做咨询/通知/交接,不是 milestone 派发。补充 message journal WAL 重放后,咨询回答、干预 ack/deliver/expire 等生命周期即使只落入 `journal/messages.jsonl`、尚未刷新 `workflow-state.json`,恢复时也会回放到 DB 与 graph,避免协作面状态倒退成 pending/queued 并被误判为“派发事件没发生”。完整三种 timing 精确注入仍未完成。
- 2026-07-05 05:06:27 +08:00 — 状态:已补 timing 精确注入回归。新增 company-flow 覆盖同一个 active executor session 同时收到 `temporary-interrupt` 与 `after-task` 干预:前者在目标里程碑仍 `executing` 时立即投递并进入 `delivered`,后者保持 `queued`;当目标 milestone 变为 `approved` 后,`/workflow status` 会投递先前等待的 after-task 消息。既有 interrupt 回归继续覆盖 requester interrupt 会让 main PM block workflow。统一 message 表迁移与机制化 standup 仍按本篇后续任务推进。
- 2026-07-05 06:13:08 +08:00 — 状态:已补协作/控制边界回归。复核 agent 把 “direct executor assignment” 塞进 `resume` message 的推测后,确认协作文案不能隐藏在控制命令文本中变成派发事实;PM 角色的 `resume` 文本若包含 assignment/dispatch/wave 语义会被拒绝或阻塞,必须回到 workflow graph/control plane。
- 2026-07-05 07:31:31 +08:00 — 状态:已补 T-1 端到端回归。新增 requester→main_pm direct/intervention 测试,覆盖 "忽视 planning 直接推进" interrupt 消息会真实唤醒 main PM、保持 inbox 可见,main PM `ack` 后 graph 变为 `acked`,并断言 `journal/messages.jsonl` 记录 queued/send、delivered、acked 生命周期。同步修正 runtime send 返回状态:即使投递很快完成,`workflow_message send` 的 send 事件仍按 queued 记账,避免 agent 把 send 返回的 delivered 误解为目标已处理。协作消息仍不是 milestone 派发通道;真实派发继续走 workflow control/update/resume/plan_complete。
- 2026-07-05 07:52:59 +08:00 — 状态:已完成 handoff 附件拒绝验收。`workflow_message send kind=handoff` 缺少 attachments 或附件非法时,工具现在在 metadata 中明确返回 `status=precondition_failed`,输出也带 `precondition_failed` 与具体原因;合法 handoff 仍走原有投递和 journal 路径。已通过定向回归: `bun test test/workflow/company-flow.test.ts --test-name-pattern "workflow message tool sends tracked messages into target inbox" --timeout 90000`;`bun typecheck`。
- 2026-07-05 07:57:57 +08:00 — 状态:已完成 `status_update` 五秒可见性验收。扩展 `records workflow member status updates from employee sessions`,要求 graph 更新后 `organization.md` 在 5 秒内同步包含 `availability=blocked_waiting`、current focus、blocker 与 progress note;`progress.md` 同步断言继续保留。已通过: `bun test test/workflow/company-flow.test.ts --test-name-pattern "records workflow member status updates from employee sessions" --timeout 90000`;`bun typecheck`。
- 2026-07-05 08:16:27 +08:00 — 状态:已完成三种 timing 验收复核。`records requester direct intervention delivery and acknowledgement timeline` 覆盖 requester→main PM `interrupt` 投递、inbox 可见、ack 关闭与 messages journal 生命周期;`respects temporary-interrupt and after-task intervention timing for active target sessions` 覆盖 active executor 上 `temporary-interrupt` 立即 delivered、`after-task` 保持 queued 并在 milestone 完成后 delivered。已通过: `bun test test/workflow/company-flow.test.ts --test-name-pattern "records requester direct intervention delivery and acknowledgement timeline|respects temporary-interrupt and after-task intervention timing" --timeout 90000`;`bun typecheck`。
- 2026-07-05 08:20:20 +08:00 — 状态:已完成 expired 升级链验收。扩展 `expires stale workflow consultations and escalates back to the source session`,覆盖 consult 30 分钟未回答后原咨询转 `expired`、发起者 requester inbox 收到升级 intervention;若该 requester 升级 intervention 继续超时未 ack,运行时将 workflow blocked,错误明确说明 escalation reached requester and expired,需要人工方向。已通过: `bun test test/workflow/company-flow.test.ts --test-name-pattern "expires stale workflow consultations and escalates back to the source session" --timeout 90000`;`bun typecheck`。
- 2026-07-05 22:45:12 +08:00 — 状态:已完成统一 message 表兼容落地。新增 `workflow_message` 表作为 consultation/intervention/handoff 的统一生命周期索引;`workflow_message send`、consult answer、intervention/handoff ack、runtime deliver/expire/escalate 与 state restore 都会同步写入或重建该表。旧 consultation/intervention 表继续服务现有 graph/API,统一表先作为权威索引与迁移桥。已通过: `bun test test/workflow/company-flow.test.ts --test-name-pattern "workflow message tool sends tracked messages|workflow message intervention send uses runtime delivery" --timeout 120000`;`bun typecheck`。剩余读取侧完全切换与 standup 机制化继续推进。
- 2026-07-05 23:36:14 +08:00 — 状态:已完成 standup 机制化止血。公司 standup 现在会向 active 成员排队 `kind=standup` 的统一消息与 inbox 兼容 intervention,要求成员用 `status_update` + `workflow_message ack` 回报;同时修复 standup 自身输出 block/resume control 时被 `cancelWorkflowRuns` 取消自身 job 的竞态,改为外层带 `exceptJobID` 显式应用控制。已通过: `bun test test/workflow/company-flow.test.ts --test-name-pattern "stalled queue|workflow-message|milestone_status approval|company standup" --timeout 120000`;`bun typecheck`。剩余读取侧完全切换继续推进。
- 2026-07-05 23:49:02 +08:00 — 状态:已完成协作消息状态倒退止血。复核 runtime delivery 回归时发现 `workflow_message send` 在后台已投递完成后仍会以 `queued` 再写统一消息表,导致 graph 显示 delivered 但 message 表倒退为 queued。现在工具层 queued upsert 不会覆盖 delivered/acked/failed 等更晚状态,且 standup ack 会保留 `kind=standup`。已通过: `bun test test/workflow/company-flow.test.ts --test-name-pattern "workflow message intervention send uses runtime delivery" --timeout 120000`;`bun test test/workflow/company-flow.test.ts --test-name-pattern "company standup|workflow message tool sends tracked messages|workflow message intervention send uses runtime delivery" --timeout 120000`;`bun test test/workflow/company-flow.test.ts --test-name-pattern "stalled queue|workflow-message|milestone_status approval|company standup" --timeout 120000`;`bun typecheck`。
- 2026-07-06 00:00:15 +08:00 — 状态:已完成统一 `workflow_message` 读取侧切换止血。`workflow_message inbox` 现在优先读取统一消息表,旧 consultation/intervention 表只作为 legacy fallback;`resolveWorkflow` 也可通过统一消息表识别所属 workflow。`answer`/`ack` 在旧表行缺失但统一表仍存在时可直接闭合消息,避免恢复或迁移后出现“统一表有记录但目标 session 看不到/无法处理”的断层。已通过: `bun test test/workflow/company-flow.test.ts --test-name-pattern "workflow message tool sends tracked messages into target inbox" --timeout 120000`;`bun test test/workflow/company-flow.test.ts --test-name-pattern "company standup|workflow message tool sends tracked messages|workflow message intervention send uses runtime delivery" --timeout 120000`;`bun typecheck`。
- 2026-07-06 00:14:30 +08:00 — 状态:已完成 managed session 伪派发投递止血。复核 agent “control plane 没 drain,改用 raw `<opencode-workflow-message>` 指派第一波 executor” 的推测后,确认真实控制面已能把 main PM/department PM stalled queue 文本转成 `force_complete`/`plan_complete` 并调度;本轮修复的是自动纠偏成功后原始 executor assignment 不再继续作为 consultation 投递,只记录 rejected message runtime journal,避免同一输出同时产生真实 milestone session 与伪直达 executor。已通过: `bun test test/workflow/company-flow.test.ts --test-name-pattern "converts main PM stalled queue fake dispatch" --timeout 120000`;`bun test test/workflow/company-flow.test.ts --test-name-pattern "stalled queue|workflow-message executor assignment|main PM milestone_status approval|manager milestone_status closes a gate" --timeout 180000`;`bun typecheck`。
