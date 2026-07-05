# 02 — 控制面可靠性:命令总线、显式状态机、看门狗

> 解决:命令排队后永不应用(P0-1)、`update_xml` 提交时不校验(P2-1)、
> 状态机无转换表(P2-2)、无超时看门狗(P2-3)。
> 主要改动面:`command.ts`、`scheduler.ts`、`workflow.ts` 命令处理段、agent 工具定义。

这是整个计划的最高优先级。ZirconEngine 事故的直接死因:部门 PM 完成需求章程后发出
`milestone_status done`,主 PM 随后 7+ 次发出 `milestone_status approved/done` 与
`resume`,所有命令均返回 "Queued workflow command" 后消失——没有应用、没有拒绝、
没有原因,13 个下游里程碑饿死 50+ 分钟,两位 PM 只能靠写 markdown 互相留言。

## 1. 病根

`command.ts:58` 把命令定义为总线事件:

```ts
export const WorkflowToolCommandEvent = BusEvent.define("workflow.tool.command", WorkflowToolCommand)
```

即 fire-and-forget:工具把命令发布到 bus 就向 agent 报告"已排队",应用与否取决于
另一端是否有订阅者活着、以及处理是否成功;`WorkflowToolCommandResult` 虽然定义了
`applied: boolean`,但结果以另一个事件发布,agent 的工具调用早已返回,拿不到它。
任何一环脱扣(订阅 fiber 未启动、处理抛错、状态转换被静默丢弃)对 agent 都表现为
同一个症状:永远 queued。

## 2. 方案 A(采纳):命令改同步 RPC 语义

**agent 工具调用 = 直接调用 `Workflow.Service` 方法,同步拿到应用后的结果或结构化拒绝。**

```ts
// 新增:统一命令入口(取代事件发布路径)
dispatchCommand: (cmd: WorkflowToolCommand) => Effect<WorkflowCommandOutcome, Workflow.Error>

export const WorkflowCommandOutcome = Schema.Struct({
  id: Schema.String,                 // 命令 ID(幂等键,见 §4)
  applied: Schema.Boolean,
  // applied=false 时必填:
  rejection: Schema.optional(Schema.Struct({
    code: Schema.Literals([
      "illegal_transition",          // 附 from/to 与该状态下的合法转换列表
      "not_authorized",              // 该角色无权执行此命令(见 §5)
      "invalid_xml",                 // 附解析错误与行列号(见 §6)
      "unknown_milestone",
      "workflow_not_active",
      "precondition_failed",         // 附具体前置条件说明
    ]),
    reason: Schema.String,           // 人话,直接可读
    allowedTransitions: Schema.optional(Schema.Array(Schema.String)),
  })),
  // applied=true 时:应用后的快照,agent 无需再发一次 status 确认
  workflowStatus: Schema.optional(WorkflowStatus),
  milestone: Schema.optional(Schema.Struct({ id: WorkflowMilestoneID, status: WorkflowMilestoneStatus })),
})
```

要点:

- 命令在调用方的 Effect 内**同步走完** 验证 → 状态转换 → DB 落库 → journal 记账,
  然后返回。调度副作用(派发下一个里程碑、发送消息)仍可异步,但**状态本身的
  改变绝不异步**;
- bus 事件保留,但降级为**通知**(`workflow.command.applied`),供 TUI/App 刷新,
  不再承载语义;
- 工具返回文案禁止出现 "queued"。要么 `已应用,requirements → done,已解锁 13 个里程碑`,
  要么 `已拒绝(illegal_transition):planning 状态不能直接标记 done,
  允许的转换:executing、blocked、cancelled;若计划已完成请先 …`。
  agent 的下一步动作完全由这条结果驱动,不需要猜。

并发控制:同一 workflow 的命令处理串行化(与 01-§5 的每 workflow 互斥锁同一把),
两个 agent 同时发命令时按到达顺序应用,后到者基于新状态验证——天然避免
"读旧状态-写覆盖"竞态。

## 3. 显式状态机

现有 14 个里程碑状态(`schema.ts:95-110`)先收敛再立表:

- **合并同义状态**:`completed → done`,`running → executing`(保留旧值解析兼容,
  写入侧只产出规范值;portable state v2 迁移时统一改写);
- **转换表成为代码中的唯一权威**(`scheduler.ts` 旁新增 `transitions.ts`):

```
pending    → planning | skipped | cancelled
planning   → executing | blocked | cancelled          // 部门 PM 计划完成 → 派发执行
executing  → reviewing | blocked | failed | cancelled
reviewing  → testing | rejected | cancelled
testing    → approved | rejected | cancelled
rejected   → planning | executing | skipped | cancelled   // 返工;attempt+1
approved   → done                                     // 主 PM/requester 验收
blocked    → planning | executing | cancelled          // 恢复到阻塞前阶段
done | skipped | failed | cancelled → (终态)
```

- 特权转换单列:`force_complete`(任意非终态 → done)与 `force_skip`
  (任意非终态 → skipped)仅 requester 与 main_pm 可用——这正是事故中
  requester 那句"忽视 planning 状态,直接继续到执行者"应有的合法出口,
  当时它连被投递的机会都没有;
- 所有转换尝试(成功与拒绝)写入 `journal/commands.jsonl`,含
  `{seq, id, ts, source: {sessionID, role}, action, from, to, outcome}`。

事故场景对照:部门 PM 在 `planning` 发 `milestone_status done` —— 新逻辑下要么
按表拒绝并明确告知"先转 executing 或走 force_complete",要么(推荐)为 PM 角色
提供语义命令 `plan_complete`,由引擎代为执行 `planning → executing` 的合法推进。
无论哪种,agent 在**一次工具调用内**得到确定答案。

## 4. 幂等与重试

- `WorkflowToolCommand.id`(现已有,`command.ts:26`)升级为强制幂等键:
  相同 id 重复提交返回首次的 outcome,不重复应用——agent 超时重发不再有副作用;
- agent 工具描述中明确:重试必须复用同一 id。

## 5. 角色权限矩阵

命令入口统一后,顺势收紧授权(现状:`update_xml` 名义上仅 PM,其余命令无校验):

| 命令 | requester | main_pm | department_pm | executor | reviewer | tester | expert |
|------|:---:|:---:|:---:|:---:|:---:|:---:|:---:|
| status / workflow_status / milestone_status(查询) | ✓ | ✓ | ✓ | ✓ | ✓ | ✓ | ✓ |
| milestone 状态转换(按表) | — | ✓ | 本部门 | 本人里程碑(限 executing→reviewing) | 本人复审(reviewing→testing/rejected) | 本人测试(testing→approved/rejected) | — |
| update_xml | — | ✓ | ✓(限本部门里程碑增删改) | — | — | — | — |
| resume / block | ✓ | ✓ | 本部门 | — | — | — | — |
| force_complete / force_skip | ✓ | ✓ | — | — | — | — | — |
| update_staffing | ✓ | ✓ | — | — | — | — | — |

越权返回 `not_authorized`,并在拒绝里说明"该命令需 X 角色,你可以向 X 发消息申请"
——把 agent 引向 03 的协作通道而不是留它撞墙。

## 6. `update_xml` 提交时校验

事故:主 PM 提交的 XML 里程碑正文含字面 `<slug>` 占位符,提交被接受、解析期才报
"unsupported workflow element"。

- `dispatchCommand` 内先跑完整 `parseWorkflowXml`(结构、唯一 ID、依赖存在、无环,
  parse.ts 已具备)再落库;失败返回 `invalid_xml` + 解析器原始信息 + 行列号;
- 追加提交前 **diff 报告**随 outcome 返回:新增/删除/修改了哪些里程碑、
  哪些运行中的里程碑受影响(reconciliation 规则见 04-§3),让 PM 在下一 turn
  确认结果符合意图;
- 正文中需要字面 XML 的场景在 parse 层支持 CDATA,并在工具描述中给出写法示例。

## 7. 调度器心跳与看门狗

命令同步化解决"命令丢失",看门狗解决"没人发命令"的静默停摆:

- **心跳**:每个活跃 workflow 的调度 fiber 周期 tick(建议 30s),每次 tick:
  重算 `readyMilestones` 并派发、驱动消息投递(03-§3)、写一条
  `journal/events.jsonl` 心跳(仅状态变化时落盘,避免刷屏);
- **停滞检测**:里程碑在同一非终态停留超过阈值(按状态配置,如 planning 30min、
  executing 120min)→ 生成一条发给 main_pm 的系统 standup 消息
  (走 03 的消息原语):"milestone X 已在 planning 停留 47min,负责人 …,
  最近一条 journal 记录 …",由主 PM 决定催办/重派/阻塞;
- **孤儿检测**:tick 时发现 `executing` 里程碑的 BackgroundJob 已消亡(崩溃/重启)
  → 自动置 `blocked` 并通知 main_pm,而不是让状态永远挂在 executing;
- 阈值可在 `start` 的 config 中按 workflow 覆盖。

## 8. 验收标准

- [x] 复现测试 T-0(见 06):`planning` 状态下发 `milestone_status done`,
      一次调用内得到明确拒绝 + 合法转换列表;按提示走 `plan_complete` 或
      `force_complete` 后,13 个依赖它的里程碑在下一个 tick 全部进入 ready;
- [x] 所有命令路径无 "queued" 语义;`applied=false` 必有 `rejection.code`;
- [x] 同一幂等键重复提交 100 次,状态转换恰好应用一次;
- [x] 两个 agent 并发对同一里程碑发冲突命令,后到者收到基于新状态的拒绝,
      DB 无中间态;
- [x] 含 `<slug>` 字面量的 XML 提交,同步返回 `invalid_xml` 并给出行列号;
- [x] kill 掉 executor 的后台 job,两个 tick 内 main_pm 收到孤儿告警并排队重试;
      重试超限后里程碑转 blocked。

## 产出记录和时间

- 2026-07-04 08:16:40 +08:00 — 状态:已完成止血子任务。workflow 工具命令新增命令 ID 与结果事件,工具调用会等待运行时确认 applied/failed/unconfirmed,不再把状态变更报告成无结果的 queued;运行时命令处理错误会回传失败消息,不再被静默吞掉。
- 2026-07-04 08:16:40 +08:00 — 状态:已完成止血子任务。增加 GlobalBus 桥接与幂等去重,避免 ToolRegistry 与 Workflow runtime 使用不同 Bus 实例时命令发布后无人消费;同一命令 ID 只应用一次。
- 2026-07-04 08:16:40 +08:00 — 状态:已完成止血子任务。`milestone_status` 在改状态前会取消该里程碑仍在运行的旧后台 job,避免旧 planning/executing attempt 结束后把手工推进状态写回去。
- 2026-07-04 08:16:40 +08:00 — 状态:部分完成。`update_xml` 仍会先解析校验再保存并返回确认结果;完整的结构化 rejection.code、显式转换表、角色权限矩阵、journal/commands.jsonl 与看门狗尚未完成。
- 2026-07-04 08:57:53 +08:00 — 状态:已完成止血验证。确认 agent 推测中的“raw `<opencode-workflow-message>` 是派发通道”不成立:该标签当前只进入咨询/通知解析,不会创建 `workflow.milestone` job、不会把 session 绑定到 milestone、不会改变 milestone 状态。已在主 PM、部门 PM、自动 continuation 与 workflow reference 提示词中明确要求:真实派发必须通过 workflow.xml / `<opencode-workflow-update>` 加 workflow 工具 `update_xml`/`resume` 的确认结果。
- 2026-07-04 09:16:40 +08:00 — 状态:部分完成。workflow 工具命令新增显式里程碑转换表与结构化拒绝结果:`milestone_status` 现在会拒绝 `planning -> done` 这类非法跳转并返回 `illegal_transition` + `allowedTransitions`;新增 `plan_complete`、`force_complete`、`force_skip` 用于计划 gate 完成与 requester/main PM override;未知 session 不再被默认当作 requester 授权。仍未完成:命令入口彻底同步化、journal/commands.jsonl、完整角色权限矩阵、幂等结果持久化与看门狗。
- 2026-07-04 09:57:56 +08:00 — 状态:已完成控制面审计止血。workflow tool 命令结果现在会写入 `journal/commands.jsonl`,记录命令 id、来源 session/role、action、前后 workflow/milestone 状态、outcome 与 rejection,用于区分“未消费”“被拒绝”“已应用”。同时修复非法 `milestone_status` 在校验前取消 milestone job 的问题:现在只有通过权限和状态转换校验后才会取消旧 job 并应用新状态,避免 agent 误发 `planning -> done` 把正在运行的部门 PM/执行 job 杀掉后又收到拒绝。
- 2026-07-04 10:05:51 +08:00 — 状态:已完成幂等止血。workflow tool 命令按 `id` 缓存首次执行结果,重复提交同一 id 会重新发布首次 outcome,不再静默丢弃;并发重复提交会等待首个命令完成后回放同一结果。缓存只在当前 runtime 进程内生效,跨重启持久化幂等仍需后续落盘实现。
- 2026-07-04 10:12:30 +08:00 — 状态:已完成幂等恢复止血。runtime 收到带 `id` 的 workflow tool 命令时会先读取 `journal/commands.jsonl`;如果该 id 已有历史 outcome,会回放历史结果并且不再重复应用状态转换。这样 runtime 重启后 agent 复用同一命令 id 重试,也不会把已落账的命令再次执行。更完整的事务化 journal/WAL 与 doctor 对账仍归 05 后续实现。
- 2026-07-04 10:24:12 +08:00 — 状态:已完成并发止血。workflow tool 状态命令现在按 workflowID 串行执行,避免两个 agent 同时读旧状态再覆盖写入;同一 workflow 的冲突命令会按实际到达顺序应用,后处理者基于已更新状态校验并返回结构化拒绝。同时补齐 `invalid_xml` 与 `precondition_failed` 的结构化 rejection,避免 `applied=false` 只有散文消息。
- 2026-07-04 10:32:39 +08:00 — 状态:已完成 `plan_complete` 语义修正。`plan_complete` 现在只确认 planning gate 并请求继续 executor 派发,不再把 milestone 直接置为 `done`,也不会取消仍在运行的 milestone job;如果 planning gate 已无活跃 job,会把该 milestone 复位为 `pending` 并触发调度,避免 PM session 自称“已派发”但执行链被跳过。
- 2026-07-04 10:41:47 +08:00 — 状态:已完成命令/派发可观测性止血。`/workflow status` 现在会从 `journal/commands.jsonl` 汇总最近命令 outcome、rejection code、前后 workflow/milestone 状态与简短原因,并显示当前 ready milestones、active milestones、各职能 busy/limit,让 agent 能直接区分 `illegal_transition`、`not_authorized`、`invalid_xml`、职能容量耗尽、依赖未满足与真正未确认的运行时问题,不再把被拒绝命令误判为 FIFO 队列未消费。
- 2026-07-04 11:41:01 +08:00 — 状态:已完成派发通道误用止血。复核 agent 推测后确认 `<opencode-workflow-message>` 仍被模型误当成“executor 派发通道”;运行时现在会检测 main PM/department PM 对 executor/reviewer/tester/department PM 发出的 assignment/dispatch/wave 类消息,要求改用 `update_xml`/`resume`/`plan_complete` 等真实控制面,并在仍未修正时显式 block,避免消息被咨询系统吞掉后 agent 误报已派发。
- 2026-07-04 12:00:15 +08:00 — 状态:已完成孤儿派发 watchdog 止血。active milestone 如果处于 planning/executing/reviewing 但已经没有对应 running `workflow.milestone` 后台 job,运行时不再把它静默复位为 pending;现在会将受影响 milestone 标记为 blocked,workflow 标记为 blocked,错误明确写出 "Workflow watchdog" 与缺失后台 job,并用 runtime/scheduler 问题提示主 PM,避免被误判为产品澄清或 XML 需求问题。
- 2026-07-04 12:32:40 +08:00 — 状态:已完成派发队列复核止血。针对 agent “控制面不 drain、尝试用 `<opencode-workflow-message>` 派发 executor” 的推测,确认真实问题不是消息标签缺派发能力,而是启动恢复路径未遵守 `OPENCODE_WORKFLOW_AUTORUN=0`,会在测试/受控场景中重新拉起活跃 workflow 并污染状态。`resumeActiveWorkflowsOnStartup` 现在同样受 autorun 开关保护,避免被恢复任务误判为用户命令队列仍在漂移。
- 2026-07-04 12:47:16 +08:00 — 状态:已完成同步工具入口止血。内置 `workflow` 工具现在通过当前 workflow runtime 注册的 dispatcher 直接调用 `Workflow.Service.dispatchCommand`,同步返回 applied/rejected 和结构化原因;不再把 agent 工具调用发布到 bus 后等待结果事件。GlobalBus/bus 事件路径保留为外部通知兼容入口,但 agent 自身不再依赖“事件被谁消费”才能推进状态。
- 2026-07-04 13:07:20 +08:00 — 状态:已完成工具层伪派发拦截。复核 agent 最新推测后确认 `workflow_message send` 也可能被误用成 “Your assignment / first wave dispatch / 直接继续到执行者”;工具现在在写入 consultation/intervention 前直接拒绝这类 PM/requester→executor/reviewer/tester/department_pm 的派发文案,并提示必须改用 `workflow` 工具的 `update_xml`、`resume` 或 `plan_complete` 且确认 `applied=true`。同时收紧 workflow reference 提示:只有咨询消息会 prompt 目标员工,且仍不等于 milestone 派发。
- 2026-07-04 13:45:23 +08:00 — 状态:已完成派发误判诊断止血。复核 agent “milestone_status 队列不 drain”推测后确认根因是 `planning -> done/approved` 非法转换,现已在工具拒绝信息和 PM 提示词中明确要求改用 `plan_complete` 或 requester/main_pm `force_complete`,避免误判成控制面卡死或转向 `workflow_message` 伪派发。
- 2026-07-04 14:43:06 +08:00 — 状态:已完成 session 输出控制面兜底。复核 agent 最新推测后确认 raw `<opencode-workflow-message>` 不是派发通道,自然不会创建 executor session;同时发现 session 只能“描述 queued/started dispatch”但没有工具调用时,`plan_complete`/`force_complete` 等控制动作不会进入命令总线。现新增 `<opencode-workflow-control action="plan_complete|force_complete|force_skip|milestone_status" milestone="...">...</opencode-workflow-control>` 解析路径,由 workflow observer 转成同一套 workflow tool command,经过权限/转换表/journal/调度,避免 session 自称已派发但运行时无事件。
- 2026-07-05 08:11:27 +08:00 — 状态:已完成孤儿 watchdog 验收修正。active milestone 的 `workflow.milestone` 后台 job 消亡时,watchdog 首次会把 milestone 复位 pending 并在 graph/intervention 中给 main PM 落一条 `queued retry` 系统报告;同一 milestone attempt 超过限制后才把 milestone 与 workflow 标记 blocked,错误明确指向 scheduler/runtime dispatch problem。已通过: `bun test test/workflow/company-flow.test.ts --test-name-pattern "retries orphaned active milestone sessions before blocking the workflow" --timeout 90000`;`bun typecheck`。
- 2026-07-04 15:03:17 +08:00 — 状态:已完成 raw 协作入口兜底。复核附件中 “raw `<opencode-workflow-message>` blocks directed at executor roles” 的推测后确认这仍不是派发通道;运行时现在在咨询解析入口先检测 main PM/department PM 对 executor/reviewer/tester/department PM 的 assignment/dispatch/wave 类 raw 消息,拒绝并写入 `journal/messages.jsonl`,不再创建 consultation/intervention、不会唤醒目标 session,避免 raw XML 绕过 `workflow_message send` 工具层拦截。
- 2026-07-04 16:37:50 +08:00 — 状态:已完成 owning PM 状态兼容止血。复核 agent “milestone_status=approved/done 不 drain、要求 Department PM 自己关闭 requirements” 的推测后确认:此前 requester/main PM 的 `planning -> done` 拒绝是正确的,但 owning Department PM 常会把计划 gate 完成表达成 `milestone_status=done/approved`,导致看起来像队列卡住。现在只有绑定在该 milestone 上的 Department PM session 可把 `planning` 状态下的 `milestone_status=done|approved` 兼容解释为 `plan_complete`;非 owner session 仍走非法转换拒绝,避免错误强关 gate。
- 2026-07-04 17:04:36 +08:00 — 状态:已完成主控 gate override 止血。复核附件中 “主 PM 已反复 queued milestone_status=approved/done 但 requirements 仍 planning” 的推测后确认:不是 FIFO 队列未 drain,而是此前 requester/main PM 对 `planning -> approved/done` 仍走非法转换,容易让 agent 误判成运行时卡死并转向 workflow-message 伪派发。现在 requester/main_pm 在 planning gate 上发送 `milestone_status=approved|done` 会被兼容解释为 `force_complete`,取消 stale active milestone job、写入 command journal 并请求继续调度;executor 等非授权角色仍被拒绝。
- 2026-07-04 18:04:03 +08:00 — 状态:已完成伪派发声明兜底。复核附件中 “我已用 documented communication channel 派发 executor,但没有 pickup” 的推测后确认:session 可能不写 raw `<opencode-workflow-message>` 或 workflow tool,只用自然语言声称 “queued/assignment/route/dispatch”。workflow observer 现在会在 PM/部门 PM 输出中优先识别这类无控制结果的派发声明,跳过普通缺失输出续写,改为要求产出真实 `update_xml`/`resume`/`plan_complete`/`force_complete` 控制结果;若仍没有有效控制或咨询,workflow 会显式 block 为 dispatch claim,避免下游 milestone 永远不创建 session 而 agent 误报已派发。
- 2026-07-04 18:25:15 +08:00 — 状态:已完成 invalid XML 行列诊断止血。workflow XML parser 现在会在不支持的结构元素上附带 `line`/`column`;里程碑正文中的 `<slug>`/`<generic>` 仍按字面量保留,但 `<slug>` 作为 ordered/parallel 下的 workflow step 会同步返回 `invalid_xml`,reason 包含 `unsupported workflow element <slug> at line ..., column ...`。已通过 parser 全量回归和 GlobalBus `update_xml` 结构化拒绝回归。
- 2026-07-04 18:39:15 +08:00 — 状态:已完成控制 XML 状态别名止血。复核 agent 输出中的 `milestone_status=approval` 后确认:模型常把 `approved` 写成 `approval/approve/accepted`,旧解析会丢弃该属性,导致控制 XML 没有有效 milestoneStatus。现在 `<opencode-workflow-control>` 会把 `approval|approve|accepted` 归一为 `approved`,`complete|finished` 归一为 `done`,`in_progress` 归一为 `executing`;同时如果控制 XML 被命令总线拒绝,observer 会把 workflow 显式 block 并写明 rejection,不再让 session 自称已关闭 gate 但运行时无后续派发。
- 2026-07-04 19:26:43 +08:00 — 状态:已完成控制面声明拦截补强。复核 agent 最新推测后确认:真实 `<opencode-workflow-control action="milestone_status" status="approval">` 已能触发 main_pm/requester gate override,但模型也会只用自然语言说“queued milestone_status/resume, queue has not drained / still planning / stalled”。这类输出以前可能绕过伪派发检测并让 session 正常结束;现在 PM/部门 PM 只要声称控制面命令已排队但未 drain/transition,且没有真实 workflow update/control 结果,就会被识别为未确认控制面声明,要求继续产出 `update_xml`/`resume`/`plan_complete`/`force_complete` 或显式 block。
- 2026-07-04 20:07:54 +08:00 — 状态:已完成运行时派发声明兜底。复核 agent 附件中“raw `<opencode-workflow-message>` 是 documented dispatch channel,会 prompt executor sessions”的推测后确认该说法不成立;现在 runtime-managed prompt 输出也会在 archive 阶段拦截 PM/部门 PM 的 assignment/dispatch/wave 类声明,先写入 `journal/messages.jsonl` reject,并跳过咨询投递。这样即使声明发生在自动 `notifyMainPM`/部门 PM runPrompt 内,也不会把 `workflow_message` 误当成真实 milestone 派发。
- 2026-07-04 20:26:10 +08:00 — 状态:已完成派发误用落账兜底。复核 agent 附件中“我用 raw `<opencode-workflow-message>` 直接 assign first wave,但没有任何派发事件”的现象后确认:这不是控制面队列不 drain,而是 workflow session 观察路径会先进入派发声明纠正流程,导致 raw message 误用未必立即写入 `journal/messages.jsonl`。现在 PM/部门 PM 输出这类 raw assignment/dispatch/wave 消息时,在纠正前先落一条 `action=reject`,明确说明 workflow-message 只用于 consultation/notification,真实派发必须使用 workflow control/tool 的 `update_xml`、`resume`、`plan_complete` 或 `force_complete` 并确认 applied。
- 2026-07-04 21:09:47 +08:00 — 状态:已完成派发推测复核诊断。复核 agent “milestone_status/dispatch queue 没有 drain、已经发出咨询消息但没有派发事件”的推测后确认:真实派发路径必须有 `journal/commands.jsonl` 或运行时控制结果可查;没有 journal 的 active workflow 不能再被解释成“可能只是异步未消费”。`workflow doctor` 现在会报告缺失 command/message journal 与 stale intervention,把“无落账派发声明”转为可见诊断。
- 2026-07-04 21:24:52 +08:00 — 状态:已完成 requester 直接执行 override 止血。复核 ZirconEngine 实例后确认 `忽视planning状态,直接继续到执行者` 只是 queued main PM intervention,没有 `journal/commands.jsonl`,所以不会关闭 `requirements` planning gate。现在 requester 对 main PM 发出“忽视/跳过 planning、直接进入执行者/后续执行”的干预时,运行时会同步转为 requester `force_complete` 关闭所有 active planning gate、取消 stale milestone job、写入 command journal,并立即请求调度下游 milestone。
- 2026-07-05 00:45:38 +08:00 — 状态:已完成派发落账顺序止血。复核 agent “control-plane queue 没 drain、已经尝试派发但没有事件”的推测后确认一个真实窗口:部分控制命令会先更新状态/启动调度,再异步写 `journal/commands.jsonl`,导致观察者可能看到状态或后续 session,却查不到命令落账。现在 `milestone_status`、`plan_complete`、`complete` 未完成分支、`scheduling`、`resume`、`update_xml`、`workflow_status` 的调度副作用都延后到 command journal 写入和结果发布之后执行;重复命令等待者也在 journal 落盘后才收到回放结果,避免“applied without audit”的派发黑洞。
- 2026-07-05 01:07:43 +08:00 — 状态:已完成自动 session 控制闭环止血。复核 agent “我已 re-issue milestone_status/plan_complete,但 requirements 仍 planning 且无派发事件”的推测后确认:runtime-managed prompt 输出会被 `workflowManagedMessageKeys` 跳过普通 observer,此前 archive 阶段只处理 workflow update 与咨询,不会把 `<opencode-workflow-control>` 转成真实 workflow tool command。现在自动 session 归档时同样解析 `resume/block/plan_complete/force_complete/milestone_status` 控制 XML,通过统一 command bus 写入 `journal/commands.jsonl`、发布结果并触发调度;含控制输出时不再误走普通咨询投递。
- 2026-07-05 01:52:08 +08:00 — 状态:已完成计划文件兜底控制。复核 agent 附件和 ZirconEngine workflow 目录后确认:requirements `plan.md` 已写出 “queued milestone_status=done + resume”,但没有 `journal/commands.jsonl`,所以不是队列未 drain,而是自然语言排队声明没有进入控制面。workflow-aware file watcher 现在会识别 `<milestone>/plan.md` 的 Handoff Summary 与 “queued milestone_status=done/approved” 完成声明,对有下游依赖的 planning gate 以 requester/main PM `force_complete` 落账并触发调度;普通部门 PM 计划仍走 `plan_complete`。真实命令仍写入 journal,避免再次出现“文件说已派发、运行时无事件”。
- 2026-07-05 02:41:38 +08:00 — 状态:已完成主控 `plan_complete` stale run 恢复。复核 agent “control-plane queue 不 drain,改用 raw workflow-message 派发 first wave” 的推测后确认:workflow-message 仍不是派发通道;真实风险是 requester/main PM 已确认 planning gate 后,旧的 active department PM run 仍会让 `plan_complete` 返回 applied 但不推进。现在 Department PM 自己的 active run 仍保持原 run 继续,但 requester/main PM 的 `plan_complete` 会取消 stale active planning run、复位 milestone 为 pending、写入 command journal 并请求重新调度 executor 派发。
- 2026-07-05 04:17:52 +08:00 — 状态:已完成咨询控制面复核止血。复核 agent “raw `<opencode-workflow-message>` 是 documented dispatch channel、可以直接 assign/prompt executor”的推测后再次确认:该通道只用于 consultation/notification,真实派发必须由 workflow control/tool、XML 图更新或 scheduler-created milestone job 产生。修复一个真实控制面缺口:被咨询员工回复里同时包含 `<opencode-workflow-control>` 与新的咨询请求时,现在不会因先识别 control 而跳过咨询展开;内部咨询答复也不会被 `runPrompt` 自动重复消费控制 XML,而是由调用方按来源 job/session 显式应用并落账。这样 main PM 咨询答复可可靠 block/resume,普通 main PM 监督/ requester 干预也能先处理咨询再应用控制结果。
- 2026-07-05 04:59:07 +08:00 — 状态:已完成派发推测再审计。复核 agent 对 “control-plane FIFO/stale command/owner acknowledgement” 的推测后确认:有 journal/result 的 workflow control 才是派发事实,raw `<opencode-workflow-message>` 与自然语言 “queued/started dispatch” 均不能关闭 gate 或创建子 session。控制面继续保留 requester/main_pm 对 planning gate 的 `milestone_status=approved|done` force_complete 兼容、owning Department PM 的 `milestone_status=done|approved` plan_complete 兼容,并通过 `exceptJobID` 数组避免后台 main PM notification 在应用控制命令时误杀来源 milestone job。重型 main PM 通知与文档索引刷新移出命令热路径,降低 “命令已应用但后续调度迟迟不跑” 的假象。
- 2026-07-05 05:15:38 +08:00 — 状态:已完成附件派发误用精确复核。针对 agent 附件中完整的 “hammering control plane / proper mechanism is raw `<opencode-workflow-message>` / assigning first wave directly / should prompt executor sessions” 文本,确认它仍是伪派发而非真实调度。补充回归覆盖多条 raw workflow-message executor assignment 同时出现时必须被 `workflowDispatchClaimWithoutControl` 识别,运行时不得创建 executor milestone session 或 consultation,必须 block 并要求改走 workflow control/tool。
- 2026-07-05 05:31:37 +08:00 — 状态:已完成 stale active run 派发恢复。复核 agent “Department PM/owner 已 re-issue `milestone_status=done` 或 `plan_complete`,但 requirements 仍 planning 且后续不派发” 的推测后确认:旧逻辑只要发现同 milestone 有 running `workflow.milestone` job,就把 owning Department PM 的确认当成“当前 active run 会继续”,这会把 stale guard job/旧 runner 卡死误判成活跃当前 run。现在只有控制命令来自该当前 job(`exceptJobID`)时才保持原 run 继续;外部/后续会话的 Department PM `milestone_status` 或 `plan_complete` 会取消 stale active planning run、复位 milestone 为 pending、写入 command journal 并请求重新调度。
- 2026-07-05 05:38:15 +08:00 — 状态:已完成控制命令幂等与拒绝语义回归。新增 `replays one workflow tool command outcome for one hundred duplicate ids`,同一 `force_complete` 命令 ID 并发提交 100 次时只产生一条 command journal、milestone 状态只完成一次、所有调用回放同一 outcome。新增 `returns structured rejection codes instead of queued command semantics`,覆盖 `update_xml`、`milestone_status`、`plan_complete`、`force_complete`、`force_skip`、`status_update`、`scheduling`、`workflow_status` 与无关联 workflow 的拒绝路径,断言 `applied=false` 必带 `rejection.code` 且结果消息不再使用 queued 语义。
- 2026-07-05 06:13:08 +08:00 — 状态:已完成 `resume` 派发误用拦截。复核 agent “用 documented communication channel / resume message 把 first wave executor assignment 发出去即可派发”的推测后确认:即使命令 action 是 `resume`,message 字段也不能承载 “Your assignment / dispatch executor / prompt executor sessions” 这类伪派发。现在 main_pm/department_pm 的 `resume` 工具命令如果夹带派发文案会同步返回 `precondition_failed`,raw `<opencode-workflow-control action="resume">...</opencode-workflow-control>` 若夹带派发声明会显式 block,提示必须使用 `update_xml`、`plan_complete` 或 `force_complete` 等真实控制路径。
- 2026-07-05 06:57:06 +08:00 — 状态:已完成派发控制面再复核。针对附件中 “control plane 没 drain,改用 raw `<opencode-workflow-message>` 直接派发 first wave” 的推测,再次确认真实派发只能来自 workflow tool/control/XML scheduler 并以 command journal/result 为准;同时补强同一 workflow 的工具命令串行回归,覆盖两个互斥 terminal `milestone_status` 并发提交时只能一个 applied、另一个返回结构化拒绝,避免把拒绝误判为 FIFO 未消费。
- 2026-07-05 07:39:14 +08:00 — 状态:已完成 T-0 聚合验收回归。扩展 `main PM milestone_status approval drains a planning gate and dispatches thirteen audit tracks`:先把 requirements 固定在 `planning` 并挂一个保护中的 `workflow.milestone` job,由 executor 误发 `milestone_status=done` 时必须同步返回 `illegal_transition`、包含合法替代 `plan_complete/force_complete` 且不取消保护 job;随后 main PM 用 `milestone_status=approved` override 关闭 gate,保护 job 被取消,`journal/commands.jsonl` 记录 rejected→applied 的前后状态,13 个 parallel audit milestone 全部关联 Department PM session。已通过该定向回归与 `packages/opencode` 下 `bun typecheck`。
- 2026-07-05 07:46:07 +08:00 — 状态:已完成附件派发推测复核。确认 raw `<opencode-workflow-message>`、`workflow_message send` 和 `resume` message 都不是 milestone 派发通道;自然语言声称 “proper dispatch channel / first wave / prompt executor sessions” 会被识别为未确认派发并拒绝或要求改走真实控制面。同步确认 main PM `milestone_status=approved` 关闭 planning gate 后会写入 command journal 并派发 13 个并行 audit track。已通过定向回归: `detects role messages that try to act as milestone dispatch|does not treat workflow-message assignment text as milestone dispatch|rejects raw workflow-message dispatch before consultation delivery|rejects resume messages that try to carry executor assignments|main PM milestone_status approval drains a planning gate and dispatches thirteen audit tracks`。
- 2026-07-05 16:24:15 +08:00 — 状态:已完成 `update_xml` 控制面派发修复。workflow tool `update_xml` 在 XML 校验保存与 command journal 落账后直接运行调度,不再复用 resume recovery 路径;这样删除/取消的 active milestone 不会被 `recoverInterruptedProgress` 重新复位为 pending,修订图会立即按新 DAG 派发后续节点。
- 2026-07-05 16:54:53 +08:00 — 状态:已完成主 PM idle 伪派发纠偏。复核附件中 “raw `<opencode-workflow-message>` 是 proper dispatch channel” 的推测后确认:真实缺口是主 PM session 已经 idle 且只声称 queued/dispatch/assignment,但普通 message updated 观察事件可能已错过。`SessionStatus.Event.Idle` 的 main PM 分支现在同样检测 `workflowDispatchClaimWithoutControl`,自动要求补交真实 workflow update/control/consultation;若纠偏输出 `force_complete`/`plan_complete` 等控制 XML,会通过统一控制面落账并继续调度,否则显式 block。已通过定向回归与 `bun typecheck`。
- 2026-07-05 17:35:35 +08:00 — 状态:已完成 Department PM idle 派发推测回归。复核附件中 “milestone_status/resume 没 drain,所以改用 raw `<opencode-workflow-message>` first wave dispatch” 的完整路径后确认:真实派发仍必须落 `journal/commands.jsonl` 与 workflow control/tool;新增 `corrects department PM stalled queue claims from idle sessions`,模拟部门 PM session 已 idle、只声称 queued/stalled 并夹带 executor assignment 时,运行时会追问纠偏为 `<opencode-workflow-control action="plan_complete"...>`,写入 `plan_complete` command journal,重启该 milestone attempt,且不会创建 executor consultation。
- 2026-07-05 18:21:46 +08:00 — 状态:已完成旧 queued 控制命令恢复止血。复核真实 ZirconEngine v1 workflow 目录后确认:所谓 “control-plane queue 不 drain” 的关键残留场景是旧工具调用只把 `Queued workflow command` 写进 session 历史,没有 `journal/commands.jsonl`,因此现代 runtime 无命令可消费。恢复路径现在会在无 journal 高水位的 v1 快照中扫描旧 session tool part,提取 queued `milestone_status`/`plan_complete`/`force_complete`/`force_skip`,优先重放 requester/main PM 关闭 planning gate 的 `done/approved`,再处理 Department PM plan_complete 兼容命令,并通过真实 workflow tool command 写 journal、触发调度。
- 2026-07-05 18:53:20 +08:00 — 状态:已完成半迁移 queued 恢复止血。根据 agent 对 stale/FIFO 队列的推测继续复核,发现旧 v1 快照若已有无关 `commands.jsonl` highWater,此前会提前跳过 legacy queued command 恢复,导致关键 `milestone_status requirements done/approved` 永久留在 session 历史里。现在恢复逻辑按 command id 去重:journal 已记录的命令不重放,但缺失且仍相关的旧 queued gate-close 会继续进入真实控制面、写 journal 并触发下游派发。
- 2026-07-05 19:45:31 +08:00 — 状态:已完成 requester raw message 派发旁路止血。根据 agent “proper mechanism is raw `<opencode-workflow-message>` / direct executor assignment” 的新推测继续复核,发现工具版 `workflow_message send` 已拦截 requester/main_pm/department_pm 到 executor 的 assignment,但 raw XML 解析路径此前只拦截 main_pm/department_pm。现在 requester 在来源会话中写 raw executor assignment 不会被当作咨询投递,会先写 `journal/messages.jsonl` reject,再按 requester direct-execution override 走真实 `force_complete`/scheduler 路径,确保后续 session 由 milestone 调度创建并绑定。
- 2026-07-05 21:02:55 +08:00 — 状态:已完成 main PM 派发推测复核止血。根据 agent “control-plane queue 没 drain,改用 raw `<opencode-workflow-message>` first wave dispatch” 的附件继续检查,确认真实缺口不是 message 通道缺派发能力,而是 idle 纠偏过度依赖模型二次输出和新建 XML workflow 未把预创建 main PM 写回 `workflow.pm_session_id`。现在 workflow 创建后会把 main PM member 写入主记录;历史 workflow 的 idle hook 也会通过 `workflow_member.role=main_pm` 兜底识别主 PM。PM/requester/部门 PM 已完成 planning gate 却只声称 queued/stalled/dispatch/assignment 时,运行时可直接推断并落真实 `force_complete`/`plan_complete`,不再等待模型重新写对 `<opencode-workflow-control>`。
- 2026-07-05 21:36:34 +08:00 — 状态:已完成 resume 入口派发恢复止血。根据 agent 对 FIFO/stale queued command 的推测复核真实 v1 workflow 目录,确认旧 `Queued workflow command` 确实残留在 session 历史中;但当 DB 中已有 workflow 行时,点击继续只走 DB 当前状态,不会重新同步该 workflow 的旧 `workflow-state.json`,导致旧 `milestone_status=approved/done` 永远不落 command journal。现在 `workflow.resume` 会先按 workflowID 同步磁盘状态,恢复仍相关的 legacy queued 控制命令后再继续调度,避免 requirements planning gate 永久挡住下游。
- 2026-07-06 01:19:11 +08:00 — 状态:已完成派发推测二次复核。根据 agent “控制面不 drain,应通过 raw workflow-message 直接派发 executor”的附件再次检查,确认 `workflow-message` 仍只是咨询/通知,真实问题在调度器把 PM 规划派发与 executor 空闲错误耦合。现在 `schedule()` 只按 Department PM 容量启动 ready milestone;PM 计划完成并进入 executing 后立即重跑调度释放 PM;executor 忙时 milestone 保持执行态并等待可用 executor,不会把 workflow 误 block 为 “No executor”。已通过: `bun test test/workflow/company-flow.test.ts --test-name-pattern "drains waiting parallel milestones|queues parallel milestones|main PM milestone_status|stalled queue|workflow-message executor assignment|shows ready milestones waiting" --timeout 240000`;`bun typecheck`。
