# 04 — 调度与流水线:失败策略、图修订 reconciliation、pipeline 模式

> 解决:失败依赖永久饿死(P1-3);补齐"agent 共同设计工程的流水线模式、
> 规划调度模式"所需的图语义。
> 主要改动面:`parse.ts`、`scheduler.ts`、`workflow.ts` 派发逻辑。

## 1. 失败依赖策略(P1-3)

现状:`readyMilestones`(scheduler.ts:8-11)只认 `approved/done/completed/skipped`
为依赖满足;依赖进入 `rejected/failed/cancelled` 后,下游永远 `pending`,
`nextWorkflowStatus` 也不会将其暴露为异常——静默停摆。

方案:

- **依赖进入终败态(failed/cancelled)或停留 rejected 超过返工上限时**,调度 tick
  将所有传递下游标记为 `blocked(reason: dependency_failed)`,并向 main_pm 发
  `report` 消息列出受影响子图;
- main_pm 的决策出口(全部是既有命令):
  - `force_skip` 失败依赖 → 下游解锁(接受缺口);
  - 重开依赖(`rejected → planning`,attempt+1)→ 下游回到 pending 等待;
  - `update_xml` 重构子图(如拆掉该依赖边);
  - `block` 整个 workflow 等 requester 决策;
- 返工上限:里程碑 `attempt` 超过阈值(默认 3,config 可调)时不再自动重开,
  强制升级 main_pm——防止 executor↔tester 无限对打消耗 token。

## 2. milestone 图语义扩展

### 2.1 `<pipeline>` 组

现有 `ordered`(串行屏障)与 `parallel`(并行屏障)之外,新增流水线组:
子里程碑按**条目**流动,前一阶段完成一个条目即可流入下一阶段,不等整批。

```xml
<pipeline items="work/shared/audit-tracks.json">
  <milestone id="audit" title="审计 {item}" department="engineering">…</milestone>
  <milestone id="review" title="复审 {item}" department="quality">…</milestone>
</pipeline>
```

- `items` 指向产物区的 JSON 数组文件(由上游里程碑产出——这正是"agent 设计
  流水线"的接口:部门 PM 生成条目清单,引擎据此展开);
- 引擎在 items 文件就绪时把组展开为 `audit@<item>` / `review@<item>` 实例
  里程碑,依赖为 `review@x ← audit@x`,不同 item 之间无依赖;
- ZirconEngine 场景对照:部门 PM 手工把 3 里程碑图重构成 16 个(1+13+2),
  未来只需产出 13 行的 tracks 清单,`update_xml` 都不必发。

### 2.2 里程碑属性扩展

```xml
<milestone id="…" department="…"
           executors="2"            <!-- 需要的 executor 实例数,默认 1 -->
           review="required|skip"   <!-- 小任务可跳过 reviewer 阶段 -->
           test="required|skip"
           timeout="120m">          <!-- 覆盖 02-§7 的停滞阈值 -->
```

parse.ts 校验属性合法性;`review=skip` 时状态机直接 `executing → testing`
(转换表相应放行,见 02-§3)。

## 3. `update_xml` 的 reconciliation(图修订对账)

现状风险:运行中 update_xml 后,已派发里程碑与新图的关系未定义。明确规则:

| 旧图里程碑 | 新图中 | 处理 |
|-----------|--------|------|
| 任意状态 | 同 ID 存在,prompt/deps 未变 | 状态原样保留 |
| pending/planning | 同 ID 存在,内容变化 | 保留状态,新内容下一阶段生效;planning 中的负责人收到 `direct` 变更通知 |
| executing 及以后 | 同 ID 存在,内容变化 | 状态保留,**本 attempt 按旧 prompt 跑完**,变更通知负责人;若需立即生效由 PM 显式重开 |
| 任意状态 | 已删除 | 转 `cancelled`,运行中的 session 收到 interrupt;`work/milestones/<id>/` 保留(01-§6) |
| — | 新增 | 进入 pending,正常调度 |

对账结果(每项落在哪一行)作为 `update_xml` outcome 的 diff 报告返回(02-§6),
并写 `journal/events.jsonl`。

## 4. 人手调度(staffing)细化

现有:成员容量、active/paused、按角色模型白名单 + 权重 + 缓存亲和
(近期 73c631a6 / 3f20660f / 11f045bd 已实现)。补齐:

- **按部门绑定**:member 增加可选 `department` 字段;派发时优先匹配
  `milestone.department` 一致的成员,其次空闲成员,最后排队——避免
  engineering 里程碑落到 product 出身、上下文全无的成员;
- **排队可见**:里程碑 ready 但无可用成员时状态置 `pending` 附
  `waitingFor: staffing`,graph 与 views/progress.md 显式展示"等人",
  而不是与"等依赖"混在一起;等待超阈值 → 看门狗提醒 main_pm 扩编
  (`update_staffing`);
- **成员生命周期与记忆连续**:成员被 pause/裁撤后其 `work/staff/<member-id>.md`
  保留;扩编时新成员冷启动提示注入同角色前任的记忆文件路径(复用
  ZirconEngine 中已验证有效的记忆交接模式,03-§7)。

## 5. 调度模式(workflow 级)

`start` config 增加 `scheduling` 策略,供 requester/main_pm 规划:

- `eager`(默认):ready 即派发,受 staffing 限制;
- `staged`:每完成一个 `ordered` 组暂停,等 main_pm 显式 `resume`(高风险
  工程的阶段闸门);
- `economical`:同一时刻最多 N 个 executing 里程碑(控 token 燃烧率);
  N 进 config。

模式本身也可由 main_pm 运行中经命令切换(记 journal)。

## 6. 验收标准

- [x] 依赖 failed 后两个 tick 内下游全部转 blocked 且 main_pm 收到子图报告;
      `force_skip` 后下游进入 ready(测试 T-2,见 06);
- [x] attempt 超限的里程碑不再自动重开,main_pm 收到升级;
- [x] `<pipeline>` 组:13 条目清单展开为 26 个实例里程碑,item A 的 review
      在 item B 的 audit 完成前即可开始(无屏障断言);
- [x] 运行中 update_xml 删除一个 executing 里程碑:session 被中断、状态转
      cancelled、`work/milestones/<id>/` 内容完好;
- [x] `review=skip` 的里程碑状态轨迹为 pending→planning→executing→testing→…,
      无 reviewing 停留。

## 产出记录和时间

- 2026-07-04 08:16:40 +08:00 — 状态:已完成止血子任务。调度器的 ready milestone 启动数量不再取 department_pm/executor/expert 三者容量最小值,改为只由 department_pm 可用容量决定启动规划;executor/expert 容量继续在各自阶段分配。这修复了 executor/expert 忙碌时其它 ready 里程碑无法进入 PM 规划的停摆风险。
- 2026-07-04 08:16:40 +08:00 — 状态:未完成。失败依赖传播、attempt 上限、`<pipeline>`、reconciliation、`review=skip` 与调度模式仍按本篇后续任务继续。
- 2026-07-04 09:25:50 +08:00 — 状态:部分完成调度止血。调度器新增失败依赖传播:pending 里程碑若直接或传递依赖 failed/cancelled,会被标记为 blocked 并把 workflow 显式 blocked,避免下游长期 pending 饥饿;当依赖通过 skipped/done/approved/completed 重新满足时,无 session 的 blocked 下游会恢复 pending 并重新参与 ready 派发。已补 `dependencyBlockedMilestones` / `dependencyUnblockedMilestones` 单测。仍未完成:完整 T-2 company-flow 集成、attempt 上限策略、`<pipeline>`、reconciliation、`review=skip` 与 workflow 级调度模式。
- 2026-07-04 12:32:40 +08:00 — 状态:已完成 T-2 company-flow 集成。`milestone_status failed` 现在不会把整个 workflow 直接终止为 failed,而是保持 workflow 可调度、调用失败依赖传播,将下游 pending 里程碑标记 blocked 并给主 PM/runtime 留出处理入口;requester/main PM 通过 `force_skip` 后,被依赖阻塞且尚未绑定 session 的下游会重新进入 ready 派发并创建对应 department PM session。仍未完成:attempt 上限策略、`<pipeline>`、reconciliation、`review=skip` 与 workflow 级调度模式。
- 2026-07-04 14:11:35 +08:00 — 状态:已完成 attempt 上限止血。部门 PM、技术顾问、执行者连续缺失必要 handoff/completion 标记时不再无限 `pending→planning` 重试;第 3 次失败会把 milestone 标记为 `blocked` 并把 workflow block 到主 PM,错误中明确写出缺失的 required output 与需要人工 resume/force-skip。仍未完成:`<pipeline>`、运行中 update_xml reconciliation、`review=skip` 与 workflow 级调度模式。
- 2026-07-04 15:23:05 +08:00 — 状态:已完成并行员工占位止血。复核“多个 engineering/同职能 milestone 只触发一个 session”的现象后,发现并行 `workflow.milestone` job 会同时读取同一份成员忙闲快照,导致多个 milestone 都选中同一个尚未写入 session 的员工。现在 PM/executor/expert 分配通过 workflow 级成员占位队列串行化:每次选人前重新读取 milestone session,选中后立即写回占位,并行 ready milestone 会分配给不同可用员工。仍未完成:`<pipeline>`、运行中 update_xml reconciliation、`review=skip` 与 workflow 级调度模式。
- 2026-07-04 15:35:46 +08:00 — 状态:已完成 `review=skip` 止血。XML parser 现在允许并校验 `<milestone review="required|skip">`,DB/portable state 会保留该策略;调度器在 executor 完成后若读取到 `review="skip"` 会跳过 department PM functional review,直接把 milestone 标记 `approved` 并继续调度下游。仍未完成:`<pipeline>`、运行中 update_xml reconciliation、workflow 级调度模式。
- 2026-07-04 15:59:07 +08:00 — 状态:已完成运行中 update_xml 删除节点对账止血。新 XML 删除一个已绑定 session 的 milestone 时,该旧节点不再被写成 `skipped`,而是保留审计记录并转为 `cancelled`;如果旧节点仍处于 planning/executing/reviewing/running,会同步取消对应 `workflow.milestone` 后台 job,避免旧 job 醒来后继续写回状态或派发错误下游。删除后的新图会继续调度新的 ready milestone。仍未完成:`<pipeline>` 与 workflow 级调度模式。
- 2026-07-04 16:14:26 +08:00 — 状态:已完成 workflow 级调度模式止血。`start` 配置新增持久化 `scheduling` 字段,支持 `eager` 默认、`staged` 阶段闸门与 `economical.maxActive` 并发上限;`staged` 会在已有阶段完成后 block 到显式 resume,`economical` 会让 ready milestone 等待容量而不是误判为无可运行任务。看门狗也改为先把 orphan active milestone 重置 pending 并重新调度,达到 attempt 上限后才 block。仍未完成:`<pipeline>` 组展开、staffing department 绑定/等待可见与运行中命令切换 scheduling。
- 2026-07-04 16:25:10 +08:00 — 状态:已完成运行中调度模式切换止血。内置 `workflow` 工具与 `<opencode-workflow-control>` 新增 `scheduling` action,允许 requester/main PM 在不中断 XML 的情况下切换 `eager|staged|economical`,并持久化到 workflow 记录、写回 progress/state、触发调度;从 `staged` 切到 `eager` 会解除阶段闸门并派发已 ready 的下一阶段。仍未完成:`<pipeline>` 组展开与 staffing department 绑定/等待可见。
- 2026-07-04 16:51:52 +08:00 — 状态:部分完成 pipeline/staffing 止血。XML parser/schema 现在接受 `<pipeline>` 容器并按 ordered 阶段语义生成 DAG 依赖,避免 main PM 产出的 pipeline XML 被判非法;完整 `items` 文件驱动的实例展开仍未完成。员工选择改为优先匹配 milestone department/specialty,未使用过的预创建泛用员工会在首次派发时重定向为对应部门,并在 `workflowDispatchSummary` 中显示 ready 但因 department PM 容量或 economical 限制等待的节点。`workflow_message` runtime 也透传 `targetSpecialty`,但该通道仍只用于咨询/干预,不会伪装成 milestone 派发。仍未完成:真正的 `<pipeline items=...>` 展开、按节点持久 `waitingFor: staffing` 字段、完整 waiting 超阈值看门狗提醒。
- 2026-07-04 17:15:26 +08:00 — 状态:已完成 `<pipeline items>` 文件展开止血。`parseWorkflowXml` 新增可选 item reader;运行时解析 workflow.xml/update_xml/file watcher 时会从 workflow 目录内读取 `items` JSON 数组,把 `<pipeline items="...">` 展开为每个 item 一组 ordered stage,不同 item 之间无批量屏障。里程碑 id 默认生成 `stage@item-slug`,标题/部门/prompt 支持 `{item}`、`{item.id}`、`{item.title}`、`{item.department}`、`{index}` 占位替换,模板内部 `depends` 会映射到同 item 的实例。已补 parser 单测和 company-flow 集成测试。仍未完成:items 文件缺失时的增量等待/自动重展开、按节点持久 `waitingFor: staffing` 字段、完整 waiting 超阈值看门狗提醒。
- 2026-07-04 17:19:31 +08:00 — 状态:已完成 pipeline items watcher 止血。workflow-aware file watcher 现在会识别当前 workflow.xml 中引用的 `<pipeline items="...">` 文件;items JSON 文件变更时,运行时会重新读取 workflow.xml、重新展开 pipeline 实例并刷新 graph/progress,不再只对 workflow.xml 本身变更生效。仍未完成:items 文件缺失时的 pending/waiting 状态表达、按节点持久 `waitingFor: staffing` 字段、完整 waiting 超阈值看门狗提醒。
- 2026-07-04 17:26:59 +08:00 — 状态:已完成 pipeline items 等待语义止血。`workflow_milestone` 新增 `waiting_for` 字段/schema 映射,当 `<pipeline items>` 文件尚未产出时,解析器生成 `waitingFor=pipeline_items` 的占位 milestone 而不是报错或派发模板工作;调度器会跳过该等待节点,`/workflow status` 显示 `pipeline_items` 等待原因。items 文件出现后 file watcher 自动重读 workflow.xml 并用真实 item 实例替换占位节点。仍未完成:staffing/scheduling 等待原因的 DB 持久字段全面写入、等待超阈值看门狗提醒。
- 2026-07-04 17:40:20 +08:00 — 状态:已完成 staffing/scheduling 等待持久化止血。调度器现在会把 ready 但因 Department PM 容量不足的节点写成 `waitingFor=staffing`,把受 `economical.maxActive` 限制的节点写成 `waitingFor=scheduling`;容量恢复或节点被实际派发时会清空等待原因并继续启动对应 milestone。`updateStaffing` 更新公司人员配置后会立即重跑调度,避免“扩编后仍要手动继续”的停摆。仍未完成:等待超阈值看门狗提醒。
- 2026-07-04 18:15:35 +08:00 — 状态:已完成等待超阈值看门狗止血。`/workflow status` 与调度器现在会检查 `waitingFor=staffing|scheduling` 且超过 30 分钟未变化的 pending milestone,自动给 main PM 排队 temporary intervention,消息列出等待节点、等待原因、部门与标题,要求扩编/调整 scheduling/force-skip/block,并刷新 `time_updated` 节流避免重复刷屏。已通过相关 company-flow 定向回归与 `bun typecheck`。
- 2026-07-05 01:07:43 +08:00 — 状态:已完成自动 session 派发推进止血。自动运行的 PM/部门 PM session 如果在纠正输出或普通输出中给出 `<opencode-workflow-control action="plan_complete|resume|force_complete|milestone_status">`,归档阶段会立即进入统一控制面并请求调度,不再因为该消息被标记为 runtime-managed 而跳过普通 observer 后无声结束;这修复了“session 表示已关闭 gate/开始派发,但后续 ready milestone 没有启动”的停滞。
- 2026-07-05 04:17:52 +08:00 — 状态:已完成派发热路径降载与咨询回流止血。复核 “session 自己结束但没有派发事件” 后确认两类真实问题:一是 executor/PM 在咨询后继续输出了预期完成内容,但归档逻辑仍用咨询前的旧结果判断,导致后续 review/dispatch 不推进;二是每次 prompt 结束同步重建 reference/archive/organization 文档并同步通知 main PM,会让大型 workflow 的调度热路径变慢并放大 OOM 风险。现在咨询答复后的 source continuation 会作为本轮有效结果继续进入里程碑判断;main PM 通知改为独立 `workflow.notification` 后台 job;reference/archive/organization 刷新改为去重后台 `workflow.artifacts` job,避免阻塞 milestone 派发。
- 2026-07-05 04:34:13 +08:00 — 状态:已完成派发推测复核。复核附件中“raw `<opencode-workflow-message>` 是 documented dispatch channel、可以直接 prompt executor sessions”的推测后确认该路径是伪派发:workflow message 只允许咨询/通知,不会创建 milestone job、不会绑定 session、不会解除 ordered/parallel 依赖。调度仍以有效 `workflow.xml`/`update_xml`/`resume`/`plan_complete`/`force_complete` 为准。补充修正 dispatch correction expectation 为按当前角色判断,避免部门 PM 的纠正输出被 main PM 规则误判;同时复核同部门 parallel 行为:多个 engineering milestone 是否同时启动取决于 `departmentPM` 容量,容量不足时会进入 `waitingFor=staffing`,容量足够时会绑定不同 Department PM session。
- 2026-07-05 04:59:07 +08:00 — 状态:已完成派发卡顿复核与热路径补强。针对 agent “requirements 仍 planning、milestone_status 队列未 drain、需用 communication plane 让 Department PM 关闭”的推测再次复查:真实派发仍必须来自 scheduler/control/tool,`workflow-message` 只做咨询/通知/交接;同职能任务未继续通常来自 staffing/scheduling 容量、stale planning run、缺少真实 control 落账或热路径被同步文档刷新拖慢。现在 main PM 监督通知后台化、reference/archive/organization 刷新去重后台化,`plan_complete`/`milestone_status` gate override 会在 journal 落账后再调度,咨询 follow-up 会作为当前 session 有效输出参与期望判断;定向回归覆盖 raw message 伪派发、staged/economical/waiting、失败依赖、update_xml 删除 active 节点和 managed prompt control。
- 2026-07-05 06:02:06 +08:00 — 状态:已完成调度验收补强。blockWorkflow 现在在 PM session 尚未回写但 Main PM member 已存在时,仍会向 Main PM 落一条 delivered system report intervention,避免失败依赖或 attempt 超限只写 workflow.error 而主控侧不可见。补强 T-2/attempt/pipeline/reconciliation/review-skip 验收:失败依赖阻塞下游并给 main PM 子图报告,`force_skip` 后恢复派发;attempt 超限不再自动重开并给 main PM 升级;13 条 pipeline items 展开为 26 个 audit/review 实例且 `review@track-01` 可在其它 audit 未完成时 ready;运行中删 active milestone 后 cancelled 且下游继续;`review=skip` 不进入 reviewing。
- 2026-07-05 06:57:06 +08:00 — 状态:已完成并行派发 session 关联热路径止血。复核 13 个 audit track 被 requirements gate 阻塞的案例后确认:后台 `workflow.milestone` job 实际已启动,但 milestone→session 引用要等完整 `updateMilestone`、session parent/title 更新与 organization/progress 文档刷新串行完成,界面和后续判断会长时间显示“未派发”。现在 milestone session 引用使用轻量 `updateMilestoneSession` 立即落库并发布 node/graph 事件,成员占位队列内跳过 title/parent/organization 副作用,这些副作用移到占位完成后执行;13 个并行 audit 节点在 main PM gate override 后都能绑定 Department PM session。
- 2026-07-05 16:24:15 +08:00 — 状态:已完成图修订后派发回归修复。运行中 `update_xml` 删除 active milestone 时,旧节点保留审计并保持 `cancelled`;修订后的 implementation/verification 等新图节点直接进入调度。同步确认 `review="skip"` 路径会跳过 functional review、标记 approved 并继续派发下游,不会停在 reviewing。
- 2026-07-05 21:02:55 +08:00 — 状态:已完成 main PM stalled queue 到并行派发回归。新增复核场景:13 个 parallel audit track 受 requirements gate 阻塞,main PM idle 输出只声称 `milestone_status=approval` 未 drain 并夹带 raw executor assignment。运行时现在会把该输出确定性转成 main PM `force_complete`,写入 command journal 后触发 scheduler,13 个 audit milestone 全部绑定 Department PM session,不会停留在 pending/gated。
- 2026-07-06 01:19:11 +08:00 — 状态:已完成 PM 规划派发与 executor 容量解耦。复核“多个同职能 milestone 只触发少数 session”的现场后确认,上一版为避免 executor 超派而把 `availableStarts` 限制为 Department PM 与 executor 双容量最小值,导致 executor=1 时大量 ready audit 轨道长期 `waitingFor=staffing`、PM 计划也不启动。现在 ready milestone 是否进入 Department PM 规划只看 PM 容量;进入 executing 后先触发调度释放 PM,再等待 executor 空位。新增 `drains waiting parallel milestones after executor assignment frees the department PM` 回归,并复跑单 PM 排队和 13-track 派发组合。
