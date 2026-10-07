# realtime-task-scheduler-go

一个由**虚拟时钟**驱动的抢占式实时调度内核实验台（Go）。支持固定脚本任务、
周期任务、互斥锁（含优先级继承）、截止时间/预算监控，以及**运行期控制指令**：
调度器跑起来之后，可以按虚拟时刻从外部下发中断、挂起/恢复、调整优先级、
动态加入任务等指令，把内核当成可交互的实验台使用。

## 虚拟时钟模型

虚拟时钟只在两种情况下推进：

1. 任务执行 `OpCompute`（每步 1 tick）；
2. 系统完全空闲时，跳转到下一个待处理时刻（事件或控制指令的生效时刻）。

调度开销、控制指令的下发与处理都**不推进**时钟。同一场景重复运行，
日志与指令回执完全一致（确定性可重放）。

## 运行期控制指令

通过 `Scheduler.Submit(cmd)` 下发，可在 `Run`/`RunUntil` 执行前或执行中
（包括从其它 goroutine 并发）提交：

```go
s.Submit(sched.Command{Tick: 3, Kind: sched.CmdInterrupt, TaskID: 1})
s.Submit(sched.Command{Tick: 6, Kind: sched.CmdSetPriority, TaskID: 2, Priority: 9})
```

| Kind           | 语义                                                         |
|----------------|--------------------------------------------------------------|
| `CmdInterrupt` | 中断：让睡眠中的任务立即就绪（提前结束睡眠）                  |
| `CmdSuspend`   | 挂起任务（就绪/运行/阻塞中均可；等锁任务留在锁等待队列中）    |
| `CmdResume`    | 恢复被挂起的任务，按挂起前的阻塞原因回到正确状态              |
| `CmdSetPriority` | 运行中调整基础优先级，继承与等待队列立即刷新                |
| `CmdAddTask`   | 运行中加入新任务（`Period>0` 时按周期任务注册，立即首次释放） |

### 生效时刻与同一 tick 的处理顺序

- 指令在虚拟时钟到达 `cmd.Tick` 后的下一个指令处理点生效；`Tick` 不晚于
  当前时刻时立即（在当前 tick）生效。每条指令的实际生效时刻记录在回执
  `Receipt.Tick` 中。
- 同一虚拟时刻内的处理顺序固定为：
  **内核事件（周期释放、睡眠唤醒）→ 控制指令（按提交顺序）→ 指令产生的事件
  （如运行期加入周期任务的首次释放）→ 抢占判定 → 调度分派**。
- 指令与事件撞在同一 tick 时不丢、不重复：睡眠任务被中断提前唤醒后，
  原唤醒事件到期会自动成为无操作。

### 回执与可区分的拒绝

每条指令产生一条 `Receipt`（`Scheduler.Receipts()`），状态为 `CmdApplied`
或 `CmdRejected`，拒绝原因用可判等的哨兵错误区分，例如：

- `ErrCmdTaskDone`：指向已结束的任务；
- `ErrCmdTaskSuspended`：指向正被挂起的任务；
- `ErrCmdTaskOnLock`：对正在等锁的任务发中断（不破坏锁等待队列）；
- `ErrCmdTaskNotSuspended` / `ErrCmdTaskNotBlocked` / `ErrCmdNoJob` /
  `ErrCmdTaskNotFound` / `ErrCmdDuplicateTask` / `ErrCmdNilTask`。

指令永远不会被悄悄吞掉。

### 与抢占、优先级继承的配合

- 优先级调整立即更新基础优先级：若任务是锁等待者，所在等待队列按
  （有效优先级降序、任务 ID 升序）重排，并沿持锁-等待链刷新继承；
  抢占判定在指令处理后的统一检查点按新优先级执行。
- 持锁任务被降优先级时，只要还有高优先级等待者，继承仍然托住其有效
  优先级，不会被中优先级任务抢占；锁释放后继承消失，抢占恢复正常。
- 等锁任务被挂起时仍留在等待队列中（继承关系保持）；锁可以移交给
  挂起中的任务（其成为持有者），恢复后继续临界区并正常释放。
- 关闭抢占（`Config{Preemptive: false}`）时，任何指令都不会绕过
  非抢占策略——指令只改变任务状态与优先级，分派决策仍由配置决定。

## 本地验证

```bash
go build ./...
go test ./sched/ -v          # 全部单测，日志标注每条用例覆盖的行为
go test ./sched/ -race -count=1   # 数据竞争检测下跑完整套测试
```

运行期控制相关用例在 `sched/control_test.go`：中断唤醒、挂起/恢复、
运行期调优先级（含继承保持、等待队列重排、非抢占模式）、等锁任务挂起、
运行期加任务、13 类拒绝原因、指令不推进时钟、同 tick 事件/指令顺序、
30 任务 × 200 指令的风暴确定性、并发提交（配合 `-race`）。
原有抢占、时间片轮转、优先级继承、超时/预算用例在
`scheduler_test.go` / `mutex_test.go` / `deadline_test.go` / `stress_test.go`。
