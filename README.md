# realtime-task-scheduler-go

可离线复现的实时任务调度内核。在单进程内用**虚拟时钟**驱动任务、共享资源与中断（外部事件），
使优先级抢占、锁竞争、截止时间超时在重复执行下得到**一致且可解释**的结果。
不依赖任何外部服务或真实硬件。

## 快速开始

```go
k := rtos.NewKernel(4) // 同优先级默认时间片 = 4 tick
m := k.NewMutex("res")
k.AddTask(rtos.TaskSpec{Name: "low",  Priority: 1, Script: []rtos.Op{
    rtos.LockOp(m), rtos.Compute(6), rtos.UnlockOp(m),
}})
k.AddTask(rtos.TaskSpec{Name: "high", Priority: 3, Phase: 2, Script: []rtos.Op{
    rtos.LockOp(m), rtos.Compute(1), rtos.UnlockOp(m),
}})
k.Run(1000)          // maxTicks 防止死循环
for _, e := range k.Log() { fmt.Println(e) } // 可解释的调度轨迹
```

任务体是**指令脚本**（`Compute/LockOp/UnlockOp/Sleep/Yield`），内核逐条解释执行，
因此不存在真实 goroutine 调度带来的不确定性。

## 调度语义

- **固定优先级抢占**：`Priority` 数值越大优先级越高。每个 tick 边界重新选优，
  更高优先级任务就绪即在下一 tick 抢占当前任务（记 `LogPreempt`）。
- **同优先级时间片轮转**：时间片（`TaskSpec.Quantum`，缺省用内核默认值）耗尽后，
  当前任务移到同优先级队尾，轮转不计入抢占。
- **关闭抢占**：`k.SetPreemptive(false)` 后当前任务一直运行到阻塞、让出或结束。
- **状态机**：`Ready / Running / Blocked / Suspended / Dormant / Terminated`。
  周期任务作业完成后进入 `Dormant` 等待下次释放；非周期任务完成后 `Terminated`；
  被策略挂起或未首次释放为 `Suspended`。所有迁移都有日志可查。

## 虚拟时钟模型

- 时钟**只随任务实际执行推进**：`Compute(n)` 每条指令消耗 n 个 tick，每 tick 推进 1；
  锁/睡眠/让出为零开销指令，不推进时钟。调度开销、真实墙钟时间不影响虚拟时钟。
- 无就绪任务时，时钟**一次性跳转**到下一事件时刻（记 `LogIdle`），等价于空闲任务执行。
- 事件（周期释放、睡眠唤醒、截止检查、外部注入）进入按 `(tick, seq)` 排序的最小堆：
  **同一 tick 多个事件的处理次序完全确定**，不丢事件、不重复触发，整体可重放。
- 外部 goroutine 可通过 `SubmitAt(tick, fn)` 注入中断式事件（线程安全）；
  已过期的注入被拒绝并记录。`ExpectExternal(n)` 让内核在事件耗尽时等待外部提交而非提前退出。

## 共享资源与优先级继承

- `Mutex` 支持互斥与**优先级继承**：高优先级任务阻塞在锁上时，持锁的低优先级任务
  立即继承等待者的最高有效优先级（沿等待链传播），避免被无关中优先级任务长时间压制。
- 解锁时锁**直接交接**给等待者中有效优先级最高者（同优先级按到达先后），
  并按剩余持锁重算、恢复继承来的优先级（`LogInherit` / `LogRestore`）。
- 加锁、等待、释放、继承恢复严格配对；非法操作被拒绝并给出**可区分**的原因
  （`errors.Is` 可判定，记录在 `k.Errors()`）：
  - `ErrNotLocked`：释放未被持有的锁
  - `ErrNotOwner`：非持锁者释放
  - `ErrDeadlockSelf`：重复申请自己已持有的锁
  - `ErrDeadlockCycle`：申请会构成等待环的锁

## 截止时间与执行预算

周期任务（`Period`）可配置相对截止时间（`Deadline`）与单作业执行预算（`Budget`）。
错过截止时间或预算耗尽时按 `Policy` 处理：

| 策略            | 行为                                             |
|-----------------|--------------------------------------------------|
| `PolicyLog`     | 仅记录告警（`LogDeadlineMiss`/`LogBudgetOverrun`），任务继续 |
| `PolicySuspend` | 强制释放所持锁并挂起任务，不再释放新作业          |
| `PolicyRestart` | 强制释放所持锁，作业从脚本开头重新执行            |

截止事件携带作业序号，过期作业的事件自动失效，不会误触发或重复触发。

## 本地验证

```sh
go build ./...
go test ./rtos/ -v        # 功能测试，日志注明每条用例覆盖的调度行为
go test ./rtos/ -race -v  # 数据竞争检测下运行全部测试（含并发压力）
```

测试覆盖：优先级抢占 / 关抢占 / 时间片轮转 / 睡眠唤醒 / 确定性重放（`scheduler_test.go`）；
优先级继承与锁交接 / 非法释放 / 死锁检测（`mutex_test.go`）；
截止时间三种策略 / 周期释放不丢事件 / 同 tick 事件定序（`deadline_test.go`）；
多内核并发仿真与并发外部事件注入（`stress_test.go`，需在 `-race` 下运行）。
