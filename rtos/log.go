package rtos

// LogKind 表示可解释日志条目的类型。
type LogKind int

const (
	LogSwitch        LogKind = iota // 上下文切换
	LogPreempt                      // 抢占发生
	LogBlock                        // 任务阻塞
	LogWake                         // 任务就绪/唤醒
	LogLock                         // 成功加锁
	LogUnlock                       // 成功解锁
	LogInherit                      // 优先级继承
	LogRestore                      // 优先级恢复
	LogDeadlineMiss                 // 错过截止时间
	LogBudgetOverrun                // 执行预算耗尽
	LogSuspend                      // 任务被挂起
	LogRestart                      // 作业被重启
	LogTerminate                    // 任务结束
	LogError                        // 非法操作被拒绝
	LogIdle                         // 空闲（时钟跳到下一事件）
)

// LogEntry 是一条带虚拟时刻的调度日志。
type LogEntry struct {
	Tick   int
	Kind   LogKind
	Task   string
	Detail string
	Err    error // 仅 LogError 条目携带，可用 errors.Is 区分原因
}
