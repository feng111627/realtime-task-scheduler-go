package sched

// TimeoutPolicy 定义截止时间超时或预算耗尽时的处理策略。
type TimeoutPolicy int

const (
	PolicySuspend TimeoutPolicy = iota // 挂起任务
	PolicyRestart                      // 重启当前 job
	PolicyWarn                         // 仅记录告警
)

// AlarmKind 区分告警原因。
type AlarmKind int

const (
	AlarmDeadlineMiss  AlarmKind = iota // 截止时间超时
	AlarmBudgetOverrun                  // 执行预算耗尽
)

// Alarm 是一条超时/预算告警记录。
type Alarm struct {
	Tick   uint64
	TaskID int
	Kind   AlarmKind
	Policy TimeoutPolicy
}
