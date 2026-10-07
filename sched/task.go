package sched

// State 表示任务生命周期状态。
type State int

const (
	StateReady     State = iota // 就绪
	StateRunning                // 运行中
	StateBlocked                // 阻塞（等锁或睡眠）
	StateSuspended              // 挂起（超时策略或手动）
	StateDone                   // 结束
)

func (s State) String() string {
	switch s {
	case StateReady:
		return "Ready"
	case StateRunning:
		return "Running"
	case StateBlocked:
		return "Blocked"
	case StateSuspended:
		return "Suspended"
	case StateDone:
		return "Done"
	default:
		return "Unknown"
	}
}

// OpKind 是任务脚本中的一步操作类型。
type OpKind int

const (
	OpCompute OpKind = iota // 占用 CPU 执行 N 个 tick
	OpLock                  // 对共享资源加锁
	OpUnlock                // 释放共享资源
	OpSleep                 // 睡眠 N 个 tick（阻塞）
	OpYield                 // 主动让出 CPU
)

// Op 描述任务脚本中的一步操作。
type Op struct {
	Kind  OpKind
	Ticks uint64 // OpCompute / OpSleep 使用
	Mutex *Mutex // OpLock / OpUnlock 使用
}

// Task 是一个被调度的任务。
type Task struct {
	ID       int
	Name     string
	Priority int // 数值越大优先级越高

	state State

	// 脚本执行位置
	program     []Op
	pc          int
	computeLeft uint64 // 当前 Compute 步剩余 tick
	sleepUntil  uint64 // OpSleep 唤醒时刻

	// 周期任务参数（period==0 表示非周期）
	period   uint64
	deadline uint64 // 相对截止时间
	budget   uint64 // 每个周期的执行预算（tick）

	// 每个 job 的运行时状态
	jobDeadline uint64 // 绝对截止时间
	budgetUsed  uint64
	nextRelease uint64 // 下次释放时刻
	released    bool   // 当前是否有未完成的 job

	// 超时处理
	policy          TimeoutPolicy
	deadlineAlarmed bool // 当前 job 是否已报告截止时间超时（去重）
	budgetAlarmed   bool // 当前 job 是否已报告预算耗尽（去重）
	lastErr         error

	// 优先级继承
	basePrio      int
	inheritedFrom *Task

	// 锁状态
	held      []*Mutex // 当前持有的锁
	waitingOn *Mutex   // 正在等待的锁（阻塞原因）

	// 运行期控制
	suspLockWait bool // 挂起时正在等锁（pc 已回滚，恢复后重新加锁）

	// 统计
	execTicks uint64
}

// State 返回任务当前状态。
func (t *Task) State() State { return t.state }

// EffectivePriority 返回考虑优先级继承后的有效优先级。
// 继承沿锁等待链传递（取链上最大值），由调度器在锁操作时维护。
func (t *Task) EffectivePriority() int {
	p := t.basePrio
	seen := map[*Task]bool{t: true}
	for cur := t.inheritedFrom; cur != nil && !seen[cur]; cur = cur.inheritedFrom {
		seen[cur] = true
		if cur.basePrio > p {
			p = cur.basePrio
		}
	}
	return p
}

// resetJob 重置每个 job 的运行时状态（预算与程序计数器）。
func (t *Task) resetJob() {
	t.pc = 0
	t.computeLeft = 0
	t.budgetUsed = 0
	t.deadlineAlarmed = false
	t.budgetAlarmed = false
}

// LastError 返回任务最近一次被拒绝的操作错误（如死锁、异常释放）。
func (t *Task) LastError() error { return t.lastErr }
