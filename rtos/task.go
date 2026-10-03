package rtos

// TaskState 表示任务生命周期状态。
type TaskState int

const (
	StateReady      TaskState = iota // 就绪
	StateRunning                     // 运行
	StateBlocked                     // 阻塞（等锁或睡眠）
	StateSuspended                   // 挂起（策略触发或尚未首次释放）
	StateDormant                     // 周期作业已完成，等待下次释放
	StateTerminated                  // 结束
)

func (s TaskState) String() string {
	switch s {
	case StateReady:
		return "Ready"
	case StateRunning:
		return "Running"
	case StateBlocked:
		return "Blocked"
	case StateSuspended:
		return "Suspended"
	case StateTerminated:
		return "Terminated"
	}
	return "Unknown"
}

// OverrunPolicy 表示截止时间错过或预算耗尽时的处理策略。
type OverrunPolicy int

const (
	PolicyLog     OverrunPolicy = iota // 仅记录告警，任务继续
	PolicySuspend                      // 挂起任务
	PolicyRestart                      // 重启当前作业（脚本从头执行）
)

// TaskSpec 描述一个任务的静态配置。
type TaskSpec struct {
	Name     string
	Priority int           // 数值越大优先级越高
	Script   []Op          // 一个作业周期内执行的指令序列
	Period   int           // 周期（tick），0 表示只释放一次
	Deadline int           // 相对截止时间（tick），0 表示不检查
	Budget   int           // 单作业最大计算 tick 数，0 表示不限制
	Policy   OverrunPolicy // 超时/超预算处理策略
	Phase    int           // 首次释放的 tick
	Quantum  int           // 同优先级时间片长度，0 使用内核默认值
}

// Task 是内核管理的任务控制块。
type Task struct {
	spec TaskSpec

	state   TaskState
	pc      int // 当前脚本下标
	opTicks int // 当前 Compute 指令已消耗的 tick

	basePrio int
	effPrio  int // 有效优先级（含继承）

	sliceLeft int // 当前时间片剩余

	jobID       int64 // 当前作业序号
	jobCompute  int   // 当前作业已消耗的计算 tick
	releaseTick int   // 当前作业释放时刻
	jobDone     bool  // 当前作业是否已完成

	held    []*Mutex // 当前持有的锁
	waiting *Mutex   // 正在等待的锁

	k *Kernel
}

// Name 返回任务名。
func (t *Task) Name() string { return t.spec.Name }

// State 返回任务当前状态。
func (t *Task) State() TaskState { return t.state }

// EffectivePriority 返回任务当前有效优先级。
func (t *Task) EffectivePriority() int { return t.effPrio }
