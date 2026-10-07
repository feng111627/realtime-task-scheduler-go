package sched

import (
	"errors"
	"sync"
)

// CmdKind 区分运行期控制指令的类型。
type CmdKind int

const (
	CmdInterrupt   CmdKind = iota // 中断：把睡眠中的任务立刻变为可运行
	CmdSuspend                    // 临时挂起任务
	CmdResume                     // 恢复被挂起的任务
	CmdSetPriority                // 运行中调整任务基础优先级
	CmdAddTask                    // 运行中加入新任务
)

func (k CmdKind) String() string {
	switch k {
	case CmdInterrupt:
		return "Interrupt"
	case CmdSuspend:
		return "Suspend"
	case CmdResume:
		return "Resume"
	case CmdSetPriority:
		return "SetPriority"
	case CmdAddTask:
		return "AddTask"
	default:
		return "Unknown"
	}
}

// 指令被拒绝时的可区分原因。
var (
	ErrCmdTaskUnknown      = errors.New("sched: command targets unknown task")
	ErrCmdTaskDone         = errors.New("sched: command targets finished task")
	ErrCmdTaskSuspended    = errors.New("sched: command targets suspended task")
	ErrCmdTaskNotSuspended = errors.New("sched: resume targets non-suspended task")
	ErrCmdTaskLockWait     = errors.New("sched: interrupt targets lock-waiting task")
	ErrCmdTaskNotBlocked   = errors.New("sched: interrupt targets non-blocked task")
	ErrCmdDuplicateTask    = errors.New("sched: command adds duplicate task ID")
	ErrCmdNoTask           = errors.New("sched: add-task command has no task")
)

// Command 是一条运行期控制指令，在指定虚拟时刻生效。
// 下发指令本身不推进虚拟时钟。
type Command struct {
	Kind   CmdKind
	TaskID int // 目标任务（CmdAddTask 时为 NewTask.ID，可省略）

	Priority int    // CmdSetPriority：新的基础优先级
	NewTask  *Task  // CmdAddTask：新任务
	Program  []Op   // CmdAddTask：新任务脚本
	Period   uint64 // CmdAddTask：非 0 时按周期任务加入
	Deadline uint64 // CmdAddTask：相对截止时间（0 不监控）
	Budget   uint64 // CmdAddTask：每周期预算（0 不监控）
	Policy   TimeoutPolicy
}

// CmdStatus 是指令处理结果的状态。
type CmdStatus int

const (
	CmdApplied  CmdStatus = iota // 已生效
	CmdNoop                      // 目标状态已满足，无需变更
	CmdRejected                  // 被拒绝（Err 给出可区分原因）
)

func (st CmdStatus) String() string {
	switch st {
	case CmdApplied:
		return "Applied"
	case CmdNoop:
		return "Noop"
	case CmdRejected:
		return "Rejected"
	default:
		return "Unknown"
	}
}

// CmdResult 记录一条指令的处理结果，不丢指令也不悄悄吞掉。
type CmdResult struct {
	Tick   uint64 // 实际生效的虚拟时刻
	Seq    uint64 // 提交序号（同 tick 内按序号处理）
	Kind   CmdKind
	TaskID int
	Status CmdStatus
	Err    error // Rejected 时的原因
}

// cmd 是队列中的待处理指令。
type cmd struct {
	tick uint64
	seq  uint64
	c    Command
}

// cmdQueue 是按 (tick, seq) 排序的确定性指令队列。
// 提交可以与调度循环并发，故用互斥锁保护。
type cmdQueue struct {
	mu    sync.Mutex
	items []cmd
	seq   uint64
}

// Submit 注册一条在虚拟时刻 tick 生效的控制指令。
// 可以在 Run 运行中从其它 goroutine 调用；调用本身不推进时钟。
// 同 tick 的指令按提交顺序处理，晚到的指令排在已提交指令之后。
// 队列始终保持 (tick, seq) 升序，读取队首为 O(1)。
func (s *Scheduler) Submit(tick uint64, c Command) {
	s.cmds.mu.Lock()
	defer s.cmds.mu.Unlock()
	it := cmd{tick: tick, seq: s.cmds.seq, c: c}
	s.cmds.seq++
	i := len(s.cmds.items)
	for i > 0 && (s.cmds.items[i-1].tick > tick ||
		(s.cmds.items[i-1].tick == tick && s.cmds.items[i-1].seq > it.seq)) {
		i--
	}
	s.cmds.items = append(s.cmds.items, cmd{})
	copy(s.cmds.items[i+1:], s.cmds.items[i:])
	s.cmds.items[i] = it
}

// CommandResults 返回已处理指令的结果（按处理顺序）。
func (s *Scheduler) CommandResults() []CmdResult {
	s.cmds.mu.Lock()
	defer s.cmds.mu.Unlock()
	out := make([]CmdResult, len(s.cmdResults))
	copy(out, s.cmdResults)
	return out
}

// nextCmdTick 返回下一条待处理指令的时刻，ok=false 表示队列为空。
func (s *Scheduler) nextCmdTick() (uint64, bool) {
	s.cmds.mu.Lock()
	defer s.cmds.mu.Unlock()
	if len(s.cmds.items) == 0 {
		return 0, false
	}
	return s.cmds.items[0].tick, true
}

// popDueCmds 取出所有 tick <= now 的指令（队列前缀，已按 (tick, seq) 升序）。
func (s *Scheduler) popDueCmds(now uint64) []cmd {
	s.cmds.mu.Lock()
	defer s.cmds.mu.Unlock()
	n := 0
	for n < len(s.cmds.items) && s.cmds.items[n].tick <= now {
		n++
	}
	if n == 0 {
		return nil
	}
	due := make([]cmd, n)
	copy(due, s.cmds.items[:n])
	s.cmds.items = append(s.cmds.items[:0], s.cmds.items[n:]...)
	return due
}

// processCommands 处理当前时刻到期的全部指令（在事件之后、抢占检查之前）。
func (s *Scheduler) processCommands() {
	for _, it := range s.popDueCmds(s.clock.Now()) {
		s.execCmd(it)
	}
}

// execCmd 执行一条指令并记录结果。
func (s *Scheduler) execCmd(it cmd) {
	res := CmdResult{Tick: s.clock.Now(), Seq: it.seq, Kind: it.c.Kind, TaskID: it.c.TaskID}
	switch it.c.Kind {
	case CmdInterrupt:
		res.Status, res.Err = s.cmdInterrupt(it.c.TaskID)
	case CmdSuspend:
		res.Status, res.Err = s.cmdSuspend(it.c.TaskID)
	case CmdSetPriority:
		res.Status, res.Err = s.cmdSetPriority(it.c.TaskID, it.c.Priority)
	case CmdAddTask:
		res.TaskID = 0
		if it.c.NewTask != nil {
			res.TaskID = it.c.NewTask.ID
		}
		res.Status, res.Err = s.cmdAddTask(it.c)
	default:
		res.Status = CmdRejected
		res.Err = errors.New("sched: command not implemented")
	}
	s.recordCmd(res)
}

// cmdSuspend 临时挂起任务：从就绪/运行/阻塞状态转入 Suspended。
// 等锁任务挂起时退出锁等待队列并回滚 pc（恢复后重新执行加锁），
// 同时重算锁持有者的继承关系，保证锁归属与继承不悬空。
func (s *Scheduler) cmdSuspend(id int) (CmdStatus, error) {
	t := s.findTask(id)
	if t == nil {
		return CmdRejected, ErrCmdTaskUnknown
	}
	switch t.state {
	case StateDone:
		return CmdRejected, ErrCmdTaskDone
	case StateSuspended:
		return CmdNoop, nil
	}
	switch t.state {
	case StateReady:
		s.removeReady(t)
	case StateRunning:
		s.current = nil
	case StateBlocked:
		if t.waitingOn != nil {
			m := t.waitingOn
			m.removeWaiter(t)
			t.waitingOn = nil
			t.pc-- // 恢复后重新执行加锁操作
			t.suspLockWait = true
			s.refreshInheritance(m.owner)
		}
		// 睡眠中：保留 sleepUntil，恢复时按剩余时间重新入队唤醒事件。
		// 等待下次释放：无额外状态需要处理。
	}
	t.state = StateSuspended
	s.logf("cmd suspend task %d", t.ID)
	return CmdApplied, nil
}

// cmdResume 恢复被挂起的任务，按挂起前的阻塞原因回到正确状态：
// 等锁挂起的回到就绪（重新加锁）；睡眠未到期的回到阻塞并重新注册唤醒；
// 周期任务 job 间隙的回到阻塞等待释放；其余回到就绪。
func (s *Scheduler) cmdResume(id int) (CmdStatus, error) {
	t := s.findTask(id)
	if t == nil {
		return CmdRejected, ErrCmdTaskUnknown
	}
	if t.state == StateDone {
		return CmdRejected, ErrCmdTaskDone
	}
	if t.state != StateSuspended {
		return CmdRejected, ErrCmdTaskNotSuspended
	}
	switch {
	case t.suspLockWait:
		t.suspLockWait = false
		t.state = StateReady
		s.ready = append(s.ready, t)
	case t.released && t.sleepUntil > s.clock.Now():
		t.state = StateBlocked
		s.events.push(t.sleepUntil, evWakeup, t)
	case t.released:
		t.state = StateReady
		s.ready = append(s.ready, t)
	default:
		t.state = StateBlocked // 周期任务等待下次释放
	}
	s.logf("cmd resume task %d -> %s", t.ID, t.state)
	return CmdApplied, nil
}

// removeReady 把任务从就绪队列移除。
func (s *Scheduler) removeReady(t *Task) {
	for i, x := range s.ready {
		if x == t {
			s.ready = append(s.ready[:i], s.ready[i+1:]...)
			return
		}
	}
}
// 等锁任务不能用中断唤醒（锁归属会悬空），返回可区分错误。
// cmdSetPriority 运行中调整任务基础优先级。
// 立即重排受影响的锁等待队列并沿等待链重算继承关系；
// 抢占是否发生由主循环按配置决定（关闭抢占时指令不会绕过）。
// 挂起中的任务允许改优先级（恢复时按新优先级参与调度）。
func (s *Scheduler) cmdSetPriority(id, prio int) (CmdStatus, error) {
	t := s.findTask(id)
	if t == nil {
		return CmdRejected, ErrCmdTaskUnknown
	}
	if t.state == StateDone {
		return CmdRejected, ErrCmdTaskDone
	}
	if t.basePrio == prio {
		return CmdNoop, nil
	}
	t.Priority = prio
	t.basePrio = prio
	// t 的有效优先级变化会影响：t 所在的锁等待队列顺序、
	// 该锁持有者及等待链上游的继承关系、t 自身的继承来源。
	if t.waitingOn != nil {
		t.waitingOn.sortWaiters()
		s.refreshInheritance(t.waitingOn.owner)
	}
	s.refreshInheritance(t)
	s.logf("cmd setpriority task %d -> %d", t.ID, prio)
	return CmdApplied, nil
}

// cmdAddTask 运行中加入新任务，在当前虚拟时刻就绪（不推进时钟）。
// 非周期任务立即进入就绪队列；周期任务在当前时刻释放首个 job。
func (s *Scheduler) cmdAddTask(c Command) (CmdStatus, error) {
	t := c.NewTask
	if t == nil {
		return CmdRejected, ErrCmdNoTask
	}
	if s.findTask(t.ID) != nil {
		return CmdRejected, ErrCmdDuplicateTask
	}
	if c.Period == 0 {
		s.AddTask(t, c.Program)
		return CmdApplied, nil
	}
	// 周期任务：与 AddPeriodic 相同，但首次释放时刻为当前时刻。
	t.program = c.Program
	t.basePrio = t.Priority
	t.period = c.Period
	t.deadline = c.Deadline
	t.budget = c.Budget
	t.policy = c.Policy
	t.state = StateBlocked
	t.nextRelease = s.clock.Now()
	s.tasks = append(s.tasks, t)
	s.events.push(t.nextRelease, evRelease, t)
	s.logf("periodic task %d (%s) added at runtime, prio %d period %d",
		t.ID, t.Name, t.Priority, c.Period)
	return CmdApplied, nil
}

// cmdInterrupt 中断唤醒：把睡眠中的任务立刻变为可运行。
// 等锁任务不能用中断唤醒（锁归属会悬空），返回可区分错误。
func (s *Scheduler) cmdInterrupt(id int) (CmdStatus, error) {
	t := s.findTask(id)
	if t == nil {
		return CmdRejected, ErrCmdTaskUnknown
	}
	switch t.state {
	case StateDone:
		return CmdRejected, ErrCmdTaskDone
	case StateSuspended:
		return CmdRejected, ErrCmdTaskSuspended
	case StateReady, StateRunning:
		return CmdNoop, nil // 本就可运行
	}
	// Blocked：区分等锁 / 睡眠 / 等待下次释放。
	if t.waitingOn != nil {
		return CmdRejected, ErrCmdTaskLockWait
	}
	if !t.released {
		return CmdRejected, ErrCmdTaskNotBlocked // 周期任务等待下次释放，无 job 可唤醒
	}
	// 睡眠中：立即唤醒。尚未到期的 evWakeup 事件之后会因状态守卫变为无操作。
	s.wake(t)
	s.logf("cmd interrupt wakes task %d", t.ID)
	return CmdApplied, nil
}

// recordCmd 记录结果并写调度日志。
func (s *Scheduler) recordCmd(res CmdResult) {
	s.cmds.mu.Lock()
	s.cmdResults = append(s.cmdResults, res)
	s.cmds.mu.Unlock()
	if res.Status == CmdRejected {
		s.logf("cmd#%d %s task %d rejected: %v", res.Seq, res.Kind, res.TaskID, res.Err)
	} else {
		s.logf("cmd#%d %s task %d %s", res.Seq, res.Kind, res.TaskID, res.Status)
	}
}

// findTask 按 ID 查找任务，找不到返回 nil。
func (s *Scheduler) findTask(id int) *Task {
	for _, t := range s.tasks {
		if t.ID == id {
			return t
		}
	}
	return nil
}
