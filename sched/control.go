package sched

import (
	"errors"
	"sync"
)

// CmdKind 区分运行期控制指令类型。
type CmdKind int

const (
	CmdInterrupt    CmdKind = iota // 中断：使阻塞中的任务立即可运行
	CmdSuspend                     // 挂起任务
	CmdResume                      // 恢复被挂起的任务
	CmdSetPriority                 // 运行中调整任务优先级
	CmdAddTask                     // 运行中加入新任务
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

// 控制指令被拒绝的原因，可区分。
var (
	ErrCmdTaskNotFound     = errors.New("sched: command targets unknown task")
	ErrCmdTaskDone         = errors.New("sched: command targets finished task")
	ErrCmdTaskSuspended    = errors.New("sched: command targets suspended task")
	ErrCmdTaskNotSuspended = errors.New("sched: resume of non-suspended task")
	ErrCmdTaskOnLock       = errors.New("sched: interrupt of lock-waiting task")
	ErrCmdTaskNotBlocked   = errors.New("sched: interrupt of non-blocked task")
	ErrCmdNoJob            = errors.New("sched: interrupt of task without released job")
	ErrCmdDuplicateTask    = errors.New("sched: command adds task with duplicate ID")
	ErrCmdNilTask          = errors.New("sched: add-task command without task")
)

// Command 是一条按虚拟时刻生效的运行期控制指令。
// Tick 为生效时刻；Tick 不大于当前时刻时在下一次指令处理点立即生效。
// 下发指令本身不推进虚拟时钟。
type Command struct {
	Tick   uint64
	Kind   CmdKind
	TaskID int

	Priority int // CmdSetPriority：新优先级

	// CmdAddTask：新任务描述（Period>0 时按周期任务注册）。
	Task     *Task
	Program  []Op
	Period   uint64
	Deadline uint64
	Budget   uint64
	Policy   TimeoutPolicy
}

// CmdStatus 是指令处理结果。
type CmdStatus int

const (
	CmdApplied  CmdStatus = iota // 已生效
	CmdRejected                  // 被拒绝（Reason 给出可区分原因）
)

// Receipt 记录一条指令的处理结果（处理顺序即指令生效顺序）。
type Receipt struct {
	Tick   uint64 // 实际生效的虚拟时刻
	Kind   CmdKind
	TaskID int
	Status CmdStatus
	Reason error // 拒绝原因；Applied 时为 nil
}

// cmdItem 是指令堆中的一项，按 (Tick, seq) 确定性排序。
type cmdItem struct {
	cmd Command
	seq uint64
}

// cmdQueue 是指令的最小堆：先按生效时刻，同时刻按提交顺序，
// 保证同 tick 多条指令的处理顺序确定且可重放。
type cmdQueue struct {
	items []cmdItem
	seq   uint64
}

func (q *cmdQueue) len() int { return len(q.items) }

func cmdLess(a, b cmdItem) bool {
	if a.cmd.Tick != b.cmd.Tick {
		return a.cmd.Tick < b.cmd.Tick
	}
	return a.seq < b.seq
}

func (q *cmdQueue) push(cmd Command) {
	it := cmdItem{cmd: cmd, seq: q.seq}
	q.seq++
	q.items = append(q.items, it)
	for i := len(q.items) - 1; i > 0; {
		p := (i - 1) / 2
		if cmdLess(q.items[i], q.items[p]) {
			q.items[i], q.items[p] = q.items[p], q.items[i]
			i = p
		} else {
			break
		}
	}
}

func (q *cmdQueue) peek() (cmdItem, bool) {
	if len(q.items) == 0 {
		return cmdItem{}, false
	}
	return q.items[0], true
}

func (q *cmdQueue) pop() cmdItem {
	top := q.items[0]
	n := len(q.items) - 1
	q.items[0] = q.items[n]
	q.items = q.items[:n]
	for i := 0; ; {
		l, r := 2*i+1, 2*i+2
		m := i
		if l < len(q.items) && cmdLess(q.items[l], q.items[m]) {
			m = l
		}
		if r < len(q.items) && cmdLess(q.items[r], q.items[m]) {
			m = r
		}
		if m == i {
			break
		}
		q.items[i], q.items[m] = q.items[m], q.items[i]
		i = m
	}
	return top
}

// controlState 是调度器的运行期控制子系统状态。
// mu 保护全部字段，支持从其它 goroutine 与 Run 并发地下发指令。
type controlState struct {
	mu       sync.Mutex
	pending  cmdQueue
	receipts []Receipt
}

// Submit 从外部下发一条控制指令；不推进虚拟时钟，可并发调用。
// 指令在虚拟时钟到达 cmd.Tick 后的下一个指令处理点生效。
func (s *Scheduler) Submit(cmd Command) {
	s.control.mu.Lock()
	s.control.pending.push(cmd)
	s.control.mu.Unlock()
}

// Receipts 返回已处理指令的回执快照（按生效顺序）。
func (s *Scheduler) Receipts() []Receipt {
	s.control.mu.Lock()
	defer s.control.mu.Unlock()
	out := make([]Receipt, len(s.control.receipts))
	copy(out, s.control.receipts)
	return out
}

// dueCommands 取出全部已到期的指令（按确定性顺序）。
func (s *Scheduler) dueCommands() []Command {
	s.control.mu.Lock()
	defer s.control.mu.Unlock()
	var out []Command
	now := s.clock.Now()
	for {
		it, ok := s.control.pending.peek()
		if !ok || it.cmd.Tick > now {
			break
		}
		out = append(out, s.control.pending.pop().cmd)
	}
	return out
}

// nextCommandTick 返回下一条待生效指令的时刻。
func (s *Scheduler) nextCommandTick() (uint64, bool) {
	s.control.mu.Lock()
	defer s.control.mu.Unlock()
	it, ok := s.control.pending.peek()
	if !ok {
		return 0, false
	}
	return it.cmd.Tick, true
}

// recordReceipt 追加一条指令回执。
func (s *Scheduler) recordReceipt(r Receipt) {
	s.control.mu.Lock()
	s.control.receipts = append(s.control.receipts, r)
	s.control.mu.Unlock()
}

// processCommands 处理当前时刻到期的全部指令并记录回执。
// 在 runUntil 中于内核事件之后、抢占检查之前调用，
// 保证同一 tick 的顺序为：内核事件 → 控制指令 → 抢占判定 → 调度。
func (s *Scheduler) processCommands() {
	for _, cmd := range s.dueCommands() {
		r := s.applyCommand(cmd)
		s.recordReceipt(r)
	}
}

// applyCommand 应用一条控制指令，返回回执。不推进虚拟时钟。
func (s *Scheduler) applyCommand(cmd Command) Receipt {
	r := Receipt{Tick: s.clock.Now(), Kind: cmd.Kind, TaskID: cmd.TaskID}
	reject := func(err error) Receipt {
		r.Status = CmdRejected
		r.Reason = err
		s.logf("command %s on task %d rejected: %v", cmd.Kind, cmd.TaskID, err)
		return r
	}
	switch cmd.Kind {
	case CmdInterrupt:
		t := s.findTask(cmd.TaskID)
		if t == nil {
			return reject(ErrCmdTaskNotFound)
		}
		if err := s.interruptTask(t); err != nil {
			return reject(err)
		}
	case CmdSuspend:
		t := s.findTask(cmd.TaskID)
		if t == nil {
			return reject(ErrCmdTaskNotFound)
		}
		if err := s.suspendTask(t); err != nil {
			return reject(err)
		}
	case CmdResume:
		t := s.findTask(cmd.TaskID)
		if t == nil {
			return reject(ErrCmdTaskNotFound)
		}
		if err := s.resumeTask(t); err != nil {
			return reject(err)
		}
	case CmdSetPriority:
		t := s.findTask(cmd.TaskID)
		if t == nil {
			return reject(ErrCmdTaskNotFound)
		}
		if err := s.setTaskPriority(t, cmd.Priority); err != nil {
			return reject(err)
		}
	case CmdAddTask:
		if cmd.Task == nil {
			return reject(ErrCmdNilTask)
		}
		r.TaskID = cmd.Task.ID
		if err := s.addTaskRuntime(cmd); err != nil {
			return reject(err)
		}
	}
	r.Status = CmdApplied
	return r
}

// findTask 按 ID 查找已注册任务。
func (s *Scheduler) findTask(id int) *Task {
	for _, t := range s.tasks {
		if t.ID == id {
			return t
		}
	}
	return nil
}

// interruptTask 实现中断指令：让睡眠中的任务立即就绪。
// 等锁任务、无已释放 job 的任务、已结束/已挂起/本就可运行的任务
// 都以可区分的原因拒绝。
func (s *Scheduler) interruptTask(t *Task) error {
	switch t.state {
	case StateDone:
		return ErrCmdTaskDone
	case StateSuspended:
		return ErrCmdTaskSuspended
	case StateReady, StateRunning:
		return ErrCmdTaskNotBlocked
	}
	// Blocked：
	if t.waitingOn != nil {
		return ErrCmdTaskOnLock // 不破坏锁等待队列
	}
	if !t.released {
		return ErrCmdNoJob // 周期任务等待下次释放中
	}
	// 睡眠中的任务：提前唤醒。原唤醒事件到期时会发现任务已非阻塞，自动成为无操作。
	t.state = StateReady
	s.ready = append(s.ready, t)
	s.logf("interrupt: task %d ready", t.ID)
	return nil
}

// suspendTask 实现挂起指令：就绪/运行/阻塞中的任务都可挂起。
// 等锁任务挂起后仍留在锁等待队列中（继承关系保持），
// 锁移交给它时它保持挂起直到恢复。
func (s *Scheduler) suspendTask(t *Task) error {
	switch t.state {
	case StateDone:
		return ErrCmdTaskDone
	case StateSuspended:
		return ErrCmdTaskSuspended
	}
	if t.state == StateReady {
		s.removeFromReady(t)
	}
	if s.current == t {
		s.current = nil
	}
	t.state = StateSuspended
	s.logf("command: task %d suspended", t.ID)
	return nil
}

// resumeTask 实现恢复指令：按任务被挂起前的阻塞原因恢复到正确状态。
func (s *Scheduler) resumeTask(t *Task) error {
	if t.state == StateDone {
		return ErrCmdTaskDone
	}
	if t.state != StateSuspended {
		return ErrCmdTaskNotSuspended
	}
	now := s.clock.Now()
	switch {
	case t.waitingOn != nil:
		// 仍在锁等待队列中，恢复为阻塞等锁。
		t.state = StateBlocked
		s.logf("command: task %d resumed (waiting on %s)", t.ID, t.waitingOn.Name)
	case t.sleepUntil > now:
		// 睡眠未到期：重新武装唤醒事件（若原事件仍在队列中，
		// 到期时会发现任务已非阻塞而成为无操作，不会重复唤醒）。
		t.state = StateBlocked
		s.events.push(t.sleepUntil, evWakeup, t)
		s.logf("command: task %d resumed (sleeping until %d)", t.ID, t.sleepUntil)
	case t.period > 0 && !t.released:
		// 周期任务等待释放期间被挂起：释放链已在挂起时中断，重新武装。
		t.state = StateBlocked
		at := t.nextRelease
		if at < now {
			at = now
		}
		s.events.push(at, evRelease, t)
		s.logf("command: task %d resumed (next release at %d)", t.ID, at)
	default:
		t.state = StateReady
		s.ready = append(s.ready, t)
		s.logf("command: task %d resumed (ready)", t.ID)
	}
	return nil
}

// setTaskPriority 在运行中调整任务优先级：
// 立即更新基础优先级，重排其所在的锁等待队列并沿等待链刷新继承，
// 抢占判定在指令处理后的统一检查点按新优先级生效。
func (s *Scheduler) setTaskPriority(t *Task, prio int) error {
	if t.state == StateDone {
		return ErrCmdTaskDone
	}
	old := t.basePrio
	t.basePrio = prio
	t.Priority = prio
	if t.waitingOn != nil {
		t.waitingOn.sortWaiters()
		s.refreshInheritance(t.waitingOn.owner)
	}
	s.refreshInheritance(t)
	s.logf("command: task %d priority %d -> %d", t.ID, old, prio)
	return nil
}

// addTaskRuntime 在运行中注册新任务，于当前虚拟时刻就绪（或首次释放）。
func (s *Scheduler) addTaskRuntime(cmd Command) error {
	t := cmd.Task
	if s.findTask(t.ID) != nil {
		return ErrCmdDuplicateTask
	}
	if cmd.Period > 0 {
		s.AddPeriodic(t, cmd.Program, cmd.Period, cmd.Deadline, cmd.Budget, cmd.Policy)
	} else {
		s.AddTask(t, cmd.Program)
	}
	s.logf("command: task %d (%s) joined at runtime", t.ID, t.Name)
	return nil
}

// removeFromReady 把任务从就绪队列中移除。
func (s *Scheduler) removeFromReady(t *Task) {
	for i, x := range s.ready {
		if x == t {
			s.ready = append(s.ready[:i], s.ready[i+1:]...)
			return
		}
	}
}
