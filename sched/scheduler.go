package sched

import (
	"errors"
	"fmt"
)

// Config 是调度器配置。
type Config struct {
	Preemptive bool   // 关闭时不发生抢占
	TimeSlice  uint64 // 同优先级时间片（tick），0 表示不轮转
}

// ErrStepLimit 表示 Run 的步数上限被耗尽（用于防止脚本死循环）。
var ErrStepLimit = errors.New("sched: step limit exceeded")

// Scheduler 是由虚拟时钟驱动的抢占式调度内核。
// 时钟只在两种情况下推进：任务执行 Compute 操作；
// 或系统完全空闲时跳转到下一事件时刻。调度开销不计时。
type Scheduler struct {
	cfg   Config
	clock Clock

	tasks  []*Task
	ready  []*Task // 就绪队列（同优先级按 FIFO，轮转时尾部重排）
	events eventQueue

	current   *Task
	sliceLeft uint64 // 当前任务剩余时间片

	cmds       cmdQueue    // 运行期控制指令队列
	cmdResults []CmdResult // 已处理指令的结果

	log    []LogEntry
	alarms []Alarm
}

// LogEntry 记录一次调度决策，供测试与审计重放。
type LogEntry struct {
	Tick   uint64
	TaskID int
	What   string
}

// New 创建调度器。
func New(cfg Config) *Scheduler { return &Scheduler{cfg: cfg} }

// Clock 返回虚拟时钟（只读视图）。
func (s *Scheduler) Clock() *Clock { return &s.clock }

// Log 返回调度日志。
func (s *Scheduler) Log() []LogEntry { return s.log }

// Alarms 返回已记录的告警。
func (s *Scheduler) Alarms() []Alarm { return s.alarms }

// AddTask 注册一个非周期任务，在 tick 0 就绪。
func (s *Scheduler) AddTask(t *Task, program []Op) {
	t.program = program
	t.basePrio = t.Priority
	t.state = StateReady
	t.released = true
	t.jobDeadline = 0 // 无截止时间监控
	s.tasks = append(s.tasks, t)
	s.ready = append(s.ready, t)
	s.logf("task %d (%s) added, prio %d", t.ID, t.Name, t.Priority)
}

// AddPeriodic 注册一个周期任务，首次释放于 tick 0。
// period 为周期，deadline 为相对截止时间（0 表示不监控），
// budget 为每个周期的执行预算（0 表示不监控），policy 为超时处理策略。
func (s *Scheduler) AddPeriodic(t *Task, program []Op, period, deadline, budget uint64, policy TimeoutPolicy) {
	t.program = program
	t.basePrio = t.Priority
	t.period = period
	t.deadline = deadline
	t.budget = budget
	t.policy = policy
	t.state = StateBlocked // 等待首次释放
	t.nextRelease = 0
	s.tasks = append(s.tasks, t)
	s.events.push(0, evRelease, t)
	s.logf("periodic task %d (%s) added, prio %d period %d", t.ID, t.Name, t.Priority, period)
}

// logf 追加一条调度日志。
func (s *Scheduler) logf(format string, args ...interface{}) {
	id := -1
	if s.current != nil {
		id = s.current.ID
	}
	s.log = append(s.log, LogEntry{
		Tick:   s.clock.Now(),
		TaskID: id,
		What:   fmt.Sprintf(format, args...),
	})
}

// wake 把阻塞中的任务放回就绪队列。
func (s *Scheduler) wake(t *Task) {
	if t.state != StateBlocked {
		return
	}
	t.state = StateReady
	s.ready = append(s.ready, t)
	s.logf("task %d ready", t.ID)
}

// Run 运行调度循环，直到所有任务结束且事件队列清空，
// 或步数达到 maxSteps（返回 ErrStepLimit）。
func (s *Scheduler) Run(maxSteps uint64) error {
	return s.runUntil(0, maxSteps)
}

// RunUntil 运行调度循环，当时钟到达 stopTick 时停止（返回 nil），
// 用于不会自然终止的周期任务场景。stopTick 为 0 表示不限制。
func (s *Scheduler) RunUntil(stopTick, maxSteps uint64) error {
	return s.runUntil(stopTick, maxSteps)
}

func (s *Scheduler) runUntil(stopTick, maxSteps uint64) error {
	for step := uint64(0); ; step++ {
		if step >= maxSteps {
			return ErrStepLimit
		}
		if stopTick > 0 && s.clock.Now() >= stopTick {
			return nil
		}
		// 1. 空闲时快进到下一事件/指令时刻（无任务可运行，不计入任何任务）。
		if s.current == nil && len(s.ready) == 0 {
			next, ok := s.nextPendingTick()
			if !ok {
				return nil // 全部完成
			}
			if next > s.clock.Now() {
				s.logf("idle until %d", next)
				s.clock.advance(next - s.clock.Now())
			}
		}
		// 2. 处理当前时刻到期的全部事件（确定性顺序）。
		for s.events.len() > 0 && s.events.peek().tick <= s.clock.Now() {
			s.dispatch(s.events.pop())
		}
		// 2.5 同 tick 的外部指令在内部事件之后处理（确定性顺序：先事件后指令）。
		s.processCommands()
		// 3. 抢占检查（关闭抢占时跳过）。
		if s.cfg.Preemptive {
			s.preemptIfNeeded()
		}
		// 4. 选出下一个运行任务。
		if s.current == nil {
			s.current = s.pickReady()
			if s.current == nil {
				continue // 就绪队列空，回到空闲处理
			}
			s.current.state = StateRunning
			s.sliceLeft = s.cfg.TimeSlice
			s.logf("dispatch task %d", s.current.ID)
		}
		// 5. 执行当前任务一步。
		s.stepTask(s.current)
		// 6. 超时与预算监控（时钟每推进一次检查全体已释放 job）。
		s.checkTimeouts()
		// 7. 时间片轮转。
		if s.current != nil && s.cfg.TimeSlice > 0 && s.sliceLeft == 0 &&
			s.current.state == StateRunning {
			s.current.state = StateReady
			s.ready = append(s.ready, s.current)
			s.logf("timeslice expired for task %d", s.current.ID)
			s.current = nil
		}
	}
}

// nextPendingTick 返回下一个待处理时刻（事件与指令取较小者），
// ok=false 表示事件与指令队列均为空。
func (s *Scheduler) nextPendingTick() (uint64, bool) {
	tick, ok := uint64(0), false
	if s.events.len() > 0 {
		tick, ok = s.events.peek().tick, true
	}
	if ct, cok := s.nextCmdTick(); cok && (!ok || ct < tick) {
		tick, ok = ct, true
	}
	return tick, ok
}

// dispatch 处理一个到期事件。
func (s *Scheduler) dispatch(e event) {
	t := e.task
	switch e.kind {
	case evRelease:
		if t.state == StateSuspended {
			return // 挂起任务不再释放新 job
		}
		if t.released {
			// 上一 job 未完成：按截止时间超时处理后再释放新 job。
			s.raise(t, AlarmDeadlineMiss)
		}
		t.released = true
		t.resetJob()
		t.jobDeadline = e.tick + t.deadline
		t.nextRelease = e.tick + t.period
		s.events.push(t.nextRelease, evRelease, t)
		if t.state == StateBlocked && t.waitingOn == nil {
			s.wake(t)
		}
		s.logf("release task %d job (deadline %d)", t.ID, t.jobDeadline)
	case evWakeup:
		if t.state == StateBlocked && t.waitingOn == nil && t.sleepUntil <= s.clock.Now() {
			s.wake(t)
		}
	}
}

// preemptIfNeeded 在存在更高有效优先级的就绪任务时抢占当前任务。
func (s *Scheduler) preemptIfNeeded() {
	if s.current == nil {
		return
	}
	for _, t := range s.ready {
		if t.EffectivePriority() > s.current.EffectivePriority() {
			s.logf("preempt task %d", s.current.ID)
			s.current.state = StateReady
			s.ready = append(s.ready, s.current)
			s.current = nil
			return
		}
	}
}

// pickReady 选出有效优先级最高的就绪任务；同优先级取队首（FIFO）。
func (s *Scheduler) pickReady() *Task {
	best := -1
	for i, t := range s.ready {
		if best < 0 || t.EffectivePriority() > s.ready[best].EffectivePriority() {
			best = i
		}
	}
	if best < 0 {
		return nil
	}
	t := s.ready[best]
	s.ready = append(s.ready[:best], s.ready[best+1:]...)
	return t
}

// stepTask 执行当前任务的一步：控制操作零耗时，
// Compute 操作每步消耗 1 tick 并推进虚拟时钟。
func (s *Scheduler) stepTask(t *Task) {
	// 继续未完成的 Compute。
	if t.computeLeft > 0 {
		t.computeLeft--
		t.execTicks++
		t.budgetUsed++
		s.clock.advance(1)
		if s.cfg.TimeSlice > 0 && s.sliceLeft > 0 {
			s.sliceLeft--
		}
		if t.computeLeft == 0 {
			t.pc++
			if t.pc >= len(t.program) {
				s.finishJob(t) // 最后一个 tick 完成即结束 job
			}
		}
		return
	}
	if t.pc >= len(t.program) {
		s.finishJob(t)
		return
	}
	op := t.program[t.pc]
	switch op.Kind {
	case OpCompute:
		if op.Ticks == 0 {
			t.pc++
			return
		}
		t.computeLeft = op.Ticks
	case OpLock:
		if err := s.lockTask(t, op.Mutex); err != nil {
			s.fault(t, err)
			return
		}
		t.pc++
		if t.state == StateBlocked {
			s.current = nil // 等锁阻塞
		}
	case OpUnlock:
		if err := s.unlockTask(t, op.Mutex); err != nil {
			s.fault(t, err)
			return
		}
		t.pc++
	case OpSleep:
		t.sleepUntil = s.clock.Now() + op.Ticks
		t.state = StateBlocked
		s.events.push(t.sleepUntil, evWakeup, t)
		t.pc++
		s.current = nil
		s.logf("task %d sleep until %d", t.ID, t.sleepUntil)
	case OpYield:
		t.pc++
		t.state = StateReady
		s.ready = append(s.ready, t)
		s.current = nil
		s.logf("task %d yield", t.ID)
	}
}

// finishJob 结束当前 job：周期任务转入等待下次释放，非周期任务结束。
func (s *Scheduler) finishJob(t *Task) {
	t.released = false
	if t.period > 0 {
		t.state = StateBlocked // 等待下次释放
		s.logf("task %d job finished", t.ID)
	} else {
		t.state = StateDone
		s.logf("task %d done", t.ID)
	}
	s.current = nil
}

// fault 处理被拒绝的任务操作：记录错误并挂起任务。
func (s *Scheduler) fault(t *Task, err error) {
	t.lastErr = err
	t.state = StateSuspended
	if s.current == t {
		s.current = nil
	}
	s.logf("task %d fault: %v", t.ID, err)
}

// checkTimeouts 在时钟推进后检查所有已释放 job 的截止时间与预算。
func (s *Scheduler) checkTimeouts() {
	for _, t := range s.tasks {
		if !t.released || t.state == StateSuspended || t.state == StateDone {
			continue
		}
		if t.budget > 0 && !t.budgetAlarmed && t.budgetUsed >= t.budget {
			s.raise(t, AlarmBudgetOverrun)
		}
		if t.deadline > 0 && !t.deadlineAlarmed && s.clock.Now() >= t.jobDeadline {
			s.raise(t, AlarmDeadlineMiss)
		}
	}
}

// raise 按任务配置的策略处理一次超时/预算事件，每个 job 每类只触发一次。
func (s *Scheduler) raise(t *Task, kind AlarmKind) {
	switch kind {
	case AlarmDeadlineMiss:
		t.deadlineAlarmed = true
	case AlarmBudgetOverrun:
		t.budgetAlarmed = true
	}
	alarm := Alarm{Tick: s.clock.Now(), TaskID: t.ID, Kind: kind, Policy: t.policy}
	s.alarms = append(s.alarms, alarm)
	s.logf("alarm kind %d on task %d", kind, t.ID)
	switch t.policy {
	case PolicyWarn:
		// 仅记录
	case PolicySuspend:
		t.released = false
		t.state = StateSuspended
		if s.current == t {
			s.current = nil
		}
	case PolicyRestart:
		t.resetJob()
		t.jobDeadline = s.clock.Now() + t.deadline
		if t.state == StateBlocked && t.waitingOn == nil {
			s.wake(t)
		}
	}
}
