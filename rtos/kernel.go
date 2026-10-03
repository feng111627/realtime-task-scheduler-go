package rtos

import (
	"container/heap"
	"sync"
)

// Kernel 是确定性实时调度内核。
type Kernel struct {
	clock      int
	preemptive bool
	defQuantum int
	tasks      []*Task
	ready      []*Task
	current    *Task
	events     eventHeap
	seq        uint64
	log        []LogEntry
	errs       []LogEntry
	maxTicks   int

	mu      sync.Mutex // 保护外部提交队列
	cond    *sync.Cond // 外部提交到达信号
	pending []event    // 外部线程提交、尚未并入堆的事件
	extWant int        // 预期的外部事件总数
	extGot  int        // 已收到的外部事件数
}

// NewKernel 创建内核。defQuantum 为同优先级轮转默认时间片。
func NewKernel(defQuantum int) *Kernel {
	k := &Kernel{preemptive: true, defQuantum: defQuantum}
	k.cond = sync.NewCond(&k.mu)
	return k
}

// ExpectExternal 声明将有 n 个外部事件通过 SubmitAt 提交。
// 内核在事件队列耗尽但外部事件未齐时阻塞等待，而不是提前退出。
func (k *Kernel) ExpectExternal(n int) {
	k.mu.Lock()
	k.extWant = n
	k.mu.Unlock()
}

// SetPreemptive 开关抢占。关闭后高优先级就绪不会打断当前任务。
func (k *Kernel) SetPreemptive(on bool) { k.preemptive = on }

// Clock 返回当前虚拟时刻。
func (k *Kernel) Clock() int { return k.clock }

// NewMutex 创建一个命名互斥锁。
func (k *Kernel) NewMutex(name string) *Mutex { return &Mutex{name: name} }

// AddTask 注册任务，返回任务句柄。
func (k *Kernel) AddTask(spec TaskSpec) *Task {
	t := &Task{spec: spec, basePrio: spec.Priority, effPrio: spec.Priority, k: k}
	k.tasks = append(k.tasks, t)
	return t
}

// Log 返回调度日志（只读视图）。
func (k *Kernel) Log() []LogEntry { return k.log }

// Errors 返回被拒绝的非法操作日志。
func (k *Kernel) Errors() []LogEntry { return k.errs }

// SubmitAt 从任意 goroutine 在指定虚拟 tick 注入一个事件，线程安全。
// 若注入时内核时钟已越过该 tick，事件被拒绝并记录错误。
func (k *Kernel) SubmitAt(tick int, fn func()) {
	k.mu.Lock()
	k.pending = append(k.pending, event{tick: tick, kind: evExternal, fn: fn})
	k.extGot++
	k.cond.Broadcast()
	k.mu.Unlock()
}

// Run 运行仿真，直到所有任务结束、无事件可推进或达到 maxTicks（防止死循环）。
// 虚拟时钟只随任务实际执行（Compute）或空闲跳转推进，与真实时间无关。
func (k *Kernel) Run(maxTicks int) {
	k.maxTicks = maxTicks
	for _, t := range k.tasks {
		t.state = StateDormant // 尚未首次释放
		k.pushEvent(event{tick: t.spec.Phase, kind: evRelease, task: t})
	}
	for k.clock < maxTicks {
		k.drainPending()
		k.processEvents()
		if !k.selectTask() {
			// 无就绪任务且事件耗尽：若仍有预期外部事件则阻塞等待。
			if len(k.events) == 0 {
				k.mu.Lock()
				for len(k.pending) == 0 && k.extGot < k.extWant {
					k.cond.Wait()
				}
				done := len(k.pending) == 0 && k.extGot >= k.extWant
				k.mu.Unlock()
				if done {
					return
				}
				continue
			}
			next := k.events[0].tick
			k.logf(LogIdle, nil, "idle jump")
			k.clock = next
			continue
		}
		k.executeTick()
		if k.allTerminated() {
			return
		}
	}
}

// pushEvent 分配全局递增序号并入堆，保证同 tick 事件的确定次序。
func (k *Kernel) pushEvent(ev event) {
	ev.seq = k.seq
	k.seq++
	heap.Push(&k.events, ev)
}

// drainPending 把外部线程提交的事件并入事件堆；已过期的拒绝并记录。
func (k *Kernel) drainPending() {
	k.mu.Lock()
	pending := k.pending
	k.pending = nil
	k.mu.Unlock()
	for _, ev := range pending {
		if ev.tick < k.clock {
			k.errs = append(k.errs, LogEntry{Tick: k.clock, Kind: LogError, Detail: "stale external event rejected"})
			continue
		}
		k.pushEvent(ev)
	}
}

// processEvents 依次处理当前 tick 的全部到期事件（堆序保证确定性）。
func (k *Kernel) processEvents() {
	for len(k.events) > 0 && k.events[0].tick <= k.clock {
		ev := heap.Pop(&k.events).(event)
		switch ev.kind {
		case evRelease:
			k.releaseJob(ev.task)
		case evWakeup:
			if ev.task.state == StateBlocked {
				k.makeReady(ev.task)
			}
		case evDeadline:
			t := ev.task
			if t.jobID == ev.job && !t.jobDone {
				k.applyPolicy(t, LogDeadlineMiss, "deadline miss")
			}
		case evExternal:
			if ev.fn != nil {
				ev.fn()
			}
		}
	}
}

// releaseJob 释放一个周期作业；若上一作业未完成先按策略处理。
func (k *Kernel) releaseJob(t *Task) {
	if !t.jobDone && t.jobID > 0 && t.state != StateSuspended && t.state != StateTerminated {
		k.applyPolicy(t, LogDeadlineMiss, "job incomplete at next release")
		if t.state == StateSuspended || t.state == StateTerminated {
			return
		}
	}
	if t.state == StateSuspended || t.state == StateTerminated {
		return
	}
	t.jobID++
	t.pc, t.opTicks, t.jobCompute = 0, 0, 0
	t.releaseTick = k.clock
	t.jobDone = false
	k.makeReady(t)
	if t.spec.Deadline > 0 {
		k.pushEvent(event{tick: k.clock + t.spec.Deadline, kind: evDeadline, task: t, job: t.jobID})
	}
	if t.spec.Period > 0 {
		k.pushEvent(event{tick: k.clock + t.spec.Period, kind: evRelease, task: t})
	}
}

// selectTask 选择下一个运行的任务；返回 false 表示无就绪任务。
func (k *Kernel) selectTask() bool {
	cur := k.current
	if cur != nil && cur.state == StateRunning {
		if !k.preemptive {
			return true // 关闭抢占：当前任务运行到阻塞/让出/结束
		}
		best := k.bestReady()
		if best >= 0 && k.ready[best].effPrio > cur.effPrio {
			k.logf(LogPreempt, cur, "preempted by "+k.ready[best].spec.Name)
			k.makeReady(cur)
			k.current = nil
		} else if best >= 0 && k.ready[best].effPrio == cur.effPrio && cur.sliceLeft <= 0 {
			k.makeReady(cur) // 同优先级时间片轮转
			k.current = nil
		} else {
			return true
		}
	}
	best := k.bestReady()
	if best < 0 {
		return false
	}
	t := k.ready[best]
	k.ready = append(k.ready[:best], k.ready[best+1:]...)
	k.current = t
	t.state = StateRunning
	if t.sliceLeft <= 0 {
		t.sliceLeft = t.spec.Quantum
		if t.sliceLeft <= 0 {
			t.sliceLeft = k.defQuantum
		}
		if t.sliceLeft <= 0 {
			t.sliceLeft = 1
		}
	}
	k.logf(LogSwitch, t, "dispatch")
	return true
}

// bestReady 返回就绪队列中有效优先级最高、到达最早的任务下标。
func (k *Kernel) bestReady() int {
	best := -1
	for i, t := range k.ready {
		if best < 0 || t.effPrio > k.ready[best].effPrio {
			best = i
		}
	}
	return best
}

// executeTick 执行当前任务：零开销指令连续执行，Compute 每次消耗一个虚拟 tick。
func (k *Kernel) executeTick() {
	t := k.current
	for t != nil && t.state == StateRunning {
		if t.pc >= len(t.spec.Script) {
			k.completeJob(t)
			return
		}
		op := t.spec.Script[t.pc]
		switch op.Kind {
		case OpCompute:
			t.opTicks++
			t.jobCompute++
			t.sliceLeft--
			k.clock++
			if t.opTicks >= op.N {
				t.pc++
				t.opTicks = 0
				if t.pc >= len(t.spec.Script) {
					k.completeJob(t)
					return
				}
			}
			if t.spec.Budget > 0 && t.jobCompute > t.spec.Budget {
				k.applyPolicy(t, LogBudgetOverrun, "budget overrun")
			}
			return // 恰好消耗一个 tick
		case OpLock:
			k.lock(t, op.M)
			t.pc++
			if t.state == StateBlocked {
				k.current = nil
				return
			}
		case OpUnlock:
			k.unlock(t, op.M)
			t.pc++
		case OpSleep:
			k.pushEvent(event{tick: k.clock + op.N, kind: evWakeup, task: t})
			t.state = StateBlocked
			k.logf(LogBlock, t, "sleep")
			t.pc++
			k.current = nil
			return
		case OpYield:
			t.pc++
			k.makeReady(t)
			k.current = nil
			return
		}
	}
	k.current = nil
}

// completeJob 结束当前作业：非周期任务终止，周期任务进入休眠等待下次释放。
func (k *Kernel) completeJob(t *Task) {
	t.jobDone = true
	k.releaseAllLocks(t)
	if t.spec.Period == 0 {
		t.state = StateTerminated
		k.logf(LogTerminate, t, "terminated")
	} else {
		t.state = StateDormant
		k.logf(LogTerminate, t, "job done")
	}
	if k.current == t {
		k.current = nil
	}
}

// applyPolicy 按任务配置的策略处理截止时间错过或预算耗尽。
func (k *Kernel) applyPolicy(t *Task, kind LogKind, detail string) {
	k.logf(kind, t, detail)
	switch t.spec.Policy {
	case PolicyLog:
		// 仅记录告警，任务继续。
	case PolicySuspend:
		k.releaseAllLocks(t)
		t.jobDone = true
		if t.state == StateReady {
			k.removeReady(t)
		}
		t.state = StateSuspended
		k.logf(LogSuspend, t, "suspended by policy")
		if k.current == t {
			k.current = nil
		}
	case PolicyRestart:
		k.releaseAllLocks(t)
		if t.state == StateReady {
			k.removeReady(t)
		}
		t.jobID++
		t.pc, t.opTicks, t.jobCompute = 0, 0, 0
		t.releaseTick = k.clock
		t.jobDone = false
		if t.spec.Deadline > 0 {
			k.pushEvent(event{tick: k.clock + t.spec.Deadline, kind: evDeadline, task: t, job: t.jobID})
		}
		k.logf(LogRestart, t, "job restarted")
		if k.current == t {
			k.current = nil
		}
		k.makeReady(t)
	}
}

// releaseAllLocks 强制释放任务持有的全部锁（用于挂起/重启/作业结束）。
func (k *Kernel) releaseAllLocks(t *Task) {
	held := append([]*Mutex(nil), t.held...)
	for _, m := range held {
		k.unlock(t, m)
	}
}

// allTerminated 报告是否所有任务都已结束。
func (k *Kernel) allTerminated() bool {
	for _, t := range k.tasks {
		if t.state != StateTerminated {
			return false
		}
	}
	return len(k.tasks) > 0
}

// logf 追加一条调度日志。
func (k *Kernel) logf(kind LogKind, t *Task, detail string) {
	name := ""
	if t != nil {
		name = t.spec.Name
	}
	k.log = append(k.log, LogEntry{Tick: k.clock, Kind: kind, Task: name, Detail: detail})
}

// recordErr 记录一次被拒绝的非法操作。
func (k *Kernel) recordErr(t *Task, err error) {
	k.errs = append(k.errs, LogEntry{Tick: k.clock, Kind: LogError, Task: t.spec.Name, Detail: err.Error(), Err: err})
}

// makeReady 将任务置为就绪态并加入就绪队列尾部。
func (k *Kernel) makeReady(t *Task) {
	t.state = StateReady
	t.sliceLeft = 0
	k.ready = append(k.ready, t)
	k.logf(LogWake, t, "ready")
}

// removeReady 将就绪队列中的任务移除。
func (k *Kernel) removeReady(t *Task) {
	for i, r := range k.ready {
		if r == t {
			k.ready = append(k.ready[:i], k.ready[i+1:]...)
			return
		}
	}
}
