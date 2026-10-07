package sched

import (
	"errors"
	"strings"
	"sync"
	"testing"
)

// receiptsFor 过滤出某任务某类指令的回执。
func receiptsFor(s *Scheduler, kind CmdKind, taskID int) []Receipt {
	var out []Receipt
	for _, r := range s.Receipts() {
		if r.Kind == kind && r.TaskID == taskID {
			out = append(out, r)
		}
	}
	return out
}

// TestInterruptWakesSleepingTask 中断指令让睡眠任务提前就绪并触发抢占。
func TestInterruptWakesSleepingTask(t *testing.T) {
	s := New(Config{Preemptive: true})
	a := &Task{ID: 1, Name: "a", Priority: 5}
	b := &Task{ID: 2, Name: "b", Priority: 1}
	s.AddTask(a, []Op{{Kind: OpSleep, Ticks: 10}, {Kind: OpCompute, Ticks: 2}})
	s.AddTask(b, []Op{{Kind: OpCompute, Ticks: 20}})
	s.Submit(Command{Tick: 3, Kind: CmdInterrupt, TaskID: 1})
	if err := s.Run(10000); err != nil {
		t.Fatalf("run: %v", err)
	}
	recs := receiptsFor(s, CmdInterrupt, 1)
	if len(recs) != 1 || recs[0].Status != CmdApplied || recs[0].Tick != 3 {
		t.Fatalf("receipt = %+v, want applied at tick 3", recs)
	}
	// a 在 tick 3 被中断唤醒并抢占 b。
	var wakeTick, preemptAt3 bool
	for _, e := range s.Log() {
		if e.What == "interrupt: task 1 ready" && e.Tick == 3 {
			wakeTick = true
		}
		if strings.Contains(e.What, "preempt task 2") && e.Tick == 3 {
			preemptAt3 = true
		}
	}
	if !wakeTick || !preemptAt3 {
		t.Fatalf("expected interrupt wake + preemption at tick 3: wake=%v preempt=%v", wakeTick, preemptAt3)
	}
	if a.State() != StateDone || b.State() != StateDone {
		t.Fatalf("states: a=%v b=%v", a.State(), b.State())
	}
	// 时钟 = b 先跑 3 tick + a 跑 2 tick + b 跑剩余 17 tick = 22，无空闲。
	if got := s.Clock().Now(); got != 22 {
		t.Fatalf("clock = %d, want 22", got)
	}
	t.Logf("covered: interrupt wakes sleeping task, preemption at command tick, clock=%d", s.Clock().Now())
}

// TestSuspendResume 挂起使任务停止被调度，恢复后从断点继续。
func TestSuspendResume(t *testing.T) {
	s := New(Config{Preemptive: true})
	a := &Task{ID: 1, Name: "a", Priority: 5}
	b := &Task{ID: 2, Name: "b", Priority: 1}
	s.AddTask(a, []Op{{Kind: OpCompute, Ticks: 10}})
	s.AddTask(b, []Op{{Kind: OpCompute, Ticks: 2}})
	s.Submit(Command{Tick: 2, Kind: CmdSuspend, TaskID: 1})
	s.Submit(Command{Tick: 6, Kind: CmdResume, TaskID: 1})
	if err := s.Run(10000); err != nil {
		t.Fatalf("run: %v", err)
	}
	for _, r := range s.Receipts() {
		if r.Status != CmdApplied {
			t.Fatalf("receipt rejected: %+v", r)
		}
	}
	// a 在 [2,6) 期间不得被调度。
	for _, e := range s.Log() {
		if e.What == "dispatch task 1" && e.Tick >= 2 && e.Tick < 6 {
			t.Fatalf("suspended task dispatched: %+v", e)
		}
	}
	// 时钟 = a 跑 2 + b 跑 2 + 空闲 2 + a 跑 8 = 14。
	if got := s.Clock().Now(); got != 14 {
		t.Fatalf("clock = %d, want 14", got)
	}
	if a.State() != StateDone || b.State() != StateDone {
		t.Fatalf("states: a=%v b=%v", a.State(), b.State())
	}
	t.Logf("covered: suspend/resume window [2,6), clock=%d", s.Clock().Now())
}

// TestSetPriorityTriggersPreemption 运行中提升优先级立即触发抢占判定。
func TestSetPriorityTriggersPreemption(t *testing.T) {
	s := New(Config{Preemptive: true})
	a := &Task{ID: 1, Name: "a", Priority: 5}
	b := &Task{ID: 2, Name: "b", Priority: 1}
	s.AddTask(a, []Op{{Kind: OpCompute, Ticks: 4}})
	s.AddTask(b, []Op{{Kind: OpCompute, Ticks: 4}})
	s.Submit(Command{Tick: 1, Kind: CmdSetPriority, TaskID: 2, Priority: 9})
	if err := s.Run(10000); err != nil {
		t.Fatalf("run: %v", err)
	}
	var preemptAt1 bool
	for _, e := range s.Log() {
		if strings.Contains(e.What, "preempt task 1") && e.Tick == 1 {
			preemptAt1 = true
		}
	}
	if !preemptAt1 {
		t.Fatal("expected preemption of task 1 at tick 1 after priority raise")
	}
	// b 应先于 a 完成。
	bDone, aDone := -1, -1
	for i, e := range s.Log() {
		if e.What == "task 2 done" && bDone < 0 {
			bDone = i
		}
		if e.What == "task 1 done" && aDone < 0 {
			aDone = i
		}
	}
	if bDone < 0 || aDone < 0 || bDone > aDone {
		t.Fatalf("b should finish first: b@%d a@%d", bDone, aDone)
	}
	t.Logf("covered: runtime priority raise -> immediate preemption")
}

// TestSetPriorityNoPreemptWhenDisabled 关闭抢占时调优先级不能绕过调度策略。
func TestSetPriorityNoPreemptWhenDisabled(t *testing.T) {
	s := New(Config{Preemptive: false})
	a := &Task{ID: 1, Name: "a", Priority: 2}
	b := &Task{ID: 2, Name: "b", Priority: 1}
	s.AddTask(a, []Op{{Kind: OpCompute, Ticks: 4}})
	s.AddTask(b, []Op{{Kind: OpCompute, Ticks: 1}})
	s.Submit(Command{Tick: 1, Kind: CmdSetPriority, TaskID: 2, Priority: 9})
	if err := s.Run(10000); err != nil {
		t.Fatalf("run: %v", err)
	}
	for _, e := range s.Log() {
		if strings.Contains(e.What, "preempt") {
			t.Fatalf("unexpected preemption with Preemptive=false: %v", e)
		}
	}
	// a 必须先完成，b 在其后运行（尽管 b 优先级已提到 9）。
	aDone, bDispatch := -1, -1
	for i, e := range s.Log() {
		if e.What == "task 1 done" && aDone < 0 {
			aDone = i
		}
		if e.What == "dispatch task 2" && bDispatch < 0 {
			bDispatch = i
		}
	}
	if aDone < 0 || bDispatch < 0 || aDone > bDispatch {
		t.Fatalf("a should run to completion first: aDone@%d bDispatch@%d", aDone, bDispatch)
	}
	t.Logf("covered: priority change does not bypass non-preemptive mode")
}

// TestSetPriorityInheritance 持锁任务被降优先级后继承仍然有效；
// 等待者优先级变化立即重排等待队列并刷新继承。
func TestSetPriorityInheritance(t *testing.T) {
	s := New(Config{Preemptive: true})
	m := &Mutex{Name: "res"}
	low := &Task{ID: 1, Name: "low", Priority: 1}
	mid := &Task{ID: 2, Name: "mid", Priority: 5}
	hi := &Task{ID: 3, Name: "hi", Priority: 9}
	s.AddTask(low, []Op{
		{Kind: OpLock, Mutex: m},
		{Kind: OpCompute, Ticks: 4},
		{Kind: OpUnlock, Mutex: m},
	})
	s.AddTask(mid, []Op{{Kind: OpSleep, Ticks: 3}, {Kind: OpCompute, Ticks: 1}})
	s.AddTask(hi, []Op{
		{Kind: OpSleep, Ticks: 1},
		{Kind: OpLock, Mutex: m},
		{Kind: OpCompute, Ticks: 1},
		{Kind: OpUnlock, Mutex: m},
	})
	// tick 2：把持锁的 low 从 1 降到 0。若继承失效，mid(5) 会抢占 low(0)。
	s.Submit(Command{Tick: 2, Kind: CmdSetPriority, TaskID: 1, Priority: 0})
	if err := s.Run(10000); err != nil {
		t.Fatalf("run: %v", err)
	}
	recs := receiptsFor(s, CmdSetPriority, 1)
	if len(recs) != 1 || recs[0].Status != CmdApplied {
		t.Fatalf("receipt = %+v", recs)
	}
	// hi 在 tick 1 唤醒时会抢占 low 一次（执行加锁操作），这是正常抢占；
	// low 在 tick 4 释放锁后继承消失，被 hi 抢占也是正常的。
	// 关键断言：low 持锁且继承生效的窗口（tick 2、3）内，
	// 即使基础优先级被降到 0，醒来的 mid(5) 也不得抢占它。
	for _, e := range s.Log() {
		if strings.Contains(e.What, "preempt task 1") && e.Tick >= 2 && e.Tick < 4 {
			t.Fatalf("lock holder with inheritance must not be preempted: %v", e)
		}
	}
	// mid 在 tick 3 醒来，但 low 持锁继承 9，mid 必须等 hi 完成（tick 5）后才能运行。
	for _, e := range s.Log() {
		if e.What == "dispatch task 2" && e.Tick >= 3 && e.Tick < 5 {
			t.Fatalf("mid ran while holder inherited priority 9: %v", e)
		}
	}
	// hi 必须先于 mid 的计算完成。
	hiDoneTick, midRunTick := -1, -1
	for _, e := range s.Log() {
		if e.What == "task 3 done" && hiDoneTick < 0 {
			hiDoneTick = int(e.Tick)
		}
		if e.What == "dispatch task 2" && e.Tick >= 3 && midRunTick < 0 {
			midRunTick = int(e.Tick)
		}
	}
	if hiDoneTick < 0 || midRunTick < 0 || hiDoneTick > midRunTick {
		t.Fatalf("hi should finish before mid runs: hi@%d mid@%d", hiDoneTick, midRunTick)
	}
	t.Logf("covered: holder priority lowered, inheritance preserved, no preemption")
}

// TestWaiterPriorityReorder 等待者升优先级后立即排到等待队列队首并先获得锁。
func TestWaiterPriorityReorder(t *testing.T) {
	s := New(Config{Preemptive: true})
	m := &Mutex{Name: "res"}
	holder := &Task{ID: 1, Name: "holder", Priority: 8}
	w1 := &Task{ID: 2, Name: "w1", Priority: 9}
	w2 := &Task{ID: 3, Name: "w2", Priority: 7}
	s.AddTask(holder, []Op{
		{Kind: OpLock, Mutex: m},
		{Kind: OpCompute, Ticks: 4},
		{Kind: OpUnlock, Mutex: m},
	})
	s.AddTask(w1, []Op{
		{Kind: OpSleep, Ticks: 1},
		{Kind: OpLock, Mutex: m},
		{Kind: OpCompute, Ticks: 1},
		{Kind: OpUnlock, Mutex: m},
	})
	s.AddTask(w2, []Op{
		{Kind: OpSleep, Ticks: 1},
		{Kind: OpLock, Mutex: m},
		{Kind: OpCompute, Ticks: 1},
		{Kind: OpUnlock, Mutex: m},
	})
	// tick 2：w1(9)、w2(7) 都在等待队列中；把 w2 提到 10，应排到队首。
	s.Submit(Command{Tick: 2, Kind: CmdSetPriority, TaskID: 3, Priority: 10})
	if err := s.Run(10000); err != nil {
		t.Fatalf("run: %v", err)
	}
	// 锁移交顺序：holder -> w2 -> w1。
	var handoff []int
	for _, e := range s.Log() {
		if e.What == "lock res handed to task 3" {
			handoff = append(handoff, 3)
		}
		if e.What == "lock res handed to task 2" {
			handoff = append(handoff, 2)
		}
	}
	if len(handoff) != 2 || handoff[0] != 3 || handoff[1] != 2 {
		t.Fatalf("handoff order = %v, want [3 2] (w2 raised to front)", handoff)
	}
	t.Logf("covered: waiter priority raise reorders wait queue, handoff=%v", handoff)
}

// TestSuspendLockWaiter 等锁任务被挂起：锁归属与等待队列不悬空，
// 锁可移交给挂起任务，恢复后正常继续。
func TestSuspendLockWaiter(t *testing.T) {
	s := New(Config{Preemptive: true})
	m := &Mutex{Name: "res"}
	low := &Task{ID: 1, Name: "low", Priority: 1}
	hi := &Task{ID: 2, Name: "hi", Priority: 9}
	s.AddTask(low, []Op{
		{Kind: OpLock, Mutex: m},
		{Kind: OpCompute, Ticks: 4},
		{Kind: OpUnlock, Mutex: m},
	})
	s.AddTask(hi, []Op{
		{Kind: OpSleep, Ticks: 1},
		{Kind: OpLock, Mutex: m},
		{Kind: OpCompute, Ticks: 1},
		{Kind: OpUnlock, Mutex: m},
	})
	s.Submit(Command{Tick: 2, Kind: CmdSuspend, TaskID: 2})
	// 第一阶段：跑到锁移交之后、恢复之前。
	if err := s.RunUntil(6, 10000); err != nil {
		t.Fatalf("run phase 1: %v", err)
	}
	// low 已在 tick 4 释放锁并移交给 hi；hi 挂起中但必须是锁的持有者。
	if m.Owner() != hi {
		t.Fatalf("owner = %v, want hi (suspended task keeps ownership)", m.Owner())
	}
	if hi.State() != StateSuspended {
		t.Fatalf("hi state = %v, want Suspended", hi.State())
	}
	if len(m.Waiters()) != 0 {
		t.Fatalf("waiters = %v, want empty after handoff", m.Waiters())
	}
	// 第二阶段：恢复 hi，它应继续执行临界区并释放锁。
	s.Submit(Command{Tick: 6, Kind: CmdResume, TaskID: 2})
	if err := s.Run(10000); err != nil {
		t.Fatalf("run phase 2: %v", err)
	}
	if hi.State() != StateDone || low.State() != StateDone {
		t.Fatalf("states: hi=%v low=%v", hi.State(), low.State())
	}
	if m.Owner() != nil {
		t.Fatalf("lock not released at end: owner=%v", m.Owner())
	}
	for _, r := range s.Receipts() {
		if r.Status != CmdApplied {
			t.Fatalf("receipt rejected: %+v", r)
		}
	}
	t.Logf("covered: suspended lock waiter keeps ownership, resumes cleanly")
}

// TestAddTaskAtRuntime 运行中加入非周期与周期任务。
func TestAddTaskAtRuntime(t *testing.T) {
	s := New(Config{Preemptive: true})
	a := &Task{ID: 1, Name: "a", Priority: 1}
	s.AddTask(a, []Op{{Kind: OpCompute, Ticks: 10}})
	s.Submit(Command{
		Tick: 3, Kind: CmdAddTask,
		Task: &Task{ID: 2, Name: "joined", Priority: 7},
		Program: []Op{
			{Kind: OpCompute, Ticks: 2},
		},
	})
	s.Submit(Command{
		Tick: 4, Kind: CmdAddTask,
		Task:    &Task{ID: 3, Name: "periodic", Priority: 6},
		Program: []Op{{Kind: OpCompute, Ticks: 1}},
		Period:  4,
	})
	if err := s.RunUntil(13, 10000); err != nil {
		t.Fatalf("run: %v", err)
	}
	recs := s.Receipts()
	if len(recs) != 2 || recs[0].Status != CmdApplied || recs[1].Status != CmdApplied {
		t.Fatalf("receipts = %+v", recs)
	}
	joined := s.findTask(2)
	if joined == nil || joined.State() != StateDone {
		t.Fatalf("joined task state = %v", joined.State())
	}
	// 周期任务在加入时刻 4 首次释放，之后每 4 tick 一次：4, 8, 12。
	var releases []uint64
	for _, e := range s.Log() {
		if strings.HasPrefix(e.What, "release task 3 job") {
			releases = append(releases, e.Tick)
		}
	}
	if len(releases) != 3 || releases[0] != 4 || releases[1] != 8 || releases[2] != 12 {
		t.Fatalf("periodic releases = %v, want [4 8 12]", releases)
	}
	t.Logf("covered: runtime join (one-shot + periodic), releases=%v", releases)
}

// TestCommandRejections 指向非法状态任务的指令必须给出可区分的拒绝原因。
func TestCommandRejections(t *testing.T) {
	s := New(Config{Preemptive: true})
	m := &Mutex{Name: "res"}
	holder := &Task{ID: 1, Name: "holder", Priority: 5}
	waiter := &Task{ID: 2, Name: "waiter", Priority: 6}
	done := &Task{ID: 3, Name: "done", Priority: 1}
	s.AddTask(holder, []Op{
		{Kind: OpLock, Mutex: m},
		{Kind: OpCompute, Ticks: 6},
		{Kind: OpUnlock, Mutex: m},
	})
	s.AddTask(waiter, []Op{
		{Kind: OpSleep, Ticks: 1},
		{Kind: OpLock, Mutex: m},
		{Kind: OpCompute, Ticks: 1},
		{Kind: OpUnlock, Mutex: m},
	})
	s.AddTask(done, []Op{{Kind: OpCompute, Ticks: 1}})
	// tick 2：waiter 已阻塞在锁上，holder 就绪（被抢占）。
	s.Submit(Command{Tick: 2, Kind: CmdInterrupt, TaskID: 2})  // 等锁中 -> ErrCmdTaskOnLock
	s.Submit(Command{Tick: 2, Kind: CmdInterrupt, TaskID: 1})  // 就绪中 -> ErrCmdTaskNotBlocked
	s.Submit(Command{Tick: 2, Kind: CmdSuspend, TaskID: 2})    // 等锁任务挂起 -> Applied
	s.Submit(Command{Tick: 2, Kind: CmdSuspend, TaskID: 2})    // 已挂起 -> ErrCmdTaskSuspended
	s.Submit(Command{Tick: 2, Kind: CmdInterrupt, TaskID: 2})  // 挂起中 -> ErrCmdTaskSuspended
	s.Submit(Command{Tick: 2, Kind: CmdResume, TaskID: 1})     // 未挂起 -> ErrCmdTaskNotSuspended
	s.Submit(Command{Tick: 2, Kind: CmdResume, TaskID: 2})     // 恢复等锁 -> Applied
	s.Submit(Command{Tick: 2, Kind: CmdInterrupt, TaskID: 99}) // 不存在 -> ErrCmdTaskNotFound
	s.Submit(Command{Tick: 2, Kind: CmdAddTask, Task: &Task{ID: 1, Name: "dup"}, Program: nil})
	s.Submit(Command{Tick: 2, Kind: CmdAddTask}) // 无任务 -> ErrCmdNilTask
	// tick 20：done 已结束。
	s.Submit(Command{Tick: 20, Kind: CmdInterrupt, TaskID: 3})   // -> ErrCmdTaskDone
	s.Submit(Command{Tick: 20, Kind: CmdSuspend, TaskID: 3})     // -> ErrCmdTaskDone
	s.Submit(Command{Tick: 20, Kind: CmdSetPriority, TaskID: 3}) // -> ErrCmdTaskDone
	if err := s.Run(10000); err != nil {
		t.Fatalf("run: %v", err)
	}
	want := []struct {
		kind   CmdKind
		taskID int
		status CmdStatus
		reason error
	}{
		{CmdInterrupt, 2, CmdRejected, ErrCmdTaskOnLock},
		{CmdInterrupt, 1, CmdRejected, ErrCmdTaskNotBlocked},
		{CmdSuspend, 2, CmdApplied, nil},
		{CmdSuspend, 2, CmdRejected, ErrCmdTaskSuspended},
		{CmdInterrupt, 2, CmdRejected, ErrCmdTaskSuspended},
		{CmdResume, 1, CmdRejected, ErrCmdTaskNotSuspended},
		{CmdResume, 2, CmdApplied, nil},
		{CmdInterrupt, 99, CmdRejected, ErrCmdTaskNotFound},
		{CmdAddTask, 1, CmdRejected, ErrCmdDuplicateTask},
		{CmdAddTask, 0, CmdRejected, ErrCmdNilTask},
		{CmdInterrupt, 3, CmdRejected, ErrCmdTaskDone},
		{CmdSuspend, 3, CmdRejected, ErrCmdTaskDone},
		{CmdSetPriority, 3, CmdRejected, ErrCmdTaskDone},
	}
	recs := s.Receipts()
	if len(recs) != len(want) {
		t.Fatalf("receipt count = %d, want %d", len(recs), len(want))
	}
	for i, w := range want {
		r := recs[i]
		if r.Kind != w.kind || r.TaskID != w.taskID || r.Status != w.status || !errors.Is(r.Reason, w.reason) {
			t.Errorf("receipt[%d] = {%v task=%d status=%d reason=%v}, want {%v task=%d status=%d reason=%v}",
				i, r.Kind, r.TaskID, r.Status, r.Reason, w.kind, w.taskID, w.status, w.reason)
		}
	}
	// 等锁任务被挂起又恢复后，整个场景必须正常跑完。
	for _, tk := range []*Task{holder, waiter, done} {
		if tk.State() != StateDone {
			t.Fatalf("task %d not done: %v", tk.ID, tk.State())
		}
	}
	t.Logf("covered: %d distinguishable rejection reasons, all tasks completed", len(want))
}

// TestCommandDoesNotAdvanceClock 下发与处理指令都不推进虚拟时钟；
// 系统空闲时时钟可跳转到指令生效时刻。
func TestCommandDoesNotAdvanceClock(t *testing.T) {
	s := New(Config{Preemptive: true})
	a := &Task{ID: 1, Name: "a", Priority: 3}
	s.AddTask(a, []Op{{Kind: OpSleep, Ticks: 5}, {Kind: OpCompute, Ticks: 1}})
	s.Submit(Command{Tick: 2, Kind: CmdSetPriority, TaskID: 1, Priority: 4})
	// 提交后时钟不变。
	if got := s.Clock().Now(); got != 0 {
		t.Fatalf("clock advanced by Submit: %d", got)
	}
	if err := s.Run(10000); err != nil {
		t.Fatalf("run: %v", err)
	}
	recs := receiptsFor(s, CmdSetPriority, 1)
	if len(recs) != 1 || recs[0].Tick != 2 {
		t.Fatalf("receipt = %+v, want applied at tick 2", recs)
	}
	// 时钟路径：空闲跳到 2（指令时刻）-> 空闲跳到 5（唤醒）-> 计算 1 = 6。
	if got := s.Clock().Now(); got != 6 {
		t.Fatalf("clock = %d, want 6", got)
	}
	var idle2, idle5 bool
	for _, e := range s.Log() {
		if e.What == "idle until 2" {
			idle2 = true
		}
		if e.What == "idle until 5" {
			idle5 = true
		}
	}
	if !idle2 || !idle5 {
		t.Fatalf("expected idle jumps to command tick 2 and wakeup 5: %v %v", idle2, idle5)
	}
	t.Logf("covered: command processing is clock-neutral, idle jump targets command tick")
}

// TestSameTickCommandEventOrdering 指令与内核事件撞在同一 tick 时顺序确定：
// 先事件后指令，重复执行结果完全一致。
func TestSameTickCommandEventOrdering(t *testing.T) {
	scenario := func() ([]LogEntry, []Receipt) {
		s := New(Config{Preemptive: true})
		p := &Task{ID: 1, Name: "p", Priority: 3}
		q := &Task{ID: 2, Name: "q", Priority: 5}
		s.AddPeriodic(p, []Op{{Kind: OpCompute, Ticks: 1}}, 4, 0, 0, PolicyWarn)
		s.AddTask(q, []Op{{Kind: OpCompute, Ticks: 10}})
		// tick 4 与周期释放撞车；tick 4 同时多条指令，按提交序处理。
		s.Submit(Command{Tick: 4, Kind: CmdSetPriority, TaskID: 1, Priority: 7})
		s.Submit(Command{Tick: 4, Kind: CmdSetPriority, TaskID: 2, Priority: 2})
		if err := s.RunUntil(9, 100000); err != nil {
			t.Fatalf("run: %v", err)
		}
		return s.Log(), s.Receipts()
	}
	log1, rec1 := scenario()
	log2, rec2 := scenario()
	if len(log1) != len(log2) {
		t.Fatalf("log length differs: %d vs %d", len(log1), len(log2))
	}
	for i := range log1 {
		if log1[i] != log2[i] {
			t.Fatalf("log differs at %d: %+v vs %+v", i, log1[i], log2[i])
		}
	}
	if len(rec1) != len(rec2) {
		t.Fatalf("receipt length differs: %d vs %d", len(rec1), len(rec2))
	}
	for i := range rec1 {
		if rec1[i] != rec2[i] {
			t.Fatalf("receipt differs at %d: %+v vs %+v", i, rec1[i], rec2[i])
		}
	}
	// 同一 tick 4 内：release 事件日志必须出现在指令日志之前。
	releaseIdx, cmdIdx := -1, -1
	for i, e := range log1 {
		if e.Tick == 4 && strings.HasPrefix(e.What, "release task 1 job") && releaseIdx < 0 {
			releaseIdx = i
		}
		if e.Tick == 4 && strings.HasPrefix(e.What, "command: task 1 priority") && cmdIdx < 0 {
			cmdIdx = i
		}
	}
	if releaseIdx < 0 || cmdIdx < 0 || releaseIdx > cmdIdx {
		t.Fatalf("event must precede command at same tick: release@%d cmd@%d", releaseIdx, cmdIdx)
	}
	t.Logf("covered: same-tick event/command ordering, %d log entries replayed identically", len(log1))
}

// TestCommandStormDeterminism 任务与指令上量后结果稳定、无顺序漂移。
func TestCommandStormDeterminism(t *testing.T) {
	scenario := func() ([]LogEntry, []Receipt, uint64) {
		s := New(Config{Preemptive: true, TimeSlice: 3})
		// 30 个任务，混合计算/睡眠/让出。
		for i := 0; i < 30; i++ {
			tk := &Task{ID: i + 1, Name: "t", Priority: (i * 7) % 10}
			s.AddTask(tk, []Op{
				{Kind: OpCompute, Ticks: uint64(i%5 + 1)},
				{Kind: OpSleep, Ticks: uint64(i%3 + 1)},
				{Kind: OpYield},
				{Kind: OpCompute, Ticks: uint64(i%4 + 1)},
			})
		}
		// 200 条指令：确定性伪随机分布在各 tick 与各任务上。
		seed := uint64(12345)
		next := func() uint64 {
			seed = seed*6364136223846793005 + 1442695040888963407
			return seed >> 33
		}
		for i := 0; i < 200; i++ {
			kind := CmdKind(next() % 4) // Interrupt/Suspend/Resume/SetPriority
			cmd := Command{
				Tick:     next() % 40,
				Kind:     kind,
				TaskID:   int(next()%30) + 1,
				Priority: int(next() % 12),
			}
			s.Submit(cmd)
		}
		if err := s.Run(1000000); err != nil {
			t.Fatalf("run: %v", err)
		}
		return s.Log(), s.Receipts(), s.Clock().Now()
	}
	log1, rec1, clk1 := scenario()
	log2, rec2, clk2 := scenario()
	if clk1 != clk2 {
		t.Fatalf("clock differs: %d vs %d", clk1, clk2)
	}
	if len(rec1) != 200 || len(rec2) != 200 {
		t.Fatalf("receipt count = %d/%d, want 200 each", len(rec1), len(rec2))
	}
	if len(log1) != len(log2) {
		t.Fatalf("log length differs: %d vs %d", len(log1), len(log2))
	}
	for i := range log1 {
		if log1[i] != log2[i] {
			t.Fatalf("log differs at %d: %+v vs %+v", i, log1[i], log2[i])
		}
	}
	for i := range rec1 {
		if rec1[i] != rec2[i] {
			t.Fatalf("receipt differs at %d: %+v vs %+v", i, rec1[i], rec2[i])
		}
	}
	t.Logf("covered: 30 tasks x 200 commands, clock=%d, %d log entries, fully deterministic",
		clk1, len(log1))
}

// TestConcurrentSubmit 从其它 goroutine 与 Run 并发下发指令（配合 -race 验证）。
func TestConcurrentSubmit(t *testing.T) {
	s := New(Config{Preemptive: true})
	p := &Task{ID: 1, Name: "p", Priority: 3}
	s.AddPeriodic(p, []Op{{Kind: OpCompute, Ticks: 1}}, 2, 0, 0, PolicyWarn)
	var wg sync.WaitGroup
	for g := 0; g < 8; g++ {
		wg.Add(1)
		go func(g int) {
			defer wg.Done()
			for i := 0; i < 25; i++ {
				s.Submit(Command{
					Tick:     uint64((g*25 + i) % 100),
					Kind:     CmdSetPriority,
					TaskID:   1,
					Priority: (g + i) % 10,
				})
			}
		}(g)
	}
	if err := s.RunUntil(100, 1000000); err != nil {
		t.Fatalf("run: %v", err)
	}
	wg.Wait()
	// 在时钟 100 之前到期的指令都必须有回执；并发提交不产生竞态。
	recs := s.Receipts()
	for _, r := range recs {
		if r.Status != CmdApplied {
			t.Fatalf("setpriority on live task rejected: %+v", r)
		}
	}
	t.Logf("covered: 8 goroutines x 25 concurrent submits, %d receipts, no data race", len(recs))
}
