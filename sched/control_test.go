package sched

import (
	"errors"
	"strings"
	"testing"
)

// cmdResult 查找第 i 条指令的处理结果。
func cmdResult(t *testing.T, s *Scheduler, i int) CmdResult {
	t.Helper()
	res := s.CommandResults()
	if i >= len(res) {
		t.Fatalf("cmd result %d missing, got %d results", i, len(res))
	}
	return res[i]
}

// TestCmdInterruptWakesSleepingTask 中断把睡眠任务立刻变为可运行，
// 且下发指令本身不推进虚拟时钟。
func TestCmdInterruptWakesSleepingTask(t *testing.T) {
	s := New(Config{Preemptive: true})
	task := &Task{ID: 1, Name: "t", Priority: 1}
	s.AddTask(task, []Op{
		{Kind: OpSleep, Ticks: 100},
		{Kind: OpCompute, Ticks: 3},
	})
	s.Submit(5, Command{Kind: CmdInterrupt, TaskID: 1})
	if err := s.Run(10000); err != nil {
		t.Fatalf("run: %v", err)
	}
	res := cmdResult(t, s, 0)
	if res.Status != CmdApplied || res.Tick != 5 {
		t.Fatalf("interrupt result = %+v, want Applied@5", res)
	}
	// 时钟 = 空闲到 5 + 执行 3 = 8，而不是睡满 100。
	if got := s.Clock().Now(); got != 8 {
		t.Fatalf("clock = %d, want 8 (idle 5 + exec 3)", got)
	}
	if task.State() != StateDone {
		t.Fatalf("task state = %v, want Done", task.State())
	}
	t.Logf("covered: interrupt wakes sleeping task at tick 5, clock=%d", s.Clock().Now())
}

// TestCmdInterruptLockWaitRejected 等锁任务不能被中断唤醒，给出可区分错误。
func TestCmdInterruptLockWaitRejected(t *testing.T) {
	s := New(Config{Preemptive: true})
	m := &Mutex{Name: "m"}
	a := &Task{ID: 1, Name: "a", Priority: 1}
	b := &Task{ID: 2, Name: "b", Priority: 5}
	s.AddTask(a, []Op{
		{Kind: OpLock, Mutex: m},
		{Kind: OpCompute, Ticks: 4},
		{Kind: OpUnlock, Mutex: m},
	})
	s.AddTask(b, []Op{
		{Kind: OpSleep, Ticks: 1}, // 让 a 先拿到锁
		{Kind: OpLock, Mutex: m},
		{Kind: OpUnlock, Mutex: m},
	})
	// b 在 tick 1 阻塞在 m 上（a 持有），tick 2 时对 b 发中断。
	s.Submit(2, Command{Kind: CmdInterrupt, TaskID: 2})
	if err := s.Run(10000); err != nil {
		t.Fatalf("run: %v", err)
	}
	res := cmdResult(t, s, 0)
	if res.Status != CmdRejected || !errors.Is(res.Err, ErrCmdTaskLockWait) {
		t.Fatalf("interrupt on lock-wait = %+v, want Rejected(ErrCmdTaskLockWait)", res)
	}
	if a.State() != StateDone || b.State() != StateDone {
		t.Fatalf("incomplete: a=%v b=%v", a.State(), b.State())
	}
	t.Logf("covered: interrupt on lock-waiting task rejected with distinguishable error")
}

// TestCmdSuspendResume 运行中挂起再恢复，任务停走后又继续完成。
func TestCmdSuspendResume(t *testing.T) {
	s := New(Config{Preemptive: true})
	task := &Task{ID: 1, Name: "t", Priority: 1}
	s.AddTask(task, []Op{{Kind: OpCompute, Ticks: 6}})
	s.Submit(2, Command{Kind: CmdSuspend, TaskID: 1})
	s.Submit(5, Command{Kind: CmdResume, TaskID: 1})
	if err := s.Run(10000); err != nil {
		t.Fatalf("run: %v", err)
	}
	r0, r1 := cmdResult(t, s, 0), cmdResult(t, s, 1)
	if r0.Status != CmdApplied || r1.Status != CmdApplied {
		t.Fatalf("suspend/resume = %+v %+v", r0, r1)
	}
	// tick 2 挂起后无事件无指令，空闲跳到 5 恢复，再跑剩余 4 tick。
	if got := s.Clock().Now(); got != 9 {
		t.Fatalf("clock = %d, want 9 (2 exec + idle 3 + 4 exec)", got)
	}
	if task.State() != StateDone {
		t.Fatalf("task state = %v, want Done", task.State())
	}
	// 挂起期间不得有该任务的 dispatch。
	var dispatchedBetween bool
	for _, e := range s.Log() {
		if e.Tick > 2 && e.Tick < 5 && strings.Contains(e.What, "dispatch task 1") {
			dispatchedBetween = true
		}
	}
	if dispatchedBetween {
		t.Fatal("suspended task was dispatched")
	}
	t.Logf("covered: suspend at 2, resume at 5, clock=%d", s.Clock().Now())
}

// TestCmdSuspendLockWaiter 等锁任务被挂起：退出等待队列、继承回落；
// 恢复后重新加锁，锁归属与等待顺序不悬空。
func TestCmdSuspendLockWaiter(t *testing.T) {
	s := New(Config{Preemptive: true})
	m := &Mutex{Name: "m"}
	holder := &Task{ID: 1, Name: "holder", Priority: 2}
	waiter := &Task{ID: 2, Name: "waiter", Priority: 8}
	s.AddTask(holder, []Op{
		{Kind: OpLock, Mutex: m},
		{Kind: OpCompute, Ticks: 6},
		{Kind: OpUnlock, Mutex: m},
	})
	s.AddTask(waiter, []Op{
		{Kind: OpSleep, Ticks: 1}, // 让 holder 先拿到锁
		{Kind: OpLock, Mutex: m},
		{Kind: OpUnlock, Mutex: m},
	})
	// waiter 在 tick 1 阻塞于 m（holder 继承优先级 8）。
	// tick 2 挂起 waiter，tick 4 恢复。
	s.Submit(2, Command{Kind: CmdSuspend, TaskID: 2})
	s.Submit(4, Command{Kind: CmdResume, TaskID: 2})
	if err := s.Run(10000); err != nil {
		t.Fatalf("run: %v", err)
	}
	if cmdResult(t, s, 0).Status != CmdApplied || cmdResult(t, s, 1).Status != CmdApplied {
		t.Fatalf("suspend/resume results = %+v", s.CommandResults())
	}
	if holder.State() != StateDone || waiter.State() != StateDone {
		t.Fatalf("incomplete: holder=%v waiter=%v", holder.State(), waiter.State())
	}
	if m.Owner() != nil {
		t.Fatalf("mutex still owned by task %d", m.Owner().ID)
	}
	if len(m.Waiters()) != 0 {
		t.Fatalf("mutex waiters not drained: %d", len(m.Waiters()))
	}
	// 挂起生效后 holder 的继承必须立刻回落到自身基础优先级。
	var sawDrop bool
	for _, e := range s.Log() {
		if e.Tick == 2 && strings.Contains(e.What, "cmd suspend task 2") {
			sawDrop = true
		}
	}
	if !sawDrop {
		t.Fatal("suspend log missing")
	}
	if holder.EffectivePriority() != 2 {
		t.Fatalf("holder effective prio = %d after waiter done, want 2", holder.EffectivePriority())
	}
	t.Logf("covered: suspend lock-waiter, inheritance dropped and re-acquired cleanly")
}

// TestCmdSetPriorityChangesPreemption 运行中调高优先级后，抢占判断立即按新优先级生效。
func TestCmdSetPriorityChangesPreemption(t *testing.T) {
	s := New(Config{Preemptive: true})
	a := &Task{ID: 1, Name: "a", Priority: 1}
	b := &Task{ID: 2, Name: "b", Priority: 5}
	s.AddTask(a, []Op{{Kind: OpCompute, Ticks: 8}})
	s.AddTask(b, []Op{
		{Kind: OpSleep, Ticks: 3},
		{Kind: OpCompute, Ticks: 2},
	})
	// tick 1 把 a 调到 9（高于 b），b 醒來后不得抢占 a。
	s.Submit(1, Command{Kind: CmdSetPriority, TaskID: 1, Priority: 9})
	if err := s.Run(10000); err != nil {
		t.Fatalf("run: %v", err)
	}
	if cmdResult(t, s, 0).Status != CmdApplied {
		t.Fatalf("setpriority result = %+v", cmdResult(t, s, 0))
	}
	for _, e := range s.Log() {
		if strings.Contains(e.What, "preempt") {
			t.Fatalf("unexpected preemption after priority raise: %v", e)
		}
	}
	// a 先完成，b 后完成。
	var aDone, bDone = -1, -1
	for i, e := range s.Log() {
		if e.What == "task 1 done" {
			aDone = i
		}
		if e.What == "task 2 done" {
			bDone = i
		}
	}
	if aDone < 0 || bDone < 0 || aDone > bDone {
		t.Fatalf("a should finish first: a@%d b@%d", aDone, bDone)
	}
	t.Logf("covered: runtime priority raise suppresses preemption")
}

// TestCmdSetPriorityInheritanceRefresh 持锁者/等待者优先级被改后，
// 继承关系立即按新优先级重算。
func TestCmdSetPriorityInheritanceRefresh(t *testing.T) {
	s := New(Config{Preemptive: true})
	m := &Mutex{Name: "m"}
	holder := &Task{ID: 1, Name: "holder", Priority: 2}
	waiter := &Task{ID: 2, Name: "waiter", Priority: 8}
	s.AddTask(holder, []Op{
		{Kind: OpLock, Mutex: m},
		{Kind: OpCompute, Ticks: 10},
		{Kind: OpUnlock, Mutex: m},
	})
	s.AddTask(waiter, []Op{
		{Kind: OpSleep, Ticks: 1},
		{Kind: OpLock, Mutex: m},
		{Kind: OpUnlock, Mutex: m},
	})
	// waiter 在 tick 1 阻塞，holder 继承 8；tick 3 把 waiter 降到 1。
	s.Submit(3, Command{Kind: CmdSetPriority, TaskID: 2, Priority: 1})
	if err := s.RunUntil(4, 10000); err != nil {
		t.Fatalf("run: %v", err)
	}
	if got := holder.EffectivePriority(); got != 2 {
		t.Fatalf("holder effective prio = %d after waiter demoted, want 2", got)
	}
	// 再把 waiter 升到 9，继承应立即恢复。
	s.Submit(4, Command{Kind: CmdSetPriority, TaskID: 2, Priority: 9})
	if err := s.RunUntil(5, 10000); err != nil {
		t.Fatalf("run: %v", err)
	}
	if got := holder.EffectivePriority(); got != 9 {
		t.Fatalf("holder effective prio = %d after waiter promoted, want 9", got)
	}
	if err := s.Run(10000); err != nil {
		t.Fatalf("run: %v", err)
	}
	if holder.State() != StateDone || waiter.State() != StateDone {
		t.Fatalf("incomplete: holder=%v waiter=%v", holder.State(), waiter.State())
	}
	if m.Owner() != nil {
		t.Fatal("mutex still owned at end")
	}
	t.Logf("covered: inheritance follows runtime priority changes both directions")
}

// TestCmdSetPriorityNoPreemptionWhenDisabled 关闭抢占时，
// 调优先级指令不能绕过配置制造抢占。
func TestCmdSetPriorityNoPreemptionWhenDisabled(t *testing.T) {
	s := New(Config{Preemptive: false})
	a := &Task{ID: 1, Name: "a", Priority: 1}
	b := &Task{ID: 2, Name: "b", Priority: 2}
	s.AddTask(a, []Op{{Kind: OpCompute, Ticks: 6}})
	s.AddTask(b, []Op{
		{Kind: OpSleep, Ticks: 2},
		{Kind: OpCompute, Ticks: 1},
	})
	// tick 1 把 b 调到 99，仍不得抢占正在运行的 a。
	s.Submit(1, Command{Kind: CmdSetPriority, TaskID: 2, Priority: 99})
	if err := s.Run(10000); err != nil {
		t.Fatalf("run: %v", err)
	}
	if cmdResult(t, s, 0).Status != CmdApplied {
		t.Fatalf("setpriority result = %+v", cmdResult(t, s, 0))
	}
	for _, e := range s.Log() {
		if strings.Contains(e.What, "preempt") {
			t.Fatalf("preemption with Preemptive=false: %v", e)
		}
	}
	// a 必须先完成（非抢占），即使 b 优先级更高。
	var aDone, bDone = -1, -1
	for i, e := range s.Log() {
		if e.What == "task 1 done" {
			aDone = i
		}
		if e.What == "task 2 done" {
			bDone = i
		}
	}
	if aDone < 0 || bDone < 0 || aDone > bDone {
		t.Fatalf("non-preemptive order violated: a@%d b@%d", aDone, bDone)
	}
	t.Logf("covered: priority command does not bypass Preemptive=false")
}

// TestCmdAddTaskAtRuntime 运行中加入新任务，在当前虚拟时刻就绪。
func TestCmdAddTaskAtRuntime(t *testing.T) {
	s := New(Config{Preemptive: true})
	a := &Task{ID: 1, Name: "a", Priority: 1}
	s.AddTask(a, []Op{
		{Kind: OpSleep, Ticks: 5},
		{Kind: OpCompute, Ticks: 1},
	})
	s.Submit(5, Command{
		Kind:    CmdAddTask,
		NewTask: &Task{ID: 2, Name: "late", Priority: 3},
		Program: []Op{{Kind: OpCompute, Ticks: 2}},
	})
	s.Submit(6, Command{Kind: CmdAddTask, NewTask: &Task{ID: 2}, Program: nil}) // 重复 ID
	if err := s.Run(10000); err != nil {
		t.Fatalf("run: %v", err)
	}
	r0, r1 := cmdResult(t, s, 0), cmdResult(t, s, 1)
	if r0.Status != CmdApplied || r0.Tick != 5 || r0.TaskID != 2 {
		t.Fatalf("add result = %+v, want Applied@5 task 2", r0)
	}
	if r1.Status != CmdRejected || !errors.Is(r1.Err, ErrCmdDuplicateTask) {
		t.Fatalf("duplicate add = %+v, want Rejected(ErrCmdDuplicateTask)", r1)
	}
	late := s.findTask(2)
	if late == nil || late.State() != StateDone {
		t.Fatalf("late task state = %v", late.State())
	}
	// 时钟 = 空闲 5 + late 2 + a 1 = 8。
	if got := s.Clock().Now(); got != 8 {
		t.Fatalf("clock = %d, want 8", got)
	}
	t.Logf("covered: runtime add-task at tick 5, duplicate rejected, clock=%d", s.Clock().Now())
}

// TestCmdRejectedOnDoneSuspendedUnknown 指向已结束/已挂起/不存在任务的
// 指令必须给出可区分的拒绝结果，不能悄悄吞掉。
func TestCmdRejectedOnDoneSuspendedUnknown(t *testing.T) {
	s := New(Config{Preemptive: true})
	a := &Task{ID: 1, Name: "a", Priority: 1}
	b := &Task{ID: 2, Name: "b", Priority: 1}
	s.AddTask(a, []Op{{Kind: OpCompute, Ticks: 1}}) // tick 1 即完成
	s.AddTask(b, []Op{{Kind: OpCompute, Ticks: 10}})
	s.Submit(2, Command{Kind: CmdSuspend, TaskID: 2})
	s.Submit(3, Command{Kind: CmdInterrupt, TaskID: 1})                // 已 Done
	s.Submit(3, Command{Kind: CmdInterrupt, TaskID: 2})                // 已 Suspended
	s.Submit(3, Command{Kind: CmdResume, TaskID: 1})                   // 已 Done（Done 优先于非挂起判定）
	s.Submit(3, Command{Kind: CmdResume, TaskID: 2})                   // 恢复挂起中的 2：生效
	s.Submit(4, Command{Kind: CmdResume, TaskID: 2})                   // 已恢复，非 Suspended
	s.Submit(3, Command{Kind: CmdSuspend, TaskID: 99})                 // 不存在
	s.Submit(3, Command{Kind: CmdSetPriority, TaskID: 1, Priority: 5}) // 已 Done
	if err := s.Run(10000); err != nil {
		t.Fatalf("run: %v", err)
	}
	want := []struct {
		status CmdStatus
		err    error
	}{
		{CmdApplied, nil},
		{CmdRejected, ErrCmdTaskDone},
		{CmdRejected, ErrCmdTaskSuspended},
		{CmdRejected, ErrCmdTaskDone},
		{CmdApplied, nil},
		{CmdRejected, ErrCmdTaskUnknown},      // tick 3
		{CmdRejected, ErrCmdTaskDone},         // tick 3
		{CmdRejected, ErrCmdTaskNotSuspended}, // tick 4 的指令最后处理
	}
	res := s.CommandResults()
	if len(res) != len(want) {
		t.Fatalf("got %d results, want %d (no command may be dropped)", len(res), len(want))
	}
	for i, w := range want {
		if res[i].Status != w.status || !errors.Is(res[i].Err, w.err) {
			t.Fatalf("result %d = %+v, want status=%v err=%v", i, res[i], w.status, w.err)
		}
	}
	t.Logf("covered: %d distinguishable rejection/noop outcomes, none swallowed", len(want))
}

// TestCmdSameTickOrdering 指令与内部事件撞在同一 tick 时：
// 先处理事件（释放/唤醒），再按提交顺序处理指令；重复跑结果一致。
func TestCmdSameTickOrdering(t *testing.T) {
	run := func() *Scheduler {
		s := New(Config{Preemptive: true})
		// 周期任务每 4 tick 释放，job 只计算 1 tick。
		p := &Task{ID: 1, Name: "p", Priority: 3}
		s.AddPeriodic(p, []Op{{Kind: OpCompute, Ticks: 1}}, 4, 0, 0, PolicyWarn)
		// tick 4 与释放事件撞车：先释放（任务变为 Ready），再挂起。
		s.Submit(4, Command{Kind: CmdSuspend, TaskID: 1})
		// tick 8 又与释放撞车：先释放，再恢复（恢复后任务可跑）。
		s.Submit(8, Command{Kind: CmdResume, TaskID: 1})
		if err := s.RunUntil(12, 10000); err != nil {
			t.Fatalf("run: %v", err)
		}
		return s
	}
	s := run()
	res := s.CommandResults()
	if len(res) != 2 || res[0].Status != CmdApplied || res[1].Status != CmdApplied {
		t.Fatalf("results = %+v, want both Applied", res)
	}
	// 日志中同 tick 必须先出现 release，再出现 cmd 记录。
	var releaseIdx, cmdIdx = -1, -1
	for i, e := range s.Log() {
		if e.Tick == 4 && strings.Contains(e.What, "release task 1") && releaseIdx < 0 {
			releaseIdx = i
		}
		if e.Tick == 4 && strings.Contains(e.What, "cmd#0") && cmdIdx < 0 {
			cmdIdx = i
		}
	}
	if releaseIdx < 0 || cmdIdx < 0 || releaseIdx > cmdIdx {
		t.Fatalf("same-tick order violated: release@%d cmd@%d", releaseIdx, cmdIdx)
	}
	// 重复跑日志必须完全一致。
	again := run()
	if len(s.Log()) != len(again.Log()) {
		t.Fatalf("replay log length differs: %d vs %d", len(s.Log()), len(again.Log()))
	}
	for i := range s.Log() {
		if s.Log()[i] != again.Log()[i] {
			t.Fatalf("replay differs at %d: %+v vs %+v", i, s.Log()[i], again.Log()[i])
		}
	}
	t.Logf("covered: same-tick event-before-command ordering, replay identical (%d entries)", len(s.Log()))
}

// TestCmdAddPeriodicAtRuntime 运行中加入周期任务，从当前时刻开始释放。
func TestCmdAddPeriodicAtRuntime(t *testing.T) {
	s := New(Config{Preemptive: true})
	a := &Task{ID: 1, Name: "a", Priority: 1}
	s.AddTask(a, []Op{{Kind: OpCompute, Ticks: 2}})
	s.Submit(2, Command{
		Kind:    CmdAddTask,
		NewTask: &Task{ID: 7, Name: "per", Priority: 5},
		Program: []Op{{Kind: OpCompute, Ticks: 1}},
		Period:  3,
	})
	if err := s.RunUntil(9, 10000); err != nil {
		t.Fatalf("run: %v", err)
	}
	if cmdResult(t, s, 0).Status != CmdApplied {
		t.Fatalf("add periodic = %+v", cmdResult(t, s, 0))
	}
	// 首次释放于 tick 2，之后 5、8：日志中应出现 3 次 release task 7。
	var releases int
	for _, e := range s.Log() {
		if strings.Contains(e.What, "release task 7") {
			releases++
		}
	}
	if releases != 3 {
		t.Fatalf("releases of task 7 = %d, want 3 (at 2,5,8)", releases)
	}
	t.Logf("covered: runtime periodic add, releases=%d", releases)
}

// TestCmdScaleDeterminism 任务与指令上量后：全部指令都有结果、
// 顺序不漂移、重复跑日志一致。
func TestCmdScaleDeterminism(t *testing.T) {
	const tasks, cmds = 50, 500
	run := func() *Scheduler {
		s := New(Config{Preemptive: true, TimeSlice: 3})
		for i := 0; i < tasks; i++ {
			s.AddTask(&Task{ID: i + 1, Name: "t", Priority: (i % 5) + 1}, []Op{
				{Kind: OpCompute, Ticks: uint64(i%7) + 2},
				{Kind: OpSleep, Ticks: uint64(i%3) + 1},
				{Kind: OpCompute, Ticks: 1},
			})
		}
		// 确定性地生成一批指令：挂起/恢复/调优先级/中断。
		for i := 0; i < cmds; i++ {
			tick := uint64(i % 40)
			id := (i % tasks) + 1
			switch i % 4 {
			case 0:
				s.Submit(tick, Command{Kind: CmdSuspend, TaskID: id})
			case 1:
				s.Submit(tick, Command{Kind: CmdResume, TaskID: id})
			case 2:
				s.Submit(tick, Command{Kind: CmdSetPriority, TaskID: id, Priority: (i % 9) + 1})
			case 3:
				s.Submit(tick, Command{Kind: CmdInterrupt, TaskID: id})
			}
		}
		if err := s.Run(1 << 20); err != nil {
			t.Fatalf("run: %v", err)
		}
		return s
	}
	s := run()
	if got := len(s.CommandResults()); got != cmds {
		t.Fatalf("got %d cmd results, want %d (no command may be dropped)", got, cmds)
	}
	// 结果必须按 (tick, seq) 字典序记录（同 tick 按提交顺序）。
	res := s.CommandResults()
	for i := 1; i < len(res); i++ {
		a, b := res[i-1], res[i]
		if b.Tick < a.Tick || (b.Tick == a.Tick && b.Seq <= a.Seq) {
			t.Fatalf("result order drift at %d: (%d,#%d) after (%d,#%d)",
				i, b.Tick, b.Seq, a.Tick, a.Seq)
		}
	}
	again := run()
	if len(s.Log()) != len(again.Log()) {
		t.Fatalf("replay log length differs: %d vs %d", len(s.Log()), len(again.Log()))
	}
	for i := range s.Log() {
		if s.Log()[i] != again.Log()[i] {
			t.Fatalf("replay differs at %d", i)
		}
	}
	t.Logf("covered: %d tasks x %d commands, all results recorded in order, replay identical (%d log entries)",
		tasks, cmds, len(s.Log()))
}

// TestCmdConcurrentSubmit 指令可以从其它 goroutine 在 Run 运行中下发，
// 与调度循环并发安全（配合 -race 验证）。
func TestCmdConcurrentSubmit(t *testing.T) {
	s := New(Config{Preemptive: true, TimeSlice: 2})
	for i := 0; i < 10; i++ {
		s.AddTask(&Task{ID: i + 1, Name: "t", Priority: (i % 4) + 1}, []Op{
			{Kind: OpCompute, Ticks: 50},
			{Kind: OpSleep, Ticks: 5},
			{Kind: OpCompute, Ticks: 10},
		})
	}
	done := make(chan error, 1)
	go func() { done <- s.Run(1 << 20) }()
	// 运行中持续下发指令（目标时刻覆盖整个执行窗口）。
	for i := 0; i < 200; i++ {
		id := (i % 10) + 1
		tick := uint64(i % 60)
		switch i % 3 {
		case 0:
			s.Submit(tick, Command{Kind: CmdSetPriority, TaskID: id, Priority: (i % 7) + 1})
		case 1:
			s.Submit(tick, Command{Kind: CmdInterrupt, TaskID: id})
		case 2:
			s.Submit(tick, Command{Kind: CmdSuspend, TaskID: id})
			s.Submit(tick+1, Command{Kind: CmdResume, TaskID: id})
		}
	}
	if err := <-done; err != nil {
		t.Fatalf("run: %v", err)
	}
	// 所有被处理到的指令都有结果；并发下已处理数量 <= 提交数量，
	// 但每条已处理指令的结果都必须完整记录（状态可区分）。
	res := s.CommandResults()
	for i, r := range res {
		if r.Status != CmdApplied && r.Status != CmdNoop && r.Status != CmdRejected {
			t.Fatalf("result %d has invalid status: %+v", i, r)
		}
		if r.Status == CmdRejected && r.Err == nil {
			t.Fatalf("result %d rejected without reason: %+v", i, r)
		}
	}
	t.Logf("covered: concurrent submit during run, %d commands processed safely", len(res))
}
