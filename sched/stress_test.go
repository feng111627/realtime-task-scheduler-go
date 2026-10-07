package sched

import (
	"strings"
	"testing"
)

// 这里的“压力测试”不是吞吐测试，而是行为契约测试：
// 同一负载下，高优先级必须先完成、虚拟时钟只随执行推进、
// 锁等待与唤醒顺序必须确定。

// TestStressPreemptionOrder 高优先级、睡眠唤醒与抢占在重复执行下结果一致。
func TestStressPreemptionOrder(t *testing.T) {
	run := func() *Scheduler {
		s := New(Config{Preemptive: true})
		// 高优先级任务在小睡眠后唤醒，模拟“事件型”负载。
		hi := &Task{ID: 1, Name: "hi", Priority: 9}
		s.AddTask(hi, []Op{
			{Kind: OpSleep, Ticks: 2},
			{Kind: OpCompute, Ticks: 3},
		})
		// 中、低优先级长任务，测试压制的上界。
		mid1 := &Task{ID: 2, Name: "m1", Priority: 5}
		mid2 := &Task{ID: 3, Name: "m2", Priority: 5}
		low := &Task{ID: 4, Name: "low", Priority: 1}
		s.AddTask(mid1, []Op{{Kind: OpCompute, Ticks: 2}})
		s.AddTask(mid2, []Op{{Kind: OpCompute, Ticks: 2}})
		s.AddTask(low, []Op{{Kind: OpCompute, Ticks: 5}})
		if err := s.Run(10000); err != nil {
			t.Fatalf("run: %v", err)
		}
		// 时钟只随执行推进：3 + 2 + 2 + 5 = 12。
		if s.Clock().Now() != 12 {
			t.Fatalf("clock = %d, want 12", s.Clock().Now())
		}
		if low.State() != StateDone || mid1.State() != StateDone || mid2.State() != StateDone {
			t.Fatalf("incomplete: %v %v %v", low.State(), mid1.State(), mid2.State())
		}
		return s
	}
	s := run()
	// 高优先级唤醒后必须先于低优先级完成。
	var hiDone, lowDone int = -1, -1
	for i, e := range s.Log() {
		if e.What == "task 1 done" && hiDone < 0 {
			hiDone = i
		}
		if e.What == "task 4 done" && lowDone < 0 {
			lowDone = i
		}
	}
	if hiDone < 0 || lowDone < 0 || hiDone > lowDone {
		t.Fatalf("hi should finish before low: hi@%d low@%d", hiDone, lowDone)
	}
	// 关键：hi 完成前低优先级不得被调度（被完全压制）。
	for i, e := range s.Log() {
		if strings.HasPrefix(e.What, "dispatch task 4") && hiDone >= 0 && i < hiDone {
			t.Fatalf("low dispatched before hi done: %v", e)
		}
	}
	// 重复执行结果必须一致。
	again := run()
	if len(s.Log()) != len(again.Log()) {
		t.Fatalf("replay log length differs: %d vs %d", len(s.Log()), len(again.Log()))
	}
	for i := range s.Log() {
		if s.Log()[i] != again.Log()[i] {
			t.Fatalf("replay differs at %d: %+v vs %+v", i, s.Log()[i], again.Log()[i])
		}
	}
	t.Logf("covered: mixed priority stress, clock=%d, hi done first, replay identical", s.Clock().Now())
}

// TestStressVirtualClock 虚拟时钟只随执行推进，小步长场景可复现。
func TestStressVirtualClock(t *testing.T) {
	s := New(Config{Preemptive: true})
	// 任务 A 睡 4 tick，任务 B 睡 8 tick，交替验证空闲快进。
	a := &Task{ID: 1, Name: "a", Priority: 8}
	b := &Task{ID: 2, Name: "b", Priority: 8}
	s.AddTask(a, []Op{
		{Kind: OpSleep, Ticks: 4},
		{Kind: OpCompute, Ticks: 2},
	})
	s.AddTask(b, []Op{
		{Kind: OpSleep, Ticks: 8},
		{Kind: OpCompute, Ticks: 2},
	})
	if err := s.Run(1000); err != nil {
		t.Fatalf("run: %v", err)
	}
	// 时钟 = 空闲快进(4+2) + 执行(2+2) = 10，调度开销不计时。
	if got := s.Clock().Now(); got != 10 {
		t.Fatalf("clock = %d, want 10 (idle 4+2, exec 2+2)", got)
	}
	// 空闲快进只发生在无任务可运行时。
	var sawIdle bool
	for _, e := range s.Log() {
		if e.What == "idle until 4" || e.What == "idle until 8" {
			sawIdle = true
		}
	}
	if !sawIdle {
		t.Fatal("expected idle fast-forward in a quiet window")
	}
	t.Logf("covered: virtual clock stress, final clock=%d", s.Clock().Now())
}

// TestStressLockContention 多任务同优先级争用一把锁，等待与唤醒顺序确定。
func TestStressLockContention(t *testing.T) {
	s := New(Config{Preemptive: true, TimeSlice: 2})
	// 三个同优先级任务争用一把锁，验证等待队列与唤醒顺序。
	a := &Task{ID: 1, Name: "a", Priority: 3}
	b := &Task{ID: 2, Name: "b", Priority: 3}
	c := &Task{ID: 3, Name: "c", Priority: 3}
	m := &Mutex{Name: "shared"}
	prog := []Op{
		{Kind: OpLock, Mutex: m},
		{Kind: OpCompute, Ticks: 2},
		{Kind: OpUnlock, Mutex: m},
	}
	s.AddTask(a, prog)
	s.AddTask(b, prog)
	s.AddTask(c, prog)
	if err := s.Run(10000); err != nil {
		t.Fatalf("run: %v", err)
	}
	if a.State() != StateDone || b.State() != StateDone || c.State() != StateDone {
		t.Fatalf("incomplete: %v %v %v", a.State(), b.State(), c.State())
	}
	// 同优先级、同时就绪：按 ID 顺序进出临界区（1,2,3）。
	// 锁移交不打 lock 日志，用 unlock 顺序反映临界区退出顺序。
	var order []int
	for _, e := range s.Log() {
		if e.What == "unlock shared by task 1" {
			order = append(order, 1)
		}
		if e.What == "unlock shared by task 2" {
			order = append(order, 2)
		}
		if e.What == "unlock shared by task 3" {
			order = append(order, 3)
		}
	}
	if len(order) != 3 || order[0] != 1 || order[1] != 2 || order[2] != 3 {
		t.Fatalf("lock order = %v, want [1 2 3]", order)
	}
	t.Logf("covered: 3-task contention, deterministic handoff order %v", order)
}
