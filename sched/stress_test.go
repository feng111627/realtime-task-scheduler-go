package sched

import (
	"strings"
	"testing"
)

// 这里的“压力测试”不是吞吐测试，而是行为契约测试：
// 同一负载下，高优先级必须先完成、虚拟时钟只随执行推进、
// 锁等待与唤醒顺序必须确定。

// TestStressKernelPreemption 高优先级事件型任务唤醒后必须压制中低优先级长任务。
func TestStressKernelPreemption(t *testing.T) {
	s := New(Config{Preemptive: true})
	// 高优先级任务在小延迟后唤醒，模拟“事件型”负载。
	hi := &Task{ID: 1, Name: "hi", Priority: 9}
	s.AddTask(hi, []Op{
		{Kind: OpSleep, Ticks: 2},
		{Kind: OpCompute, Ticks: 3},
	})
	// 中、低优先级长任务，测试压制的上界。
	mid1 := &Task{ID: 2, Name: "m1", Priority: 5}
	mid2 := &Task{ID: 3, Name: "m2", Priority: 5}
	low := &Task{ID: 4, Name: "low", Priority: 1}
	s.AddTask(mid1, []Op{{Kind: OpCompute, Ticks: 3}})
	s.AddTask(mid2, []Op{{Kind: OpCompute, Ticks: 3}})
	s.AddTask(low, []Op{{Kind: OpCompute, Ticks: 3}})
	if err := s.Run(10000); err != nil {
		t.Fatalf("run: %v", err)
	}
	// 时钟 = 计算 3*4 = 12（hi 睡眠期间中低优先级在跑，无空闲）。
	if got := s.Clock().Now(); got != 12 {
		t.Fatalf("clock = %d, want 12", got)
	}
	for _, tk := range []*Task{hi, mid1, mid2, low} {
		if tk.State() != StateDone {
			t.Fatalf("task %d not done: %v", tk.ID, tk.State())
		}
	}
	// 高优先级唤醒后必须先于所有中/低优先级完成。
	hiDone, othersDone := -1, -1
	for i, e := range s.Log() {
		if e.What == "task 1 done" && hiDone < 0 {
			hiDone = i
		}
		if (e.What == "task 2 done" || e.What == "task 3 done" || e.What == "task 4 done") && othersDone < 0 {
			othersDone = i
		}
	}
	if hiDone < 0 || othersDone < 0 || hiDone > othersDone {
		t.Fatalf("hi should finish first: hi@%d others@%d", hiDone, othersDone)
	}
	// 关键：hi 完成前，低优先级不得被调度（被中优先级压制）。
	for i, e := range s.Log() {
		if e.What == "dispatch task 4" && hiDone >= 0 && i < hiDone {
			t.Fatalf("low dispatched before hi done: %v", e)
		}
	}
	t.Logf("covered: mixed-priority preemption stress, clock=%d, hi done first", s.Clock().Now())
}

// TestStressOneTickClock 虚拟时钟只随执行与空闲跳转推进，小步长场景可复现。
func TestStressOneTickClock(t *testing.T) {
	s := New(Config{Preemptive: true})
	// 任务 A：睡 4 tick 后算 2 tick；任务 B：睡 8 tick 后算 2 tick。
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
	// 时钟 = 空闲 4 + A 计算 2 + 空闲 2 + B 计算 2 = 10。
	if got := s.Clock().Now(); got != 10 {
		t.Fatalf("clock = %d, want 10 (4 idle + 2 + 2 idle + 2)", got)
	}
	// 空闲快进只发生在无任务可运行时，且两段安静窗口都要出现。
	var sawIdle4, sawIdle8 bool
	for _, e := range s.Log() {
		if strings.Contains(e.What, "idle until 4") {
			sawIdle4 = true
		}
		if strings.Contains(e.What, "idle until 8") {
			sawIdle8 = true
		}
	}
	if !sawIdle4 || !sawIdle8 {
		t.Fatalf("expected idle fast-forward in both quiet windows: idle4=%v idle8=%v", sawIdle4, sawIdle8)
	}
	t.Logf("covered: virtual clock stress, final clock=%d", s.Clock().Now())
}

// TestStressContentionHandoff 多任务争用一把锁时等待队列与移交顺序确定。
func TestStressContentionHandoff(t *testing.T) {
	s := New(Config{Preemptive: true, TimeSlice: 2})
	// 三个同优先级任务争用一把锁，验证等待队列与唤醒顺序。
	m := &Mutex{Name: "shared"}
	program := []Op{
		{Kind: OpLock, Mutex: m},
		{Kind: OpCompute, Ticks: 2},
		{Kind: OpUnlock, Mutex: m},
	}
	a := &Task{ID: 1, Name: "a", Priority: 3}
	b := &Task{ID: 2, Name: "b", Priority: 3}
	c := &Task{ID: 3, Name: "c", Priority: 3}
	s.AddTask(a, program)
	s.AddTask(b, program)
	s.AddTask(c, program)
	if err := s.Run(10000); err != nil {
		t.Fatalf("run: %v", err)
	}
	for _, tk := range []*Task{a, b, c} {
		if tk.State() != StateDone {
			t.Fatalf("task %d not done: %v", tk.ID, tk.State())
		}
	}
	// 同优先级、同时就绪：按 ID 顺序获得锁（1,2,3）。
	var order []int
	for _, e := range s.Log() {
		for id := 1; id <= 3; id++ {
			suffix := "task " + string(rune('0'+id))
			if e.What == "lock shared by "+suffix || e.What == "lock shared handed to "+suffix {
				order = append(order, id)
			}
		}
	}
	if len(order) != 3 || order[0] != 1 || order[1] != 2 || order[2] != 3 {
		t.Fatalf("lock order = %v, want [1 2 3]", order)
	}
	t.Logf("covered: 3-task contention, deterministic handoff order %v", order)
}
