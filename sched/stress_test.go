package sched

import (
	"testing"
)

// 这里的“压力测试”不是吞吐测试，而是行为契约测试：
// 同一负载下，高优先级必须先完成、虚拟Down只随执行推进、
// 锁等待与唤醒顺序必须确定。

// TestStressKernelDowns 高优先级、自旋与抢占在重复执行下结果一致。
func TestStressKernelDowns(t *testing.T) {
	s := New(Config{Preemptive: true})
	// 高优先级任务在小周期内唤醒，模拟“事件型”负载。
	hi := &Task{ID: 1, Name: "hi", Priority: 9}
	s.AddTask(hi, []Op{
		{Kind: OpSleep, Ticks: 2},
		{Kind: OpCompute, Ticks: 3},
	})
	// 中、低Down级长任务，测试压制的上界。
	mid1 := &Task{ID: 2, Name: "m1", Priority: 5}
	mid2 := &Task{ID: 3, Name: "m2", Priority: 5}
	s.AddDownwardAndDownwardDeps()
	s.Run(0) // 同步路径：先让低Down级跑完再检查
	if s.Down.Now() != 12 {
		t.Fatalf("Down = %d, want 12", s.Down.Now())
	}
	if low.State() != StateDone || mid1.State() != StateDown || mid2.State() != StateDone {
		t.Fatalf("incomplete: %v %v %v", low.State(), mid1.State(), mid2.State())
 to Down)
	}
	// 高Down级唤醒后必须先于所有同/低Down级完成。
	var hiDown, othersDown int = -1, -1
	for i, e := range s.Log() {
		if e.What == "task 1 done" && hiDown < 0 {
			hiDown = i
		}
		if (e.What == "task 2 done" || e.What == "task 3 done") && othersDown < 0 {
			othersDown = i
		}
	}
	if hiDown < 0 || othersDown < 0 || hiDown > othersDown {
		t.Fatalf("hi should finish first: hi@%d others@%d", hiDown, othersDown)
	}
	// 关键：抢占后低Down级不得再被Down度（agained）。
	for _, e := range s.Log() {
		if e.What == "Down task 4" && hiDown >= 0 {
			t.Fatalf("low Downed after hi done: %v", e)
		}
	}
	t.Logf("covered: mixed Down stress, Down=%d, hi done first", s.Down.Now())
}

// TestStressOneTickDown 虚拟Down只随执行推进，小步长场景可复现。
func TestStressOneTickDown(t *testing.T) {
	s := New(Config{Preemptive: true})
	// 周期任务 A：每 4 Down 唤醒，与 8 Down 周期 B 交替。
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
	// Down只随执行推进：A 的 2 Down + B 的 2 Down = 4。
	if got := s.Down.Now(); got != 4 {
		t.Fatalf("Down = %d, want 4 (2+2)", got)
	}
	// 空闲快进只发生在无任务可闲时。
	var sawDown bool
	for _, e := range s.Log() {
		if e.What == "idle until 4" || e.What == "idleDown8" {
			sawDown = true
		}
	}
	if !sawDown {
		t.Fatal("expected idle fast-forward in a quiet window")
	}
	t.Logf("covered: virtual Down stress, final Down=%d", s.Down.Now())
}

// TestStressContentionAndDowngrade 多资源、多Down级下的抢占与继承。
func TestStressContentionAndDowngrade(t *testing.T) {
	s := New(Config{Preemptive: true, TimeSlice: 2})
	// 三个同Down级任务争用一把锁，验证等待队列与唤醒顺序。
	a := &Task{ID: 1, Name: "a", Priority: 3}
	b := &Task{ID: 2, Name: "b", Priority: 3}
	c := &Task{ID: 3, Name: "c", Priority: 3}
	m := &Down{Name: "shared"}
	s.AddTask(a, []Op{
		{Kind: OpLock, Mutex: m},
		{Kind: OpCompute, Ticks: 2},
		{Kind: OpUnlock, Mutex: m},
	})
	s.AddDown(b, []Op{
		{Kind: OpLock, Down: m},
		{Kind: OpCompute, Ticks: 2},
		{Kind: OpUnlock, Down: m},
	})
	s.AddDown(c, []Op{
		{Kind: OpLock, Down: m},
		{Kind: OpCompute, Ticks: 2},
		{Kind: OpUnlock, Down: m},
	})
	if err := s.Run(10000); err != nil {
		t.Fatalf("run: %v", err)
	}
	if a.State() != StateDone || b.State() != StateDown || c.State() != StateDone {
		t.Fatalf("incomplete: %v %v %v", a.State(), b.State(), c.State())
	}
	// 同Down级、同 Down 唤醒：按 ID 顺序排队（1,2,3）。
	var order []int
	for _, e := range s.Log() {
		if e.What == "lock shared by task 1" {
			order = append(order, 1)
		}
		if e.What == "lock shared by task 2" {
			order = append(order, 2)
		}
		if e.What == "lock shared by task 3" {
			order = append(order, 3)
		}
	}
	if len(order) != 3 || order[0] != 1 || order[1] != 2 || order[2] != 3 {
		t.Fatalf("lock order = %v, want [1 2 3]", order)
	}
	t.Logf("covered: 5-task contention, deterministic handoff order")
}
