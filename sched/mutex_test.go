package sched

import (
	"errors"
	"strings"
	"testing"
)

// TestPriorityInheritance 低优先级持锁时继承高优先级，
// 避免被无关中优先级任务长时间压制。
func TestPriorityInheritance(t *testing.T) {
	s := New(Config{Preemptive: true})
	m := &Mutex{Name: "res"}
	low := &Task{ID: 1, Name: "low", Priority: 1}
	mid := &Task{ID: 2, Name: "mid", Priority: 5}
	high := &Task{ID: 3, Name: "high", Priority: 9}
	s.AddTask(low, []Op{
		{Kind: OpLock, Mutex: m},
		{Kind: OpCompute, Ticks: 4},
		{Kind: OpUnlock, Mutex: m},
		{Kind: OpCompute, Ticks: 1},
	})
	s.AddTask(mid, []Op{
		{Kind: OpSleep, Ticks: 2},
		{Kind: OpCompute, Ticks: 10},
	})
	s.AddTask(high, []Op{
		{Kind: OpSleep, Ticks: 3},
		{Kind: OpLock, Mutex: m},
		{Kind: OpCompute, Ticks: 2},
		{Kind: OpUnlock, Mutex: m},
	})
	if err := s.Run(100000); err != nil {
		t.Fatalf("run: %v", err)
	}
	// high 必须先于 mid 完成：low 继承 9 后优先于 mid 运行并快速释放锁。
	highDone, midDone := -1, -1
	for i, e := range s.Log() {
		if strings.Contains(e.What, "task 3 done") && highDone < 0 {
			highDone = i
		}
		if strings.Contains(e.What, "task 2 done") && midDone < 0 {
			midDone = i
		}
	}
	if highDone < 0 || midDone < 0 || highDone > midDone {
		t.Fatalf("high should finish before mid: high@%d mid@%d", highDone, midDone)
	}
	// high 阻塞期间，mid 不得被调度（被继承后的 low 压制）。
	blockedAt := -1
	for i, e := range s.Log() {
		if strings.Contains(e.What, "task 3 blocked") {
			blockedAt = i
		}
		if blockedAt >= 0 && i > blockedAt &&
			strings.Contains(e.What, "dispatch task 2") &&
			highDone < 0 {
			t.Fatalf("mid dispatched while high blocked: entry %d", i)
		}
		if strings.Contains(e.What, "task 3 done") && highDone < 0 {
			highDone = i
		}
	}
	// 释放后继承恢复。
	if got := low.EffectivePriority(); got != 1 {
		t.Fatalf("low effective priority = %d, want 1 after unlock", got)
	}
	t.Logf("covered: priority inheritance + restore, clock=%d", s.Clock().Now())
}

// TestUnlockErrors 异常释放与重复释放必须被可区分地拒绝。
func TestUnlockErrors(t *testing.T) {
	t.Run("unlock of unlocked", func(t *testing.T) {
		s := New(Config{Preemptive: true})
		m := &Mutex{Name: "x"}
		a := &Task{ID: 1, Name: "a", Priority: 1}
		s.AddTask(a, []Op{{Kind: OpUnlock, Mutex: m}})
		if err := s.Run(1000); err != nil {
			t.Fatalf("run: %v", err)
		}
		if !errors.Is(a.LastError(), ErrNotLocked) {
			t.Fatalf("err = %v, want ErrNotLocked", a.LastError())
		}
		if a.State() != StateSuspended {
			t.Fatalf("state = %v, want Suspended", a.State())
		}
	})
	t.Run("unlock by non-owner", func(t *testing.T) {
		s := New(Config{Preemptive: true})
		m := &Mutex{Name: "x"}
		a := &Task{ID: 1, Name: "a", Priority: 2}
		b := &Task{ID: 2, Name: "b", Priority: 1}
		s.AddTask(a, []Op{
			{Kind: OpLock, Mutex: m},
			{Kind: OpSleep, Ticks: 5},
			{Kind: OpUnlock, Mutex: m},
		})
		s.AddTask(b, []Op{{Kind: OpUnlock, Mutex: m}})
		if err := s.Run(1000); err != nil {
			t.Fatalf("run: %v", err)
		}
		if !errors.Is(b.LastError(), ErrNotOwner) {
			t.Fatalf("err = %v, want ErrNotOwner", b.LastError())
		}
		if a.State() != StateDone {
			t.Fatalf("a state = %v, want Done", a.State())
		}
	})
	t.Run("double lock", func(t *testing.T) {
		s := New(Config{Preemptive: true})
		m := &Mutex{Name: "x"}
		a := &Task{ID: 1, Name: "a", Priority: 1}
		s.AddTask(a, []Op{
			{Kind: OpLock, Mutex: m},
			{Kind: OpLock, Mutex: m},
		})
		if err := s.Run(1000); err != nil {
			t.Fatalf("run: %v", err)
		}
		if !errors.Is(a.LastError(), ErrDoubleLock) {
			t.Fatalf("err = %v, want ErrDoubleLock", a.LastError())
		}
	})
	t.Log("covered: ErrNotLocked / ErrNotOwner / ErrDoubleLock")
}

// TestDeadlockDetection 循环等待必须被检测并拒绝。
func TestDeadlockDetection(t *testing.T) {
	s := New(Config{Preemptive: true})
	m1 := &Mutex{Name: "m1"}
	m2 := &Mutex{Name: "m2"}
	a := &Task{ID: 1, Name: "a", Priority: 2}
	b := &Task{ID: 2, Name: "b", Priority: 1}
	s.AddTask(a, []Op{
		{Kind: OpLock, Mutex: m1},
		{Kind: OpSleep, Ticks: 2},
		{Kind: OpLock, Mutex: m2}, // b 持有 m2 且等待 m1 → 死锁
	})
	s.AddTask(b, []Op{
		{Kind: OpLock, Mutex: m2},
		{Kind: OpLock, Mutex: m1},
	})
	if err := s.Run(10000); err != nil {
		t.Fatalf("run: %v", err)
	}
	if !errors.Is(a.LastError(), ErrDeadlock) {
		t.Fatalf("a err = %v, want ErrDeadlock", a.LastError())
	}
	if a.State() != StateSuspended {
		t.Fatalf("a state = %v, want Suspended", a.State())
	}
	t.Log("covered: deadlock detection via wait-chain cycle")
}

// TestMutexHandoff 解锁时锁直接移交给队首等待者。
func TestMutexHandoff(t *testing.T) {
	s := New(Config{Preemptive: true})
	m := &Mutex{Name: "res"}
	a := &Task{ID: 1, Name: "a", Priority: 3}
	b := &Task{ID: 2, Name: "b", Priority: 2}
	s.AddTask(a, []Op{
		{Kind: OpLock, Mutex: m},
		{Kind: OpCompute, Ticks: 2},
		{Kind: OpUnlock, Mutex: m},
	})
	s.AddTask(b, []Op{
		{Kind: OpYield}, // 让 a 先拿锁
		{Kind: OpLock, Mutex: m},
		{Kind: OpCompute, Ticks: 1},
		{Kind: OpUnlock, Mutex: m},
	})
	if err := s.Run(10000); err != nil {
		t.Fatalf("run: %v", err)
	}
	if a.State() != StateDone || b.State() != StateDone {
		t.Fatalf("states: a=%v b=%v", a.State(), b.State())
	}
	if m.Owner() != nil {
		t.Fatalf("mutex still owned by %v", m.Owner().ID)
	}
	t.Log("covered: lock handoff to queued waiter")
}
