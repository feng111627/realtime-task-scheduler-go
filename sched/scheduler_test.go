package sched

import (
	"fmt"
	"strings"
	"testing"
)

// dispatchSeq 从日志中提取 dispatch 的任务 ID 序列。
func dispatchSeq(s *Scheduler) []int {
	var out []int
	for _, e := range s.Log() {
		var id int
		if _, err := fmt.Sscanf(e.What, "dispatch task %d", &id); err == nil {
			out = append(out, id)
		}
	}
	return out
}

// TestPreemption 高优先级任务就绪时必须抢占低优先级任务。
func TestPreemption(t *testing.T) {
	s := New(Config{Preemptive: true})
	low := &Task{ID: 1, Name: "low", Priority: 1}
	high := &Task{ID: 2, Name: "high", Priority: 5}
	s.AddTask(low, []Op{{Kind: OpCompute, Ticks: 10}})
	s.AddTask(high, []Op{
		{Kind: OpSleep, Ticks: 3},
		{Kind: OpCompute, Ticks: 2},
	})
	if err := s.Run(10000); err != nil {
		t.Fatalf("run: %v", err)
	}
	if low.State() != StateDone || high.State() != StateDone {
		t.Fatalf("states: low=%v high=%v", low.State(), high.State())
	}
	// 总时长 = 10 + 2 个计算 tick，无空闲。
	if got := s.Clock().Now(); got != 12 {
		t.Fatalf("clock = %d, want 12", got)
	}
	// 必须出现抢占记录，且 high 先于 low 完成。
	var preempted bool
	var highDone, lowDone int = -1, -1
	for i, e := range s.Log() {
		if strings.Contains(e.What, "preempt task 1") {
			preempted = true
		}
		if strings.Contains(e.What, "task 2 done") && highDone < 0 {
			highDone = i
		}
		if strings.Contains(e.What, "task 1 done") && lowDone < 0 {
			lowDone = i
		}
	}
	if !preempted {
		t.Fatal("expected preemption of low task")
	}
	if highDone < 0 || lowDone < 0 || highDone > lowDone {
		t.Fatalf("high should finish before low: high@%d low@%d", highDone, lowDone)
	}
	t.Logf("covered: preemption, clock=%d, log entries=%d", s.Clock().Now(), len(s.Log()))
}

// TestNoPreemptionWhenDisabled 关闭抢占时不得发生抢占。
func TestNoPreemptionWhenDisabled(t *testing.T) {
	s := New(Config{Preemptive: false})
	low := &Task{ID: 1, Name: "low", Priority: 1}
	high := &Task{ID: 2, Name: "high", Priority: 5}
	s.AddTask(low, []Op{{Kind: OpCompute, Ticks: 10}})
	s.AddTask(high, []Op{
		{Kind: OpSleep, Ticks: 3},
		{Kind: OpCompute, Ticks: 2},
	})
	if err := s.Run(10000); err != nil {
		t.Fatalf("run: %v", err)
	}
	for _, e := range s.Log() {
		if strings.Contains(e.What, "preempt") {
			t.Fatalf("unexpected preemption: %v", e)
		}
	}
	// low 先完成，high 在其后运行。
	var lowDone, highDone = -1, -1
	for i, e := range s.Log() {
		if strings.Contains(e.What, "task 1 done") {
			lowDone = i
		}
		if strings.Contains(e.What, "task 2 done") {
			highDone = i
		}
	}
	if lowDone < 0 || highDone < 0 || lowDone > highDone {
		t.Fatalf("low should finish first: low@%d high@%d", lowDone, highDone)
	}
	t.Logf("covered: non-preemptive scheduling, clock=%d", s.Clock().Now())
}

// TestRoundRobin 同优先级任务按时间片轮转。
func TestRoundRobin(t *testing.T) {
	s := New(Config{Preemptive: true, TimeSlice: 2})
	a := &Task{ID: 1, Name: "a", Priority: 3}
	b := &Task{ID: 2, Name: "b", Priority: 3}
	s.AddTask(a, []Op{{Kind: OpCompute, Ticks: 5}})
	s.AddTask(b, []Op{{Kind: OpCompute, Ticks: 5}})
	if err := s.Run(10000); err != nil {
		t.Fatalf("run: %v", err)
	}
	seq := dispatchSeq(s)
	// 期望调度序列：1,2,1,2,1,2（各 2 tick，最后一次 1 tick）。
	want := []int{1, 2, 1, 2, 1, 2}
	if len(seq) != len(want) {
		t.Fatalf("dispatch seq = %v, want %v", seq, want)
	}
	for i := range want {
		if seq[i] != want[i] {
			t.Fatalf("dispatch seq = %v, want %v", seq, want)
		}
	}
	if got := s.Clock().Now(); got != 10 {
		t.Fatalf("clock = %d, want 10", got)
	}
	t.Logf("covered: round-robin timeslice, seq=%v", seq)
}

// TestVirtualClock 虚拟时钟只随任务执行与空闲事件跳转推进。
func TestVirtualClock(t *testing.T) {
	s := New(Config{Preemptive: true})
	task := &Task{ID: 1, Name: "t", Priority: 1}
	s.AddTask(task, []Op{
		{Kind: OpSleep, Ticks: 5},
		{Kind: OpCompute, Ticks: 3},
	})
	if err := s.Run(10000); err != nil {
		t.Fatalf("run: %v", err)
	}
	if got := s.Clock().Now(); got != 8 {
		t.Fatalf("clock = %d, want 8 (5 sleep + 3 compute)", got)
	}
	var sawIdle bool
	for _, e := range s.Log() {
		if strings.Contains(e.What, "idle until 5") {
			sawIdle = true
		}
	}
	if !sawIdle {
		t.Fatal("expected idle fast-forward log")
	}
	t.Logf("covered: virtual clock idle jump, clock=%d", s.Clock().Now())
}

// TestReplayDeterminism 同一场景重复执行结果必须一致。
func TestReplayDeterminism(t *testing.T) {
	run := func() []LogEntry {
		s := New(Config{Preemptive: true, TimeSlice: 2})
		s.AddTask(&Task{ID: 1, Name: "a", Priority: 2}, []Op{
			{Kind: OpCompute, Ticks: 3}, {Kind: OpSleep, Ticks: 2}, {Kind: OpCompute, Ticks: 2},
		})
		s.AddTask(&Task{ID: 2, Name: "b", Priority: 2}, []Op{
			{Kind: OpCompute, Ticks: 4}, {Kind: OpYield}, {Kind: OpCompute, Ticks: 1},
		})
		s.AddTask(&Task{ID: 3, Name: "c", Priority: 6}, []Op{
			{Kind: OpSleep, Ticks: 1}, {Kind: OpCompute, Ticks: 2},
		})
		if err := s.Run(100000); err != nil {
			t.Fatalf("run: %v", err)
		}
		return s.Log()
	}
	first, second := run(), run()
	if len(first) != len(second) {
		t.Fatalf("log length differs: %d vs %d", len(first), len(second))
	}
	for i := range first {
		if first[i] != second[i] {
			t.Fatalf("log differs at %d: %+v vs %+v", i, first[i], second[i])
		}
	}
	t.Logf("covered: replay determinism, %d log entries identical", len(first))
}
