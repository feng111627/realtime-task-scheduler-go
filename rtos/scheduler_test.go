package rtos

import (
	"strings"
	"testing"
)

// dispatches 提取日志中的调度/抢占序列，便于断言执行顺序。
func dispatches(log []LogEntry) []string {
	var out []string
	for _, e := range log {
		if e.Kind == LogSwitch {
			out = append(out, e.Task+"@"+itoa(e.Tick))
		}
	}
	return out
}

func itoa(n int) string {
	if n == 0 {
		return "0"
	}
	neg := n < 0
	if neg {
		n = -n
	}
	var b [20]byte
	i := len(b)
	for n > 0 {
		i--
		b[i] = byte('0' + n%10)
		n /= 10
	}
	if neg {
		i--
		b[i] = '-'
	}
	return string(b[i:])
}

func countKind(log []LogEntry, kind LogKind) int {
	n := 0
	for _, e := range log {
		if e.Kind == kind {
			n++
		}
	}
	return n
}

// TestPreemption 验证高优先级任务就绪时立即抢占低优先级任务。
func TestPreemption(t *testing.T) {
	k := NewKernel(4)
	k.AddTask(TaskSpec{Name: "low", Priority: 1, Script: []Op{Compute(10)}})
	k.AddTask(TaskSpec{Name: "high", Priority: 5, Phase: 3, Script: []Op{Compute(2)}})
	k.Run(1000)

	got := strings.Join(dispatches(k.Log()), " ")
	want := "low@0 high@3 low@5"
	if got != want {
		t.Fatalf("dispatch order = %q, want %q", got, want)
	}
	if countKind(k.Log(), LogPreempt) != 1 {
		t.Fatalf("expected exactly 1 preemption, got %d", countKind(k.Log(), LogPreempt))
	}
	if k.Clock() != 12 {
		t.Fatalf("clock = %d, want 12 (10 low + 2 high ticks of real work)", k.Clock())
	}
	t.Logf("covered: priority preemption, clock=work-only; dispatches=%s", got)
}

// TestPreemptionDisabled 验证关闭抢占后高优先级任务不得打断当前任务。
func TestPreemptionDisabled(t *testing.T) {
	k := NewKernel(4)
	k.SetPreemptive(false)
	k.AddTask(TaskSpec{Name: "low", Priority: 1, Script: []Op{Compute(10)}})
	k.AddTask(TaskSpec{Name: "high", Priority: 5, Phase: 3, Script: []Op{Compute(2)}})
	k.Run(1000)

	got := strings.Join(dispatches(k.Log()), " ")
	want := "low@0 high@10"
	if got != want {
		t.Fatalf("dispatch order = %q, want %q", got, want)
	}
	if countKind(k.Log(), LogPreempt) != 0 {
		t.Fatal("no preemption allowed when preemptive scheduling is off")
	}
	t.Logf("covered: non-preemptive scheduling; dispatches=%s", got)
}

// TestRoundRobin 验证同优先级任务按时间片轮转。
func TestRoundRobin(t *testing.T) {
	k := NewKernel(2)
	k.AddTask(TaskSpec{Name: "a", Priority: 3, Script: []Op{Compute(4)}})
	k.AddTask(TaskSpec{Name: "b", Priority: 3, Script: []Op{Compute(4)}})
	k.Run(1000)

	got := strings.Join(dispatches(k.Log()), " ")
	want := "a@0 b@2 a@4 b@6"
	if got != want {
		t.Fatalf("dispatch order = %q, want %q", got, want)
	}
	if countKind(k.Log(), LogPreempt) != 0 {
		t.Fatal("round-robin rotation must not count as preemption")
	}
	t.Logf("covered: same-priority round-robin quantum=2; dispatches=%s", got)
}

// TestSleepWakeup 验证睡眠阻塞与到期唤醒的状态迁移。
func TestSleepWakeup(t *testing.T) {
	k := NewKernel(4)
	k.AddTask(TaskSpec{Name: "s", Priority: 2, Script: []Op{Compute(2), Sleep(5), Compute(2)}})
	k.Run(1000)

	if k.Clock() != 9 { // 2 work + 5 sleep (idle jump) + 2 work
		t.Fatalf("clock = %d, want 9", k.Clock())
	}
	if countKind(k.Log(), LogBlock) != 1 || countKind(k.Log(), LogWake) < 2 {
		t.Fatalf("expected 1 block and >=2 wakes, log=%v", k.Log())
	}
	if countKind(k.Log(), LogIdle) != 1 {
		t.Fatalf("expected 1 idle jump during sleep, got %d", countKind(k.Log(), LogIdle))
	}
	t.Log("covered: block on sleep, idle clock jump, wakeup to ready")
}

// TestDeterministicReplay 验证相同场景重复执行产生完全一致的日志。
func TestDeterministicReplay(t *testing.T) {
	build := func() *Kernel {
		k := NewKernel(2)
		m := k.NewMutex("res")
		k.AddTask(TaskSpec{Name: "t1", Priority: 1, Script: []Op{Compute(3), LockOp(m), Compute(2), UnlockOp(m)}})
		k.AddTask(TaskSpec{Name: "t2", Priority: 3, Phase: 1, Script: []Op{Compute(1), LockOp(m), Compute(1), UnlockOp(m)}})
		k.AddTask(TaskSpec{Name: "t3", Priority: 2, Phase: 2, Script: []Op{Compute(2), Sleep(3), Compute(1)}})
		k.Run(500)
		return k
	}
	l1, l2 := build().Log(), build().Log()
	if len(l1) != len(l2) {
		t.Fatalf("log length differs between replays: %d vs %d", len(l1), len(l2))
	}
	for i := range l1 {
		if l1[i] != l2[i] {
			t.Fatalf("replay diverged at entry %d: %+v vs %+v", i, l1[i], l2[i])
		}
	}
	t.Logf("covered: deterministic replay, %d log entries identical across runs", len(l1))
}
