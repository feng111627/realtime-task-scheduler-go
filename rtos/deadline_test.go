package rtos

import (
	"strings"
	"testing"
)

// TestDeadlineMissSuspend 验证周期任务错过截止时间后按策略挂起。
func TestDeadlineMissSuspend(t *testing.T) {
	k := NewKernel(4)
	task := k.AddTask(TaskSpec{
		Name: "p", Priority: 2, Period: 10, Deadline: 4,
		Policy: PolicySuspend, Script: []Op{Compute(6)},
	})
	k.Run(100)

	if task.State() != StateSuspended {
		t.Fatalf("task state = %v, want Suspended", task.State())
	}
	if countKind(k.Log(), LogDeadlineMiss) != 1 {
		t.Fatalf("expected 1 deadline miss, got %d", countKind(k.Log(), LogDeadlineMiss))
	}
	if countKind(k.Log(), LogSuspend) != 1 {
		t.Fatalf("expected 1 suspend, got %d", countKind(k.Log(), LogSuspend))
	}
	// 挂起后不得再被释放执行：作业完成的日志不得出现。
	for _, e := range k.Log() {
		if e.Kind == LogTerminate {
			t.Fatal("suspended task must never complete a job")
		}
	}
	t.Log("covered: deadline miss -> PolicySuspend; no further releases execute")
}

// TestDeadlinePolicyLog 验证仅记录告警策略：错过截止但任务继续到完成。
func TestDeadlinePolicyLog(t *testing.T) {
	k := NewKernel(4)
	task := k.AddTask(TaskSpec{
		Name: "p", Priority: 2, Deadline: 2,
		Policy: PolicyLog, Script: []Op{Compute(4)},
	})
	k.Run(100)

	if task.State() != StateTerminated {
		t.Fatalf("task state = %v, want Terminated", task.State())
	}
	if countKind(k.Log(), LogDeadlineMiss) != 1 {
		t.Fatalf("expected 1 deadline miss alarm, got %d", countKind(k.Log(), LogDeadlineMiss))
	}
	if k.Clock() != 4 {
		t.Fatalf("clock = %d, want 4 (task ran to completion)", k.Clock())
	}
	t.Log("covered: deadline miss -> PolicyLog; task continues and terminates")
}

// TestBudgetOverrunRestart 验证预算耗尽触发作业重启，且重启从头执行脚本。
func TestBudgetOverrunRestart(t *testing.T) {
	k := NewKernel(4)
	k.AddTask(TaskSpec{
		Name: "p", Priority: 2, Budget: 3,
		Policy: PolicyRestart, Script: []Op{Compute(5)},
	})
	k.Run(20)

	overruns := countKind(k.Log(), LogBudgetOverrun)
	restarts := countKind(k.Log(), LogRestart)
	if overruns < 2 || restarts != overruns {
		t.Fatalf("overruns=%d restarts=%d, want equal and >=2", overruns, restarts)
	}
	// 每次重启消耗 3+1 个 tick（第 4 个 tick 触发超预算），20 tick 内应重启多次。
	t.Logf("covered: budget overrun -> PolicyRestart; overruns=%d restarts=%d clock=%d",
		overruns, restarts, k.Clock())
}

// TestPeriodicReleases 验证周期释放不丢事件：每个周期恰好释放一个作业。
func TestPeriodicReleases(t *testing.T) {
	k := NewKernel(4)
	k.AddTask(TaskSpec{
		Name: "p", Priority: 2, Period: 5, Deadline: 4,
		Policy: PolicyLog, Script: []Op{Compute(1)},
	})
	k.Run(12) // 释放点：0、5、10

	jobs := 0
	for _, e := range k.Log() {
		if e.Kind == LogTerminate && strings.Contains(e.Detail, "job done") {
			jobs++
		}
	}
	if jobs != 3 {
		t.Fatalf("completed jobs = %d, want 3 (no lost/duplicated releases)", jobs)
	}
	if countKind(k.Log(), LogDeadlineMiss) != 0 {
		t.Fatal("no deadline miss expected: budget fits within deadline")
	}
	t.Log("covered: periodic releases at t=0,5,10; exactly one job per period")
}

// TestSameTickDeterminism 验证同一 tick 多个事件同时到达时结果确定且可重放。
func TestSameTickDeterminism(t *testing.T) {
	build := func() *Kernel {
		k := NewKernel(1)
		// 三个任务同一 tick 释放；t1 的截止事件与 t2/t3 的释放同 tick。
		k.AddTask(TaskSpec{Name: "t1", Priority: 1, Deadline: 2, Policy: PolicyLog, Script: []Op{Compute(3)}})
		k.AddTask(TaskSpec{Name: "t2", Priority: 3, Phase: 2, Script: []Op{Compute(1)}})
		k.AddTask(TaskSpec{Name: "t3", Priority: 2, Phase: 2, Script: []Op{Compute(1)}})
		k.Run(100)
		return k
	}
	l1, l2 := build().Log(), build().Log()
	if len(l1) != len(l2) {
		t.Fatalf("replay length mismatch: %d vs %d", len(l1), len(l2))
	}
	for i := range l1 {
		if l1[i].Tick != l2[i].Tick || l1[i].Kind != l2[i].Kind || l1[i].Task != l2[i].Task || l1[i].Detail != l2[i].Detail {
			t.Fatalf("replay diverged at %d: %+v vs %+v", i, l1[i], l2[i])
		}
	}
	// 同 tick 释放时高优先级先运行：t2 先于 t3。
	got := strings.Join(dispatches(l1), " ")
	if !strings.Contains(got, "t2@2 t3@3") {
		t.Fatalf("same-tick releases must run in priority order, got %q", got)
	}
	t.Logf("covered: same-tick event ordering deterministic; dispatches=%s", got)
}
