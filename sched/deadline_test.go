package sched

import (
	"fmt"
	"strings"
	"testing"
)

// countLog 统计包含子串的日志条数。
func countLog(s *Scheduler, sub string) int {
	n := 0
	for _, e := range s.Log() {
		if strings.Contains(e.What, sub) {
			n++
		}
	}
	return n
}

// TestDeadlineMissWarn 截止时间超时按 Warn 策略仅记录告警，任务继续。
func TestDeadlineMissWarn(t *testing.T) {
	s := New(Config{Preemptive: true})
	task := &Task{ID: 1, Name: "t", Priority: 1}
	// 周期 10，相对截止 3，但需要 5 tick：每个 job 必然超时一次。
	s.AddPeriodic(task, []Op{{Kind: OpCompute, Ticks: 5}}, 10, 3, 0, PolicyWarn)
	if err := s.RunUntil(25, 100000); err != nil {
		t.Fatalf("run: %v", err)
	}
	var misses int
	for _, a := range s.Alarms() {
		if a.Kind != AlarmDeadlineMiss || a.Policy != PolicyWarn {
			t.Fatalf("unexpected alarm %+v", a)
		}
		misses++
	}
	// tick 0、10、20 各释放一次，共 3 个 job，每个恰好超时一次（去重）。
	if misses != 3 {
		t.Fatalf("deadline misses = %d, want 3", misses)
	}
	if got := countLog(s, "release task 1"); got != 3 {
		t.Fatalf("releases = %d, want 3 (no lost/duplicate events)", got)
	}
	t.Logf("covered: deadline miss with Warn policy, misses=%d", misses)
}

// TestDeadlineMissSuspend 超时后挂起任务，不再释放新 job。
func TestDeadlineMissSuspend(t *testing.T) {
	s := New(Config{Preemptive: true})
	task := &Task{ID: 1, Name: "t", Priority: 1}
	s.AddPeriodic(task, []Op{{Kind: OpCompute, Ticks: 5}}, 10, 3, 0, PolicySuspend)
	if err := s.Run(100000); err != nil {
		t.Fatalf("run: %v", err)
	}
	if task.State() != StateSuspended {
		t.Fatalf("state = %v, want Suspended", task.State())
	}
	if len(s.Alarms()) != 1 || s.Alarms()[0].Kind != AlarmDeadlineMiss {
		t.Fatalf("alarms = %+v, want single DeadlineMiss", s.Alarms())
	}
	// 挂起后不得再有释放事件生效。
	if got := countLog(s, "release task 1"); got != 1 {
		t.Fatalf("releases = %d, want 1 (suspended afterwards)", got)
	}
	t.Logf("covered: deadline miss with Suspend policy, clock=%d", s.Clock().Now())
}

// TestDeadlineMissRestart 超时后重启当前 job。
func TestDeadlineMissRestart(t *testing.T) {
	s := New(Config{Preemptive: true})
	blocker := &Task{ID: 1, Name: "blocker", Priority: 5}
	task := &Task{ID: 2, Name: "t", Priority: 1}
	s.AddTask(blocker, []Op{{Kind: OpCompute, Ticks: 4}})
	// 截止 3，但前 4 tick 被 blocker 占用：job 在 t=3 超时重启，
	// 重启后截止 6，t=4..6 执行 2 tick 完成。
	s.AddPeriodic(task, []Op{{Kind: OpCompute, Ticks: 2}}, 100, 3, 0, PolicyRestart)
	if err := s.RunUntil(50, 100000); err != nil {
		t.Fatalf("run: %v", err)
	}
	if len(s.Alarms()) != 1 || s.Alarms()[0].Kind != AlarmDeadlineMiss ||
		s.Alarms()[0].Policy != PolicyRestart {
		t.Fatalf("alarms = %+v, want single DeadlineMiss/Restart", s.Alarms())
	}
	if got := countLog(s, "task 2 job finished"); got != 1 {
		t.Fatalf("job finishes = %d, want 1 (restarted job completed)", got)
	}
	t.Logf("covered: deadline miss with Restart policy, alarm@%d", s.Alarms()[0].Tick)
}

// TestBudgetOverrun 执行预算耗尽触发告警并按策略处理。
func TestBudgetOverrun(t *testing.T) {
	s := New(Config{Preemptive: true})
	task := &Task{ID: 1, Name: "t", Priority: 1}
	// 预算 2，但需要 5 tick。
	s.AddPeriodic(task, []Op{{Kind: OpCompute, Ticks: 5}}, 50, 0, 2, PolicySuspend)
	if err := s.Run(100000); err != nil {
		t.Fatalf("run: %v", err)
	}
	if len(s.Alarms()) != 1 || s.Alarms()[0].Kind != AlarmBudgetOverrun {
		t.Fatalf("alarms = %+v, want single BudgetOverrun", s.Alarms())
	}
	if task.State() != StateSuspended {
		t.Fatalf("state = %v, want Suspended", task.State())
	}
	// 预算告警应在时钟 2 触发。
	if s.Alarms()[0].Tick != 2 {
		t.Fatalf("alarm tick = %d, want 2", s.Alarms()[0].Tick)
	}
	t.Log("covered: budget overrun with Suspend policy")
}

// TestBudgetExactFit 预算恰好够用时不得误报。
func TestBudgetExactFit(t *testing.T) {
	s := New(Config{Preemptive: true})
	task := &Task{ID: 1, Name: "t", Priority: 1}
	s.AddPeriodic(task, []Op{{Kind: OpCompute, Ticks: 3}}, 10, 3, 3, PolicySuspend)
	if err := s.RunUntil(10, 100000); err != nil {
		t.Fatalf("run: %v", err)
	}
	if len(s.Alarms()) != 0 {
		t.Fatalf("unexpected alarms: %+v", s.Alarms())
	}
	if got := countLog(s, "task 1 job finished"); got != 1 {
		t.Fatalf("job finishes = %d, want 1", got)
	}
	t.Log("covered: exact-fit budget/deadline, no false alarm")
}

// TestSameTickDeterminism 同一 tick 多个事件同时到达时结果确定且可重放。
func TestSameTickDeterminism(t *testing.T) {
	run := func() (string, []LogEntry) {
		s := New(Config{Preemptive: true, TimeSlice: 2})
		// 两个周期任务同刻释放；一个睡眠任务同刻唤醒。
		s.AddPeriodic(&Task{ID: 1, Name: "p1", Priority: 4},
			[]Op{{Kind: OpCompute, Ticks: 1}}, 4, 4, 0, PolicyWarn)
		s.AddPeriodic(&Task{ID: 2, Name: "p2", Priority: 6},
			[]Op{{Kind: OpCompute, Ticks: 1}}, 4, 4, 0, PolicyWarn)
		s.AddTask(&Task{ID: 3, Name: "s3", Priority: 5}, []Op{
			{Kind: OpSleep, Ticks: 4},
			{Kind: OpCompute, Ticks: 1},
		})
		if err := s.RunUntil(13, 1000000); err != nil {
			t.Fatalf("run: %v", err)
		}
		var b strings.Builder
		for _, e := range s.Log() {
			fmt.Fprintf(&b, "%d|%d|%s\n", e.Tick, e.TaskID, e.What)
		}
		return b.String(), s.Log()
	}
	first, log1 := run()
	second, _ := run()
	if first != second {
		t.Fatalf("replay differs:\n--- first ---\n%s\n--- second ---\n%s", first, second)
	}
	// 每个周期任务在 tick 0/4/8/12 各释放一次，共 4 次，不丢不重。
	s := New(Config{Preemptive: true, TimeSlice: 2})
	s.AddPeriodic(&Task{ID: 1, Name: "p1", Priority: 4},
		[]Op{{Kind: OpCompute, Ticks: 1}}, 4, 4, 0, PolicyWarn)
	s.AddPeriodic(&Task{ID: 2, Name: "p2", Priority: 6},
		[]Op{{Kind: OpCompute, Ticks: 1}}, 4, 4, 0, PolicyWarn)
	if err := s.RunUntil(13, 1000000); err != nil {
		t.Fatalf("run: %v", err)
	}
	if got := countLog(s, "release task 1"); got != 4 {
		t.Fatalf("p1 releases = %d, want 4", got)
	}
	if got := countLog(s, "release task 2"); got != 4 {
		t.Fatalf("p2 releases = %d, want 4", got)
	}
	_ = log1
	t.Logf("covered: same-tick event determinism, %d identical log bytes", len(first))
}
