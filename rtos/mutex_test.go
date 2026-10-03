package rtos

import (
	"errors"
	"strings"
	"testing"
)

// TestPriorityInheritance 验证优先级继承消除无界优先级反转：
// 低优先级持锁期间，高优先级等锁时，中优先级任务不得插队压制低优先级。
func TestPriorityInheritance(t *testing.T) {
	k := NewKernel(4)
	m := k.NewMutex("res")
	k.AddTask(TaskSpec{Name: "low", Priority: 1, Script: []Op{LockOp(m), Compute(6), UnlockOp(m)}})
	k.AddTask(TaskSpec{Name: "mid", Priority: 2, Phase: 1, Script: []Op{Compute(4)}})
	k.AddTask(TaskSpec{Name: "high", Priority: 3, Phase: 2, Script: []Op{LockOp(m), Compute(1), UnlockOp(m)}})
	k.Run(1000)

	got := strings.Join(dispatches(k.Log()), " ")
	want := "low@0 mid@1 high@2 low@2 high@7 mid@8"
	if got != want {
		t.Fatalf("dispatch order = %q, want %q", got, want)
	}
	// 必须出现继承与恢复各一次。
	if countKind(k.Log(), LogInherit) != 1 {
		t.Fatalf("expected 1 inheritance, got %d", countKind(k.Log(), LogInherit))
	}
	if countKind(k.Log(), LogRestore) != 1 {
		t.Fatalf("expected 1 restore, got %d", countKind(k.Log(), LogRestore))
	}
	// 高优先级任务必须在低优先级释放锁的同一 tick 接手（锁交接）。
	var handoff bool
	for _, e := range k.Log() {
		if e.Kind == LogLock && e.Task == "high" && strings.Contains(e.Detail, "handoff") {
			handoff = true
		}
	}
	if !handoff {
		t.Fatal("expected lock handoff from low to high")
	}
	t.Logf("covered: priority inheritance, lock handoff, restore; dispatches=%s", got)
}

// TestMutexErrors 验证异常释放、重复释放被拒绝且原因可区分。
func TestMutexErrors(t *testing.T) {
	k := NewKernel(4)
	m := k.NewMutex("res")
	// a：未持锁先释放（ErrNotLocked）；随后加锁再重复加锁（ErrDeadlockSelf）。
	k.AddTask(TaskSpec{Name: "a", Priority: 1, Script: []Op{
		UnlockOp(m), LockOp(m), LockOp(m), Compute(2), UnlockOp(m),
	}})
	// b：高优先级，尝试释放 a 持有的锁（ErrNotOwner）。
	k.AddTask(TaskSpec{Name: "b", Priority: 2, Phase: 1, Script: []Op{UnlockOp(m)}})
	k.Run(1000)

	errs := k.Errors()
	if len(errs) != 3 {
		t.Fatalf("expected 3 rejected operations, got %d: %v", len(errs), errs)
	}
	if !errors.Is(errs[0].Err, ErrNotLocked) {
		t.Fatalf("err[0] = %v, want ErrNotLocked", errs[0].Err)
	}
	if !errors.Is(errs[1].Err, ErrDeadlockSelf) {
		t.Fatalf("err[1] = %v, want ErrDeadlockSelf", errs[1].Err)
	}
	if !errors.Is(errs[2].Err, ErrNotOwner) {
		t.Fatalf("err[2] = %v, want ErrNotOwner", errs[2].Err)
	}
	// 非法操作不得破坏正常配对：a 的加锁/解锁各成功一次。
	if countKind(k.Log(), LogLock) != 1 || countKind(k.Log(), LogUnlock) != 1 {
		t.Fatalf("lock/unlock pairing broken, log=%v", k.Log())
	}
	t.Log("covered: ErrNotLocked / ErrDeadlockSelf / ErrNotOwner distinguishable via errors.Is")
}

// TestDeadlockCycle 验证循环等待被检测并拒绝，而不是真的死锁。
func TestDeadlockCycle(t *testing.T) {
	k := NewKernel(4)
	m1 := k.NewMutex("m1")
	m2 := k.NewMutex("m2")
	k.AddTask(TaskSpec{Name: "a", Priority: 1, Script: []Op{
		LockOp(m1), Compute(1), LockOp(m2), Compute(1), UnlockOp(m2), UnlockOp(m1),
	}})
	k.AddTask(TaskSpec{Name: "b", Priority: 2, Phase: 1, Script: []Op{
		LockOp(m2), Compute(1), LockOp(m1), Compute(1), UnlockOp(m1), UnlockOp(m2),
	}})
	k.Run(1000)

	errs := k.Errors()
	// a 的 Lock(m2) 构成等待环被拒绝；随后脚本中的 Unlock(m2) 因未持有同样被拒绝。
	if len(errs) != 2 {
		t.Fatalf("expected 2 rejected operations, got %v", errs)
	}
	if !errors.Is(errs[0].Err, ErrDeadlockCycle) {
		t.Fatalf("err[0] = %v, want ErrDeadlockCycle", errs[0].Err)
	}
	if !errors.Is(errs[1].Err, ErrNotOwner) {
		t.Fatalf("err[1] = %v, want ErrNotOwner", errs[1].Err)
	}
	// 被拒绝后双方仍应运行到结束，仿真不得挂死。
	if countKind(k.Log(), LogTerminate) != 2 {
		t.Fatalf("both tasks must terminate, log=%v", k.Log())
	}
	t.Log("covered: wait-for cycle detected and rejected; simulation completes")
}
