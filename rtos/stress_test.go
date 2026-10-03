package rtos

import (
	"fmt"
	"sync"
	"testing"
)

// TestConcurrentKernels 多 goroutine 并行运行独立内核仿真，
// 在 -race 下验证无数据竞争，且每个内核结果确定。
func TestConcurrentKernels(t *testing.T) {
	const workers = 8
	var wg sync.WaitGroup
	errs := make(chan error, workers)
	for g := 0; g < workers; g++ {
		wg.Add(1)
		go func(seed int) {
			defer wg.Done()
			var prev []LogEntry
			for rep := 0; rep < 2; rep++ { // 每个 worker 内重放一次，校验确定性
				k := NewKernel(2)
				m1 := k.NewMutex("a")
				m2 := k.NewMutex("b")
				for i := 0; i < 20; i++ {
					k.AddTask(TaskSpec{
						Name:     fmt.Sprintf("w%d-t%d", seed, i),
						Priority: (i+seed)%4 + 1,
						Phase:    i % 5,
						Script: []Op{
							Compute(1), LockOp(m1), Compute(1), UnlockOp(m1),
							Sleep(2), LockOp(m2), Compute(1), UnlockOp(m2),
						},
					})
				}
				k.Run(3000)
				log := k.Log()
				if len(log) == 0 {
					errs <- fmt.Errorf("worker %d: empty log", seed)
					return
				}
				if rep == 1 {
					if len(log) != len(prev) {
						errs <- fmt.Errorf("worker %d: replay length %d != %d", seed, len(log), len(prev))
						return
					}
					for i := range log {
						if log[i] != prev[i] {
							errs <- fmt.Errorf("worker %d: replay diverged at %d", seed, i)
							return
						}
					}
				}
				prev = log
			}
		}(g)
	}
	wg.Wait()
	close(errs)
	for err := range errs {
		t.Fatal(err)
	}
	t.Logf("covered: %d concurrent deterministic kernels x2 replays, no data races", workers)
}

// TestConcurrentExternalEvents 多 goroutine 并发调用 SubmitAt（线程安全），
// 验证不丢事件、不重复触发，且按 tick 顺序执行。
func TestConcurrentExternalEvents(t *testing.T) {
	k := NewKernel(1)
	const submitters = 4
	const per = 50
	const total = submitters * per

	var mu sync.Mutex
	var executed []int

	var wg sync.WaitGroup
	for g := 0; g < submitters; g++ {
		wg.Add(1)
		go func(base int) {
			defer wg.Done()
			for i := 0; i < per; i++ {
				tick := (base*per + i) * 2 // 每个事件独占一个 tick，次序完全确定
				k.SubmitAt(tick, func() {
					mu.Lock()
					executed = append(executed, tick)
					mu.Unlock()
				})
			}
		}(g)
	}
	wg.Wait() // 并发提交全部完成后开始仿真
	k.Run(100000)

	if len(executed) != total {
		t.Fatalf("executed %d events, want %d (lost or duplicated)", len(executed), total)
	}
	for i := 1; i < len(executed); i++ {
		if executed[i] <= executed[i-1] {
			t.Fatalf("events out of order at %d: %v", i, executed[i-1:i+1])
		}
	}
	if k.Clock() != (total-1)*2 {
		t.Fatalf("clock = %d, want %d", k.Clock(), (total-1)*2)
	}
	t.Logf("covered: %d goroutines x %d concurrent SubmitAt, exactly-once in tick order", submitters, per)
}

// TestLiveEventChain 验证运行期注入：内核在事件耗尽时阻塞等待外部提交，
// 每个事件处理时再提交下一个事件（模拟中断链），结果确定。
func TestLiveEventChain(t *testing.T) {
	k := NewKernel(1)
	const total = 100
	k.ExpectExternal(total)

	var mu sync.Mutex
	var executed []int
	var chain func(tick int)
	chain = func(tick int) {
		k.SubmitAt(tick, func() {
			mu.Lock()
			executed = append(executed, tick)
			mu.Unlock()
			if tick+2 < total*2 {
				chain(tick + 2) // 在处理中提交下一个事件
			}
		})
	}
	go chain(0) // 首个事件由外部 goroutine 注入，触发内核的等待路径
	k.Run(100000)

	if len(executed) != total {
		t.Fatalf("executed %d events, want %d", len(executed), total)
	}
	for i, tick := range executed {
		if tick != i*2 {
			t.Fatalf("event %d at tick %d, want %d", i, tick, i*2)
		}
	}
	t.Logf("covered: kernel blocks waiting for live submissions; chain of %d events in order", total)
}
