package rtos

// evKind 表示内核事件类型。
type evKind int

const (
	evRelease  evKind = iota // 周期作业释放
	evWakeup                 // 睡眠到期唤醒
	evDeadline               // 截止时间检查
	evExternal               // 外部注入事件
)

// event 是事件队列中的一个确定排序的事件。
type event struct {
	tick int
	seq  uint64 // 同 tick 内的确定次序
	kind evKind
	task *Task
	job  int64  // 仅 evDeadline 使用：校验作业序号
	fn   func() // 仅 evExternal 使用
}

// eventHeap 是按 (tick, seq) 排序的最小堆。
type eventHeap []event

func (h eventHeap) Len() int { return len(h) }
func (h eventHeap) Less(i, j int) bool {
	if h[i].tick != h[j].tick {
		return h[i].tick < h[j].tick
	}
	return h[i].seq < h[j].seq
}
func (h eventHeap) Swap(i, j int)       { h[i], h[j] = h[j], h[i] }
func (h *eventHeap) Push(x interface{}) { *h = append(*h, x.(event)) }
func (h *eventHeap) Pop() interface{} {
	old := *h
	n := len(old)
	e := old[n-1]
	*h = old[:n-1]
	return e
}
