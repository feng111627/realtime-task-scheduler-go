package sched

// eventKind 区分同一 tick 内到达的事件类型。
type eventKind int

const (
	evRelease eventKind = iota // 周期任务释放
	evWakeup                   // 睡眠任务唤醒
)

// event 是事件队列中的一个待处理事件。
// 同一 tick 内的事件按 (kind, 任务优先级降序, 任务ID升序, seq) 排序，
// 保证结果确定且可重放。
type event struct {
	tick uint64
	kind eventKind
	task *Task
	seq  uint64
}

// eventQueue 是确定性的最小堆事件队列。
type eventQueue struct {
	items []event
	seq   uint64
}

func (q *eventQueue) len() int { return len(q.items) }

// less 定义事件的确定性全序：
// 先按 tick，再按事件类型（释放先于唤醒），再按任务有效优先级降序，
// 再按任务 ID 升序，最后按插入序号，保证同 tick 多事件结果可重放。
func less(a, b event) bool {
	if a.tick != b.tick {
		return a.tick < b.tick
	}
	if a.kind != b.kind {
		return a.kind < b.kind
	}
	pa, pb := a.task.EffectivePriority(), b.task.EffectivePriority()
	if pa != pb {
		return pa > pb
	}
	if a.task.ID != b.task.ID {
		return a.task.ID < b.task.ID
	}
	return a.seq < b.seq
}

// push 插入一个事件。
func (q *eventQueue) push(tick uint64, kind eventKind, t *Task) {
	e := event{tick: tick, kind: kind, task: t, seq: q.seq}
	q.seq++
	q.items = append(q.items, e)
	for i := len(q.items) - 1; i > 0; {
		p := (i - 1) / 2
		if less(q.items[i], q.items[p]) {
			q.items[i], q.items[p] = q.items[p], q.items[i]
			i = p
		} else {
			break
		}
	}
}

// removeFor 移除指定任务的某类全部待处理事件（如中断取消睡眠唤醒）。
// 移除后重建堆，保持确定性全序。
func (q *eventQueue) removeFor(t *Task, kind eventKind) {
	kept := q.items[:0]
	for _, e := range q.items {
		if e.task != t || e.kind != kind {
			kept = append(kept, e)
		}
	}
	q.items = kept
	// 堆化：自底向上下沉。
	for i := len(q.items)/2 - 1; i >= 0; i-- {
		for j := i; ; {
			l, r := 2*j+1, 2*j+2
			m := j
			if l < len(q.items) && less(q.items[l], q.items[m]) {
				m = l
			}
			if r < len(q.items) && less(q.items[r], q.items[m]) {
				m = r
			}
			if m == j {
				break
			}
			q.items[j], q.items[m] = q.items[m], q.items[j]
			j = m
		}
	}
}

// peek 返回队首事件，调用方需先确认 len>0。
func (q *eventQueue) peek() event { return q.items[0] }

// pop 弹出队首事件。
func (q *eventQueue) pop() event {
	top := q.items[0]
	n := len(q.items) - 1
	q.items[0] = q.items[n]
	q.items = q.items[:n]
	for i := 0; ; {
		l, r := 2*i+1, 2*i+2
		m := i
		if l < len(q.items) && less(q.items[l], q.items[m]) {
			m = l
		}
		if r < len(q.items) && less(q.items[r], q.items[m]) {
			m = r
		}
		if m == i {
			break
		}
		q.items[i], q.items[m] = q.items[m], q.items[i]
		i = m
	}
	return top
}
