package sched

import "errors"

// 互斥锁相关错误，原因可区分。
var (
	ErrNotLocked  = errors.New("sched: unlock of unlocked mutex")
	ErrNotOwner   = errors.New("sched: unlock by non-owner task")
	ErrDeadlock   = errors.New("sched: lock would cause deadlock")
	ErrDoubleLock = errors.New("sched: task already holds the mutex")
)

// Mutex 是任务间共享资源的互斥锁，支持优先级继承。
type Mutex struct {
	Name    string
	owner   *Task
	waiters []*Task // 等待队列，按 (有效优先级降序, 任务ID升序) 排列
}

// Owner 返回当前持锁任务，未持有时返回 nil。
func (m *Mutex) Owner() *Task { return m.owner }

// Waiters 返回当前等待队列的快照（有序）。
func (m *Mutex) Waiters() []*Task {
	out := make([]*Task, len(m.waiters))
	copy(out, m.waiters)
	return out
}

// lockTask 让任务 t 尝试加锁 m。
// 成功则 t 成为持有者；被占用则 t 阻塞并触发优先级继承；
// 重复加锁或死锁等待返回可区分的错误。
func (s *Scheduler) lockTask(t *Task, m *Mutex) error {
	if m.owner == t {
		return ErrDoubleLock
	}
	if m.owner == nil {
		m.owner = t
		t.held = append(t.held, m)
		s.logf("lock %s by task %d", m.Name, t.ID)
		return nil
	}
	// 死锁检测：沿持锁-等待链检查是否回到 t。
	for o := m.owner; o != nil; {
		if o == t {
			return ErrDeadlock
		}
		if o.waitingOn == nil {
			break
		}
		o = o.waitingOn.owner
	}
	// 阻塞并入等待队列。
	m.insertWaiter(t)
	t.waitingOn = m
	t.state = StateBlocked
	s.logf("task %d blocked on %s", t.ID, m.Name)
	// 持有者继承等待者优先级，并沿等待链传播。
	s.refreshInheritance(m.owner)
	return nil
}

// unlockTask 让任务 t 释放锁 m。
// 严格配对：未持锁释放、非持有者释放均报错。
// 释放后恢复持有者优先级，并唤醒队首等待者。
func (s *Scheduler) unlockTask(t *Task, m *Mutex) error {
	if m.owner == nil {
		return ErrNotLocked
	}
	if m.owner != t {
		return ErrNotOwner
	}
	m.owner = nil
	t.held = removeMutex(t.held, m)
	s.logf("unlock %s by task %d", m.Name, t.ID)
	// 恢复 t 的继承状态。
	s.refreshInheritance(t)
	// 唤醒队首等待者，锁直接移交。
	if len(m.waiters) > 0 {
		next := m.waiters[0]
		m.waiters = m.waiters[1:]
		next.waitingOn = nil
		m.owner = next
		next.held = append(next.held, m)
		s.logf("lock %s handed to task %d", m.Name, next.ID)
		s.wake(next)
		s.refreshInheritance(next)
	}
	return nil
}

// insertWaiter 按 (有效优先级降序, 任务ID升序) 将任务插入等待队列。
func (m *Mutex) insertWaiter(t *Task) {
	i := 0
	for i < len(m.waiters) {
		w := m.waiters[i]
		if w.EffectivePriority() < t.EffectivePriority() ||
			(w.EffectivePriority() == t.EffectivePriority() && w.ID > t.ID) {
			break
		}
		i++
	}
	m.waiters = append(m.waiters, nil)
	copy(m.waiters[i+1:], m.waiters[i:])
	m.waiters[i] = t
}

// sortWaiters 按 (有效优先级降序, 任务ID升序) 重排等待队列。
// 运行期优先级变更后调用，保证等待顺序立即反映新优先级。
func (m *Mutex) sortWaiters() {
	for i := 1; i < len(m.waiters); i++ {
		for j := i; j > 0; j-- {
			a, b := m.waiters[j-1], m.waiters[j]
			if a.EffectivePriority() > b.EffectivePriority() ||
				(a.EffectivePriority() == b.EffectivePriority() && a.ID < b.ID) {
				break
			}
			m.waiters[j-1], m.waiters[j] = b, a
		}
	}
}

// refreshInheritance 重算 t 的继承来源，并沿等待链向上传播。
// 继承来源取 t 持有的所有锁的队首等待者中有效优先级最高者。
func (s *Scheduler) refreshInheritance(t *Task) {
	seen := map[*Task]bool{}
	for cur := t; cur != nil && !seen[cur]; {
		seen[cur] = true
		var best *Task
		for _, m := range cur.held {
			if len(m.waiters) == 0 {
				continue
			}
			w := m.waiters[0]
			if best == nil || w.EffectivePriority() > best.EffectivePriority() ||
				(w.EffectivePriority() == best.EffectivePriority() && w.ID < best.ID) {
				best = w
			}
		}
		cur.inheritedFrom = best
		if cur.waitingOn == nil {
			break
		}
		cur = cur.waitingOn.owner
	}
}

func removeMutex(list []*Mutex, m *Mutex) []*Mutex {
	for i, x := range list {
		if x == m {
			return append(list[:i], list[i+1:]...)
		}
	}
	return list
}
