package rtos

import (
	"errors"
	"fmt"
)

// 互斥锁相关错误，可用 errors.Is 区分。
var (
	ErrNotLocked     = errors.New("rtos: unlock of unlocked mutex")
	ErrNotOwner      = errors.New("rtos: unlock by non-owner task")
	ErrDeadlockSelf  = errors.New("rtos: task re-locks mutex it already holds")
	ErrDeadlockCycle = errors.New("rtos: lock would create a wait-for cycle")
)

// Mutex 是支持优先级继承的互斥锁。
type Mutex struct {
	name    string
	owner   *Task
	waiters []*Task // 等待队列，按到达先后；选锁主时取有效优先级最高者
}

// Name 返回锁名称。
func (m *Mutex) Name() string { return m.name }

// lock 让 t 尝试获得 m。非法操作被拒绝并记录可区分错误。
func (k *Kernel) lock(t *Task, m *Mutex) {
	switch {
	case m.owner == t:
		k.recordErr(t, ErrDeadlockSelf)
		return
	case m.owner == nil:
		m.owner = t
		t.held = append(t.held, m)
		k.logf(LogLock, t, "lock "+m.name)
		return
	}
	// 等待环检测：沿 owner 的等待链查找是否回到 t。
	for o := m.owner; o != nil; {
		if o == t {
			k.recordErr(t, ErrDeadlockCycle)
			return
		}
		if o.waiting == nil {
			break
		}
		o = o.waiting.owner
	}
	// 阻塞并入等待队列，触发优先级继承（沿等待链传播）。
	m.waiters = append(m.waiters, t)
	t.waiting = m
	t.state = StateBlocked
	k.logf(LogBlock, t, "wait "+m.name)
	k.boost(m.owner, t.effPrio)
}

// boost 沿等待链把优先级 prio 传播给所有需要继承的任务。
func (k *Kernel) boost(t *Task, prio int) {
	for t != nil && t.effPrio < prio {
		t.effPrio = prio
		k.logf(LogInherit, t, fmt.Sprintf("inherit prio=%d", prio))
		if t.waiting == nil {
			break
		}
		t = t.waiting.owner
	}
}

// unlock 让 t 释放 m，并把锁直接交接给最高优先级等待者。
func (k *Kernel) unlock(t *Task, m *Mutex) {
	switch {
	case m.owner == nil:
		k.recordErr(t, ErrNotLocked)
		return
	case m.owner != t:
		k.recordErr(t, ErrNotOwner)
		return
	}
	for i, h := range t.held {
		if h == m {
			t.held = append(t.held[:i], t.held[i+1:]...)
			break
		}
	}
	k.logf(LogUnlock, t, "unlock "+m.name)
	if len(m.waiters) > 0 {
		// 选有效优先级最高、到达最早的等待者直接交接。
		best := 0
		for i, w := range m.waiters {
			if w.effPrio > m.waiters[best].effPrio {
				best = i
			}
		}
		w := m.waiters[best]
		m.waiters = append(m.waiters[:best], m.waiters[best+1:]...)
		m.owner = w
		w.waiting = nil
		w.held = append(w.held, m)
		k.logf(LogLock, w, "handoff "+m.name)
		k.makeReady(w)
	} else {
		m.owner = nil
	}
	k.restorePrio(t)
}

// restorePrio 依据剩余持锁的等待者重算有效优先级，并沿等待链向上恢复。
func (k *Kernel) restorePrio(t *Task) {
	for t != nil {
		p := t.basePrio
		for _, m := range t.held {
			for _, w := range m.waiters {
				if w.effPrio > p {
					p = w.effPrio
				}
			}
		}
		if p != t.effPrio {
			t.effPrio = p
			k.logf(LogRestore, t, fmt.Sprintf("restore prio=%d", p))
		}
		if t.waiting == nil {
			return
		}
		t = t.waiting.owner
	}
}
