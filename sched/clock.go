package sched

// Clock 是虚拟时钟：只能随任务实际执行推进，
// 与真实时间和调度开销无关。
type Clock struct {
	now uint64
}

// Now 返回当前虚拟时刻（单位：tick）。
func (c *Clock) Now() uint64 { return c.now }

// advance 推进时钟，仅允许调度器在任务执行时调用。
func (c *Clock) advance(ticks uint64) { c.now += ticks }
