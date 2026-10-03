package rtos

// OpKind 表示任务脚本中一条指令的类型。
type OpKind int

const (
	OpCompute OpKind = iota // 占用 CPU N 个虚拟 tick
	OpLock                  // 申请互斥锁
	OpUnlock                // 释放互斥锁
	OpSleep                 // 阻塞 N 个虚拟 tick
	OpYield                 // 主动让出处理器
)

// Op 是任务脚本中的一条指令。
type Op struct {
	Kind OpKind
	N    int    // Compute/Sleep 的 tick 数
	M    *Mutex // Lock/Unlock 的目标锁
}

// Compute 构造一条消耗 n 个虚拟 tick 的指令。
func Compute(n int) Op { return Op{Kind: OpCompute, N: n} }

// LockOp 构造一条加锁指令。
func LockOp(m *Mutex) Op { return Op{Kind: OpLock, M: m} }

// UnlockOp 构造一条解锁指令。
func UnlockOp(m *Mutex) Op { return Op{Kind: OpUnlock, M: m} }

// Sleep 构造一条阻塞 n 个虚拟 tick 的指令。
func Sleep(n int) Op { return Op{Kind: OpSleep, N: n} }

// Yield 构造一条主动让出处理器的指令。
func Yield() Op { return Op{Kind: OpYield} }
