package taskgroup

import (
	"context"
	"sync/atomic"
	"time"
)

const (
	DEFAULTTIMEOUT_WAITTODONE = 2 * time.Second // 預設的等待時間(2秒)
)

// 給外部使用
type ITaskHandle interface {
	Cancel()                                  // 中途取消工作
	WaitToDone()                              // 等待任務成功、逾時、取消或panic，任何其一發生則返回
	IsSuccess() bool                          // 自WaitToDone()返回後，檢測是結果是否為成功
	IsCanceled() bool                         // 自WaitToDone()返回後，檢測是否為外部呼叫Cancel()
	IsPanic() bool                            // 自WaitToDone()返回後，檢測是否為panic
	IsTimeout() (deadline time.Time, ok bool) // 自WaitToDone()返回後，檢測是否為timeout
}

// 給執行任務的協程使用
type ITaskController interface {
	JobIsCanceled() bool     // 檢測目前執行中的任務是否已被取消或逾時
	GetCtx() context.Context // 獲取Context
	Success()                // 標記任務success
}

type taskHandle struct {
	ctx         context.Context
	cancel      context.CancelFunc
	statusSeted uint32
	isSuccess   bool
	isCanceled  bool
	hasPanic    bool
}

// 由外部取消工作
func (d *taskHandle) Cancel() {
	if atomic.CompareAndSwapUint32(&d.statusSeted, 0, 1) {
		d.isCanceled = true
		if d.cancel != nil {
			d.cancel()
		}
	}
}

// 給外部使用，等待任務成功、逾時、取消或panic，任何其一發生則返回
func (d *taskHandle) WaitToDone() {
	<-d.ctx.Done()
	atomic.StoreUint32(&d.statusSeted, 1)
}

// 自WaitToDone()返回後，檢測是結果是否為成功
func (d *taskHandle) IsSuccess() bool {
	return d.isSuccess
}

// 自WaitToDone()返回後，檢測是否為外部呼叫Cancel()
func (d *taskHandle) IsCanceled() bool {
	if d.isSuccess {
		return false
	}
	return d.isCanceled
}

// 自WaitToDone()返回後，檢測是否為panic
func (d *taskHandle) IsPanic() bool {
	return d.hasPanic
}

// 自WaitToDone()返回後，檢測是否為timeout
func (d *taskHandle) IsTimeout() (deadline time.Time, ok bool) {
	return d.ctx.Deadline()
}

// 給執行任務的協程使用，檢測目前執行中的任務是否已被取消或逾時
func (d *taskHandle) JobIsCanceled() bool {
	if atomic.LoadUint32(&d.statusSeted) != 0 {
		return true
	}
	select {
	case <-d.ctx.Done():
		return true
	default:
		return false
	}
}

// 獲取Context
func (d *taskHandle) GetCtx() context.Context {
	return d.ctx
}

// 給執行任務的協程使用，標記任務success
func (d *taskHandle) Success() {
	if atomic.CompareAndSwapUint32(&d.statusSeted, 0, 1) {
		d.isSuccess = true
		if d.cancel != nil {
			d.cancel()
		}
	}
}

func (d *taskHandle) Close() {
	if atomic.CompareAndSwapUint32(&d.statusSeted, 0, 1) {
		if d.cancel != nil {
			d.cancel()
		}
	}
}

func (d *taskHandle) CancelWithPanic() {
	if atomic.CompareAndSwapUint32(&d.statusSeted, 0, 1) {
		d.hasPanic = true
		if d.cancel != nil {
			d.cancel()
		}
	}
}
