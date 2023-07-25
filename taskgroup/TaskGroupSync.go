package taskgroup

import (
	"context"
	"sync"
	"time"
)

type TaskGroupSync struct {
	TaskGroup
	ProcessingTask map[string]struct{}
}

// ownerId : 讓不同的Owner能不重複即可
func (tg *TaskGroupSync) Task(ctx context.Context, f TASK_HANDLER, data interface{}, ownerId string, timeout time.Duration) ITaskHandle {
	if f == nil {
		return nil
	}
	node := taskNodePool.Get().(*sTaskInfoLinkNode)
	node.function = f
	node.data = data
	node.ownerId = ownerId
	th := &taskHandle{}
	if ctx == nil {
		ctx = context.Background()
	}
	if timeout > 0 {
		th.ctx, th.cancel = context.WithTimeout(ctx, timeout)
	} else {
		th.ctx, th.cancel = context.WithCancel(ctx)
	}
	node.th = th
	tg.taskPush(node)
	return th
}

func (tg *TaskGroupSync) getValidNode() *sTaskInfoLinkNode {
	var node *sTaskInfoLinkNode
	var ok bool

	node = tg.firstNode
	for node != nil && node.ownerId != "" {
		if _, ok = tg.ProcessingTask[node.ownerId]; !ok {
			tg.ProcessingTask[node.ownerId] = struct{}{}
			return node
		}
		node = node.next
	}
	return node
}

func (tg *TaskGroupSync) taskmanWorkSync(myNumber int) {
	var node, prev, next *sTaskInfoLinkNode

	tg.cond.L.Lock()
	for {
		node = tg.getValidNode()
		for node == nil {
			// Wait() 會解除 Lock() 等到醒來才重新取得，所以無法保證不會被捷足先登，所以這邊用for而不是if
			tg.cond.Wait()
			node = tg.getValidNode()
		}

		// pop並縫合
		next = node.next
		prev = node.previous
		if prev != nil {
			prev.next = node.next
		} else {
			tg.firstNode = next
		}
		if next != nil {
			next.previous = node.previous
		} else {
			tg.lastNode = prev
		}

		tg.cond.L.Unlock()
		node.previous = nil
		node.next = nil

		if node.function == nil { // shutdown
			// shutdown不必delete(tg.ProcessingTask, node.ownerId)，因為ownerId必為""
			node.data = nil
			node.th = nil
			node.sameOwnerNext = nil
			taskNodePool.Put(node)
			tg.taskWG.Done()
			return
		}
		select {
		case <-node.th.ctx.Done():
		default:
			tg.taskFunctionExec(node.th, node.function, node.data)
		}

		tg.cond.L.Lock()
		if node.ownerId != "" {
			delete(tg.ProcessingTask, node.ownerId)
		}

		node.data = nil
		node.th = nil
		taskNodePool.Put(node)
	}
}

func (tg *TaskGroupSync) Taskman(num int) {
	tg.closeAll()
	tg.mutex.Lock()
	if num > 0 {
		tg.taskWG.Add(num)
		tg.taskmanNum = num
		for ; num > 0; num-- {
			go tg.taskmanWorkSync(num)
		}
	}
	tg.mutex.Unlock()
}

func NewTaskGroupSync(groupName string, onPanic TASK_PANIC) *TaskGroupSync {
	tg := TaskGroupSync{TaskGroup: TaskGroup{name: groupName, onPanic: onPanic}, ProcessingTask: make(map[string]struct{})}
	tg.cond = sync.NewCond(&tg.mutex)
	return &tg
}
