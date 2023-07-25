package taskgroup

import (
	"context"
	"sync"
	"time"
)

type TASK_HANDLER func(taskInfo ITaskController, data interface{})
type TASK_PANIC func(tg *TaskGroup, err any)

type sTaskInfoLinkNode struct {
	function       TASK_HANDLER // nil用來指示執行緒結束
	data           interface{}
	ownerId        string
	th             *taskHandle
	sameOwnerNext  *sTaskInfoLinkNode
	previous, next *sTaskInfoLinkNode
}

var taskNodePool = sync.Pool{
	New: func() interface{} {
		return new(sTaskInfoLinkNode)
	},
}

type TaskGroup struct {
	mutex      sync.Mutex
	taskWG     sync.WaitGroup
	cond       *sync.Cond
	onPanic    TASK_PANIC
	name       string
	taskmanNum int
	firstNode  *sTaskInfoLinkNode
	lastNode   *sTaskInfoLinkNode
}

func (tg *TaskGroup) Task(ctx context.Context, f TASK_HANDLER, data interface{}, timeout time.Duration) ITaskHandle {
	if f == nil {
		return nil
	}
	node := taskNodePool.Get().(*sTaskInfoLinkNode)
	node.function = f
	node.data = data
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

func (tg *TaskGroup) taskPush(node *sTaskInfoLinkNode) {
	tg.mutex.Lock()
	if tg.lastNode != nil {
		node.previous = tg.lastNode
		tg.lastNode.next = node
		tg.lastNode = node
	} else {
		tg.firstNode = node
		tg.lastNode = node
	}
	tg.cond.Signal()
	tg.mutex.Unlock()
}

func (tg *TaskGroup) gorountineClose() {
	node := taskNodePool.Get().(*sTaskInfoLinkNode)
	node.function = nil
	node.data = nil
	node.ownerId = ""
	tg.taskPush(node)
}

func (tg *TaskGroup) taskFunctionExec(th *taskHandle, function TASK_HANDLER, data interface{}) {
	defer func() {
		if err := recover(); err != nil {
			th.CancelWithPanic()
			if tg.onPanic != nil {
				tg.onPanic(tg, err)
			}
		}
		th.Close()
	}()
	function(th, data)
}

func (tg *TaskGroup) taskmanWork(myNumber int) {
	var node *sTaskInfoLinkNode

	for {
		node = nil
		tg.cond.L.Lock()
		for tg.firstNode == nil {
			// Wait() 會解除 Lock() 等到醒來才重新取得，無法保證不會被捷足先登，所以這邊用for而不是if
			tg.cond.Wait()
		}
		node = tg.firstNode

		// firstNode下移
		tg.firstNode = tg.firstNode.next
		if tg.firstNode != nil {
			tg.firstNode.previous = nil
		} else {
			tg.lastNode = nil
		}

		tg.cond.L.Unlock()
		node.next = nil

		if node.function == nil {
			node.data = nil
			taskNodePool.Put(node)
			tg.taskWG.Done()
			return
		}
		select {
		case <-node.th.ctx.Done():
		default:
			tg.taskFunctionExec(node.th, node.function, node.data)
		}
		node.data = nil
		node.th = nil
		taskNodePool.Put(node)
	}
}

func (tg *TaskGroup) Taskman(num int) {
	tg.closeAll()
	tg.mutex.Lock()
	if num > 0 {
		tg.taskWG.Add(num)
		tg.taskmanNum = num
		for ; num > 0; num-- {
			go tg.taskmanWork(num)
		}
	}
	tg.mutex.Unlock()
}

func (tg *TaskGroup) Shutdown() {
	tg.closeAll()
}

func (tg *TaskGroup) WaitForShutdownComplete() {
	tg.taskWG.Wait()
}

func (tg *TaskGroup) closeAll() {
	tg.mutex.Lock()
	num := tg.taskmanNum
	tg.mutex.Unlock()
	for num > 0 {
		num--
		tg.gorountineClose()
	}
}

func NewTaskGroup(groupName string, onPanic TASK_PANIC) *TaskGroup {
	tg := TaskGroup{name: groupName, onPanic: onPanic}
	tg.cond = sync.NewCond(&tg.mutex)
	return &tg
}
