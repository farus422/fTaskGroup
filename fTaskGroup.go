package fTaskGroup

import (
	"context"
	"sync"

	fcb "github.com/farus422/fCallstack"
	flog "github.com/farus422/fLogSystem"
)

type TTASK_HANDLER func(data interface{})

type sTaskInfoLinkNode struct {
	function       TTASK_HANDLER
	data           interface{}
	ownerID        int64
	sameOwnerPrev  *sTaskInfoLinkNode
	sameOwnerNext  *sTaskInfoLinkNode
	previous, next *sTaskInfoLinkNode
}

type STaskGroup struct {
	mutex        sync.Mutex
	ctx          context.Context
	cancel       context.CancelFunc
	serverWG     *sync.WaitGroup
	taskWG       sync.WaitGroup
	cond         *sync.Cond
	logPublisher *flog.SPublisher
	name         string
	taskmanNum   int
	firstNode    *sTaskInfoLinkNode
	lastNode     *sTaskInfoLinkNode
}

var taskNodePool = sync.Pool{
	New: func() interface{} {
		return new(sTaskInfoLinkNode)
	},
}

func (tg *STaskGroup) Task(f TTASK_HANDLER, data interface{}) {
	if f == nil {
		return
	}
	node := taskNodePool.Get().(*sTaskInfoLinkNode)
	node.function = f
	node.data = data
	tg.taskPush(node)
}

func (tg *STaskGroup) taskPush(node *sTaskInfoLinkNode) {
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

func (tg *STaskGroup) taskFunctionExec(function TTASK_HANDLER, data interface{}) {
	defer func() {
		if err := recover(); err != nil {
			if tg.logPublisher != nil {
				// log := flog.NewLog(flog.LOGLEVELError, "").AddPanicCallstack(0, ".(*STaskGroup).taskFunctionExec")
				log := flog.Panic(flog.LOGLEVELError, ".(*STaskGroup).taskFunctionExec", "")
				tg.logPublisher.Publish(log.SetCaption("%s() 發生panic, %v", log.GetFunctionName(), err))
			}
			return
		}
	}()
	function(data)
}
func (tg *STaskGroup) taskmanWork(myNumber int) {
	var node *sTaskInfoLinkNode

	for {
		node = nil
		tg.cond.L.Lock()
		for tg.firstNode == nil {
			// cond.Wait 有個漏洞，不能保證 Wait() 之後不會被別人捷足先登，所以這邊用for而不是if
			// Wait() 會解除 Lock() 等到醒來才重新取得，所以無法保證不會被捷足先登
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
			// fmt.Printf("Taskman #%d - Closed\n", myNumber)
			tg.taskWG.Done()
			return
		}
		tg.taskFunctionExec(node.function, node.data)
		node.data = nil
		taskNodePool.Put(node)
	}
}
func (tg *STaskGroup) Taskman(num int) {
	tg.Shutdown()
	tg.mutex.Lock()
	if num > 0 {
		tg.serverWG.Add(1)
		tg.taskWG.Add(num)
		tg.taskmanNum = num
		for ; num > 0; num-- {
			go tg.taskmanWork(num)
		}
	}
	tg.mutex.Unlock()
}

func (tg *STaskGroup) Shutdown() {
	tg.mutex.Lock()
	if tg.taskmanNum > 0 {
		tg.mutex.Unlock()
		for i := tg.taskmanNum; i > 0; i-- {
			node := taskNodePool.Get().(*sTaskInfoLinkNode)
			node.function = nil
			node.data = nil
			tg.taskPush(node)
		}
		tg.taskWG.Wait()
		tg.serverWG.Done()
	} else {
		tg.mutex.Unlock()
	}
	// fmt.Printf("Taskman %d taskman is closed - shutdown\n", tg.taskmanNum)
}

func NewTaskGroup(ctx context.Context, groupName string, publisher *flog.SPublisher, serverWG *sync.WaitGroup) *STaskGroup {
	tg := STaskGroup{serverWG: serverWG, logPublisher: publisher, name: groupName}
	if ctx == nil {
		tg.ctx, tg.cancel = context.WithCancel(context.Background())
	} else {
		tg.ctx, tg.cancel = context.WithCancel(ctx)
	}
	tg.cond = sync.NewCond(&tg.mutex)
	return &tg
}

func init() {
	fcb.AddDefaultHiddenCaller(".(*STaskGroup).taskFunctionExec")
}
