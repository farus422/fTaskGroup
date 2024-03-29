package fTaskGroup

import (
	"context"
	"sync"

	flog "github.com/farus422/fLogSystem"
)

type STaskGroupSync struct {
	STaskGroup
	// idTasks map[string]*sTaskInfoLinkNode
	ProcessingTask map[string]struct{} // 改為執行時期檢查
}

// idOwner : 讓不同的Owner能不重複即可
func (tg *STaskGroupSync) Task(ctx context.Context, f TTASK_HANDLER, data interface{}, ownerId string) *STaskHandle {
	if f == nil {
		return nil
	}
	node := taskNodePool.Get().(*sTaskInfoLinkNode)
	node.function = f
	node.data = data
	node.ownerId = ownerId
	th := &STaskHandle{}
	if ctx == nil {
		th.Ctx, th.Cancel = context.WithCancel(context.Background())
	} else {
		th.Ctx, th.Cancel = context.WithCancel(ctx)
	}
	node.th = th
	tg.taskPush(node, ownerId)
	return th
}

func (tg *STaskGroupSync) taskPush(node *sTaskInfoLinkNode, ownerId string) {
	tg.mutex.Lock()

	// lastSameOwner := tg.idTasks[ownerId]
	// if lastSameOwner != nil {
	// 	lastSameOwner.sameOwnerNext = node
	// 	node.sameOwnerPrev = lastSameOwner
	// }
	// tg.idTasks[ownerId] = node

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

// var test_wonerState = [10]bool{false, false, false, false, false, false, false, false, false, false}
// var test_taskmanRun = [10]string{"", "", "", "", "", "", "", "", "", ""}
// var test_gofRun int = 0

func (tg *STaskGroupSync) getValidNode() *sTaskInfoLinkNode {
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
	// for node = tg.firstNode; (node != nil) && (node.sameOwnerPrev != nil); node = node.next {
	// }
	return node
}

func (tg *STaskGroupSync) taskmanWorkSync(myNumber int) {
	var node, prev, next *sTaskInfoLinkNode

	// rCount := 0
	tg.cond.L.Lock()
	for {
		node = tg.getValidNode()
		for node == nil {
			// cond.Wait 有個漏洞，不能保證 Wait() 之後不會被別人捷足先登，所以這邊用for而不是if
			// Wait() 會解除 Lock() 等到醒來才重新取得，所以無法保證不會被捷足先登
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

		// if node.function != nil {
		// 	rCount++
		// 	test_gofRun++
		// 	if test_wonerState[node.ownerID] {
		// 		fmt.Printf("Error!! ownerID %d are already running\n", node.ownerID)
		// 	}
		// 	test_wonerState[node.ownerID] = true
		// 	test_taskmanRun[myNumber-1] = fmt.Sprintf("%d", node.ownerID)
		// 	fmt.Printf("%03d %d run  = [%1s, %1s, %1s, %1s, %1s, %1s, %1s, %1s, %1s, %1s]\n", rCount, myNumber, test_taskmanRun[0], test_taskmanRun[1], test_taskmanRun[2], test_taskmanRun[3], test_taskmanRun[4], test_taskmanRun[5], test_taskmanRun[6], test_taskmanRun[7], test_taskmanRun[8], test_taskmanRun[9])
		// 	if test_gofRun == tg.taskmanNum {
		// 		fmt.Printf("Taskman is all in running, tg.taskmanNum = %d, test_gofRun = %d\n", test_gofRun, test_gofRun)
		// 	}
		// }

		tg.cond.L.Unlock()
		node.previous = nil
		node.next = nil

		if node.function == nil { // shutdown
			// shutdown不必delete(tg.ProcessingTask, node.ownerId)，因為ownerId必為""
			node.data = nil
			node.th = nil
			node.sameOwnerNext = nil
			taskNodePool.Put(node)
			// fmt.Printf("Taskman #%d - Closed\n", myNumber)
			tg.taskWG.Done()
			return
		}
		select {
		case <-node.th.Ctx.Done():
		default:
			tg.taskFunctionExec(node.th, node.function, node.data)
		}

		tg.cond.L.Lock()
		if node.ownerId != "" {
			delete(tg.ProcessingTask, node.ownerId)
		}
		// if node.sameOwnerNext != nil {
		// 	node.sameOwnerNext.sameOwnerPrev = nil
		// 	node.sameOwnerNext = nil
		// } else {
		// 	tg.idTasks[node.ownerId] = nil
		// }

		// if test_wonerState[node.ownerID] == false {
		// 	fmt.Printf("Error!! ownerID %d are already stop\n", node.ownerID)
		// }
		// test_wonerState[node.ownerID] = false
		// test_taskmanRun[myNumber-1] = ""
		// test_gofRun--
		// fmt.Printf("%03d %d stop = [%1s, %1s, %1s, %1s, %1s, %1s, %1s, %1s, %1s, %1s]\n", rCount, myNumber, test_taskmanRun[0], test_taskmanRun[1], test_taskmanRun[2], test_taskmanRun[3], test_taskmanRun[4], test_taskmanRun[5], test_taskmanRun[6], test_taskmanRun[7], test_taskmanRun[8], test_taskmanRun[9])

		node.data = nil
		node.th = nil
		taskNodePool.Put(node)
	}
}
func (tg *STaskGroupSync) Taskman(num int) {
	tg.Shutdown()
	tg.mutex.Lock()
	if num > 0 {
		tg.serverWG.Add(1)
		tg.taskWG.Add(num)
		tg.taskmanNum = num
		for ; num > 0; num-- {
			go tg.taskmanWorkSync(num)
		}
	}
	tg.mutex.Unlock()
}

func NewTaskGroupSync(groupName string, publisher *flog.SPublisher, serverWG *sync.WaitGroup) *STaskGroupSync {
	// tg := STaskGroupSync{STaskGroup: STaskGroup{serverWG: serverWG, logPublisher: publisher, name: groupName}, idTasks: make(map[string]*sTaskInfoLinkNode)}
	tg := STaskGroupSync{STaskGroup: STaskGroup{serverWG: serverWG, logPublisher: publisher, name: groupName}}
	tg.cond = sync.NewCond(&tg.mutex)
	return &tg
}
