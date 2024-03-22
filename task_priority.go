package minerva

type TaskPriority struct {
	task     ITask
	priority int // The lower, the higher priority
	index    int // The index of the item in the heap.
}
type PriorityQueue []*TaskPriority

func (pq PriorityQueue) Len() int { return len(pq) }
func (pq PriorityQueue) Swap(i, j int) {
	pq[i], pq[j] = pq[j], pq[i]
	pq[i].index = i
	pq[j].index = j
}

func (pq *PriorityQueue) Push(x interface{}) {
	n := len(*pq)
	item := x.(*TaskPriority)
	item.index = n
	*pq = append(*pq, item)
}

func (pq *PriorityQueue) Pop() interface{} {
	old := *pq
	n := len(old)
	item := old[n-1]
	old[n-1] = nil
	item.index = -1
	*pq = old[0 : n-1]
	return item
}
func (pq PriorityQueue) Less(i, j int) bool {
	if pq[i].priority == pq[j].priority {
		return pq[i].task.GetCreatedAt().Before(pq[j].task.GetCreatedAt())
	}
	return pq[i].priority < pq[j].priority
}
