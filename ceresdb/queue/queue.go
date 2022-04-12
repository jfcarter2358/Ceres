package queue

type QueueObject struct {
	Auth        string
	QueryString string
	Data        []map[string]interface{}
	Finished    bool
	Err         error
}

var Queue []*QueueObject

func InitQueue() {
	Queue = make([]*QueueObject, 0)
}

func AddToQueue(queueObject *QueueObject) {
	Queue = append(Queue, queueObject)
}

func PopQueue() {
	Queue = Queue[1:]
}
