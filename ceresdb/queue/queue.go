package queue

import (
	"ceresdb/aql"
	"ceresdb/auth"
	"ceresdb/config"
	"ceresdb/constants"
	"ceresdb/index"
	"ceresdb/logger"
	"fmt"
	"time"
)

type QueueObject struct {
	User     auth.User
	Query    string
	Output   interface{}
	Finished bool
	Err      error
}

var Queue []*QueueObject

func InitQueue() {
	Queue = make([]*QueueObject, 0)
}

func AddToQueue(qu *QueueObject) {
	Queue = append(Queue, qu)
}

func PopQueue() {
	Queue = Queue[1:]
}

func Run() {
	logger.Infof("", "Starting query queue")
	if config.Config.RetentionPeriod > 0 {
		PruneRetention()
	}
	for {
		if len(Queue) > 0 {
			if Queue[0] != nil && !Queue[0].Finished {
				out, err := aql.ProcessQuery(Queue[0].Query, Queue[0].User)
				Queue[0].Output = out
				Queue[0].Err = err
				Queue[0].Finished = true
			} else {
				time.Sleep(1 * time.Millisecond)
			}
		} else {
			time.Sleep(1 * time.Millisecond)
		}
	}
}

func PruneRetention() {
	for {
		for dbName, d := range index.IndexIDs {
			for cName, _ := range d {
				now := time.Now()
				timeCutoff := int(now.Unix()) - config.Config.RetentionPeriod
				qq := fmt.Sprintf("DELETE RECORD FROM %s.%s WHERE {\"$lte\":{\"%s\":\"%d\"}}", dbName, cName, constants.TIME_KEY, timeCutoff)
				u := auth.User{
					Username: config.Config.AdminUsername,
					Password: "",
					Groups:   []string{constants.GROUP_ADMIN},
					Roles:    []string{constants.ROLE_ADMIN},
				}
				q := QueueObject{
					User:     u,
					Query:    qq,
					Output:   nil,
					Finished: false,
					Err:      nil,
				}
				logger.Tracef("", "Adding retention prune for %s.%s to queue", dbName, cName)
				AddToQueue(&q)
				for !q.Finished {
					time.Sleep(1 * time.Millisecond)
				}
				PopQueue()
				logger.Tracef("", "Retention prune finished for %s.%s", dbName, cName)
			}
		}
		time.Sleep(time.Duration(config.Config.RetentionInterval) * time.Second)
	}
}
