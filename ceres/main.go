// main.go

package main

import (
	"bufio"
	"ceres/aql"
	"ceres/auth"
	"ceres/config"
	"ceres/freespace"
	"ceres/logging"
	"ceres/manager"
	"ceres/queue"
	"ceres/schema"
	"encoding/json"
	"fmt"
	"net"
	"os"
	"strings"
	"time"
)

type Query struct {
	Auth        string `json:"_auth"`
	QueryString string `json:"query"`
}

func main() {
	config.ReadConfigFile()
	logging.Initialize(config.Config.LogLevel)
	logging.INFO("Starting Ceres server")
	freespace.LoadFreeSpace()
	schema.LoadSchema()
	queue.InitQueue()

	logging.TRACE("Ensuring data directory exists")
	os.MkdirAll(config.Config.DataDir, 0755)

	logging.TRACE("Ensuring _auth database exists")
	auth.CheckAuthDatabase()

	logging.INFO(fmt.Sprintf("Listening for connections on port %v", config.Config.Port))
	l, err := net.Listen("tcp4", fmt.Sprintf(":%v", config.Config.Port))
	if err != nil {
		logging.ERROR(err.Error())
		return
	}
	defer l.Close()

	go queryProcessor()

	for {
		c, err := l.Accept()
		if err != nil {
			logging.ERROR(err.Error())
			return
		}
		go handleConnection(c)
	}
}

func queryProcessor() {
	for true {
		if len(queue.Queue) > 0 {
			if queue.Queue[0].Finished == false {
				data, err := handleQuery(*queue.Queue[0])
				queue.Queue[0].Data = data
				queue.Queue[0].Err = err
				queue.Queue[0].Finished = true
			} else {
				time.Sleep(1 * time.Millisecond)
			}
		} else {
			time.Sleep(1 * time.Millisecond)
		}
	}
}

func handleQuery(query queue.QueueObject) ([]map[string]interface{}, error) {
	authString := query.Auth
	text := query.QueryString

	parts := strings.Split(authString, ":")
	username := parts[0]
	password := parts[1]

	actions, err := aql.Parse(text)
	if err != nil {
		return nil, err
	}
	previousIDs := make([]string, 0)
	dataOut := make([]map[string]interface{}, 0)
	for _, action := range actions {
		if err := auth.VerifyUserAction(username, password, action); err != nil {
			return nil, err
		}
		if err := auth.ProtectWrite(action); err != nil {
			return nil, err
		}
		data, err := manager.ProcessAction(action, previousIDs, false)
		if err != nil {
			return nil, err
		}
		if data != nil {
			if len(data) > 0 {
				if _, ok := data[0][".id"]; ok {
					previousIDs = make([]string, 0)
					for _, val := range data {
						previousIDs = append(previousIDs, val[".id"].(string))
					}
				}
			}
		}
		dataOut = data
	}
	return dataOut, nil
}

func handleConnection(c net.Conn) {
	logging.DEBUG(fmt.Sprintf("Serving %s", c.RemoteAddr().String()))
	for {
		netData, err := bufio.NewReader(c).ReadString('\n')
		if err != nil {
			if err.Error() == "EOF" {
				logging.DEBUG("Connection closed")
			} else {
				logging.ERROR(err.Error())
			}
			return
		}

		temp := strings.TrimSpace(string(netData))
		if temp == "EOD" {
			break
		}

		query := Query{}
		json.Unmarshal([]byte(temp), &query)
		logging.TRACE(fmt.Sprintf("Query: %v", query.QueryString))
		queueObject := queue.QueueObject{
			Auth:        query.Auth,
			QueryString: query.QueryString,
			Finished:    false,
		}
		queue.AddToQueue(&queueObject)
		for queueObject.Finished != true {
			time.Sleep(1 * time.Millisecond)
		}
		queue.PopQueue()
		logging.TRACE("Query finished, sending data")
		if queueObject.Err != nil {
			logging.ERROR(queueObject.Err.Error())
			dataByte, _ := json.Marshal(map[string]string{"error": queueObject.Err.Error()})
			dataString := string(dataByte)
			dataString += "EOD"
			c.Write([]byte(dataString))
		} else {
			dataByte, _ := json.Marshal(queueObject.Data)
			dataString := string(dataByte)
			dataString += "EOD"
			c.Write([]byte(dataString))
		}
	}
	c.Close()
}
