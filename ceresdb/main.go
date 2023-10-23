// main.go

package main

import (
	"ceresdb/aql"
	"ceresdb/auth"
	"ceresdb/config"
	"ceresdb/freespace"
	"ceresdb/logging"
	"ceresdb/manager"
	"ceresdb/queue"
	"ceresdb/schema"
	"encoding/json"
	"fmt"
	"io/ioutil"
	"net/http"
	"os"
	"path/filepath"
	"reflect"
	"strconv"
	"strings"
	"time"

	"github.com/gin-gonic/gin"
)

type Query struct {
	Auth        string `json:"_auth"`
	QueryString string `json:"query"`
}

// type Snapshot struct {
// 	FreeSpace freespace.FreeSpaceStruct `json:"free_space"`
// 	Schema    schema.SchemaStruct       `json:"schema"`
// 	Data      map[string]interface{}    `json:"data"`
// 	Indices   map[string]interface{}    `json:"indices"`
// }

var router *gin.Engine

// const SNAPSHOT_DELAY = 5

func main() {
	gin.SetMode(gin.ReleaseMode)

	config.ReadConfigFile()
	logging.Initialize(config.Config.LogLevel)
	logging.INFO("Starting Ceres server")
	// freespace.LoadFreeSpace()
	// schema.LoadSchema()
	queue.InitQueue()

	logging.TRACE("Ensuring data directory exists")
	os.MkdirAll(config.Config.DataDir, 0755)

	logging.TRACE("Ensuring _auth database exists")
	auth.CheckAuthDatabase()

	if config.Config.Leader != "" {
		go snapshotProcessor()
	}

	go queryProcessor()

	routerPort := ":" + strconv.Itoa(config.Config.Port)

	logging.INFO(fmt.Sprintf("Listening for connections on port %v", config.Config.Port))
	router = gin.Default()

	// Initialize the routes
	initializeRoutes()

	// Start serving the application
	router.Run(routerPort)
}

func snapshotProcessor() {
	for {
		if config.Config.FollowerAuth == "" {
			logging.FATAL("Follower auth information required to connect to a leader")
			os.Exit(1)
		}
		url := fmt.Sprintf("http://%s@%s/api/snapshot", config.Config.FollowerAuth, config.Config.Leader)
		resp, err := http.Get(url)
		if err != nil {
			logging.ERROR(fmt.Sprintf("Unable to contact leader: %v", err))
			time.Sleep(SNAPSHOT_DELAY * time.Second)
			continue
		}
		body, err := ioutil.ReadAll(resp.Body)
		if err != nil {
			logging.ERROR(fmt.Sprintf("Unable to read response body: %v", err))
			time.Sleep(SNAPSHOT_DELAY * time.Second)
			continue
		}

		var snapshot Snapshot

		err = json.Unmarshal(body, &snapshot)
		if err != nil {
			logging.ERROR(fmt.Sprintf("Unable to read snapshot json: %v", err))
			time.Sleep(SNAPSHOT_DELAY * time.Second)
			continue
		}

		freespace_bytes, _ := json.Marshal(snapshot.FreeSpace)
		schema_bytes, _ := json.Marshal(snapshot.Schema)

		json.Unmarshal(freespace_bytes, &freespace.FreeSpace)
		json.Unmarshal(schema_bytes, &schema.Schema)

		freespace.WriteFreeSpace()
		schema.WriteSchema()

		err = writeDataToStructure(config.Config.DataDir, snapshot.Data)
		if err != nil {
			logging.ERROR(fmt.Sprintf("Unable to write data: %v", err))
		}
		err = writeDataToStructure(config.Config.IndexDir, snapshot.Indices)
		if err != nil {
			logging.ERROR(fmt.Sprintf("Unable to write indices: %v", err))
		}
		time.Sleep(SNAPSHOT_DELAY * time.Second)
	}
}

func queryProcessor() {
	for {
		if len(queue.Queue) > 0 {
			if queue.Queue[0] != nil && !queue.Queue[0].Finished {
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
	if query.Snapshot {
		logging.DEBUG("Begin handling Snapshot")
		dataOut, err := handleSnapshot()
		if err != nil {
			return nil, err
		}
		logging.DEBUG("Done!")
		return dataOut, nil
	}

	logging.DEBUG("Begin handling query")
	authString := query.Auth
	text := query.QueryString

	parts := strings.Split(authString, ":")
	username := parts[0]
	password := parts[1]

	logging.TRACE("Parsing AQL")
	actions, err := aql.Parse(text)
	if err != nil {
		return nil, err
	}
	previousIDs := make([]string, 0)
	dataOut := make([]map[string]interface{}, 0)
	logging.TRACE("Processing actions")
	for _, action := range actions {
		if err := auth.VerifyUserAction(username, password, action); err != nil {
			return nil, err
		}
		if err := auth.ProtectWrite(action); err != nil {
			return nil, err
		}
		data, err := manager.ProcessAction(action, previousIDs, dataOut, false)
		if err != nil {
			return nil, err
		}
		if data != nil {
			if len(data) > 0 {
				if _, ok := data[0][".id"]; ok {
					previousIDs = make([]string, 0)
					for _, val := range data {
						if val[".id"] != nil {
							previousIDs = append(previousIDs, val[".id"].(string))
						}
					}
				}
			}
		}
		logging.DEBUG(fmt.Sprintf("Action: %v", action))
		logging.TRACE(fmt.Sprintf("Data: %v", data))
		dataOut = data
	}
	logging.TRACE(fmt.Sprintf("Data out: %v", dataOut))
	logging.DEBUG("Done!")
	return dataOut, nil
}

func handleSnapshot() ([]map[string]interface{}, error) {
	snapshot := Snapshot{}
	data, err := readDataFromStructure(config.Config.DataDir)
	if err != nil {
		return nil, err
	}
	snapshot.Data = data
	indices, err := readDataFromStructure(config.Config.IndexDir)
	if err != nil {
		return nil, err
	}
	snapshot.Indices = indices
	snapshot.FreeSpace = freespace.FreeSpace
	snapshot.Schema = schema.Schema
	var outputSingle map[string]interface{}
	singleBytes, _ := json.Marshal(snapshot)
	json.Unmarshal(singleBytes, &outputSingle)

	return []map[string]interface{}{outputSingle}, nil
}

func handleQueryEndpoint(c *gin.Context) {
	var query Query

	logging.TRACE("Getting basic auth info")
	user, password, hasAuth := c.Request.BasicAuth()
	if !hasAuth {
		c.JSON(http.StatusForbidden, gin.H{"error": "Authentication required"})
		return
	}

	c.BindJSON(&query)

	query.Auth = fmt.Sprintf("%s:%s", user, password)

	logging.TRACE(fmt.Sprintf("Query: %v", query.QueryString))
	queueObject := queue.QueueObject{
		Auth:        query.Auth,
		QueryString: query.QueryString,
		Finished:    false,
	}
	queue.AddToQueue(&queueObject)
	for !queueObject.Finished {
		time.Sleep(1 * time.Millisecond)
	}
	queue.PopQueue()
	logging.TRACE("Query finished, sending data")

	if queueObject.Err != nil {
		logging.ERROR(queueObject.Err.Error())
		c.JSON(http.StatusInternalServerError, gin.H{"error": queueObject.Err.Error()})
	} else {
		c.JSON(http.StatusOK, queueObject.Data)
	}
}

func handleSnapshotEndpoint(c *gin.Context) {
	logging.TRACE("Getting basic auth info")
	user, password, hasAuth := c.Request.BasicAuth()
	if !hasAuth {
		c.JSON(http.StatusForbidden, gin.H{"error": "Authentication required"})
		return
	}

	if err := auth.VerifyCredentials(user, password); err != nil {
		logging.ERROR(err.Error())
		c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
		return
	}

	auth := fmt.Sprintf("%s:%s", user, password)

	queueObject := queue.QueueObject{
		Auth:     auth,
		Finished: false,
		Snapshot: true,
	}
	queue.AddToQueue(&queueObject)
	for !queueObject.Finished {
		time.Sleep(1 * time.Millisecond)
	}
	queue.PopQueue()
	logging.TRACE("Snapshot finished, sending data")

	if queueObject.Err != nil {
		logging.ERROR(queueObject.Err.Error())
		c.JSON(http.StatusInternalServerError, gin.H{"error": queueObject.Err.Error()})
	} else {
		c.JSON(http.StatusOK, queueObject.Data[0])
	}
}

func walkPath(root string) ([]string, []string, error) {
	var files []string
	var dirs []string
	paths, err := ioutil.ReadDir(root)
	for _, path := range paths {
		if path.IsDir() {
			dirs = append(dirs, path.Name())
		} else {
			files = append(files, path.Name())
		}
	}
	return files, dirs, err
}

func readDataFromStructure(path string) (map[string]interface{}, error) {
	files, dirs, err := walkPath(path)
	if err != nil {
		return nil, err
	}
	output := map[string]interface{}{}
	for _, fileName := range files {
		data, err := os.ReadFile(filepath.Join(path, fileName))
		if err != nil {
			return nil, err
		}
		output[fileName] = string(data)
	}
	for _, dirName := range dirs {
		dirPath := filepath.Join(path, dirName)
		contents, err := readDataFromStructure(dirPath)
		if err != nil {
			return nil, err
		}
		output[dirName] = contents
	}
	return output, nil
}

func writeDataToStructure(path string, input map[string]interface{}) error {
	for key, value := range input {
		childPath := filepath.Join(path, key)
		t := reflect.TypeOf(value)
		switch t.Kind().String() {
		case "string":
			// Is a file
			logging.TRACE(fmt.Sprintf("Writing data to %s", childPath))
			if err := os.WriteFile(childPath, []byte(value.(string)), 0644); err != nil {
				return err
			}
		default:
			// Is a directory
			logging.TRACE(fmt.Sprintf("Removing existing directory at %s", childPath))
			if err := os.RemoveAll(childPath); err != nil {
				return err
			}
			logging.TRACE(fmt.Sprintf("Creating directory at %s", childPath))
			if err := os.MkdirAll(childPath, os.ModePerm); err != nil {
				return err
			}
			if err := writeDataToStructure(childPath, value.(map[string]interface{})); err != nil {
				return err
			}
		}
	}
	return nil
}
