package main

import (
	"ceresdb/config"
	"ceresdb/logger"
	"ceresdb/queue"
	"ceresdb/startup"
	"os"
	"strconv"

	"github.com/gin-gonic/gin"
)

type Query struct {
	Auth        string `json:"_auth"`
	QueryString string `json:"query"`
}

var router *gin.Engine

// const SNAPSHOT_DELAY = 5

func main() {
	gin.SetMode(gin.ReleaseMode)

	if err := startup.Setup(); err != nil {
		logger.Fatalf("", "Error setting up database: %s", err.Error())
	}

	logger.Infof("", "Starting Ceres server")
	// freespace.LoadFreeSpace()
	// schema.LoadSchema()
	queue.InitQueue()

	logger.Tracef("", "Ensuring data directory exists")
	os.MkdirAll(config.Config.DataDir, 0755)

	go queue.Run()

	routerPort := ":" + strconv.Itoa(config.Config.Port)
	router = gin.Default()

	// Initialize the routes
	logger.Infof("", "Initializing routes")
	initializeRoutes()

	// Start serving the application
	logger.Infof("", "Listening for connections on port %v", config.Config.Port)
	router.Run(routerPort)
}
