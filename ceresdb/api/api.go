package api

import (
	"ceresdb/auth"
	"ceresdb/config"
	"ceresdb/constants"
	"ceresdb/logger"
	"ceresdb/query"
	"ceresdb/queue"
	"ceresdb/utils"
	"encoding/base64"
	"fmt"
	"net/http"
	"strings"
	"time"

	"github.com/gin-gonic/gin"
)

type Input struct {
	Auth  string `json:"auth"`
	Query string `json:"query"`
}

func Query(ctx *gin.Context) {
	var in Input
	if err := ctx.BindJSON(&in); err != nil {
		utils.Error(err, ctx, http.StatusInternalServerError)
		return
	}

	logger.Tracef("", "Received request: %v", in)

	authBytes, err := base64.URLEncoding.DecodeString(in.Auth)
	if err != nil {
		utils.Error(err, ctx, http.StatusInternalServerError)
		return
	}
	logger.Tracef("", "auth string: %s", string(authBytes))
	parts := strings.Split(string(authBytes), ":")
	logger.Tracef("", "auth string parts: %v", parts)
	if len(parts) != 2 {
		utils.Error(fmt.Errorf("invalid auth string"), ctx, http.StatusUnauthorized)
		return
	}
	uApi := auth.User{
		Username: config.Config.AdminUsername,
		Password: "",
		Groups:   []string{constants.GROUP_ADMIN},
		Roles:    []string{constants.ROLE_ADMIN},
	}
	logger.Tracef("", "Getting user from auth")
	u, err := query.GetUser(parts[0], uApi)
	if err != nil {
		utils.Error(err, ctx, http.StatusUnauthorized)
		return
	}

	q := queue.QueueObject{
		User:     u,
		Query:    in.Query,
		Output:   nil,
		Finished: false,
		Err:      nil,
	}
	logger.Tracef("", "Adding query to queue")
	queue.AddToQueue(&q)
	for !q.Finished {
		time.Sleep(1 * time.Millisecond)
	}
	queue.PopQueue()
	logger.Tracef("", "Query finished")

	if q.Err != nil {
		utils.Error(q.Err, ctx, http.StatusInternalServerError)
		return
	}
	ctx.JSON(http.StatusOK, q.Output)
}
