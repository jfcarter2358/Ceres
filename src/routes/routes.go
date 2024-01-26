package routes

import "github.com/gin-gonic/gin"

func initializeRoutes(router *gin.Engine) {
	apiRoutes := router.Group("/api")
	{
		apiRoutes.POST("/query", handleQueryEndpoint)
		apiRoutes.GET("/snapshot", handleSnapshotEndpoint)
	}
}
