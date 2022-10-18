// routes.go

package main

func initializeRoutes() {
	apiRoutes := router.Group("/api")
	{
		apiRoutes.POST("/query", handleQueryEndpoint)
		apiRoutes.GET("/snapshot", handleSnapshotEndpoint)
	}
}
