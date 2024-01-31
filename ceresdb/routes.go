// routes.go

package main

import "ceresdb/api"

func initializeRoutes() {
	apiRoutes := router.Group("/api")
	{
		apiRoutes.POST("/query", api.Query)
	}
}
