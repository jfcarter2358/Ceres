package startup

import (
	"ceresdb/auth"
	"ceresdb/collection"
	"ceresdb/config"
	"ceresdb/constants"
	"ceresdb/database"
	"ceresdb/index"
	"ceresdb/logger"
	"ceresdb/query"
	"ceresdb/schema"
	"os"
)

func Setup() error {
	config.ReadConfig()
	logger.SetLevel(config.Config.LogLevel)
	logger.SetFormat(config.Config.LogFormat)
	os.MkdirAll(config.Config.DataDir, 0777)
	logger.Infof("", "Starting up...")
	logger.Debugf("", "Loading databases")
	if err := database.LoadDatabases(); err != nil {
		return err
	}
	logger.Debugf("", "Loading collections")
	if err := collection.LoadCollections(); err != nil {
		return err
	}
	logger.Debugf("", "Loading schemas")
	if err := schema.LoadSchemas(); err != nil {
		return err
	}
	logger.Debugf("", "Loading indices")
	if err := index.LoadIndexKeys(); err != nil {
		return err
	}
	if err := index.LoadIndices(); err != nil {
		return err
	}
	if err := index.LoadIndexIDs(); err != nil {
		return err
	}
	if err := index.LoadIndexCache(); err != nil {
		return err
	}
	logger.Debugf("", "Ensuring %s database exists", constants.AUTH_DB_NAME)
	if _, ok := database.Databases[constants.AUTH_DB_NAME]; !ok {
		logger.Tracef("", "%s database does not exist, creating", constants.AUTH_DB_NAME)
		if err := database.Create(constants.AUTH_DB_NAME); err != nil {
			return err
		}
	}
	logger.Debugf("", "Ensuring %s.%s collection exists", constants.AUTH_DB_NAME, constants.AUTH_COLLECTION_NAME)
	if _, ok := collection.Collections[constants.AUTH_DB_NAME][constants.AUTH_COLLECTION_NAME]; !ok {
		logger.Tracef("", "%s.%s collection does not exist, creating", constants.AUTH_DB_NAME, constants.AUTH_COLLECTION_NAME)
		if err := collection.Create(constants.AUTH_DB_NAME, constants.AUTH_COLLECTION_NAME); err != nil {
			return err
		}
		if err := schema.BuildSchema(constants.AUTH_DB_NAME, constants.AUTH_COLLECTION_NAME, constants.AUTH_SCHEMA); err != nil {
			return err
		}
		index.BuildIndex(constants.AUTH_DB_NAME, constants.AUTH_COLLECTION_NAME)
		query.CreateUser(config.Config.AdminUsername, config.Config.AdminPassword, auth.User{}, true)
	}
	return nil

}
