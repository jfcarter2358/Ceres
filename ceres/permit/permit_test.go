package permit

import (
	"ceres/collection"
	"ceres/config"
	"ceres/freespace"
	"ceres/record"
	"ceres/schema"
	"os"
	"path/filepath"
	"testing"
)

func createDatabase(database string) {
	// Initialize
	os.Setenv("CERES_CONFIG_PATH", "../../test/.ceres/config/config.json")
	config.ReadConfigFile()
	freespace.LoadFreeSpace()
	schema.LoadSchema()

	// Create database
	dataPath := filepath.Join(config.Config.DataDir, database)
	indexPath := filepath.Join(config.Config.IndexDir, database)
	os.MkdirAll(dataPath, 0755)
	os.MkdirAll(indexPath, 0755)
	freespace.FreeSpace.Databases[database] = freespace.FreeSpaceDatabase{}
	schema.Schema.Databases[database] = schema.SchemaDatabase{}
	freespace.WriteFreeSpace()
	schema.WriteSchema()

	// Create auth collection
	collection.Post(database, "_users", map[string]interface{}{"username": "STRING", "role": "STRING"})
	inputData := []map[string]interface{}{{"username": "ceres", "role": "ADMIN"}}
	record.Post(database, "_users", inputData)
}

func deleteDatabase(database string) {
	dataPath := filepath.Join(config.Config.DataDir, database)
	indexPath := filepath.Join(config.Config.IndexDir, database)
	os.RemoveAll(dataPath)
	os.RemoveAll(indexPath)
	delete(freespace.FreeSpace.Databases, database)
	delete(schema.Schema.Databases, database)
}

func TestPatch(t *testing.T) {
	err := Patch()

	if err == nil {
		t.Errorf("Error was incorrect, got: %v, want: %v", err, "<non-nil>")
	}
}
