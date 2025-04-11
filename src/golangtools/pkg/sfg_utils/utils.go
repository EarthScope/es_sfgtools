package sfg_utils

import (
	"path/filepath"
	"runtime"

	tiledb "github.com/TileDB-Inc/TileDB-Go"
	"github.com/joho/godotenv"
	log "github.com/sirupsen/logrus"
)

func ArrayExists(arrayPath string) bool {
	ctx, err := tiledb.NewContext(nil)
	if err != nil {
		log.Errorf("failed creating TileDB co	ntext: %v", err)
		return false
	}
	defer ctx.Free()

	schema, err := tiledb.LoadArraySchema(ctx, arrayPath)
	if err != nil{
		log.Errorf("failed to load TileDB array schema: %v",err)
	}
	if schema == nil {
		return false
	} else {
		return true
	}
}

func LoadEnv() {
	    // Get the file path of the current source file
    _, currentFile, _, ok := runtime.Caller(0)
    if !ok {
        log.Fatalf("Unable to get the current file path")
    }
	// src/golangtools/cmd/tdb2rnx/main.go

	// src/.env
	// Get the directory of the current file
	dir := filepath.Dir(currentFile)
	for i := 0; i < 3; i++ {
	// Move up three directories
		dir = filepath.Dir(dir)
	}
	// Construct the path to the .env file
	envFilePath := filepath.Join(dir, ".env")


	// Load the .env file
	log.Infof("Loading .env file from %s", envFilePath)
	err := godotenv.Load(envFilePath)
	if err != nil {
		log.Warn("Error loading .env file", err)
	}
}
// src/golangtools/cmd/tdb2rnx/main.go
// src/.env