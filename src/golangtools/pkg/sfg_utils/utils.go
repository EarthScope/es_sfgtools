package sfg_utils

import (
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
	err := godotenv.Load("../../../.env")
	if err != nil {
		log.Warn("Error loading .env file", err)
	}
}