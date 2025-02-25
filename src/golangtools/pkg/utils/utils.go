package utils

import (
	tiledb "github.com/TileDB-Inc/TileDB-Go"
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