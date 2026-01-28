// Author: Franklyn Dunbar  | Contact franklyn.dunbar@earthscope.org | Dec 2024
package main

import (
	"flag"
	"sync"

	sfg_utils "github.com/EarthScope/es_sfgtools/src/golangtools/pkg/sfg_utils"
	log "github.com/sirupsen/logrus"
	"gitlab.com/earthscope/gnsstools/pkg/encoding/tiledbgnss"
)


func main() {
	sfg_utils.LoadEnv()
	tdbPathPtr := flag.String("tdb", "", "Path to the TileDB array")
	numProcsPtr := flag.Int("procs", 10, "Number of concurrent processes")
	flag.Parse()

	filenames := flag.Args()
	if len(filenames) == 0 {
		flag.PrintDefaults()
		log.Fatalln("no files specified")
	}

	if !sfg_utils.ArrayExists(*tdbPathPtr) {
		err := tiledbgnss.CreateArray("s3://earthscope-tiledb-schema-dev-us-east-2-ebamji/GNSS_OBS_SCHEMA_V3.tdb/", *tdbPathPtr, "us-east-2")
		if err != nil {
			log.Errorf("error creating array: %v",err)
		}
	} else {
		log.Infof("array %s already exists", *tdbPathPtr)
	}
	var wg sync.WaitGroup
	sem := make(chan struct{}, *numProcsPtr) // Limit to 10 concurrent goroutines

	for _, filename := range filenames {
		wg.Add(1)
		go func(filename string) {
			defer wg.Done()
			sem <- struct{}{}
			defer func() { <-sem }()
			epochs := sfg_utils.ProcessFileNOVASCII(filename)
			if len(epochs) == 0 {
				log.Warnf("no epochs found in file %s", filename)
				return
			}
			log.Infof("processed %d epochs from file %s", len(epochs), filename)
			err := tiledbgnss.WriteObsV3Array( *tdbPathPtr,"us-east-2",epochs)
			if err != nil {
				log.Errorf("error writing epochs to array: %v",err)
			}
			epochs = nil
		}(filename)
	}
	wg.Wait()
}


