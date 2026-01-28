// Author: Franklyn Dunbar  | Contact franklyn.dunbar@earthscope.org | Dec 2024
package main

import (
	"bufio"
	"flag"
	"io"
	"sync"

	sfg_utils "github.com/EarthScope/es_sfgtools/src/golangtools/pkg/sfg_utils"
	log "github.com/sirupsen/logrus"
	novatelascii "gitlab.com/earthscope/gnsstools/pkg/encoding/novatel/novatel_ascii"
	"gitlab.com/earthscope/gnsstools/pkg/encoding/tiledbgnss"
)


type Reader struct {
	Reader *bufio.Reader
}

func NewReader(r io.Reader) Reader {
	return Reader{Reader: bufio.NewReader(r)}
}

func (reader Reader) NextMessage() (message novatelascii.Message, err error) {
	message,err = sfg_utils.DeserializeNOV00bin(reader.Reader)
	if err != nil {
		if err == io.EOF {
			return message, err
		}
	}
	return message, nil
}


func main() {
	sfg_utils.LoadEnv()
	tdbPathPtr := flag.String("tdb", "", "Path to the TileDB GNSS array")
	numProcsPtr := flag.Int("procs", 10, "Number of concurrent processes")
	tdbPositionPtr := flag.String("tdbpos", "", "Path to the TileDB position array")
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
			epochs,insCompleteRecords := sfg_utils.ProcessFileNOV000(filename)
			if len(epochs) == 0 {
				log.Warnf("no GNSS epochs found in file %s", filename)
				return
			}
			if len(insCompleteRecords) == 0 {
				log.Warnf("no INS records found in file %s", filename)
				return
			}
			log.Infof("Writing %d GNS epochs from file %s to TileDB array %s", len(epochs), filename, *tdbPathPtr)
			err := tiledbgnss.WriteObsV3Array(*tdbPathPtr, "us-east-2", epochs)
			if err != nil {
				log.Errorf("error writing epochs to array: %v",err)
			}
			if *tdbPositionPtr != "" {
				log.Infof("writing %d INS position records from file %s to TileDB array %s", len(insCompleteRecords), filename, *tdbPositionPtr)
				err := sfg_utils.WriteINSPOSRecordToTileDB(*tdbPositionPtr, "us-east-2", insCompleteRecords)
				if err != nil {
					log.Errorf("error writing INS position records to array: %v", err)
				}
			}
		}(filename)
	
	}
	wg.Wait()
}

