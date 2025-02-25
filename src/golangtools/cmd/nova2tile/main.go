// Author: Franklyn Dunbar  | Contact franklyn.dunbar@earthscope.org | Dec 2024
package main

import (
	"bufio"
	"flag"
	"io"
	"os"
	"sync"

	utils "github.com/EarthScope/es_sfgtools/src/golangtools/pkg/utils"
	log "github.com/sirupsen/logrus"
	"gitlab.com/earthscope/gnsstools/pkg/common/gnss/observation"
	novatelascii "gitlab.com/earthscope/gnsstools/pkg/encoding/novatel/novatel_ascii"
	"gitlab.com/earthscope/gnsstools/pkg/encoding/tiledbgnss"
)

// processFileNOVASCII reads a NOVATEL ASCII file and processes its contents to extract GNSS epochs.
// It takes a filename as input and returns a slice of observation.Epoch.
//
// The function performs the following steps:
// 1. Opens the specified file.
// 2. Creates a new scanner to read NOVATEL ASCII messages from the file.
// 3. Iterates over the messages in the file.
// 4. For each "RANGEA" message, deserializes the message data and converts it to a GNSS epoch.
// 5. Appends the GNSS epoch to the result slice.
//
// If an error occurs while opening the file or reading messages, the function logs the error and terminates the program.
func processFileNOVASCII(filename string) []observation.Epoch{
	file, err := os.Open(filename)
	if err != nil {
		log.Fatalln(err)
	}
	defer file.Close()
	scanner := novatelascii.NewScanner(bufio.NewReader(file))
	epochs := []observation.Epoch{}

	MessageLoop:
		for {
			msg,err := scanner.NextMessage()
			if err != nil {
				if err == io.EOF {
					break MessageLoop
				}
			}

			switch m := msg.(type) {
				case novatelascii.LongMessage:
					if m.Msg == "RANGEA" {
						rangea, err := novatelascii.DeserializeRANGEA(m.Data)
						if err != nil {
							log.Errorln(err)
							continue MessageLoop
						}
						epoch, err := rangea.SerializeGNSSEpoch(m.Time())
						if err != nil {
							log.Errorln(err)
							continue MessageLoop
						}
						epochs = append(epochs, epoch)				
					}
				case novatelascii.ShortMessage:
					if m.Msg == "RANGEA" {
						rangea, err := novatelascii.DeserializeRANGEA(m.Data)
						if err != nil {
							log.Errorln(err)
							continue MessageLoop
						}
						epoch, err := rangea.SerializeGNSSEpoch(m.Time())
						if err != nil {
							log.Errorln(err)
							continue MessageLoop
						}
						epochs = append(epochs, epoch)				
					}
				}
			}
			return epochs
	}

func main() {
	tdbPathPtr := flag.String("tdb", "", "Path to the TileDB array")
	numProcsPtr := flag.Int("procs", 10, "Number of concurrent processes")
	flag.Parse()

	filenames := flag.Args()
	if len(filenames) == 0 {
		flag.PrintDefaults()
		log.Fatalln("no files specified")
	}

	if !utils.ArrayExists(*tdbPathPtr) {
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
			epochs := processFileNOVASCII(filename)
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


