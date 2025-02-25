// Author: Franklyn Dunbar  | Contact franklyn.dunbar@earthscope.org | Dec 2024
package main

import (
	"bufio"
	"flag"
	"io"
	"os"
	"sync"
	"time"

	utils "github.com/EarthScope/es_sfgtools/src/golangtools/pkg/utils"
	log "github.com/sirupsen/logrus"
	"gitlab.com/earthscope/gnsstools/pkg/common/gnss/observation"
	novatelbinary "gitlab.com/earthscope/gnsstools/pkg/encoding/novatel/novatel_binary"
	"gitlab.com/earthscope/gnsstools/pkg/encoding/tiledbgnss"
)

// processFileNOVB processes a NOVB file and returns a slice of observation.Epoch.
// It reads the file, scans for messages, and extracts epochs from messages with ID 140.
// If an error occurs while opening the file, it logs a fatal error.
// If an error occurs while reading a message, it logs a warning and continues.
// If an error occurs while serializing an epoch, it logs an error and continues.
// It skips epochs with no satellites.
//
// Parameters:
//   - file: The path to the NOVB file to be processed.
//
// Returns:
//   - A slice of observation.Epoch containing the extracted epochs.
func processFileNOVB(file string) ([]observation.Epoch,error) {
	f, err := os.Open(file)
	if err != nil {
		log.Fatalf("failed opening file: %s", err)
	}
	defer f.Close()

	reader := bufio.NewReader(f)
	epochs := []observation.Epoch{}
	MessageLoop:
		for {
			msg,err := novatelbinary.DeserializeMessage(reader)
			if err != nil {
				if err == io.EOF {
					break MessageLoop

				}
				if err == bufio.ErrBufferFull{
					log.Warnf("buffer full: %s", err)
					reader.Reset(f)
				}
				//log.Warnf("failed reading message: %s", err)
				continue MessageLoop
			}
			if msg.MessageID == 140 {
				msg140 := msg.DeserializeMessage140()
				epoch, err := msg140.SerializeGNSSEpoch(msg.Time())
				if err != nil {
					log.Errorf("failed serializing epoch: %s", err)
					continue MessageLoop
				}
				if len(epoch.Satellites) == 0 {
					continue MessageLoop
				}
				epochs = append(epochs, epoch)
			} else {
				continue MessageLoop
			}
		}
	return epochs,nil
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
	log.Info("Num procs: ", *numProcsPtr)
	if !utils.ArrayExists(*tdbPathPtr) {
		err := tiledbgnss.CreateArray("s3://earthscope-tiledb-schema-dev-us-east-2-ebamji/GNSS_OBS_SCHEMA_V3.tdb/", *tdbPathPtr, "us-east-2")
		if err != nil {
			log.Errorf("error creating array: %v",err)
		}
	} else {
		log.Infof("array %s already exists", *tdbPathPtr)
	}
	startTime := time.Now()
	var wg sync.WaitGroup
	sem := make(chan struct{}, *numProcsPtr) // Limit to 10 concurrent goroutines
		for _, filename := range filenames {
			wg.Add(1)
			go func(filename string) {
				defer wg.Done()
				sem <- struct{}{}
				defer func() { <-sem }()
			epochs,err := processFileNOVB(filename)
			if err != nil {
				log.Errorf("error processing file: %v",err)
				return
			}
			if len(epochs) == 0 {
				log.Warnf("no epochs found in file %s", filename)
				return
			}
			log.Infof("processed %d epochs from file %s", len(epochs), filename)
			err = tiledbgnss.WriteObsV3Array( *tdbPathPtr,"us-east-2",epochs)
			if err != nil {
				log.Errorf("error writing epochs to array: %v",err)
			}
			}(filename)
	}
	wg.Wait()
	log.Infof("processed %d files in %s", len(filenames), time.Since(startTime))
}
