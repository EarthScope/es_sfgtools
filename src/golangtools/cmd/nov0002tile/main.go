// Author: Franklyn Dunbar  | Contact franklyn.dunbar@earthscope.org | Dec 2024
package main

import (
	"bufio"
	"flag"
	"io"
	"os"
	"sync"

	sfg_utils "github.com/EarthScope/es_sfgtools/src/golangtools/pkg/sfg_utils"
	log "github.com/sirupsen/logrus"
	"gitlab.com/earthscope/gnsstools/pkg/common/gnss/observation"
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


// processFileNOV000 processes a NOV000 file containing GNSS and INS messages.
// It reads the file, parses messages such as RANGEA, INSPVAA, and INSSTDEVA,
// and deserializes them into corresponding records. The function merges INSPVAA
// and INSSTDEVA records into complete INS records, computes time differences for
// GNSS and INS epochs, and returns slices of GNSS epochs and merged INS records.
//
// Parameters:
//   - file: The path to the NOV000.bin file to be processed.
//
// Returns:
//   - []observation.Epoch: A slice of GNSS epoch records parsed from the file.
//   - []sfg_utils.INSCompleteRecord: A slice of merged INS complete records.
//
// The function logs errors encountered during file reading and message deserialization,
// and logs the number of INSPVAA and INSSTDEVA records found.
func processFileNOV000(file string) ([]observation.Epoch, []sfg_utils.INSCompleteRecord) {
	f,err := os.Open(file)
	if err != nil {
		log.Fatalf("failed opening file %s, %s ",file, err)
	}
	defer f.Close()
	reader := NewReader(bufio.NewReader(f))
	epochs := []observation.Epoch{}
	insEpochs := []sfg_utils.InspvaaRecord{}
	insStdDevEpochs := []sfg_utils.INSSTDEVARecord{}

	epochLoop:
		for {
			message,err := reader.NextMessage()
			if err != nil {
				if err == io.EOF {
					err = f.Close()
					if err != nil {
						log.Errorln(err)
					}
					break epochLoop
				}
				log.Println(err)
			}
			
			switch m:=message.(type) {
				case novatelascii.LongMessage:
					
					// Deserialize the message based on its type

					// Check if the message is a GNSS RANGEA message
					if m.Msg == "RANGEA" {
						rangea, err := novatelascii.DeserializeRANGEA(m.Data)
						if err != nil {
							continue epochLoop
						}
						epoch, err := rangea.SerializeGNSSEpoch(m.Time())
						if err != nil {
							continue epochLoop
						}
						epochs = append(epochs, epoch)
					// Check if the message is an INSPVAA message
					} else if m.Msg == "INSPVAA" {
						record, err := sfg_utils.DeserializeINSPVAARecord(m.Data, m.Time())
						if err != nil {
							log.Errorf("error deserializing INSPVAA record: %s", err)
							continue epochLoop
						}
						insEpochs = append(insEpochs, record)
				
					// Check if the message is an INSSTDEVA message
					} else if m.Msg == "INSSTDEVA" {
						record, err := sfg_utils.DeserializeINSSTDEVARecord(m.Data, m.Time())
						if err != nil {
							log.Errorf("error deserializing INSSTDEVA record: %s", err)
							continue epochLoop
						}
						insStdDevEpochs = append(insStdDevEpochs, record)
					}
				}
		}
	log.Infof("Found %d INSPVAA records, %d INSSTDEVA records", len(insEpochs), len(insStdDevEpochs))
	// Merge INSPVAA and INSSTDEVA records
	insCompleteRecords := sfg_utils.MergeINSPVAAAndINSSTDEVA(insEpochs, insStdDevEpochs)
	sfg_utils.GetTimeDiffGNSS(epochs)
	sfg_utils.GetTimeDiffsINSPVA(insCompleteRecords)
	return epochs, insCompleteRecords
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
			epochs,insCompleteRecords := processFileNOV000(filename)
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

