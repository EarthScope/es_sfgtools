package sfg_utils

import (
	"bufio"
	"fmt"
	"io"
	"log/slog"
	"os"
	"path/filepath"
	"runtime"
	"sort"
	"time"

	tiledb "github.com/TileDB-Inc/TileDB-Go"
	"github.com/joho/godotenv"
	log "github.com/sirupsen/logrus"
	"gitlab.com/earthscope/gnsstools/pkg/common/gnss/observation"
	novatelascii "gitlab.com/earthscope/gnsstools/pkg/encoding/novatel/novatel_ascii"
	"gitlab.com/earthscope/gnsstools/pkg/encoding/rinex"
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

func SortEpochsByTime(epochs []observation.Epoch) {
	sort.Slice(epochs, func(i, j int) bool {
		return epochs[i].Time.Before(epochs[j].Time)
	})
}

func BatchEpochsByDay(epochs []observation.Epoch) map[string][]observation.Epoch {
	batchedEpochs := make(map[string][]observation.Epoch)
	// first, sort epochs by time
	SortEpochsByTime(epochs)
	for _, epoch := range epochs {
		startYear,startMonth,startDay := epoch.Time.Date()
		dayKey := fmt.Sprintf("%04d-%02d-%02d", startYear, startMonth, startDay)
		batchedEpochs[dayKey] = append(batchedEpochs[dayKey], epoch)
	}
	for dayKey := range batchedEpochs {
		log.Infof("Batched %d epochs for day %s", len(batchedEpochs[dayKey]), dayKey)
	}
	return batchedEpochs
}

func WriteEpochs(epochs []observation.Epoch,settings *rinex.Settings) error {
	startYear,startMonth,startDay := epochs[0].Time.Date()
	currentDate := time.Date(startYear,startMonth,startDay,0,0,0,0,time.UTC)
	dayOfYear := currentDate.YearDay()
	yy := startYear % 100
	filename := fmt.Sprintf("%s%03d0.%02do", settings.MarkerName, dayOfYear, yy)
	log.Infof("Generating Daily RINEX File For Year %d, Month %d, Day %d To %s",startYear,startMonth,startDay,filename)
	
	// Check if the file already exists
	if _, err := os.Stat(filename); err == nil {
		log.Warnf("File Already Exists: %s",filename)
		// delete the file
		err := os.Remove(filename)
		if err != nil {
			log.Errorf("failed deleting existing file: %s", err)
		}
	}
	outFile, err := os.OpenFile(filename, os.O_RDWR|os.O_CREATE, 0644)
	if err != nil {
		log.Errorf("Error opening output file: %s", err)
		os.Exit(1)
	}

	defer outFile.Close()
		header, err := rinex.NewHeader(settings)
		if err != nil {
			slog.Error("Error creating RINEX header", "error", err)
			os.Exit(1)
		}

		header.Write(outFile)

		for _, e := range epochs {
			err = rinex.SerializeRnxObs(outFile, e, settings)
			if err != nil {
				slog.Error("Error writing observation", "error", err)
			}
		}
	return nil
}
type Reader struct {
	Reader *bufio.Reader
}

func NewReader(r io.Reader) Reader {
	return Reader{Reader: bufio.NewReader(r)}
}

func (reader Reader) nextMessageNOV00bin() (message novatelascii.Message, err error) {
	message, err = DeserializeNOV00bin(reader.Reader)
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
//   - []INSCompleteRecord: A slice of merged INS complete records.
//
// The function logs errors encountered during file reading and message deserialization,
// and logs the number of INSPVAA and INSSTDEVA records found.
func ProcessFileNOV000(file string) ([]observation.Epoch, []INSCompleteRecord) {

	f,err := os.Open(file)
	if err != nil {
		log.Fatalf("failed opening file %s, %s ",file, err)
	}
	defer f.Close()
	reader := NewReader(bufio.NewReader(f))
	epochs := []observation.Epoch{}
	insEpochs := []InspvaaRecord{}
	insStdDevEpochs := []INSSTDEVARecord{}

	epochLoop:
		for {
			message,err := reader.nextMessageNOV00bin()
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
						record, err := DeserializeINSPVAARecord(m.Data, m.Time())
						if err != nil {
							log.Errorf("error deserializing INSPVAA record: %s", err)
							continue epochLoop
						}
						insEpochs = append(insEpochs, record)
				
					// Check if the message is an INSSTDEVA message
					} else if m.Msg == "INSSTDEVA" {
						record, err := DeserializeINSSTDEVARecord(m.Data, m.Time())
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
	insCompleteRecords := MergeINSPVAAAndINSSTDEVA(insEpochs, insStdDevEpochs)
	GetTimeDiffGNSS(epochs)
	GetTimeDiffsINSPVA(insCompleteRecords)
	return epochs, insCompleteRecords
}	