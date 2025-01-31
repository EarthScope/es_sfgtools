package main

import (
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"os"

	log "github.com/sirupsen/logrus"

	"gitlab.com/earthscope/gnsstools/pkg/encoding/rinex"
	"gitlab.com/earthscope/gnsstools/pkg/encoding/tiledbgnss"
)

type BodyParameters struct {
	URI         string                    `json:"uri"`
	Region      string                    `json:"region"`
	QueryParams tiledbgnss.ObsQueryParams `json:"query"`
}

func main() {
	tdbPathPtr := flag.String("tdb", "", "Path to the TileDB array")
	metaPtr := flag.String("settings", "", "settings file")
	//jsonMetaFilePath := flag.String("meta", "", "path to JSON file")
	flag.Parse()
	log.SetOutput(os.Stdout)



	// Parse settings from JSON
	settings, err := parseSettings(*metaPtr)
	if err != nil {
		log.Fatalf("failed parsing settings: %s", err)
	}
	if settings == nil {
		log.Fatalf("failed parsing settings")
	}

	timeStart,timeEnd,err := tiledbgnss.GetTimeRange(*tdbPathPtr)

	if err != nil {
		log.Fatalln(err)
	}

	fmt.Print(timeStart)
	fmt.Print(timeEnd)

	// timeSlices := tiledbgnss.GetDateArranged(timeStart,timeEnd)

	// fmt.Print("\n Time Slices \n")
	// for _,timeSlice := range timeSlices {
	// 	// Clear the epochs slice
		
	// 	fmt.Print("Start: ",timeSlice.Start,"\n")
	// 	fmt.Print("End: ",timeSlice.End)
	// 	fmt.Print("\n\n")
	// 	queryParams := tiledbgnss.ObsQueryParams{
	// 		Time: []tiledbgnss.TimeRange{timeSlice},
	// 	}
	// 	epochs, err := tiledbgnss.ReadObsV3Array(
	// 		*tdbPathPtr, "us-east-2", queryParams)
	// 	if err != nil {
	// 		fmt.Print("Error Reading TDB: ",err)
	// 	}
	// 	fmt.Print("Epochs: ",len(epochs),"\n")
	// 	settings.TimeOfFirst = epochs[0].Time
	// 	settings.TimeOfLast = epochs[len(epochs)-1].Time	
	// 	err = processDailyEpoch(epochs, settings)
	// 	epochs = nil // Clear the epochs list
	// 	if err != nil {
	// 		fmt.Print("Error Processing Daily Epoch: ",err)
	// 	}
	// 	fmt.Print("\n\n ===================== \n\n")

	//}
}

// func processDailyEpoch(epochs []observation.Epoch, settings *rinex.Settings) error {
// 	// sort epochs by time
// 	sort.Slice(epochs, func(i, j int) bool {
// 		return epochs[i].Time.Before(epochs[j].Time)
// 	})

// 	startYear,startMonth,startDay := epochs[0].Time.Date()
// 	endYear,endMonth,endDay := epochs[len(epochs)-1].Time.Date()

// 	fmt.Print("Start Date: ",startYear,startMonth,startDay,"\n")
// 	fmt.Print("End Date: ",endYear,endMonth,endDay)
// 	fmt.Print("\n")
// 	currentDate := time.Date(startYear,startMonth,startDay,0,0,0,0,time.UTC)
// 	dayOfYear := currentDate.YearDay()
// 	outFile := &os.File{}
// 	yy := startYear % 100
// 	filename := fmt.Sprintf("%s%03d0.%02do", settings.MarkerName, dayOfYear, yy)
// 	fmt.Print("\nWriting to Filename: ",filename,"\n")
// 	outFile, err := os.OpenFile(filename, os.O_RDWR|os.O_CREATE, 0644)

// 	if err != nil {
// 		return fmt.Errorf("failed creating output file: %s", err)
// 	}
// 	header, err := rinex.NewHeader(settings)
// 	if err != nil {
// 		return fmt.Errorf("failed creating RINEX header: %s", err)
// 	}
// 	err = header.Write(outFile)
// 	if err != nil {
// 		return fmt.Errorf("failed writing RINEX header: %s", err)
// 	}

// 	for _, epoch := range epochs {
// 		if epoch.Time.Day() != currentDate.Day() {
// 			// close current output file if it exists
// 			log.Warnf("Detected Epoch Out of Range: %s > %s",epoch.Time,currentDate)
// 			if outFile != nil {
// 					err := outFile.Close()
// 					if err != nil {
// 						log.Warnf("failed closing file: %s", err)
// 					}
// 				}
// 			break

// 		}
// 		err = rinex.SerializeRnxObs(outFile, epoch, settings)

// 		if err != nil {
// 			log.Warnf("failed writing observation: %s", err)
// 		}

// 	}
// 	defer outFile.Close()
	
// 	return nil
// }

// Helper function to parse metadata from the JSON file
func parseSettings(path string) (*rinex.Settings, error) {
	file, err := os.Open(path)
	if err != nil {
		return nil, fmt.Errorf("failed opening settings file: %s", err)
	}
	defer file.Close()
	bytes, err := io.ReadAll(file)
	if err != nil {
		return nil, fmt.Errorf("failed reading settings file: %s", err)
	}
	var settings = &rinex.Settings{}
	if err := json.Unmarshal(bytes, settings); err != nil {
		return nil, fmt.Errorf("failed parsing settings file: %s", err)
	}
	return settings, nil
}
	