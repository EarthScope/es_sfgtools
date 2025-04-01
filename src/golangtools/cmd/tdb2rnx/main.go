// Author: Franklyn Dunbar  | Contact franklyn.dunbar@earthscope.org | Dec 2024
package main

import (
	"encoding/json"
	"fmt"
	"io"
	"os"
	"time"

	log "github.com/sirupsen/logrus"

	"gitlab.com/earthscope/gnsstools/pkg/common/gnss/observation"
	"gitlab.com/earthscope/gnsstools/pkg/encoding/rinex"
	"gitlab.com/earthscope/gnsstools/pkg/encoding/tiledbgnss"
)


type BodyParameters struct {
	URI         string                    `json:"uri"`
	Region      string                    `json:"region"`
	QueryParams tiledbgnss.ObsQueryParams `json:"query"`
}

func WriteFirstEpochBatch(epochs []observation.Epoch, settings *rinex.Settings) (string,error) {


	if settings.RinexVersion.Major == rinex.RinexMajorVersion3 || settings.RinexVersion.Major == rinex.RinexMajorVersion4 {
	// Write the RINEX header
		for _, epoch := range epochs {

			settings.ObservationsBySystem.AddEpoch(epoch)
		}
	} 

	startYear,startMonth,startDay := epochs[0].Time.Date()

	currentDate := time.Date(startYear,startMonth,startDay,0,0,0,0,time.UTC)
	dayOfYear := currentDate.YearDay()
	outFile := &os.File{}
	yy := startYear % 100
	filename := fmt.Sprintf("%s%03d0.%02do", settings.MarkerName, dayOfYear, yy)
	log.Infof("Generating Daily RINEX File For Year %d, Month %d, Day %d To %s",startYear,startMonth,startDay,filename)
	
	// Check if the file already exists
	if _, err := os.Stat(filename); err == nil {
		log.Warnf("File Already Exists: %s",filename)
		// delete the file
		err := os.Remove(filename)
		if err != nil {
			return filename,fmt.Errorf("failed deleting existing file: %s", err)
		}
	}
	outFile, err := os.OpenFile(filename, os.O_RDWR|os.O_CREATE, 0644)

	if err != nil {
		return filename,fmt.Errorf("failed creating output file: %s", err)
	}
	header, err := rinex.NewHeader(settings)
	if err != nil {
		return filename,fmt.Errorf("failed creating RINEX header: %s", err)
	}
	err = header.Write(outFile)
	if err != nil {
		return filename,fmt.Errorf("failed writing RINEX header: %s", err)
	}

	for _, epoch := range epochs {
		if epoch.Time.Day() != currentDate.Day() {
			// close current output file if it exists
			log.Warnf("Detected Epoch Out of Range: %s > %s",epoch.Time,currentDate)
			if outFile != nil {
					err := outFile.Close()
					if err != nil {
						log.Warnf("failed closing file: %s", err)
					}
				}
			break

		}
		err = rinex.SerializeRnxObs(outFile, epoch, settings)

		if err != nil {
			log.Warnf("failed writing observation: %s", err)
		}

	}
	defer outFile.Close()
	
	return filename,nil
}

// WriteEpochs appends the provided epochs to the given file
func WriteEpochs(epochs []observation.Epoch,filename string,settings *rinex.Settings) error {
	log.Infof("Writing Epochs To File: %s",filename)
	outFile, err := os.OpenFile(filename, os.O_RDWR|os.O_APPEND, 0644)
	if err != nil {
		return fmt.Errorf("failed creating output file:%s %s", filename,err)
	}
	defer outFile.Close()
	
	for _, epoch := range epochs {
		err = rinex.SerializeRnxObs(outFile, epoch, settings)
		if err != nil {
			log.Warnf("failed writing observation: %s", err)
		}
	}
	return nil
}


// Helper function to parse metadata from the JSON file
func ParseSettings(path string) (*rinex.Settings, error) {
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
	
func GetHourSlice(daySlice tiledbgnss.TimeRange,interval int) ([]tiledbgnss.TimeRange) {
	if interval < 1 {
		log.Warn("Invalid interval (%d), defaulting to 1 hour from ", interval)
		interval = 1
	} else if interval > 24 {
		log.Warn("Invalid interval (%d), defaulting to 24 hour from ", interval)
		interval = 24
	}
	// break daySlice into 1 hour slices
	hourSlices := []tiledbgnss.TimeRange{}
	prevTime := daySlice.Start
	for i := interval; i <= 24; i += interval {
		log.Debugf("PrevTime: %s, Interval: %d",prevTime,i)
	
		endTime := prevTime.Add(time.Duration(interval) * time.Hour)
		// If the end time is exactly a day after the start time, set the end time to the end of the day
		if endTime.After(daySlice.End) {
			endTime = daySlice.End
		}
		hourSlices = append(hourSlices, tiledbgnss.TimeRange{Start: prevTime, End: endTime})
		prevTime = endTime
	}
	return hourSlices
}

// func main() {
// 	startTime := time.Now()
// 	tdbPathPtr := flag.String("tdb", "", "Path to the TileDB array")
// 	metaPtr := flag.String("settings", "", "settings file")
// 	timeIntervals := flag.Int("timeint", 1, "Break array queries into intervals of N hours")
// 	processingYear := flag.Int("year", 0, "If set, only process data for the given year")

// 	flag.Parse()
// 	log.SetOutput(os.Stdout)

// 	// Parse settings from JSON
// 	settings, err := ParseSettings(*metaPtr)
// 	if err != nil {
// 		log.Fatalf("failed parsing settings: %s", err)
// 	}


// 	timeStart,timeEnd,err := tiledbgnss.GetTimeRange(*tdbPathPtr,"us-east-2")
// 	if err != nil {
// 		log.Fatalln(err)
// 	}
// 	log.Infof("Time Range: %s - %s Found At %s",timeStart,timeEnd,*tdbPathPtr)
// 	daySlices := tiledbgnss.GetDateArranged(timeStart,timeEnd)
// 	if *processingYear != 0 {
// 		daySlicesModified := []tiledbgnss.TimeRange{}
// 		for _,slice := range daySlices {
// 			if slice.Start.Year() == *processingYear {
// 				daySlicesModified = append(daySlicesModified,slice)
// 			}
// 		}
// 		daySlices = daySlicesModified
// 	}
// 	if len(daySlices) == 0 {
// 		log.Warn("No Day Slices Found For The Year ",*processingYear)
// 		return
// 	}

// 	for _,daySlice := range daySlices {
	
		
// 		// break daySlice into 1 hour slices
// 		hourSlices := GetHourSlice(daySlice,*timeIntervals)
// 		batchNum := 0
// 		var currentFile string
// 		for _, hourSlice := range hourSlices {
// 			// Read the epochs from the TDB
// 			queryParams := tiledbgnss.ObsQueryParams{
// 				Time: []tiledbgnss.TimeRange{hourSlice},
// 			}
// 			epochs, err := tiledbgnss.ReadObsV3Array(
// 				*tdbPathPtr, "us-east-2", queryParams)
// 			if err != nil {
// 				log.Debug("Error Reading TDB: ",err)
// 			}
			
// 			if len(epochs) == 0 {
// 				log.Debug("No epochs found for the given time slice")
// 				continue
// 			}
// 			log.Infof("Found %d Epochs From Array Within Timespan: %s",len(epochs),hourSlice)

// 			if batchNum == 0 {
// 				settings.TimeOfFirst = epochs[0].Time
// 				settings.TimeOfLast = daySlice.End// TODO find a way to update time of last OBS 
// 				filename,err := WriteFirstEpochBatch(epochs,settings)
// 				if err != nil {
// 					log.Warnf("Error Writing First Epoch Batch: %s",err)
// 					break
// 				}
// 				log.Infof("Wrote First Epoch Batch To: %s",filename)
// 				currentFile = filename
				
// 			} else {
// 				err := WriteEpochs(epochs,currentFile,settings)
// 				if err != nil {
// 					log.Warnf("Error Writing Epochs: %s",err)
// 					break
// 				}
// 				log.Infof("Wrote Epochs To: %s",currentFile)
// 			}
// 			batchNum++
// 			epochs = nil // Clear the epochs list
// 		}
// 		log.Infof("==================== COMPLETE ====================")

// 	}
	
// 	log.Infof("Total Time Elapsed: %s",time.Since(startTime))
// }

func main(){
	metaPtr := "/Users/franklyndunbar/Project/SeaFloorGeodesy/Data/SFGMain/cascadia-gorda/NCL1/rinex_metav2.json"
	tdbPtr := "/Users/franklyndunbar/Project/SeaFloorGeodesy/Data/SFGMain/cascadia-gorda/NCL1/TileDB/rangea_db.tdb"
	timeIntPtr := 24
	yearPtr := 2023
	settings,err := ParseSettings(metaPtr)
	if err != nil {
		log.Fatalf("Error parsing settings: %s", err)
	}
	timeStart,timeEnd,err := tiledbgnss.GetTimeRange(tdbPtr,"us-east-2")
	if err != nil {
		log.Fatalln(err)
	}
	log.Infof("Time Range: %s - %s Found At %s",timeStart,timeEnd,tdbPtr)
	daySlices := tiledbgnss.GetDateArranged(timeStart,timeEnd)
	if yearPtr != 0 {
		daySlicesModified := []tiledbgnss.TimeRange{}
		for _,slice := range daySlices {
			if slice.Start.Year() == yearPtr {
				daySlicesModified = append(daySlicesModified,slice)
			}
		}
		daySlices = daySlicesModified
	}
	if len(daySlices) == 0 {
		log.Warn("No Day Slices Found For The Year ",yearPtr)
		return
	}

	for _,daySlice := range daySlices {
	
		
		// break daySlice into 1 hour slices
		hourSlices := GetHourSlice(daySlice,timeIntPtr)
		batchNum := 0
		var currentFile string
		for _, hourSlice := range hourSlices {
			// Read the epochs from the TDB
			queryParams := tiledbgnss.ObsQueryParams{
				Time: []tiledbgnss.TimeRange{hourSlice},
			}
			epochs, err := tiledbgnss.ReadObsV3Array(
				tdbPtr, "us-east-2", queryParams)
			if err != nil {
				log.Debug("Error Reading TDB: ",err)
			}
			
			if len(epochs) == 0 {
				log.Debug("No epochs found for the given time slice")
				continue
			}
			log.Infof("Found %d Epochs From Array Within Timespan: %s",len(epochs),hourSlice)

			if batchNum == 0 {
				settings.TimeOfFirst = epochs[0].Time
				settings.TimeOfLast = daySlice.End// TODO find a way to update time of last OBS 
				filename,err := WriteFirstEpochBatch(epochs,settings)
				if err != nil {
					log.Warnf("Error Writing First Epoch Batch: %s",err)
					break
				}
				log.Infof("Wrote First Epoch Batch To: %s",filename)
				currentFile = filename
				
			} else {
				err := WriteEpochs(epochs,currentFile,settings)
				if err != nil {
					log.Warnf("Error Writing Epochs: %s",err)
					break
				}
				log.Infof("Wrote Epochs To: %s",currentFile)
			}
			batchNum++
			epochs = nil // Clear the epochs list
		}
		log.Infof("==================== COMPLETE ====================")

	}
}
