package main

import (
	"encoding/json"
	"flag"
	"io"
	"log/slog"
	"os"
	"sync"
	"time"

	"github.com/EarthScope/es_sfgtools/src/golangtools/pkg/sfg_utils"
	"gitlab.com/earthscope/gnsstools/pkg/common/gnss/observation"
	"gitlab.com/earthscope/gnsstools/pkg/encoding/rinex"
)

var (
	VERSION string
)

func main() {
	// Initialize slog with text handler
	handler := slog.NewTextHandler(os.Stderr, nil)
	logger := slog.New(handler)
	slog.SetDefault(logger)

	// parse command line args
	metaPtr := flag.String("settings", "", "settings file")
	moduloPtr := flag.Int64("modulo", 0, "decimation modulo in milliseconds (e.g., 100 for 10 Hz , 1000 for 1 Hz, 15000 for 15s intervals). If 0, no decimation is applied.")
	numRoutinesPtr := flag.Int("numroutines", 1, "number of concurrent goroutines to use for processing files")
	
	flag.Parse()

	filenames := flag.Args()
	if len(filenames) == 0 {
		flag.PrintDefaults()
		slog.Error("No files specified")
		os.Exit(1)
	}

	if *metaPtr == "" {
		flag.PrintDefaults()
		slog.Error("Missing required arguments")
		os.Exit(1)
	}

	metaFile, err := os.Open(*metaPtr)
	if err != nil {
		slog.Error("Error opening json file", "error", err)
		os.Exit(1)
	}
	defer metaFile.Close()

	metaBytes, err := io.ReadAll(metaFile)
	if err != nil {
		slog.Error("Error reading json file", "error", err)
		os.Exit(1)
	}

	settings := rinex.NewSettings()
	if err := json.Unmarshal(metaBytes, &settings); err != nil {
		slog.Error("Error unmarshalling json", "error", err)
		os.Exit(1)
	}
	filename_times := []sfg_utils.FileTime{}
	// sort files by first epoch time
	filename_times, err = sfg_utils.SortFilesByFirstEpochNOVB(filenames)
	if err != nil {
		slog.Error("Error sorting files by first epoch", "error", err)
		os.Exit(1)
	}

	// batch files by day, decimate and write out day by day to save RAM. If modulo is specified, decimate to reduce number of epochs in memory at once

	filename_times_batched := make(map[string][]sfg_utils.FileTime)
	for _, fileTime := range filename_times {
		daykey := sfg_utils.GetYMDKey(fileTime.Time)
		filename_times_batched[daykey] = append(filename_times_batched[daykey], fileTime)
	}


	epoch_count := 0
	var wg sync.WaitGroup
	sem := make(chan struct{}, *numRoutinesPtr) // Limit to numRoutinesPtr concurrent goroutines

	batched_epochs := make(map[string][]observation.Epoch)
	mu := sync.Mutex{}
	for YMD_KEY, fileTimes := range filename_times_batched {
		wg.Add(1)
		go func(fileNameTimes []sfg_utils.FileTime) {
			defer wg.Done()
			sem <- struct{}{}
			defer func() { <-sem }()
			
			for _, fileNameTime := range fileNameTimes {
				file_epochs, err := sfg_utils.ProcessFileNOVB(fileNameTime.Filename)

				if err != nil {
					slog.Error("Error processing file", "filename", fileNameTime.Filename, "error", err)
					return
				}
				if len(file_epochs) == 0 {
					slog.Warn("No epochs found in file", "filename", fileNameTime.Filename)
					return
				}
				if *moduloPtr > 0 {
					file_epochs = sfg_utils.DecimateEpochs(file_epochs, *moduloPtr)
				}
				epoch_count += len(file_epochs)

				batched_epochs_sub,err := sfg_utils.BatchEpochsByDay(file_epochs)
				// delete file_epochs to save RAM
				if err != nil {
					slog.Error("Error batching epochs by day", "error", err)
					return
				}
				file_epochs = nil
			
				for key, day_epoch_batch := range batched_epochs_sub {
					mu.Lock()
					// join with main batched_epochs map
					batched_epochs[key] = append(batched_epochs[key], day_epoch_batch...)
					delete(batched_epochs_sub, key)
					mu.Unlock()
				}	
			

					// get year, day of year from first epoch
				startYear, startMonth, startDay := fileNameTime.Time.Date()
				currentDate := time.Date(startYear, startMonth, startDay, 0, 0, 0, 0, time.UTC)
				dayOfYear := currentDate.YearDay()
				slog.Info("Processed file", "filename", fileNameTime.Filename, "Year", startYear, "Day of Year", dayOfYear, "num_epochs", len(file_epochs))
			}
			// after processing all files for the day, write out the day's epochs and clear from memory to save RAM
		
			if batched_epochs[YMD_KEY] != nil {
				err := sfg_utils.WriteEpochs(batched_epochs[YMD_KEY], settings)
				if err != nil {
					slog.Error("Error writing epochs", "error", err)
				}
				delete(batched_epochs, YMD_KEY)
			}
			
			
		}(fileTimes)
	}
	wg.Wait()

	slog.Info("\n================================================")
	slog.Info("\nTotal epochs processed", "count", epoch_count)


}
