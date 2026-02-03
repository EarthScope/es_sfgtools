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
	var wg sync.WaitGroup
	sem := make(chan struct{}, 10) // Limit to 10 concurrent goroutines

	epochs := []observation.Epoch{}
	mu := sync.Mutex{}
	for _, novatel_filename := range filenames {
		wg.Add(1)
		go func(novatel_filename string) {
			defer wg.Done()
			sem <- struct{}{}
			defer func() { <-sem }()
			file_epochs, _ := sfg_utils.ProcessFileNOV000(novatel_filename)
			if len(file_epochs) == 0 {
				slog.Warn("No epochs found in file", "filename", novatel_filename)
				return
			}
			startYear,startMonth,startDay := file_epochs[0].Time.Date()
			currentDate := time.Date(startYear,startMonth,startDay,0,0,0,0,time.UTC)
			dayOfYear := currentDate.YearDay()
			slog.Info("Processed file", "filename", novatel_filename,"Year", startYear,"Day of Year",dayOfYear, "DayOfYear", dayOfYear, "num_epochs", len(file_epochs))
			mu.Lock()
			epochs = append(epochs, file_epochs...)
			mu.Unlock()
		}(novatel_filename)
	}
	wg.Wait()


	slog.Info("Total epochs processed", "count", len(epochs))
	batchedEpochs, err := sfg_utils.BatchEpochsByDay(epochs)
	if err != nil {
		slog.Error("Error batching epochs by day", "error", err)
		os.Exit(1)
	}

	sem = make(chan struct{}, 10) // Limit to 10 concurrent goroutines
	for dayKey, dayEpochs := range batchedEpochs {
		wg.Add(1)
		go func(dayKey string, dayEpochs []observation.Epoch) {
			defer wg.Done()
			sem <- struct{}{}
			defer func() { <-sem }()
			slog.Info("Writing RINEX for day", "day", dayKey, "num_epochs", len(dayEpochs))
			err := sfg_utils.WriteEpochs(dayEpochs,settings)
			if err != nil {
				slog.Error("Error writing epochs", "error", err)
			}
		}(dayKey, dayEpochs)
	}
	wg.Wait()
}
	
