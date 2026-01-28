package main

import (
	"encoding/json"
	"flag"
	"io"
	"log/slog"
	"os"

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
	epochs := []observation.Epoch{}
	for _, novatel_filename := range filenames {

		file_epochs, _ := sfg_utils.ProcessFileNOVB(novatel_filename)
		epochs = append(epochs, file_epochs...)
		slog.Info("Processed file", "filename", novatel_filename, "num_epochs", len(file_epochs))
	}

	slog.Info("Total epochs processed", "count", len(epochs))
	batchedEpochs := sfg_utils.BatchEpochsByDay(epochs)


	for dayKey, dayEpochs := range batchedEpochs {
		slog.Info("Writing RINEX for day", "day", dayKey, "num_epochs", len(dayEpochs))
		err := sfg_utils.WriteEpochs(dayEpochs,settings)
		if err != nil {
			slog.Error("Error writing epochs", "error", err)
		}
	}
}
