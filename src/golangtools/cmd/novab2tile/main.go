// Author: Franklyn Dunbar  | Contact franklyn.dunbar@earthscope.org | Dec 2024
package main

import (
	"bufio"
	"flag"
	"io"
	"os"

	log "github.com/sirupsen/logrus"
	"gitlab.com/earthscope/gnsstools/pkg/encoding/gnss/observation"
	novatelbinary "gitlab.com/earthscope/gnsstools/pkg/encoding/novatel/novatel_binary"
	"gitlab.com/earthscope/gnsstools/pkg/encoding/tiledbgnss"
)


func main() {

	tdbPathPtr := flag.String("tdb", "", "Path to the TileDB array")
	flag.Parse()

	filenames := flag.Args()
	if len(filenames) == 0 {
		flag.PrintDefaults()
		log.Fatalln("no files specified")
		
	}

	err := tiledbgnss.CreateArray("s3://earthscope-tiledb-schema-dev-us-east-2-ebamji/GNSS_OBS_SCHEMA_V3.tdb/", *tdbPathPtr, "us-east-2")
	if err != nil {
		// t.Errorf("Error creating array: %v", err)
		log.Errorf("error creating array: %v",err)
	}
	for _, filename := range filenames {
		epochs := processFile(filename)
		if len(epochs) == 0 {
			log.Warnf("no epochs found in file %s", filename)
			continue
		}
		log.Infof("processed %d epochs from file %s", len(epochs), filename)
		err := tiledbgnss.WriteObsV3Array( *tdbPathPtr,"us-east-2",epochs)
		if err != nil {
			log.Errorf("error writing epochs to array: %v",err)
		}
		epochs = nil
	
	}
}

func processFile(file string) []observation.Epoch{
	f, err := os.Open(file)
	if err != nil {
		log.Fatalf("failed opening file: %s", err)
	}
	defer f.Close()

	scanner := novatelbinary.NewScanner(bufio.NewReader(f))
	epochs := []observation.Epoch{}
	MessageLoop:
		for {
			msg,err := scanner.NextMessage()

			if err != nil {
				if err == io.EOF {
					break MessageLoop
				}
				log.Warnf("failed reading message: %s", err)
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
			}
		}
	return epochs
}

