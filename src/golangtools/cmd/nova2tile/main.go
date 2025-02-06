package main

import (
	"bufio"
	"flag"
	"io"
	"os"

	log "github.com/sirupsen/logrus"
	"gitlab.com/earthscope/gnsstools/pkg/common/gnss/observation"
	novatelascii "gitlab.com/earthscope/gnsstools/pkg/encoding/novatel/novatel_ascii"
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

	epochs := []observation.Epoch{}



	for _, filename := range filenames {

		filename, err := os.Open(filename)
		if err != nil {
			log.Fatalln(err)
		}
		defer filename.Close()

		log.Debugln("opening", filename.Name())

		scanner := novatelascii.NewScanner(bufio.NewReader(filename))


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
								return
							}
							epoch, err := rangea.SerializeGNSSEpoch(m.Time())
							if err != nil {
								log.Errorln(err)
								return
							}
							epochs = append(epochs, epoch)
												
						}
					case novatelascii.ShortMessage:

						if m.Msg == "RANGEA" {

							rangea, err := novatelascii.DeserializeRANGEA(m.Data)
							if err != nil {
								log.Errorln(err)
								return
							}
							epoch, err := rangea.SerializeGNSSEpoch(m.Time())
							if err != nil {
								log.Errorln(err)
								return
							}
							epochs = append(epochs, epoch)
												
						}
				
					}
				}
			
		// Create a TileDB array

		err = tiledbgnss.WriteObsV3Array(*tdbPathPtr, "us-east-2", epochs)
		if err != nil {
			log.Errorf("error writing array: %v", err)
		}
		epochs = []observation.Epoch{}

	}
}

