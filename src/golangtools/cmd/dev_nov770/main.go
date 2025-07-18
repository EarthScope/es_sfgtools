// Author: Franklyn Dunbar  | Contact franklyn.dunbar@earthscope.org | Dec 2024
package main

import (
	"bufio"
	"flag"
	"io"
	"os"

	log "github.com/sirupsen/logrus"
	"gitlab.com/earthscope/gnsstools/pkg/common/gnss/observation"
	novatelbinary "gitlab.com/earthscope/gnsstools/pkg/encoding/novatel/novatel_binary"
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
				print(msg.Data, "\n")
			}
		}
	return epochs,nil
}


func main() {
	flag.Parse()
	filenames := flag.Args()
	if len(filenames) == 0 {
		flag.PrintDefaults()
		log.Fatalln("no files specified")
		
	}

		for _, filename := range filenames {
		
		epochs,err := processFileNOVB(filename)
		if err != nil {
			log.Errorf("error processing file: %v",err)
			return
		}
		if len(epochs) == 0 {
			log.Warnf("no epochs found in file %s", filename)
			return
		}
		
		}
	}
	