// Author: Franklyn Dunbar  | Contact franklyn.dunbar@earthscope.org | Dec 2024
package main

import (
	"bufio"
	"errors"
	"flag"
	"io"
	"os"

	"github.com/bamiaux/iobit"
	log "github.com/sirupsen/logrus"
	"gitlab.com/earthscope/gnsstools/pkg/common/gnss/observation"
	novatelbinary "gitlab.com/earthscope/gnsstools/pkg/encoding/novatel/novatel_binary"
)

var ErrNilReader = errors.New("nil reader")


type Message_507 struct {
	// The number of inspvaa records in the message
	NumberOfRecords uint32
	// The inspvaa records
	InspvaaRecords []InspvaaRecord
}
type InspvaaRecord struct {
	// 32 bits - 1/1000 s
	GNSSWeek uint32 // binary bytes: 4, binary offset H
	Seconds  uint64 // binary bytes: 8 , binary offset H+4
	Latitude uint64 // binary bytes: 8, binary offset H+12
	Longitude uint64 // binary bytes: 8, binary offset H+20
	Height uint64 // binary bytes: 8, binary offset H+28
	NorthVelocity uint64 // binary bytes: 8, binary offset H+36
	EastVelocity  uint64 // binary bytes: 8, binary offset H+44
	UpVelocity    uint64 // binary bytes: 8, binary offset H+52
	Roll          uint64 // binary bytes: 8, binary offset H+60
	Pitch        uint64 // binary bytes: 8, binary offset H+68
	Azimuth      uint64 // binary bytes: 8, binary offset H+76
	Status       string // binary bytes: variable, binary offset H+84
}

func DeserializeINSPVAARecord(r *iobit.Reader) (InspvaaRecord, error) {
	var rec InspvaaRecord

	rec.GNSSWeek = r.Le32()
	rec.Seconds = r.Le64()
	rec.Latitude = r.Le64()
	rec.Longitude = r.Le64()
	rec.Height = r.Le64()
	rec.NorthVelocity = r.Le64()
	rec.EastVelocity = r.Le64()
	rec.UpVelocity = r.Le64()
	rec.Roll = r.Le64()
	rec.Pitch = r.Le64()
	rec.Azimuth = r.Le64()

	// For Status, assuming it's a fixed length (e.g., 4 bytes), adjust as needed

	statusBytes := r.Le32()

	rec.Status = string(statusBytes)

	return rec, nil
}

func DeserializeMessage507(msg *novatelbinary.Message) Message_507 {

	r := iobit.NewReader(msg.Data)
	msg507 := Message_507{}
	msg507.NumberOfRecords = r.Le32()
	msg507.InspvaaRecords = []InspvaaRecord{}

	for i := 0; i < int(msg507.NumberOfRecords); i++ {
		record, err := DeserializeINSPVAARecord(&r)
		if err != nil {
			return Message_507{}
		}
		msg507.InspvaaRecords = append(msg507.InspvaaRecords, record)
	}

	return msg507
}

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
	found_message_ids := make(map[uint16]bool)

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
			found_message_ids[msg.MessageID] = true
			log.Infof("Message ID: %d, Time: %s", msg.MessageID, msg.Time())
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
			} else if msg.MessageID == 507 {
				log.Info("Processing Message 507")
				msg507 := DeserializeMessage507(&msg)
				print(msg507.InspvaaRecords, "\n")
			}
		}
	log.Infof("Found message IDs: %v", found_message_ids)
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