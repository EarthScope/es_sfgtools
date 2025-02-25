// Author: Franklyn Dunbar  | Contact franklyn.dunbar@earthscope.org | Dec 2024
package main

import (
	"bufio"
	"flag"
	"fmt"
	"io"
	"os"
	"strconv"
	"strings"
	"sync"

	utils "github.com/EarthScope/es_sfgtools/src/golangtools/pkg/utils"
	log "github.com/sirupsen/logrus"
	"gitlab.com/earthscope/gnsstools/pkg/common/gnss/observation"
	novatelascii "gitlab.com/earthscope/gnsstools/pkg/encoding/novatel/novatel_ascii"
	"gitlab.com/earthscope/gnsstools/pkg/encoding/tiledbgnss"
)

func removeBeforeASCIISyncChar(s string) (string, error) {
	longMessageIndex := strings.Index(s, "#")
	shortMessageIndex := strings.Index(s, "%")
	switch {
	case longMessageIndex != -1:
		return s[longMessageIndex:], nil
	case shortMessageIndex != -1:
		return s[shortMessageIndex:], nil
	default:
		return "", fmt.Errorf("novatel ASCII sync char not found")
	}
}


func processBuffer(buffer []byte) (message novatelascii.Message, err error) {
	stringArray := string(buffer)
	trimmedLine,err := removeBeforeASCIISyncChar(stringArray)
	if err != nil {
		return message, err
	}
	//fmt.Print("\n Trimmed Line: ", trimmedLine)
	endOfHeaderIndex := strings.Index(trimmedLine, ";")
	endOfDataIndex := strings.Index(trimmedLine, "*")

	if endOfDataIndex <= endOfHeaderIndex {
		return message, fmt.Errorf("message is missing checksum")
		// endOfDataIndex = len(trimmedLine) - 1
	}
	if endOfDataIndex == -1 {
		return message, fmt.Errorf("message is missing checksum")
		// endOfDataIndex = len(trimmedLine) - 1
	}
	if endOfHeaderIndex< 2 {
		return message, fmt.Errorf("message is too short")
	}
	splitHeaderText := strings.Split(trimmedLine[1:endOfHeaderIndex], ",")
	if len(splitHeaderText) < 10 {
		return message, fmt.Errorf("message header is too short")
	}
	switch trimmedLine[0] {
		case '#': // long
			sequence, err := strconv.Atoi(splitHeaderText[2])
			if err != nil {
				return message, err
			}
			idleTime, err := strconv.ParseFloat(splitHeaderText[3], 64)
			if err != nil {
				return message, err
			}
			week, err := strconv.ParseFloat(splitHeaderText[5], 64)
			if err != nil {
				return message, err
			}
			seconds, err := strconv.ParseFloat(splitHeaderText[6], 64)
			if err != nil {
				return message, err
			}
			recStatus, err := strconv.ParseFloat(splitHeaderText[7], 64)
			if err != nil {
				return message, err
			}
			recSWVersion, err := strconv.ParseFloat(splitHeaderText[9], 64)
			if err != nil {
				return message, err
			}
			longMessage := novatelascii.LongMessage{
				Sync:         string(trimmedLine[0]),
				Msg:          splitHeaderText[0],
				Port:         splitHeaderText[1],
				Sequence:     sequence,
				IdleTime:     idleTime,
				TimeStatus:   splitHeaderText[4],
				Week:         week,
				Seconds:      seconds,
				RecStatus:    recStatus,
				Reserved:     splitHeaderText[8],
				RecSWVersion: recSWVersion,
				Data:         trimmedLine[endOfHeaderIndex+1 : endOfDataIndex],
				Checksum:     trimmedLine[endOfDataIndex:],
			}
			return longMessage, nil
	default:
		return novatelascii.LongMessage{}, fmt.Errorf("unknown error")
	}

}

	
func DeserializeNOV00bin(r *bufio.Reader) (message novatelascii.Message, err error) {
	var stx byte = 0x2 // start of text, 2 in decimal
	var etx byte = 0x3 // end of text, 3 in decimal
	var log_start byte = 0x23 // log start, 35 in decimal ASCII #
	var log_done byte = 0x2A// log done, 2 in decimal, * in Ascii
	var got_start_of_text bool = false
	var got_end_of_text bool = false
	var got_start_of_log bool = false
	var got_end_of_log bool = false
	var buffer []byte

	for {
		peekByte, err := r.Peek(1)
		if err != nil {
			switch {
			case err == io.EOF || err == bufio.ErrBufferFull:
				// do not advance the reader
				return message, err
			default:
				// advance the reader
				_, err := r.Discard(1)
				if err != nil {
					log.Warnf("error discarding byte (%s)", err)
				}
				return message, fmt.Errorf("error peeking byte (%s)", err)
			}
		}
		if peekByte[0] == stx {
			got_start_of_text = true
		} else if peekByte[0] == log_start {
			got_start_of_log = true
			buffer = []byte{}
		} else if peekByte[0] == etx{
			got_end_of_text = true
		} else if peekByte[0] == log_done {
			got_end_of_log = true
			got_end_of_text = false
		}
		if got_end_of_text && got_end_of_log {
			buffer = append(buffer, peekByte[0])
			_, err := r.Discard(1)
			if err != nil {
				log.Warnf("error discarding byte (%s)", err)
			}
			message, err := processBuffer(buffer)
			if err != nil {
				break
			}
			return message, err
		} else if got_start_of_text && got_start_of_log{
			buffer = append(buffer, peekByte[0])
		}
		_, err = r.Discard(1)
		if err != nil {
			log.Warnf("error discarding byte (%s)", err)
		}
	}
	

	return novatelascii.LongMessage{}, fmt.Errorf("unknown error")

}
	
type Reader struct {
	Reader *bufio.Reader
}

func NewReader(r io.Reader) Reader {
	return Reader{Reader: bufio.NewReader(r)}
}

func (reader Reader) NextMessage() (message novatelascii.Message, err error) {
	message,err = DeserializeNOV00bin(reader.Reader)
	if err != nil {
		if err == io.EOF {
			return message, err
		}
	}
	return message, nil
}

// processFileNOV000 reads a file containing GNSS data, processes it, and returns a slice of observation.Epoch.
// It opens the specified file, reads messages using a custom reader, and processes "RANGEA" messages
// to extract GNSS epoch data. The function handles errors appropriately and ensures the file is closed
// after processing.
//
// Parameters:
//   - file: The path to the file to be processed.
//
// Returns:
//   - A slice of observation.Epoch containing the processed GNSS epoch data.
func processFileNOV000(file string) []observation.Epoch{
	f,err := os.Open(file)
	if err != nil {
		log.Fatalf("failed opening file %s, %s ",file, err)
	}
	defer f.Close()
	reader := NewReader(bufio.NewReader(f))
	epochs := []observation.Epoch{}

	epochLoop:
		for {
			message,err := reader.NextMessage()
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
					if m.Msg == "RANGEA" {
						rangea, err := novatelascii.DeserializeRANGEA(m.Data)
						if err != nil {
							continue
						}
						epoch, err := rangea.SerializeGNSSEpoch(m.Time())
						if err != nil {
							continue
						}
						epochs = append(epochs, epoch)
					}
				}
		}
	return epochs
}	

func main() {
	tdbPathPtr := flag.String("tdb", "", "Path to the TileDB array")
	numProcsPtr := flag.Int("procs", 10, "Number of concurrent processes")
	flag.Parse()
	filenames := flag.Args()
	if len(filenames) == 0 {
		flag.PrintDefaults()
		log.Fatalln("no files specified")
	}
	if !utils.ArrayExists(*tdbPathPtr) {
		err := tiledbgnss.CreateArray("s3://earthscope-tiledb-schema-dev-us-east-2-ebamji/GNSS_OBS_SCHEMA_V3.tdb/", *tdbPathPtr, "us-east-2")
		if err != nil {
			log.Errorf("error creating array: %v",err)
		}
	} else {
		log.Infof("array %s already exists", *tdbPathPtr)
	}
	var wg sync.WaitGroup
	sem := make(chan struct{}, *numProcsPtr) // Limit to 10 concurrent goroutines
	for _, filename := range filenames {
		wg.Add(1)
		go func(filename string) {
			defer wg.Done()
			sem <- struct{}{}
			defer func() { <-sem }()
			epochs := processFileNOV000(filename)
			if len(epochs) == 0 {
				log.Warnf("no epochs found in file %s", filename)
				return
			}
			log.Infof("processed %d epochs from file %s", len(epochs), filename)
			err := tiledbgnss.WriteObsV3Array( *tdbPathPtr,"us-east-2",epochs)
			if err != nil {
				log.Errorf("error writing epochs to array: %v",err)
			}
		}(filename)
	
	}
	wg.Wait()
}

