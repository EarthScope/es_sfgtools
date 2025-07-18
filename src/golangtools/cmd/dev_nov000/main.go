package main

import (
	"bufio"
	"flag"
	"fmt"
	"io"
	"os"
	"sort"
	"strconv"
	"strings"
	"time"

	log "github.com/sirupsen/logrus"
	"gitlab.com/earthscope/gnsstools/pkg/common/gnss/observation"
	novatelascii "gitlab.com/earthscope/gnsstools/pkg/encoding/novatel/novatel_ascii"
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

type InspvaaRecord struct {
	RecordTime time.Time
	GNSSWeek int
	GNSSSecondsofWeek float64
	Latitude float64
	Longitude float64
	Height float64
	NorthVelocity float64
	EastVelocity float64
	UpVelocity float64
	Roll float64
	Pitch float64
	Azimuth float64
	Status string
}

type INSSTDEVARecord struct {
	RecordTime time.Time
	LatitudeSigma float64
	LongitudeSigma float64
	HeightSigma float64
	NorthVelocitySigma float64
	EastVelocitySigma float64
	UpVelocitySigma float64
	RollSigma float64
	PitchSigma float64
	AzimuthSigma float64
}

type INSCompleteRecord struct {
	InspvaaRecord InspvaaRecord
	INSSTDEVARecord INSSTDEVARecord
}

func DeserializeINSPVAARecord(data string,time time.Time) (InspvaaRecord, error) {
	// 2267,580261.050000000,45.30245563418,-124.96561111107,-28.6138,-0.2412,0.6377,0.2949,2.627875295,0.299460630,70.416827684,INS_SOLUTION_GOOD
	record := InspvaaRecord{}
	record.RecordTime = time
	parts := strings.Split(data, ",")
	if len(parts) < 12 {
		return InspvaaRecord{}, fmt.Errorf("invalid INSPVAA record: %s", data)
	}
	week, err := strconv.Atoi(parts[0])
	if err != nil {
		return InspvaaRecord{}, fmt.Errorf("error deserializing INSPVAA (%s)", err)
	}
	record.GNSSWeek = week
	seconds, err := strconv.ParseFloat(parts[1], 64)
	if err != nil {
		return InspvaaRecord{}, fmt.Errorf("error deserializing INSPVAA (%s)", err)
	}
	record.GNSSSecondsofWeek = seconds // seconds since the start of the week

	latitude, err := strconv.ParseFloat(parts[2], 64)
	if err != nil {
		return InspvaaRecord{}, fmt.Errorf("error deserializing INSPVAA (%s)", err)
	}
	record.Latitude = latitude

	longitude, err := strconv.ParseFloat(parts[3], 64)
	if err != nil {
		return InspvaaRecord{}, fmt.Errorf("error deserializing INSPVAA (%s)", err)
	}
	record.Longitude = longitude

	height, err := strconv.ParseFloat(parts[4], 64)
	if err != nil {
		return InspvaaRecord{}, fmt.Errorf("error deserializing INSPVAA (%s)", err)
	}
	record.Height = height

	northVelocity, err := strconv.ParseFloat(parts[5], 64)
	if err != nil {
		return InspvaaRecord{}, fmt.Errorf("error deserializing INSPVAA (%s)", err)
	}
	record.NorthVelocity = northVelocity

	eastVelocity, err := strconv.ParseFloat(parts[6], 64)
	if err != nil {
		return InspvaaRecord{}, fmt.Errorf("error deserializing INSPVAA (%s)", err)
	}
	record.EastVelocity = eastVelocity

	upVelocity, err := strconv.ParseFloat(parts[7], 64)
	if err != nil {		return InspvaaRecord{}, fmt.Errorf("error deserializing INSPVAA (%s)", err)
	}
	record.UpVelocity = upVelocity

	roll, err := strconv.ParseFloat(parts[8], 64)
	if err != nil {
		return InspvaaRecord{}, fmt.Errorf("error deserializing INSPVAA (%s)", err)
	}
	record.Roll = roll

	pitch, err := strconv.ParseFloat(parts[9], 64)
	if err != nil {
		return InspvaaRecord{}, fmt.Errorf("error deserializing INSPVAA (%s)", err)
	}
	record.Pitch = pitch

	azimuth, err := strconv.ParseFloat(parts[10], 64)
	if err != nil {
		return InspvaaRecord{}, fmt.Errorf("error deserializing INSPVAA (%s)", err)
	}
	record.Azimuth = azimuth

	status := strings.Join(parts[11:], ",")
	record.Status = status	

	return record, nil

}

func DeserializeINSSTDEVARecord(data string, time time.Time) (INSSTDEVARecord, error) {
	record := INSSTDEVARecord{}
	record.RecordTime = time
	parts := strings.Split(data, ",")
	if len(parts) < 9 {
		return INSSTDEVARecord{}, fmt.Errorf("invalid INSSTDEVA record: %s", data)
	}

	latitudeSigma, err := strconv.ParseFloat(parts[0], 64)
	if err != nil {
		return INSSTDEVARecord{}, fmt.Errorf("error deserializing INSSTDEVA (%s)", err)
	}
	record.LatitudeSigma = latitudeSigma

	longitudeSigma, err := strconv.ParseFloat(parts[1], 64)
	if err != nil {
		return INSSTDEVARecord{}, fmt.Errorf("error deserializing INSSTDEVA (%s)", err)
	}
	record.LongitudeSigma = longitudeSigma

	heightSigma, err := strconv.ParseFloat(parts[2], 64)
	if err != nil {
		return INSSTDEVARecord{}, fmt.Errorf("error deserializing INSSTDEVA (%s)", err)
	}
	record.HeightSigma = heightSigma

	northVelocitySigma, err := strconv.ParseFloat(parts[3], 64)
	if err != nil {
		return INSSTDEVARecord{}, fmt.Errorf("error deserializing INSSTDEVA (%s)", err)
	}
	record.NorthVelocitySigma = northVelocitySigma

	eastVelocitySigma, err := strconv.ParseFloat(parts[4], 64)
	if err != nil {
		return INSSTDEVARecord{}, fmt.Errorf("error deserializing INSSTDEVA (%s)", err)
	}
	record.EastVelocitySigma = eastVelocitySigma

	upVelocitySigma, err := strconv.ParseFloat(parts[5], 64)
	if err != nil {
		return INSSTDEVARecord{}, fmt.Errorf("error deserializing INSSTDEVA (%s)", err)
	}
	record.UpVelocitySigma = upVelocitySigma

	rollSigma, err := strconv.ParseFloat(parts[6], 64)
	if err != nil {
		return INSSTDEVARecord{}, fmt.Errorf("error deserializing INSSTDEVA (%s)", err)
	}
	record.RollSigma = rollSigma

	pitchSigma, err := strconv.ParseFloat(parts[7], 64)
	if err != nil {
		return INSSTDEVARecord{}, fmt.Errorf("error deserializing INSSTDEVA (%s)", err)
	}
	record.PitchSigma = pitchSigma

	azimuthSigma, err := strconv.ParseFloat(parts[8], 64)
	if err != nil {
		return INSSTDEVARecord{}, fmt.Errorf("error deserializing INSSTDEVA (%s)", err)
	}
	record.AzimuthSigma = azimuthSigma

	return record, nil
}

func MergeINSPVAAAndINSSTDEVA(INSPVAARecords []InspvaaRecord, INSSTDEVRecords []INSSTDEVARecord) []INSCompleteRecord {
	// sort the slices by RecordTime
	sort.Slice(INSPVAARecords, func(i, j int) bool {
		return INSPVAARecords[i].RecordTime.Before(INSPVAARecords[j].RecordTime)
	})
	sort.Slice(INSSTDEVRecords, func(i, j int) bool {
		return INSSTDEVRecords[i].RecordTime.Before(INSSTDEVRecords[j].RecordTime)
	})
	var matchedRecords []INSCompleteRecord
	i := 0
	j := 0
	for i < len(INSPVAARecords) && j < len(INSSTDEVRecords) {
		elemA := INSPVAARecords[i]
		elemB := INSSTDEVRecords[j]
		if elemA.RecordTime == elemB.RecordTime {
			pair := INSCompleteRecord{elemA, elemB}
			matchedRecords = append(matchedRecords, pair)
			i++
			j++
		} else if elemA.RecordTime.Before(elemB.RecordTime) {
			i++
		} else {
			j++
		}
	}
	if len(matchedRecords) == 0 {
		log.Warnf("No matching elements found between the two lists")
		return nil
	}
	log.Infof("Found %d matching elements between the two lists", len(matchedRecords))
	// Print the matching elements
	return matchedRecords
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
    // defer func() {
    //     if r := recover(); r != nil {
    //         log.Printf("Recovered from panic: %v", r)
    //     }
    // }()

	f,err := os.Open(file)
	if err != nil {
		log.Fatalf("failed opening file %s, %s ",file, err)
	}
	defer f.Close()
	reader := NewReader(bufio.NewReader(f))
	epochs := []observation.Epoch{}

	insEpochs := []InspvaaRecord{}
	insStdDevEpochs := []INSSTDEVARecord{}
	//insCompleteRecords := []INSCompleteRecord{}
	found_messages := make(map[string]bool)
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
					found_messages[m.Msg] = true
					if m.Msg == "RANGEA" {

						rangea, err := novatelascii.DeserializeRANGEA(m.Data)
						if err != nil {
							
							continue epochLoop

						}
						epoch, err := rangea.SerializeGNSSEpoch(m.Time())
						if err != nil {
						
							continue epochLoop
						}
						epochs = append(epochs, epoch)
					} else if m.Msg == "INSPVAA" {
						record, err := DeserializeINSPVAARecord(m.Data, m.Time())
						if err != nil {
							log.Errorf("error deserializing INSPVAA record: %s", err)
							continue epochLoop
						}
						insEpochs = append(insEpochs, record)
				
						
					} else if m.Msg == "INSSTDEVA" {
				
						record, err := DeserializeINSSTDEVARecord(m.Data, m.Time())
						if err != nil {
							log.Errorf("error deserializing INSSTDEVA record: %s", err)
							continue epochLoop
						}
						insStdDevEpochs = append(insStdDevEpochs, record)
					
					}
				}
		}
	sortedTimes := MergeINSPVAAAndINSSTDEVA(insEpochs, insStdDevEpochs)
	if len(sortedTimes) == 0 {
		log.Warnf("no matching times found between INSPVAA and INSSTDEVA records")
		return epochs
	}
	log.Infof("Found %d matching times between INSPVAA and INSSTDEVA records", len(sortedTimes))
	log.Infof("INSSTDEVA Records: %d, INSPVAA Records: %d", len(insStdDevEpochs), len(insEpochs))
	
	log.Infof("Found messages: %v", found_messages)
	return epochs
}	

func main() {

	flag.Parse()
	filenames := flag.Args()
	if len(filenames) == 0 {
		flag.PrintDefaults()
		log.Fatalln("no files specified")
	}
	
	for _, filename := range filenames {
		
		epochs := processFileNOV000(filename)
		if len(epochs) == 0 {
			log.Warnf("no epochs found in file %s", filename)
			return
		}
		log.Infof("processed %d epochs from file %s", len(epochs), filename)
		
		}
	}



