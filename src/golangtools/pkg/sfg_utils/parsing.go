package sfg_utils

import (
	"bufio"
	"fmt"
	"io"
	"log/slog"
	"os"
	"sort"
	"strconv"
	"strings"
	"time"

	log "github.com/labstack/gommon/log"
	"gitlab.com/earthscope/gnsstools/pkg/common/gnss/observation"
	novatelascii "gitlab.com/earthscope/gnsstools/pkg/encoding/novatel/novatel_ascii"
	novatelbinary "gitlab.com/earthscope/gnsstools/pkg/encoding/novatel/novatel_binary"
)

type InspvaaRecord struct {
	time              time.Time
	GNSSWeek          int
	GNSSSecondsofWeek float64
	latitude          float64
	longitude         float64
	height            float64
	northVelocity     float64
	eastVelocity      float64
	upVelocity        float64
	roll              float64
	pitch             float64
	azimuth           float64
	// status string
}

type INSSTDEVARecord struct {
	time              time.Time
	latitude_std      float64
	longitude_std     float64
	height_std        float64
	northVelocity_std float64
	eastVelocity_std  float64
	upVelocity_std    float64
	roll_std          float64
	pitch_std         float64
	azimuth_std       float64
}

type INSCompleteRecord struct {
	time              time.Time
	GNSSWeek          int
	GNSSSecondsofWeek float64
	latitude          float64
	longitude         float64
	height            float64
	northVelocity     float64
	eastVelocity      float64
	upVelocity        float64
	roll              float64
	pitch             float64
	azimuth           float64
	latitude_std      float64
	longitude_std     float64
	height_std        float64
	northVelocity_std float64
	eastVelocity_std  float64
	upVelocity_std    float64
	roll_std          float64
	pitch_std         float64
	azimuth_std       float64
	// status string
}

func MergeINSRecordsFlat(insPvaa InspvaaRecord, insStdDev INSSTDEVARecord) INSCompleteRecord {
	return INSCompleteRecord{
		time:              insPvaa.time,
		GNSSWeek:          insPvaa.GNSSWeek,
		GNSSSecondsofWeek: insPvaa.GNSSSecondsofWeek,
		latitude:          insPvaa.latitude,
		longitude:         insPvaa.longitude,
		height:            insPvaa.height,
		northVelocity:     insPvaa.northVelocity,
		eastVelocity:      insPvaa.eastVelocity,
		upVelocity:        insPvaa.upVelocity,
		roll:              insPvaa.roll,
		pitch:             insPvaa.pitch,
		azimuth:           insPvaa.azimuth,
		latitude_std:      insStdDev.latitude_std,
		longitude_std:     insStdDev.longitude_std,
		height_std:        insStdDev.height_std,
		northVelocity_std: insStdDev.northVelocity_std,
		eastVelocity_std:  insStdDev.eastVelocity_std,
		upVelocity_std:    insStdDev.upVelocity_std,
		roll_std:          insStdDev.roll_std,
		pitch_std:         insStdDev.pitch_std,
		azimuth_std:       insStdDev.azimuth_std,
		// status:                  insPvaa.status,
	}
}

func DeserializeINSPVAARecord(data string, time time.Time) (InspvaaRecord, error) {
	// 2267,580261.050000000,45.30245563418,-124.96561111107,-28.6138,-0.2412,0.6377,0.2949,2.627875295,0.299460630,70.416827684,INS_SOLUTION_GOOD
	record := InspvaaRecord{}
	record.time = time
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
	record.latitude = latitude

	longitude, err := strconv.ParseFloat(parts[3], 64)
	if err != nil {
		return InspvaaRecord{}, fmt.Errorf("error deserializing INSPVAA (%s)", err)
	}
	record.longitude = longitude

	height, err := strconv.ParseFloat(parts[4], 64)
	if err != nil {
		return InspvaaRecord{}, fmt.Errorf("error deserializing INSPVAA (%s)", err)
	}
	record.height = height

	northVelocity, err := strconv.ParseFloat(parts[5], 64)
	if err != nil {
		return InspvaaRecord{}, fmt.Errorf("error deserializing INSPVAA (%s)", err)
	}
	record.northVelocity = northVelocity

	eastVelocity, err := strconv.ParseFloat(parts[6], 64)
	if err != nil {
		return InspvaaRecord{}, fmt.Errorf("error deserializing INSPVAA (%s)", err)
	}
	record.eastVelocity = eastVelocity

	upVelocity, err := strconv.ParseFloat(parts[7], 64)
	if err != nil {
		return InspvaaRecord{}, fmt.Errorf("error deserializing INSPVAA (%s)", err)
	}
	record.upVelocity = upVelocity

	roll, err := strconv.ParseFloat(parts[8], 64)
	if err != nil {
		return InspvaaRecord{}, fmt.Errorf("error deserializing INSPVAA (%s)", err)
	}
	record.roll = roll

	pitch, err := strconv.ParseFloat(parts[9], 64)
	if err != nil {
		return InspvaaRecord{}, fmt.Errorf("error deserializing INSPVAA (%s)", err)
	}
	record.pitch = pitch

	azimuth, err := strconv.ParseFloat(parts[10], 64)
	if err != nil {
		return InspvaaRecord{}, fmt.Errorf("error deserializing INSPVAA (%s)", err)
	}
	record.azimuth = azimuth

	// status := strings.Join(parts[11:], ",")
	// record.status = status

	return record, nil

}

func DeserializeINSSTDEVARecord(data string, time time.Time) (INSSTDEVARecord, error) {
	record := INSSTDEVARecord{}
	record.time = time
	parts := strings.Split(data, ",")
	if len(parts) < 9 {
		return INSSTDEVARecord{}, fmt.Errorf("invalid INSSTDEVA record: %s", data)
	}

	latitude_std, err := strconv.ParseFloat(parts[0], 64)
	if err != nil {
		return INSSTDEVARecord{}, fmt.Errorf("error deserializing INSSTDEVA (%s)", err)
	}
	record.latitude_std = latitude_std

	longitude_std, err := strconv.ParseFloat(parts[1], 64)
	if err != nil {
		return INSSTDEVARecord{}, fmt.Errorf("error deserializing INSSTDEVA (%s)", err)
	}
	record.longitude_std = longitude_std

	height_std, err := strconv.ParseFloat(parts[2], 64)
	if err != nil {
		return INSSTDEVARecord{}, fmt.Errorf("error deserializing INSSTDEVA (%s)", err)
	}
	record.height_std = height_std

	northVelocity_std, err := strconv.ParseFloat(parts[3], 64)
	if err != nil {
		return INSSTDEVARecord{}, fmt.Errorf("error deserializing INSSTDEVA (%s)", err)
	}
	record.northVelocity_std = northVelocity_std

	eastVelocity_std, err := strconv.ParseFloat(parts[4], 64)
	if err != nil {
		return INSSTDEVARecord{}, fmt.Errorf("error deserializing INSSTDEVA (%s)", err)
	}
	record.eastVelocity_std = eastVelocity_std

	upVelocity_std, err := strconv.ParseFloat(parts[5], 64)
	if err != nil {
		return INSSTDEVARecord{}, fmt.Errorf("error deserializing INSSTDEVA (%s)", err)
	}
	record.upVelocity_std = upVelocity_std

	roll_std, err := strconv.ParseFloat(parts[6], 64)
	if err != nil {
		return INSSTDEVARecord{}, fmt.Errorf("error deserializing INSSTDEVA (%s)", err)
	}
	record.roll_std = roll_std

	pitch_std, err := strconv.ParseFloat(parts[7], 64)
	if err != nil {
		return INSSTDEVARecord{}, fmt.Errorf("error deserializing INSSTDEVA (%s)", err)
	}
	record.pitch_std = pitch_std

	azimuth_std, err := strconv.ParseFloat(parts[8], 64)
	if err != nil {
		return INSSTDEVARecord{}, fmt.Errorf("error deserializing INSSTDEVA (%s)", err)
	}
	record.azimuth_std = azimuth_std

	return record, nil
}

func MergeINSPVAAAndINSSTDEVA(INSPVAARecords []InspvaaRecord, INSSTDEVRecords []INSSTDEVARecord) []INSCompleteRecord {
	// sort the slices by time
	sort.Slice(INSPVAARecords, func(i, j int) bool {
		return INSPVAARecords[i].time.Before(INSPVAARecords[j].time)
	})
	sort.Slice(INSSTDEVRecords, func(i, j int) bool {
		return INSSTDEVRecords[i].time.Before(INSSTDEVRecords[j].time)
	})
	var matchedRecords []INSCompleteRecord
	i := 0
	j := 0
	foundMatch := 0
	var elemB INSSTDEVARecord

	for i < len(INSPVAARecords) {
		inspvaarecord := INSPVAARecords[i]
		if j < len(INSSTDEVRecords) {
			elemB = INSSTDEVRecords[j]
		} else {
			elemB = INSSTDEVARecord{}
		}

		if inspvaarecord.time.Equal(elemB.time) {
			foundMatch++
			merged := MergeINSRecordsFlat(inspvaarecord, elemB)
			matchedRecords = append(matchedRecords, merged)
			i++
			j++
		} else {
			merged := MergeINSRecordsFlat(inspvaarecord, INSSTDEVARecord{})
			matchedRecords = append(matchedRecords, merged)
			i++

		}

	}

	log.Infof("Found %d matching elements between the two lists", foundMatch)
	// Print the matching elements
	return matchedRecords
}
func GetTimeDiffsINSPVA(list []INSCompleteRecord) []float64 {
	var diffs []float64
	minDiff := 100000.0 // 1000 seconds
	for i := 1; i < len(list); i++ {
		difference := list[i].time.Sub(list[i-1].time).Seconds()
		if difference < minDiff {
			minDiff = difference
		}
		if difference < 1 {
			diffs = append(diffs, difference)
		}
	}
	var diffs_average float64
	if len(diffs) > 0 {
		var sum float64
		for _, v := range diffs {
			sum += v
		}
		diffs_average = sum / float64(len(diffs))
	}
	log.Infof("INSPVA Average time difference: %f seconds Minimum time difference: %f seconds", diffs_average, minDiff)
	return diffs
}

func GetTimeDiffGNSS(list []observation.Epoch) []float64 {
	var diffs []float64
	minDiff := 100000.0 // 1000 seconds
	for i := 1; i < len(list); i++ {
		difference := list[i].Time.Sub(list[i-1].Time).Seconds()
		if difference < minDiff {
			minDiff = difference
		}
		if difference < 1 {
			diffs = append(diffs, difference)
		}
	}
	var diffs_average float64
	if len(diffs) > 0 {
		var sum float64
		for _, v := range diffs {
			sum += v
		}
		diffs_average = sum / float64(len(diffs))
	}
	log.Infof("GNSS Average time difference: %f seconds Minimum time difference: %f seconds", diffs_average, minDiff)
	return diffs
}

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
	trimmedLine, err := removeBeforeASCIISyncChar(stringArray)
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
	if endOfHeaderIndex < 2 {
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
	var stx byte = 0x2        // start of text, 2 in decimal
	var etx byte = 0x3        // end of text, 3 in decimal
	var log_start byte = 0x23 // log start, 35 in decimal ASCII #
	var log_done byte = 0x2A  // log done, 2 in decimal, * in Ascii
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
		} else if peekByte[0] == etx {
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
		} else if got_start_of_text && got_start_of_log {
			buffer = append(buffer, peekByte[0])
		}
		_, err = r.Discard(1)
		if err != nil {
			log.Warnf("error discarding byte (%s)", err)
		}
	}

	return novatelascii.LongMessage{}, fmt.Errorf("unknown error")

}

// processFileNOVASCII reads a NOVATEL ASCII file and processes its contents to extract GNSS epochs.
// It takes a filename as input and returns a slice of observation.Epoch.
//
// The function performs the following steps:
// 1. Opens the specified file.
// 2. Creates a new scanner to read NOVATEL ASCII messages from the file.
// 3. Iterates over the messages in the file.
// 4. For each "RANGEA" message, deserializes the message data and converts it to a GNSS epoch.
// 5. Appends the GNSS epoch to the result slice.
//
// If an error occurs while opening the file or reading messages, the function logs the error and terminates the program.
func ProcessFileNOVASCII(filename string) []observation.Epoch {
	file, err := os.Open(filename)
	if err != nil {
		log.Fatal(err)
	}
	defer file.Close()
	epochs := []observation.Epoch{}
	scanner := novatelascii.NewScanner(bufio.NewReader(file))
epochLoop:
	for {
		msg, err := scanner.NextMessage()
		if err != nil {
			if err == io.EOF {
				err = file.Close()
				if err != nil {
					slog.Error("Error closing file", "error", err)
				}
				break epochLoop
			}
			slog.Debug("Error reading message", "error", err)
		}
		// log.Debugf("%+v", msg)
		switch m := msg.(type) {
		case novatelascii.LongMessage:
			if m.Msg == "RANGEA" {
				rangea, err := novatelascii.DeserializeRANGEA(m.Data)
				if err != nil {
					slog.Error("Error deserializing RANGEA", "error", err)
				}
				// slog.Debug("Message time", "time", m.Time())
				epoch, err := rangea.SerializeGNSSEpoch(m.Time())
				if err != nil {
					slog.Error("Error serializing GNSS epoch", "error", err)
				}
				epochs = append(epochs, epoch)
			}
		case novatelascii.ShortMessage:
			if m.Msg == "RANGEA" {
				rangea, err := novatelascii.DeserializeRANGEA(m.Data)
				if err != nil {
					slog.Error("Error deserializing RANGEA", "error", err)
				}
				epoch, err := rangea.SerializeGNSSEpoch(m.Time())
				if err != nil {
					slog.Error("Error serializing GNSS epoch", "error", err)
				}
				epochs = append(epochs, epoch)
			}
		}
	}

	return epochs
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
func ProcessFileNOVB(file string) ([]observation.Epoch, error) {
	f, err := os.Open(file)
	if err != nil {
		log.Fatalf("failed opening file: %s", err)
	}
	defer f.Close()

	reader := bufio.NewReader(f)
	epochs := []observation.Epoch{}
MessageLoop:
	for {
		msg, err := novatelbinary.DeserializeMessage(reader)
		if err != nil {
			if err == io.EOF {
				break MessageLoop

			}
			if err == bufio.ErrBufferFull {
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
			continue MessageLoop
		}
	}
	return epochs, nil
}
