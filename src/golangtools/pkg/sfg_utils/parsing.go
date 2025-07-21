package sfg_utils

import (
	"fmt"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/labstack/gommon/log"
	"gitlab.com/earthscope/gnsstools/pkg/common/gnss/observation"
)

type InspvaaRecord struct {
	time time.Time
	GNSSWeek int
	GNSSSecondsofWeek float64
	latitude float64
	longitude float64
	height float64
	northVelocity float64
	eastVelocity float64
	upVelocity float64
	roll float64
	pitch float64
	azimuth float64
	// status string
}

type INSSTDEVARecord struct {
	time time.Time
	latitude_std float64
	longitude_std float64
	height_std float64
	northVelocity_std float64
	eastVelocity_std float64
	upVelocity_std float64
	roll_std float64
	pitch_std float64
	azimuth_std float64
}

type INSCompleteRecord struct {
	time time.Time
	GNSSWeek int
	GNSSSecondsofWeek float64
	latitude float64
	longitude float64
	height float64
	northVelocity float64
	eastVelocity float64
	upVelocity float64
	roll float64
	pitch float64
	azimuth float64
	latitude_std float64
	longitude_std float64
	height_std float64
	northVelocity_std float64
	eastVelocity_std float64
	upVelocity_std float64
	roll_std float64
	pitch_std float64
	azimuth_std float64
	// status string
}

func MergeINSRecordsFlat(insPvaa InspvaaRecord, insStdDev INSSTDEVARecord) INSCompleteRecord {
	return INSCompleteRecord{
		time:               insPvaa.time,
		GNSSWeek:                 insPvaa.GNSSWeek,
		GNSSSecondsofWeek:        insPvaa.GNSSSecondsofWeek,
		latitude:                 insPvaa.latitude,
		longitude:                insPvaa.longitude,
		height:                   insPvaa.height,
		northVelocity:            insPvaa.northVelocity,
		eastVelocity:             insPvaa.eastVelocity,
		upVelocity:               insPvaa.upVelocity,
		roll:                     insPvaa.roll,
		pitch:                    insPvaa.pitch,
		azimuth:                  insPvaa.azimuth,
		latitude_std:            insStdDev.latitude_std,
		longitude_std:           insStdDev.longitude_std,
		height_std:              insStdDev.height_std,
		northVelocity_std:       insStdDev.northVelocity_std,
		eastVelocity_std:        insStdDev.eastVelocity_std,
		upVelocity_std:         insStdDev.upVelocity_std,
		roll_std:                insStdDev.roll_std,
		pitch_std:               insStdDev.pitch_std,
		azimuth_std:             insStdDev.azimuth_std,
		// status:                  insPvaa.status,
	}
}

func DeserializeINSPVAARecord(data string,time time.Time) (InspvaaRecord, error) {
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
	if err != nil {		return InspvaaRecord{}, fmt.Errorf("error deserializing INSPVAA (%s)", err)
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
			inspvaarecord = INSPVAARecords[i]
		} else{
			merged := MergeINSRecordsFlat(inspvaarecord, INSSTDEVARecord{})
			matchedRecords = append(matchedRecords, merged)
			i++

		}
	
	}
		

	log.Infof("Found %d matching elements between the two lists", foundMatch)
	// Print the matching elements
	return matchedRecords
}
func getTimeDiffsINSPVA(list []INSCompleteRecord ) []float64 {
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

func getTimeDiffGNSS(list []observation.Epoch ) []float64 {
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
