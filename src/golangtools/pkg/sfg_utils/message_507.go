package sfg_utils

import (
	"errors"

	"github.com/bamiaux/iobit"
)

type Message_507 struct {
	// The number of inspvaa records in the message
	NumberOfRecords uint32
	// The inspvaa records
	InspvaaRecords []InspvaaRecord
}
type InspvaaRecord struct {
	// 32 bits - 1/1000 s
	GNSSWeek uint32 // binary bytes: 4, binary offset H
	Seconds uint32 // binary bytes: 8 , binary offset H+4
	Latitude float64 // binary bytes: 8, binary offset H+12
	Longitude float64 // binary bytes: 8, binary offset H+20
	Height float64 // binary bytes: 8, binary offset H+28
	NorthVelocity float64 // binary bytes: 8, binary offset H+36
	EastVelocity float64 // binary bytes: 8, binary offset H+44
	UpVelocity float64 // binary bytes: 8, binary offset H+52
	Roll float64 // binary bytes: 8, binary offset H+60
	Pitch float64 // binary bytes: 8, binary offset H+68
	Azimuth float64 // binary bytes: 8, binary offset H+76
	Status string // binary bytes: variable, binary offset H+84
}

func DeserializeINSPVAARecord(r *iobit.Reader) (record InspvaaRecord, err error) {
	if r == nil {
		return record, ErrNilReader
	}
	var inspvaarecord = InspvaaRecord{}
	n1 := r.Byte()

	inspvaarecord.GNSSWeek = extractBitsUint32(n1, 0, 4)
	inspvaarecord.Seconds = extractBitsUint32(n1, 4, 12)
	inspvaarecord.Latitude = extractBitsFloat64(n1, 12, 20)
	inspvaarecord.Longitude = extractBitsFloat64(n1, 20, 28)
	inspvaarecord.Height = extractBitsFloat64(n1, 28, 36)
	inspvaarecord.NorthVelocity = extractBitsFloat64(n1, 36, 44)
	inspvaarecord.EastVelocity = extractBitsFloat64(n1, 44, 52)
	inspvaarecord.UpVelocity = extractBitsFloat64(n1, 52, 60)
	inspvaarecord.Roll = extractBitsFloat64(n1, 60, 68)
	inspvaarecord.Pitch = extractBitsFloat64(n1, 68, 76)
	inspvaarecord.Azimuth = extractBitsFloat64(n1, 76, 84)

	return inspvaarecord, nil
}

func (msg *Message) DeserializeMessage507(r *iobit.Reader) (Message_507, error) {
	if r == nil {
		return Message_507{}, errors.New("nil reader")
	}

	var msg507 Message_507
	msg507.NumberOfRecords = r.ReadUint32()

	for i := uint32(0); i < msg507.NumberOfRecords; i++ {
		record, err := DeserializeINSPVAARecord(r)
		if err != nil {
			return Message_507{}, err
		}
		msg507.InspvaaRecords = append(msg507.InspvaaRecords, record)
	}

	return msg507, nil
}