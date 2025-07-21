package sfg_utils

import (
	"fmt"

	tiledb "github.com/TileDB-Inc/TileDB-Go"
)
func WriteINSPOSRecordToTileDB(arr string, region string, insRecords []INSCompleteRecord) error {
	if len(insRecords) == 0 {
		return fmt.Errorf("no INS records to write")
	}

	// Dimension buffers
	d0Buffer := []int64{} // Time dimension

	// Attribute buffers
	azimuthBuffer := []float64{}
	pitchBuffer := []float64{}
	rollBuffer := []float64{}
	latitudeBuffer := []float64{}
	longitudeBuffer := []float64{}
	heightBuffer := []float64{}
	latitudeSTDBuffer := []float64{}
	longitudeSTDBuffer := []float64{}
	heightSTDBuffer := []float64{}
	northVelocityBuffer := []float64{}
	eastVelocityBuffer := []float64{}
	upVelocityBuffer := []float64{}
	northVelocity_stdBuffer := []float64{}
	eastVelocity_stdBuffer := []float64{}
	upVelocity_stdBuffer := []float64{}
	rollStdBuffer := []float64{}
	pitchStdBuffer := []float64{}
	azimuthStdBuffer := []float64{}
	//statusBuffer := []string{}
	latitudeSTDBufferValidity := []uint8{}
	longitudeSTDBufferValidity := []uint8{}
	heightSTDBufferValidity := []uint8{}
	northVelocity_stdBufferValidity := []uint8{}
	eastVelocity_stdBufferValidity := []uint8{}
	upVelocity_stdBufferValidity := []uint8{}
	rollStdBufferValidity := []uint8{}
	pitchStdBufferValidity := []uint8{}
	azimuthStdBufferValidity := []uint8{}
/*
PositionAttributes = [
    attribute_dict["azimuth"],
    attribute_dict["pitch"],
    attribute_dict["roll"],
    attribute_dict["latitude"],
    attribute_dict["longitude"],
    attribute_dict["height"],
    attribute_dict["latitude_std"],
    attribute_dict["east_sigma"],
    attribute_dict["up_sigma"],
    attribute_dict["northVelocity"],
    attribute_dict["eastVelocity"],
    attribute_dict["upVelocity"],
    attribute_dict["northVelocity_std"],
    attribute_dict["eastVelocity_std"],
    attribute_dict["upVelocity_std"],
    attribute_dict["roll_std"],
    attribute_dict["pitch_std"],
    attribute_dict["azimuth_std"],
    attribute_dict["status"],
]
*/
	for _, record := range insRecords {
		d0Buffer = append(d0Buffer, record.time.UnixNano())
		azimuthBuffer = append(azimuthBuffer, record.azimuth)
		pitchBuffer = append(pitchBuffer, record.pitch)
		rollBuffer = append(rollBuffer, record.roll)
		latitudeBuffer = append(latitudeBuffer, record.latitude)
		longitudeBuffer = append(longitudeBuffer, record.longitude)
		heightBuffer = append(heightBuffer, record.height)
		latitudeSTDBuffer = append(latitudeSTDBuffer, record.latitude_std)
		longitudeSTDBuffer = append(longitudeSTDBuffer, record.longitude_std)
		heightSTDBuffer = append(heightSTDBuffer, record.height_std)
		northVelocityBuffer = append(northVelocityBuffer, record.northVelocity)
		eastVelocityBuffer = append(eastVelocityBuffer, record.eastVelocity)
		upVelocityBuffer = append(upVelocityBuffer, record.upVelocity)
		northVelocity_stdBuffer = append(northVelocity_stdBuffer, record.northVelocity_std)
		eastVelocity_stdBuffer = append(eastVelocity_stdBuffer, record.eastVelocity_std)
		upVelocity_stdBuffer = append(upVelocity_stdBuffer, record.upVelocity_std)
		rollStdBuffer = append(rollStdBuffer, record.roll_std)
		pitchStdBuffer = append(pitchStdBuffer, record.pitch_std)
		azimuthStdBuffer = append(azimuthStdBuffer, record.azimuth_std)
		//statusBuffer = append(statusBuffer, record.status)
		if record.latitude_std != 0 {
			latitudeSTDBufferValidity = append(latitudeSTDBufferValidity, 1)
		} else {
			latitudeSTDBufferValidity = append(latitudeSTDBufferValidity, 0)
		}
		if record.longitude_std != 0 {
			longitudeSTDBufferValidity = append(longitudeSTDBufferValidity, 1)
		} else {
			longitudeSTDBufferValidity = append(longitudeSTDBufferValidity, 0)
		}
		if record.height_std != 0 {
			heightSTDBufferValidity = append(heightSTDBufferValidity, 1)
		} else {
			heightSTDBufferValidity = append(heightSTDBufferValidity, 0)
		}
		if record.northVelocity_std != 0 {
			northVelocity_stdBufferValidity = append(northVelocity_stdBufferValidity, 1)
		} else {
			northVelocity_stdBufferValidity = append(northVelocity_stdBufferValidity, 0)
		}
		if record.eastVelocity_std != 0 {
			eastVelocity_stdBufferValidity = append(eastVelocity_stdBufferValidity, 1)
		} else {
			eastVelocity_stdBufferValidity = append(eastVelocity_stdBufferValidity, 0)
		}
		if record.upVelocity_std != 0 {
			upVelocity_stdBufferValidity = append(upVelocity_stdBufferValidity, 1)
		} else {
			upVelocity_stdBufferValidity = append(upVelocity_stdBufferValidity, 0)
		}
		if record.roll_std != 0 {
			rollStdBufferValidity = append(rollStdBufferValidity, 1)
		} else {
			rollStdBufferValidity = append(rollStdBufferValidity, 0)
		}
		if record.pitch_std != 0 {
			pitchStdBufferValidity = append(pitchStdBufferValidity, 1)
		} else {
			pitchStdBufferValidity = append(pitchStdBufferValidity, 0)
		}
		if record.azimuth_std != 0 {
			azimuthStdBufferValidity = append(azimuthStdBufferValidity, 1)
		} else {
			azimuthStdBufferValidity = append(azimuthStdBufferValidity, 0)
		}
	}
	// Create TileDB context
	config, err := tiledb.NewConfig()
	if err != nil {
		return err
	}

	err = config.Set("vfs.s3.region", region)
	if err != nil {
		return err
	}
	ctx,err := tiledb.NewContext(config)
	if err != nil {
		return fmt.Errorf("error creating TileDB context with config: %v", err)
	}
	defer ctx.Free()

	array,err := tiledb.NewArray(ctx, arr)
	if err != nil {
		return fmt.Errorf("error creating TileDB array: %v", err)
	}
	defer array.Free()

	err = array.Open(tiledb.TILEDB_WRITE)
	if err != nil {
		return fmt.Errorf("error opening TileDB array for writing: %v", err)
	}
	defer array.Close()

	query, err := tiledb.NewQuery(ctx, array)
	if err != nil {
		return fmt.Errorf("error creating TileDB query: %v", err)
	}
	defer query.Free()

	err = query.SetLayout(tiledb.TILEDB_UNORDERED)
	if err != nil {
		return err
	}

	_, err = query.SetDataBuffer("time", d0Buffer)
	if err != nil {
		return err
	}
	_, err = query.SetDataBuffer("azimuth", azimuthBuffer)
	if err != nil {
		return err
	}
	_, err = query.SetDataBuffer("pitch", pitchBuffer)
	if err != nil {
		return err
	}
	_, err = query.SetDataBuffer("roll", rollBuffer)
	if err != nil {
		return err
	}
	_, err = query.SetDataBuffer("latitude", latitudeBuffer)
	if err != nil {
		return err
	}
	_, err = query.SetDataBuffer("longitude", longitudeBuffer)
	if err != nil {
		return err
	}
	_, err = query.SetDataBuffer("height", heightBuffer)
	if err != nil {
		return err
	}
	_, err = query.SetDataBuffer("latitude_std", latitudeSTDBuffer)
	if err != nil {
		return err
	}
	_, err = query.SetValidityBuffer("latitude_std", latitudeSTDBufferValidity)
	if err != nil {
		return err
	}
	_, err = query.SetDataBuffer("longitude_std", longitudeSTDBuffer)
	if err != nil {
		return err
	}
	_, err = query.SetValidityBuffer("longitude_std", longitudeSTDBufferValidity)
	if err != nil {
		return err
	}
	_, err = query.SetDataBuffer("height_std", heightSTDBuffer)
	if err != nil {
		return err
	}
	_, err = query.SetValidityBuffer("height_std", heightSTDBufferValidity)
	if err != nil {
		return err
	}
	_, err = query.SetDataBuffer("northVelocity", northVelocityBuffer)
	if err != nil {
		return err
	}
	_, err = query.SetDataBuffer("eastVelocity", eastVelocityBuffer)
	if err != nil {
		return err
	}
	_, err = query.SetDataBuffer("upVelocity", upVelocityBuffer)
	if err != nil {
		return err
	}
	_, err = query.SetDataBuffer("northVelocity_std", northVelocity_stdBuffer)
	if err != nil {
		return err
	}
	_, err = query.SetValidityBuffer("northVelocity_std", northVelocity_stdBufferValidity)
	if err != nil {
		return err
	}
	_, err = query.SetDataBuffer("eastVelocity_std", eastVelocity_stdBuffer)
	if err != nil {
		return err
	}
	_, err = query.SetValidityBuffer("eastVelocity_std", eastVelocity_stdBufferValidity)
	if err != nil {
		return err
	}
	_, err = query.SetDataBuffer("upVelocity_std", upVelocity_stdBuffer)
	if err != nil {
		return err
	}
	_, err = query.SetValidityBuffer("upVelocity_std", upVelocity_stdBufferValidity)
	if err != nil {
		return err
	}
	_, err = query.SetDataBuffer("roll_std", rollStdBuffer)
	if err != nil {
		return err
	}
	_, err = query.SetValidityBuffer("roll_std", rollStdBufferValidity)
	if err != nil {
		return err
	}
	_, err = query.SetDataBuffer("pitch_std", pitchStdBuffer)
	if err != nil {
		return err
	}
	_, err = query.SetValidityBuffer("pitch_std", pitchStdBufferValidity)
	if err != nil {
		return err
	}
	_, err = query.SetDataBuffer("azimuth_std", azimuthStdBuffer)
	if err != nil {
		return err
	}
	_, err = query.SetValidityBuffer("azimuth_std", azimuthStdBufferValidity)
	if err != nil {
		return err
	}
	// _, err = query.SetDataBuffer("status", statusBuffer)
	// if err != nil {
	// 	return err
	// }

	err = query.Submit()
	if err != nil {
		return err
	}

	err = query.Finalize()
	if err != nil {
		return err
	}

	return nil
}