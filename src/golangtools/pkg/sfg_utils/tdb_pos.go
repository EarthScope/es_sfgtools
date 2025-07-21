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
	northSigmaBuffer := []float64{}
	eastSigmaBuffer := []float64{}
	upSigmaBuffer := []float64{}
	northVelocityBuffer := []float64{}
	eastVelocityBuffer := []float64{}
	upVelocityBuffer := []float64{}
	northVelocitySigmaBuffer := []float64{}
	eastVelocitySigmaBuffer := []float64{}
	upVelocitySigmaBuffer := []float64{}
	rollStdBuffer := []float64{}
	pitchStdBuffer := []float64{}
	azimuthStdBuffer := []float64{}
	statusBuffer := []string{}
/*
PositionAttributes = [
    attribute_dict["azimuth"],
    attribute_dict["pitch"],
    attribute_dict["roll"],
    attribute_dict["latitude"],
    attribute_dict["longitude"],
    attribute_dict["height"],
    attribute_dict["north_sigma"],
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
		northSigmaBuffer = append(northSigmaBuffer, record.north_std)
		eastSigmaBuffer = append(eastSigmaBuffer, record.east_std)
		upSigmaBuffer = append(upSigmaBuffer, record.up_std)
		northVelocityBuffer = append(northVelocityBuffer, record.northVelocity)
		eastVelocityBuffer = append(eastVelocityBuffer, record.eastVelocity)
		upVelocityBuffer = append(upVelocityBuffer, record.upVelocity)
		northVelocitySigmaBuffer = append(northVelocitySigmaBuffer, record.northVelocitySigma)
		eastVelocitySigmaBuffer = append(eastVelocitySigmaBuffer, record.eastVelocitySigma)
		upVelocitySigmaBuffer = append(upVelocitySigmaBuffer, record.upVelocitySigma)
		rollStdBuffer = append(rollStdBuffer, record.roll_std)
		pitchStdBuffer = append(pitchStdBuffer, record.pitch_std)
		azimuthStdBuffer = append(azimuthStdBuffer, record.azimuth_std)
		statusBuffer = append(statusBuffer, record.status)
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
	_, err = query.SetDataBuffer("north_sigma", northSigmaBuffer)
	if err != nil {
		return err
	}
	_, err = query.SetDataBuffer("east_sigma", eastSigmaBuffer)
	if err != nil {
		return err
	}
	_, err = query.SetDataBuffer("up_sigma", upSigmaBuffer)
	if err != nil {
		return err
	}
	_, err = query.SetDataBuffer("north_velocity", northVelocityBuffer)
	if err != nil {
		return err
	}
	_, err = query.SetDataBuffer("east_velocity", eastVelocityBuffer)
	if err != nil {
		return err
	}
	_, err = query.SetDataBuffer("up_velocity", upVelocityBuffer)
	if err != nil {
		return err
	}
	_, err = query.SetDataBuffer("north_velocity_sigma", northVelocitySigmaBuffer)
	if err != nil {
		return err
	}
	_, err = query.SetDataBuffer("east_velocity_sigma", eastVelocitySigmaBuffer)
	if err != nil {
		return err
	}
	_, err = query.SetDataBuffer("up_velocity_sigma", upVelocitySigmaBuffer)
	if err != nil {
		return err
	}
	_, err = query.SetDataBuffer("roll_std", rollStdBuffer)
	if err != nil {
		return err
	}
	_, err = query.SetDataBuffer("pitch_std", pitchStdBuffer)
	if err != nil {
		return err
	}
	_, err = query.SetDataBuffer("azimuth_std", azimuthStdBuffer)
	if err != nil {
		return err
	}
	_, err = query.SetDataBuffer("status", statusBuffer)
	if err != nil {
		return err
	}

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