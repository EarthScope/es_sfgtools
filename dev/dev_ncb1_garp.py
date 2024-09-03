import sys
from pathlib import Path
import json
import es_sfgtools
import es_sfgtools.processing as sfg_proc
import es_sfgtools.modeling.garpos_tools as sfg_mod
import numpy as np
import pandas as pd
from datetime import datetime, timedelta


data_dir = Path("/Users/franklyndunbar/Project/SeaFloorGeodesy/Data/TestSV3/")
gp_data_dir = data_dir / "Garpos"

catalog_dir = Path("/Users/franklyndunbar/Project/SeaFloorGeodesy/Data/TestSV3/Garpos/prep_NCB1_catalog.json")
cdt_file_path = data_dir/"fromjohn" / "ctd" / "CTD_NCB1_Ch_Mi"
master_file_path = data_dir/"fromjohn" / "NCB1.master"

cdt_shema = sfg_proc.schemas.files.CTDFile(location=cdt_file_path)
master_schema = sfg_proc.schemas.files.MasterFile(location=master_file_path)

with open(catalog_dir,'r') as f:
    catalog_ncb1 = json.load(f)

# get second key of catalog_ncb1
date_key = list(catalog_ncb1.keys())[0]

if __name__ == "__main__":
    for date_key in list(catalog_ncb1.keys())[4:]:
        if "11" in date_key:
            continue
        print(f"PROCESSING DAY: {date_key}\n")
        site_file = gp_data_dir.parent / f"ncb1{date_key}_site.json"

        if not site_file.exists():
        
            gnss_df = pd.read_csv(catalog_ncb1[date_key]["gnss"])
            acoustic_df = pd.read_csv(catalog_ncb1[date_key]["acoustic"])
            imu_df = pd.read_csv(catalog_ncb1[date_key]["imu"])

            gnss_df = sfg_proc.schemas.observables.PositionDataFrame.validate(gnss_df.drop_duplicates(),lazy=True)
            acoustic_df = sfg_proc.schemas.observables.AcousticDataFrame.validate(acoustic_df.drop_duplicates(),lazy=True)
            imu_df = sfg_proc.schemas.observables.IMUDataFrame.validate(imu_df.drop_duplicates(),lazy=True)
            
            min_time = max(max(acoustic_df.TriggerTime.min(),gnss_df.time.min()),imu_df.Time.min()) 
            max_time = min_time + pd.Timedelta(seconds=10000)
            acoustic_df = acoustic_df[(acoustic_df.TriggerTime >= min_time+pd.Timedelta(seconds=1000)) & (acoustic_df.TriggerTime <= max_time)]
            gnss_df = gnss_df[(gnss_df.time >= min_time) & (gnss_df.time <= max_time)]
            imu_df = imu_df[(imu_df.Time >= min_time) & (imu_df.Time <= max_time)]
        
            # DUCT TAPE JESSE FIXES
            acoustic_df.PingTime  += 18
            # acoustic_df.PingTime -= 0.13
            acoustic_df.ReturnTime += 18
            # acoustic_df.ReturnTime -= 0.13
   
            site_config = sfg_proc.site_functions.masterfile_to_siteconfig(master_schema)
            svp_df: pd.DataFrame = sfg_proc.functions.seabird_functions.ctd_to_soundvelocity(
                cdt_shema
            )
            atd_offset = sfg_proc.schemas.ATDOffset(forward=0.0053, rightward=0, downward=0.92813)
    
            site_config.name = "NCB1"
            garpos_input = sfg_mod.functions.garpos_input_from_site_obs(
                site_config=site_config,
                sound_velocity=svp_df,
                atd_offset=atd_offset,
                acoustic_data=acoustic_df,
                imu_data=imu_df,
                gnss_data=gnss_df
            )
            print("Writing site file")
            with open(site_file, "w") as f:
                json.dump(garpos_input.model_dump_json(), f)
        else:
            print("Loading site file")
            with open(site_file,'r') as f:
                garpos_input = sfg_mod.schemas.GarposInput.model_validate_json(json.load(f))

        # garpos_input.observation.sound_speed_data = pd.concat([garpos_input.observation.sound_speed_data,pd.DataFrame([{'depth':0,'speed':1501.1129394}])]).sort_values('depth')
        # garpos_input.observation.sound_speed_data.depth *= -1
        # garpos_input.observation.sound_speed_data = pd.concat([garpos_input.observation.sound_speed_data,pd.DataFrame([{'depth':0,'speed':1501.1129394}])]).sort_values('depth')
        # garpos_input.observation.sound_speed_data = garpos_input.observation.sound_speed_data.drop_duplicates(subset='speed',keep='first')
        # garpos_input.observation.shot_data = garpos_input.observation.shot_data.drop_duplicates(subset='RT',keep='first')
        # garpos_input.observation.shot_data = garpos_input.observation.shot_data.dropna().iloc[:50000]

        # garpos_input.observation.sound_speed_data = pd.read_csv(
        #     "/Users/franklyndunbar/Project/SeaFloorGeodesy/garpos/sample/obsdata/SAGA/SAGA.1903.kaiyo_k4-svp.csv")

        garpos_fixed = sfg_mod.schemas.GarposFixed()
        garpos_fixed.inversion_params.rejectcriteria = 2
        # garpos_fixed.inversion_params.mu_t =[0.1]
        garpos_fixed.inversion_params.maxloop = 200
        garpos_input.site.delta_center_position.east_sigma = 100.0
        garpos_input.site.delta_center_position.north_sigma = 100.0
        garpos_input.site.delta_center_position.up_sigma = 100

        # for transponder in garpos_input.site.transponders:
        #     transponder.position_enu.east_sigma = 1.0
        #     transponder.position_enu.north_sigma = 1.0
        #garpos_input.site.center_enu = sfg_mod.schemas.PositionENU()
        #garpos_input.observation.shot_data.TT += 0.13
        #sfg_mod.functions.plot_enu_llh_side_by_side(garpos_input)
        #garpos_input.observation.shot_data.TT -= (garpos_input.observation.shot_data.turnaroundtime/1000)
        lat_desc = garpos_input.observation.shot_data.ant_n0.describe()
        lon_desc = garpos_input.observation.shot_data.ant_e0.describe()
        print(f"Northing: {lat_desc}\n\n Easting: {lon_desc}\n\n")
        print(garpos_input.observation.shot_data.iloc[0])

        results = sfg_mod.functions.main(
            input=garpos_input,
            fixed=garpos_fixed,
            working_dir=gp_data_dir.parent
        )
        with open(gp_data_dir.parent / f"d1_{date_key}_results.json", "w") as f:
            f.write(results.model_dump_json())
