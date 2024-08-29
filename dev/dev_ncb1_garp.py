import sys
from pathlib import Path
import json
import es_sfgtools
import es_sfgtools.processing as sfg_proc
import es_sfgtools.modeling.garpos_tools as sfg_mod
import pandas as pd

data_dir = Path("/Users/franklyndunbar/Project/SeaFloorGeodesy/Data/TestSV3/")
gp_data_dir = data_dir/"Garpos" / "prep_NCB1.json"

cdt_file_path = data_dir/"fromjohn" / "ctd" / "CTD_NCB1_Ch_Mi"
master_file_path = data_dir/"fromjohn" / "NCB1.master"

cdt_shema = sfg_proc.schemas.files.CTDFile(location=cdt_file_path)
master_schema = sfg_proc.schemas.files.MasterFile(location=master_file_path)


with open(gp_data_dir) as f:
    gp_data = json.load(f)

dayone = list(gp_data.keys())[0]
gp_data_dayone = gp_data[dayone]

site_file = gp_data_dir.parent / f"{dayone}_site.json"
if not site_file.exists():
    site_config = sfg_proc.site_functions.masterfile_to_siteconfig(master_schema)
    svp_df: pd.DataFrame = sfg_proc.functions.seabird_functions.ctd_to_soundvelocity(
        cdt_shema
    )
    atd_offset = sfg_proc.schemas.ATDOffset(forward=0.0053, rightward=0, downward=0.92813)
    acoustic_df = pd.read_csv(gp_data_dayone["acoustic"])
    imu_df = pd.read_csv(gp_data_dayone["imu"])
    gnss_df = pd.read_csv(gp_data_dayone["gnss"])

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
    with open(site_file) as f:
        garpos_input = sfg_mod.schemas.GarposInput.model_validate_json(json.load(f))

garpos_fixed = sfg_mod.schemas.GarposFixed()
garpos_fixed.inversion_params.rejectcriteria = 1.75
garpos_fixed.inversion_params.maxloop = 1000


if __name__ == "__main__":
    results = sfg_mod.functions.main(
        input=garpos_input,
        fixed=garpos_fixed,
        working_dir=gp_data_dir.parent
    )

    print(results)
    