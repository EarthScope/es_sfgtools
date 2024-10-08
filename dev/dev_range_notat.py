from pathlib import Path
import es_sfgtools
from es_sfgtools.processing.schemas import DFPO00RawFile,PositionDataFrame,IMUDataFrame,AcousticDataFrame,MasterFile,CTDFile,ATDOffset
from es_sfgtools.processing.functions import gnss_functions,imu_functions,acoustic_functions,site_functions,seabird_functions,dev_dfop00_to_shotdata
from es_sfgtools.modeling.garpos_tools import garpos_input_from_site_obs,main,GarposFixed,dev_garpos_input_from_site_obs,InversionParams
from es_sfgtools.modeling.garpos_tools.functions import plot_enu_llh_side_by_side as plot_enu
from es_sfgtools.modeling.garpos_tools.functions import dev_main
import pandas as pd
import matplotlib.pyplot as plt
data_dir = Path("/Users/franklyndunbar/Project/SeaFloorGeodesy/Data/TestSV3/")
gp_data_dir = data_dir / "Garpos"

catalog_dir = Path(
    "/Users/franklyndunbar/Project/SeaFloorGeodesy/Data/TestSV3/Garpos/prep_NCB1_catalog.json"
)
cdt_file_path = data_dir / "fromjohn" / "ctd" / "CTD_NCB1_Ch_Mi"
master_file_path = data_dir / "fromjohn" / "NCB1.master"

cdt_shema = CTDFile(local_path=cdt_file_path)
master_schema = MasterFile(local_path=master_file_path)
svp_df = seabird_functions.ctd_to_soundvelocity(cdt_shema)
svp_df_path = data_dir/"processed"/"sound_velocity.csv"
svp_df.to_csv(svp_df_path,index=False)
dfo_path = Path(
    "/Users/franklyndunbar/Project/SeaFloorGeodesy/Data/NCB1/HR/329653_002_20210906_141932_00051_DFOP00.raw"
)

gnss_path = Path(
    "/Users/franklyndunbar/Project/SeaFloorGeodesy/Data/TestSV3/NCB/NCB1/TestSV3/processed/140_gnss.csv"
)

if __name__ == "__main__":
    dfo_obj = DFPO00RawFile(local_path=dfo_path)

    site_config = site_functions.build_site(
        config_source=master_schema,
        svp_source=svp_df_path,
        name="NCB1",
        campaign="TestSV3",
        date=pd.Timestamp("2021-09-06"),
        atd_offset={"forward": 0.0053, "rightward": 0, "downward": 0.92813},
    )

    # gnss_df = pd.read_csv(gnss_path)
    # gnss_df.time = pd.to_datetime(gnss_df.time)
    # imu_df = imu_functions.dfpo00_to_imudf(source=dfo_obj)
    # acoustic_df = acoustic_functions.dev_dfpo00_to_acousticdf(source=dfo_obj)
    shot_data = dev_dfop00_to_shotdata(source=dfo_obj)
    first_day = shot_data.triggerTime.min().date()
    shot_data = shot_data[shot_data.triggerTime.dt.date == first_day]

    shot_data_path = data_dir / "processed" / "testshot_data.csv"

    shot_data.to_csv(shot_data_path, index=False)

    inversion_params = InversionParams()
    inversion_params.rejectcriteria = 2
    inversion_params.traveltimescale = 1e-3

    results = dev_main(
        site_config=site_config,
        hyper_params=inversion_params,
        shot_data=shot_data_path,
    )
    # # garpos_input = dev_garpos_input_from_site_obs(
    # #     site_config=site_config,
    # #     sound_velocity=svp_df,
    # #     atd_offset=site_config.atd_offset,
    # #     shot_data=shot_data,
    # # )

    # # Filter input observation to be only one day
    # first_day = garpos_input.observation.shot_data.triggerTime.min().date()
    # garpos_input.observation.shot_data = garpos_input.observation.shot_data[
    #     garpos_input.observation.shot_data.triggerTime.dt.date == first_day
    # ]
    # garpos_input.observation.shot_data = garpos_input.observation.shot_data[
    #     garpos_input.observation.shot_data.TT > 0
    # ]
    # # min_time = max(max(acoustic_df.TriggerTime.min(),gnss_df.time.min()),imu_df.Time.min()) 
    # # max_time = min_time + pd.Timedelta(seconds=10000)
    # # acoustic_df = acoustic_df[(acoustic_df.TriggerTime >= min_time+pd.Timedelta(seconds=1000)) & (acoustic_df.TriggerTime <= max_time)]
    # # gnss_df = gnss_df[(gnss_df.time >= min_time) & (gnss_df.time <= max_time)]
    # # imu_df = imu_df[(imu_df.Time >= min_time) & (imu_df.Time <= max_time)]

    # # garpos_input_test = garpos_input_from_site_obs(
    # #     site_config=site_config,
    # #     sound_velocity=svp_df,
    # #     atd_offset=atd_offset,
    # #     acoustic_data=acoustic_df,
    # #     imu_data=imu_df,
    # #     gnss_data=gnss_df
    # # )
    # # #plot_enu(garpos_input)
    # garpos_fixed = GarposFixed()
    # garpos_fixed.inversion_params.rejectcriteria = 2
    # garpos_fixed.inversion_params.traveltimescale = 1e-3
    # # garpos_fixed.inversion_params.mu_t =[0.1]
    # garpos_fixed.inversion_params.maxloop = 100
    # garpos_input.site.delta_center_position.east_sigma = 1
    # garpos_input.site.delta_center_position.north_sigma = 1
    # garpos_input.site.delta_center_position.up_sigma = 0


    # results = main(
    #     input=garpos_input,
    #     fixed=garpos_fixed,
    #     working_dir=gp_data_dir.parent
    # )
    keep = ~results.shot_data.flag
    x = results.shot_data[keep].ST.values
    y = results.shot_data[keep].ResiRange.values
    colors = ['c','b','g']
    for mt in results.shot_data[keep].MT.unique():
        x =results.shot_data[keep][results.shot_data[keep].MT == mt].ST.values
        y = results.shot_data[keep][results.shot_data[keep].MT == mt].ResiRange.values
        plt.scatter(x,y,s=2,c=colors.pop())
    # plt.scatter(x, y,s=0.6)
    plt.show()
    plt.savefig(gp_data_dir / "residuals.png")
    print(results)
