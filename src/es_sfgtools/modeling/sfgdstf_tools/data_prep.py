"""
This module contains functions for preparing SFGDSTF data for modeling.
"""

import pandas as pd
import pymap3d as pm
import julian
from es_sfgtools.modeling.garpos_tools.schemas import GarposInput, ObservationData, GPPositionLLH, GPPositionENU, GPTransponder, GPATDOffset
from es_sfgtools.data_models.community_standards import SFGDSTFSeafloorAcousticData
from es_sfgtools.data_models.metadata.site import Site
from es_sfgtools.data_models.observables import ShotDataFrame

def sfgdstf_to_garpos(
    sfgdstf_df: pd.DataFrame,
    site_metadata: Site,
    survey_id: str,
    shot_data_path: "Path",
    sound_speed_path: "Path",
) -> GarposInput:
    """
    Converts a SFGDSTFSeafloorAcousticData dataframe to a GarposInput object.

    Args:
        sfgdstf_df (pd.DataFrame): The SFGDSTFSeafloorAcousticData dataframe.
        site_metadata (Site): The site metadata.
        survey_id (str): The survey ID.
        shot_data_path (Path): The path to write the shot data csv to.
        sound_speed_path (Path): The path to the sound speed data.

    Returns:
        GarposInput: The GarposInput object.
    """

    def datetime_to_mjd_seconds(dt: pd.Series) -> pd.Series:
        return dt.apply(lambda x: julian.to_jd(x, fmt='jd') * 86400.0)

    # Get the site center from the metadata
    site_center_llh = GPPositionLLH(
        latitude=site_metadata.reference_frame.latitude,
        longitude=site_metadata.reference_frame.longitude,
        height=site_metadata.reference_frame.height,
    )

    # Convert ECEF to ENU
    e0, n0, u0 = pm.ecef2enu(
        sfgdstf_df["X_transmit"].to_numpy(),
        sfgdstf_df["Y_transmit"].to_numpy(),
        sfgdstf_df["Z_transmit"].to_numpy(),
        site_center_llh.latitude,
        site_center_llh.longitude,
        site_center_llh.height,
    )
    e1, n1, u1 = pm.ecef2enu(
        sfgdstf_df["X_receive"].to_numpy(),
        sfgdstf_df["Y_receive"].to_numpy(),
        sfgdstf_df["Z_receive"].to_numpy(),
        site_center_llh.latitude,
        site_center_llh.longitude,
        site_center_llh.height,
    )

    # Create the ObservationData dataframe
    obs_data_df = pd.DataFrame()
    obs_data_df["ST"] = datetime_to_mjd_seconds(sfgdstf_df["T_transmit"])
    obs_data_df["RT"] = datetime_to_mjd_seconds(sfgdstf_df["T_receive"])
    obs_data_df["TT"] = sfgdstf_df["TravelTime"]
    obs_data_df["ant_e0"] = e0
    obs_data_df["ant_n0"] = n0
    obs_data_df["ant_u0"] = u0
    obs_data_df["ant_e1"] = e1
    obs_data_df["ant_n1"] = n1
    obs_data_df["ant_u1"] = u1
    obs_data_df["head0"] = sfgdstf_df["heading0"]
    obs_data_df["pitch0"] = sfgdstf_df["pitch0"]
    obs_data_df["roll0"] = sfgdstf_df["roll0"]
    obs_data_df["head1"] = sfgdstf_df["heading0"] # No heading1 in SFGDSTF, using heading0
    obs_data_df["pitch1"] = sfgdstf_df["pitch0"] # No pitch1 in SFGDSTF, using pitch0
    obs_data_df["roll1"] = sfgdstf_df["roll0"] # No roll1 in SFGDSTF, using roll0
    obs_data_df["MT"] = sfgdstf_df["transponderID"]
    obs_data_df["flag"] = sfgdstf_df["quality_flag"].apply(lambda x: x != "A")
    obs_data_df["SET"] = "S01"
    obs_data_df["LN"] = "L01"
    obs_data_df["gamma"] = 0.0
    obs_data_df["ResiTT"] = 0.0
    obs_data_df["TakeOff"] = 0.0
    
    # Validate the dataframe
    validated_df = ObservationData.validate(obs_data_df)

    # Save the dataframe to a CSV file
    validated_df.to_csv(shot_data_path, index=False)

    # Create the GarposInput object
    transponders = []
    for tp in site_metadata.benchmarks:
        transponders.append(
            GPTransponder(
                id=tp.id,
                position_enu=GPPositionENU(
                    east=tp.east,
                    north=tp.north,
                    up=tp.up,
                ),
            )
        )
    
    campaign = site_metadata.get_campaign(site_metadata.campaigns[0].id)

    array_center_enu = GPPositionENU(
        east=campaign.surveys[0].array_center.east,
        north=campaign.surveys[0].array_center.north,
        up=campaign.surveys[0].array_center.up,
    )

    atd_offset = GPATDOffset(
        forward=site_metadata.vessel.acoustic_transducer.atd_offset.forward,
        rightward=site_metadata.vessel.acoustic_transducer.atd_offset.rightward,
        downward=site_metadata.vessel.acoustic_transducer.atd_offset.downward,
    )

    garpos_input = GarposInput(
        site_name=site_metadata.name,
        campaign_id=campaign.id,
        survey_id=survey_id,
        site_center_llh=site_center_llh,
        array_center_enu=array_center_enu,
        transponders=transponders,
        sound_speed_data=sound_speed_path,
        atd_offset=atd_offset,
        start_date=sfgdstf_df["T_transmit"].min(),
        end_date=sfgdstf_df["T_transmit"].max(),
        shot_data=shot_data_path,
        n_shot=len(sfgdstf_df),
    )

    return garpos_input

def sfgdstf_to_shotdata(sfgdstf_df: pd.DataFrame) -> pd.DataFrame:
    """
    Converts a SFGDSTFSeafloorAcousticData dataframe to a ShotDataFrame dataframe.
    """
    shotdata_df = pd.DataFrame()
    shotdata_df["transponderID"] = sfgdstf_df["transponderID"]
    shotdata_df["pingTime"] = sfgdstf_df["T_transmit"].apply(lambda x: x.timestamp())
    shotdata_df["returnTime"] = sfgdstf_df["T_receive"].apply(lambda x: x.timestamp())
    shotdata_df["tt"] = sfgdstf_df["TravelTime"]
    shotdata_df["dbv"] = sfgdstf_df["dbV"]
    shotdata_df["snr"] = sfgdstf_df["aSNR"]
    shotdata_df["xc"] = sfgdstf_df["acc"]
    shotdata_df["head0"] = sfgdstf_df["heading0"]
    shotdata_df["pitch0"] = sfgdstf_df["pitch0"]
    shotdata_df["roll0"] = sfgdstf_df["roll0"]
    shotdata_df["east0"] = sfgdstf_df["X_transmit"]
    shotdata_df["north0"] = sfgdstf_df["Y_transmit"]
    shotdata_df["up0"] = sfgdstf_df["Z_transmit"]
    shotdata_df["head1"] = sfgdstf_df["heading0"] # No heading1 in SFGDSTF, using heading0
    shotdata_df["pitch1"] = sfgdstf_df["pitch0"] # No pitch1 in SFGDSTF, using pitch0
    shotdata_df["roll1"] = sfgdstf_df["roll0"] # No roll1 in SFGDSTF, using roll0
    shotdata_df["east1"] = sfgdstf_df["X_receive"]
    shotdata_df["north1"] = sfgdstf_df["Y_receive"]
    shotdata_df["up1"] = sfgdstf_df["Z_receive"]
    shotdata_df["east_std0"] = sfgdstf_df["trans_sigX0"]
    shotdata_df["north_std0"] = sfgdstf_df["trans_sigY0"]
    shotdata_df["up_std0"] = sfgdstf_df["trans_sigZ0"]
    shotdata_df["east_std1"] = sfgdstf_df["trans_sigX1"]
    shotdata_df["north_std1"] = sfgdstf_df["trans_sigY1"]
    shotdata_df["up_std1"] = sfgdstf_df["trans_sigZ1"]
    shotdata_df["tat"] = 0.0

    return ShotDataFrame.validate(shotdata_df)
