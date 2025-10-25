from pathlib import Path

from es_sfgtools.data_models.community_standards import SFGDTSFSite
from es_sfgtools.data_models.metadata import Site, Vessel
from es_sfgtools.sonardyne_tools.sv3_operations import (
    dfop00_to_SFGDSTFSeafloorAcousticData,
)
from es_sfgtools.data_mgmt.ingestion.archive_pull import load_site_metadata, load_vessel_metadata

test_dfo = Path(
    "/Users/franklyndunbar/Project/SeaFloorGeodesy/Data/SFGMain/cascadia-gorda/NCC1/2022_A_1065/raw/329653_002_20220505_022443_00086_DFOP00.raw"
)
site_metadata:Site = load_site_metadata(
    network="cascadia-gorda",
    station="NCC1",
)
vessel_metadata:Vessel = load_vessel_metadata(
    site_metadata.campaigns[0].vesselCode
)



community_site = SFGDTSFSite.from_site_vessel(site_metadata, vessel_metadata)

community_df = dfop00_to_SFGDSTFSeafloorAcousticData(test_dfo, community_site)

print(community_df.head())