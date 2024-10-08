import es_sfgtools
from es_sfgtools.processing.operations.gnss_ops import dev_merge_rinex,rinex_get_meta
from es_sfgtools.processing.assets.file_schemas import AssetType, AssetEntry

from pathlib import Path

rinex_dir = Path(
    "/Users/franklyndunbar/Project/SeaFloorGeodesy/Data/TestGage/intermediate"
)
rinex_file_paths = list(rinex_dir.glob("*.2018O"))
network = "NCB"
station = "NCB1"
survey = "TestSV3"

rinex_file_assets = [
    rinex_get_meta(AssetEntry(
        local_path=p, 
        type=AssetType.RINEX,
        id=id,
        network=network,
        station=station,
        survey=survey)) for id,p in enumerate(rinex_file_paths)
]


merged_rinexs = dev_merge_rinex(rinex_file_assets[:4],working_dir=rinex_dir)
print(merged_rinexs)