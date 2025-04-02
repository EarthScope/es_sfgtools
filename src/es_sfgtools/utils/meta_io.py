from typing import Union
from pathlib import Path
import re
from datetime import datetime
import yaml
from .metadata.vessel import AtdOffset
from .metadata.site import Site, ArrayCenter
from .metadata.benchmark import Location, Benchmark, Transponder,TAT

STATION_OFFSETS = {"5209": 200, "5210": 320, "5211": 440, "5212": 560}
MASTER_STATION_ID = {"0": "5209", "1": "5210", "2": "5211", "3": "5212"}


def masterfile_to_siteconfig(
    source: Union[str, Path], site: Site = None
) -> Union[Site, None]:
    """
    Convert a MasterFile to a SiteConfig
    """

    if isinstance(source, str):
        source = Path(source)

    assert source.exists(), f"Masterfile {source} does not exist"

    lat_lon_line = re.compile(r"Latitude/Longitude array center")
    non_alphabet = re.compile("[a-c,e-z,A-Z]")
    geoid_undulation_pat = re.compile(r"Geoid undulation at sea surface point")
    site = Site()
    benchmark = Benchmark()
    geoid_undulation = None
    with open(source, "r") as f:
        lines = f.readlines()

        for idx, line in enumerate(lines):
            if idx == 0:
                # get date
                # ex. 2018-05-18 00:00:00
                date = line.strip()
                start_date = datetime.strptime(date, "%Y-%m-%d %H:%M:%S")
                site.timeOrigin = start_date
            elif not non_alphabet.search(line) and idx > 1:
                line_processed = line.strip().split()
                # 0    16       54.316094952    -158.461771055    -2096.780      0.200000d0       1479.900
                id = line_processed[0]
                lat = float(line_processed[2])
                lon = float(line_processed[3])
                height = float(line_processed[4])
                id = MASTER_STATION_ID[id]
                # TODO This is not the var
                offset = float(line_processed[5].replace("d0", ""))
                transponder_position = Location(
                    latitude=lat, longitude=lon, elevation=height
                )
                tat = TAT(value=offset)
                transponder = Transponder(tat=[tat], start=start_date, address=id)
                benchmark = Benchmark(
                    aPrioriLocation=transponder_position,
                    start=site.timeOrigin,
                    transponders=[transponder],
                )

                site.benchmarks.append(benchmark)

            elif geoid_undulation_pat.search(line):
                # "+10.300d0           ! Geoid undulation at sea surface point"
                geoid_undulation = float(
                    line.split()[0].replace("d0", "")
                )  # TODO verify sign
                site.localGeoidHeight = geoid_undulation

            elif lat_lon_line.search(line):
                # 54.3324d0 -158.4692d0   ! Latitude/Longitude array center (decimal degrees)
                line_processed = [
                    float(x.replace("d0", ""))
                    for x in line.split("!")[0].strip().split()
                ]
                lat, lon = line_processed[0], line_processed[1]
                site.arrayCenter = Location(x=lat, y=lon, z=geoid_undulation)
                break

    return site


def leverarmfile_to_atdoffset(
    source: Union[str, Path]
) -> AtdOffset:
    """
    Read the ATD offset from a "lever_arms" file
    format is [rightward,forward,downward] [m]


    0.0 +0.575 -0.844

    """

    with open(source, "r") as f:
        line = f.readlines()[0]
        values = [float(x) for x in line.split()]
        forward = values[1]
        rightward = values[0]
        downward = values[2]

    return AtdOffset(x=forward, y=rightward, z=downward)

def update_from_yaml(site:Site, source: Union[str, Path]):
    """
    Update the Site instance with data from a YAML file.

    Args:
        yaml_file (Union[str, Path]): Path to the YAML file containing site metadata.
    """
    with open(source, "r") as file:
        data = yaml.safe_load(file)

    # Update site attributes
    site.names = [data.get("site_id", site.names[0] if site.names else None)]
    site.networks = [
        data.get("campaign", site.networks[0] if site.networks else None)
    ]
    site.timeOrigin = datetime.fromisoformat(
        data.get(
            "time_origin", site.timeOrigin.isoformat() if site.timeOrigin else None
        )
    )

    # Update array center
    if "array_center" in data:
        site.arrayCenter = ArrayCenter(
            x=data["array_center"].get(
                "lat", site.arrayCenter.x if site.arrayCenter else None
            ),
            y=data["array_center"].get(
                "lon", site.arrayCenter.y if site.arrayCenter else None
            ),
            z=None,  # Assuming no z-coordinate in the YAML file
        )

    # Update transponders
    if "transponders" in data:
        site.benchmarks = []
        for transponder_data in data["transponders"]:
            transponder = Transponder(
                lat=transponder_data["lat"],
                lon=transponder_data["lon"],
                height=transponder_data["height"],
                internal_delay=transponder_data["internal_delay"],
                sv_mean=transponder_data["sv_mean"],
                pxp_id=transponder_data["pxp_id"],
            )
            benchmark = Benchmark(
                name=transponder_data["pxp_id"],
                transponders=[transponder],
            )
            site.benchmarks.append(benchmark)
