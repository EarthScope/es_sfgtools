from pydantic import BaseModel, Field, field_serializer
from typing import Dict,Optional
from enum import Enum
from pathlib import Path
import json
from collections import UserDict
from pathlib import Path
import pandas as pd

from .site import Site
from .vessel import Vessel

class CatalogType(Enum):
    Data = "Data"
    MetaData ="Meta-Data"


class StationData(BaseModel):
    name: str = Field(..., description="The station's name")
    shotdata: str = Field(default=None, description="The station's shotdata TileDB URI")
    shotdata_pre: str = Field(
        default=None, description="Pre-update shotdata"
    )
    kinpositiondata: str = Field(
        default=None, description="The station's RINEX derived position TileDB URI"
    )
    gnssobsdata: str = Field(
        default=None, description="The station's raw gnss observables TileDB URI"
    )
    gnssobsdata_secondary: str = Field(
        default=None, description="The station's secondary gnss observables TileDB URI"
    )
    imupositiondata: str = Field(
        default=None, description="The station's position data TileDB URI"
    )
    acousticdata: str = Field(
        default=None, description="The station's acoustic data TileDB URI"
    )
    qcdata: str = Field(
        default=None, description="The station's QC processed shotdata TileDB URI"
    )
    qcdata_pre: str = Field(
        default=None, description="Pre-update QC shotdata TileDB URI"
    )
    qckinpositiondata: str = Field(
        default=None, description="The station's RINEX derived position TileDB URI"
    )

class NetworkData(BaseModel):
    name: str = Field(..., description="The network name")
    stations: Dict[str, StationData|Site] = Field(default={}, description="Stations in the network")


class MetaDataCatalog(BaseModel):
    name: Optional[str] = Field(default="",description="The catalog name")
    networks : Dict[str,NetworkData] = Field(default={}, description="Network catalog")
    info: Optional[str] = Field(default="", description="Optional catalog meta")
    type: CatalogType = Field(description="Catalog Type (meta-data or data)")

    @classmethod
    def load_data(cls, path: str | Path | dict, name=None, info=None) -> "MetaDataCatalog":

        if not isinstance(path, dict):
            with open(path, "r") as file:
                raw_data = json.load(file)
        else:
            raw_data = path

        return cls(
            type=CatalogType.Data,
            name=name,
            info=info,
            networks={
                network_name: NetworkData(
                    name=network_name,
                    stations={
                        station_id: StationData(
                            name=station_id,
                            shotdata=station_info.get("shotdata", ""),
                        )
                        for station_id, station_info in station_dict.items()
                    },
                )
                for network_name, station_dict in raw_data.items()
                if network_name not in ["name", "info"]
                and isinstance(station_dict, dict)
            },
        )

    @classmethod
    def load_metadata(cls, data_path:Path, name=None, info=None) -> 'MetaDataCatalog':
        # load vessels
        vessels = {}
        for vessel in (data_path/"vessels").iterdir():
            if not vessel.is_file() or not vessel.name.endswith(".json"):
                continue
            # Load the vessel JSON file
            vessels[vessel.stem] = Vessel.from_json(vessel)

        network_data = {}
        # load network data
        for network in (data_path / "networks").iterdir():
            # Skip if not a directory (ignores system files like .DS_Store)
            if not network.is_dir():
                continue
            stations = {}

            for station in network.iterdir():
                # Skip directories and non-json files
                if not station.is_file() or not station.name.endswith(".json"):
                    continue

                # Load the site JSON file
                built_site = Site.from_json(station)

                for campaign in built_site.campaigns:
                    try:
                        # Load the vessel json
                        campaign.vessel = vessels[campaign.vesselCode]
                    except KeyError:
                        print(f"Unable to load vessel metadata for {station.name} {campaign.name}")
                        campaign.vessel = None


                stations[station.stem] = built_site

            network_data[network.stem] = NetworkData(
                name=network.stem,
                stations=stations
            )
        return cls(
            type=CatalogType.MetaData,
            name=name,
            info=info,
            networks=network_data
        )

    def show(self):
        """
        Displays an abridged structured representation of the catalog data in JSON format.

        The method organizes the data into a nested dictionary structure. The output format
        depends on the catalog type:
        - For "Meta-Data", it includes networks, stations, campaigns, and surveys.
        - For "Data", it includes networks, stations, and their shotdata.

        The resulting dictionary is serialized into a JSON string and printed with indentation
        for readability.

        Raises:
        AttributeError: If the object structure does not match the expected attributes.
        Example:
            >>> catalog_dir = Path("/path/to/catalog/directory")
            >>> DATA = Catalog.load_metadata(data_path,name="sfg metadata",info="metadata for sfg")
            {
            "alaska-shumagins": {
                "IVB1": {
                "name": "2022_A_1049",
                "start": "2022-07-17T13:42:19.870000",
                "end": "2022-07-24T11:18:33.870000",
                "surveys": [
                    {
                    "survey_id": "2022_A_1049_1",
                    "start": "2022-07-17T13:42:19.870000",
                    "end": "2022-07-18T11:33:33.870000"
                    },
                    {
                    "survey_id": "2022_A_1049_2",
                    "start": "2022-07-18T13:42:19.870000",
                    "end": "2022-07-21T11:18:33.870000"
                    }
                ]
                },
            }
            >>>
            # Outputs the JSON representation of the catalog data to the console.
        """
        to_show = {}
        if self.type == CatalogType.MetaData:
            for network_name, network in self.networks.items():
                if not hasattr(to_show, network_name):
                    to_show[network_name] = {}
                for station_name, station in network.stations.items():
                    if isinstance(station, Site):
                        if not hasattr(to_show[network_name], station_name):
                            to_show[network_name][station_name] = {}
                        for campaign in station.campaigns:
                            campaign_info = {
                                "name": campaign.name,
                                "start": campaign.start.isoformat(),
                                "end": campaign.end.isoformat(),
                            }
                        surveys_info = []
                        for survey in campaign.surveys:
                            surveys_info.append(
                                {
                                    "survey_id": survey.id,
                                    "start": survey.start.isoformat(),
                                    "end": survey.end.isoformat(),
                                }
                            )
                        campaign_info["surveys"] = surveys_info
                        to_show[network_name][station_name] = campaign_info

        elif self.type == CatalogType.Data:
            
            for network_name, network in self.networks.items():
                if not hasattr(to_show, network_name):
                    to_show[network_name] = {}

                for station_name, station in network.stations.items():
                    if isinstance(station, StationData):
                        to_show[network_name][station_name] = {
                            "shotdata": station.shotdata
                        }

        print(json.dumps(to_show, indent=2))

    @field_serializer("type")
    def serialize_type(self, value: CatalogType) -> str:
        return value.value
