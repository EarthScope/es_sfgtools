from __future__ import annotations
import json
from pathlib import Path
from typing import Dict, List, Optional

from pydantic import BaseModel, Field

from .site import Site
from .vessel import Vessel


class StationMetadata(BaseModel):
    """
    A container for all metadata related to a single station, including the site
    information and all associated vessel configurations.
    """

    site: Site = Field(..., description="The site metadata for the station.")
    vessels: Dict[str, Vessel] = Field(
        default_factory=dict,
        description="A dictionary mapping vessel names to their metadata.",
    )


class MetadataCatalog(BaseModel):
    """
    A catalog for storing and managing metadata for multiple seafloor geodesy
    networks and stations.
    """

    networks: Dict[str, Dict[str, StationMetadata]] = Field(
        default_factory=dict,
        description="A nested dictionary mapping network names to station names to their metadata.",
    )

    def add_station(
        self, network_id: str, station_id: str, station_metadata: StationMetadata
    ):
        """
        Adds or updates the metadata for a specific station within a network.

        If the network does not exist, it will be created.
        """
        if network_id not in self.networks:
            self.networks[network_id] = {}
        self.networks[network_id][station_id] = station_metadata

    def get_station(
        self, network_id: str, station_id: str
    ) -> Optional[StationMetadata]:
        """
        Retrieves the metadata for a specific station.
        """
        return self.networks.get(network_id, {}).get(station_id)

    def save(self, path: Path):
        """
        Saves the catalog to a JSON file.
        """
        with open(path, "w") as f:
            json.dump(self.model_dump(), f, indent=4)

    @classmethod
    def load(cls, path: Path) -> "MetadataCatalog":
        """
        Loads a catalog from a JSON file.
        """
        if not path.exists():
            return cls()
        with open(path, "r") as f:
            return cls(**json.load(f))
