from typing import List, Tuple, ParamSpec, Callable, Concatenate, TypeVar, Protocol, Optional
import numpy as np
from functools import wraps
from es_sfgtools.logging import ProcessLogger as logger
from es_sfgtools.tiledb_tools.tiledb_schemas import TDBShotDataArray, TDBKinPositionArray

P = ParamSpec("P")
R = TypeVar("R")


class HasNetworkStationCampaign(Protocol):
    current_network: Optional[str]
    current_station: Optional[str]
    current_campaign: Optional[str]


def check_network_station_campaign(
    func: Callable[Concatenate[HasNetworkStationCampaign, P], R],
) -> Callable[Concatenate[HasNetworkStationCampaign, P], R]:
    @wraps(func)
    def wrapper(
        self: HasNetworkStationCampaign, *args: P.args, **kwargs: P.kwargs
    ) -> R:
        if self.current_network is None:
            raise ValueError("Network name not set, use change_working_station")
        if self.current_station is None:
            raise ValueError("Station name not set, use change_working_station")
        if self.current_campaign is None:
            raise ValueError("campaign name not set, use change_working_station")
        return func(self, *args, **kwargs)

    return wrapper


def get_merge_signature_shotdata(
    shotdata: TDBShotDataArray, kin_position: TDBKinPositionArray
) -> Tuple[List[str], List[np.datetime64]]:
    """
    Get the merge signature for the shotdata and kin_position data

    Args:
        shotdata (TDBShotDataArray): The shotdata array
        kin_position (TDBKinPositionArray): The kinposition array

    Returns:
        Tuple[List[str], List[np.datetime64]]: The merge signature and the dates to merge
    """

    merge_signature = []
    shotdata_dates: np.ndarray = shotdata.get_unique_dates(
        "pingTime"
    )  # get the unique dates from the shotdata
    kin_position_dates: np.ndarray = kin_position.get_unique_dates(
        "time"
    )  # get the unique dates from the kin_position

    # get the intersection of the dates
    dates = np.intersect1d(shotdata_dates, kin_position_dates).tolist()
    if len(dates) == 0:
        error_message = "No common dates found between shotdata and kin_position"
        logger.logerr(error_message)
        raise ValueError(error_message)

    for date in dates:
        merge_signature.append(str(date))

    return merge_signature, dates