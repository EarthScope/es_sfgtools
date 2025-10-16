# External imports

from typing import ParamSpec
from typing import Callable, Concatenate, TypeVar, Protocol, Optional

from functools import wraps
# Local imports

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


