from es_sfgtools.processing.assets.file_schemas import (
    AssetEntry,
    AssetType,
    MultiAssetEntry,
    MultiAssetPre,
)
from es_sfgtools.processing.assets.observables import (
    ShotDataFrame,
    PositionDataFrame,
    AcousticDataFrame,
    GNSSDataFrame,
)
import sqlalchemy as sa
from typing import List, Union, Callable, Dict
from pathlib import Path
import pandas as pd
from datetime import datetime
from collections import defaultdict
from sklearn.gaussian_process import GaussianProcessRegressor
from sklearn.gaussian_process.kernels import RBF, WhiteKernel
from sklearn.neighbors import KDTree
from .database import Assets, MultiAssets
from ..operations.gnss_ops import (
    dev_merge_rinex,
    rinex_get_meta,
    dev_merge_rinex_multiasset,
)
import logging

logger = logging.getLogger(__name__)


class PridePdpConfig:
    def __init__(self, config: Dict):
        self.config = config
