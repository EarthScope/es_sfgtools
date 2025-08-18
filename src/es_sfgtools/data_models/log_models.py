"""
Author: Franklyn Dunbar
Date: 2024-03-07
Email: franklyn.dunbar@earthscope.org
"""

import pandas as pd
from pydantic import BaseModel, Field
from typing import List, Union, Optional
from datetime import datetime, timedelta
import pymap3d as pm
import numpy as np

from ..logging import ProcessLogger as logger
from decimal import Decimal, getcontext

# Set precision for Decimal operations
getcontext().prec = 10



class SV3InterrogationData(BaseModel):
    head0: Decimal
    pitch0: Decimal
    roll0: Decimal
    east0: Decimal
    north0: Decimal
    up0: Decimal
    east_std0: Optional[Decimal] = None
    north_std0: Optional[Decimal] = None
    up_std0: Optional[Decimal] = None
    pingTime: Decimal

class SV3ReplyData(BaseModel):
    head1: Decimal
    pitch1: Decimal
    roll1: Decimal
    east1: Decimal
    north1: Decimal
    up1: Decimal
    transponderID: str
    dbv: Decimal
    snr: Decimal
    xc: Decimal
    tt: Decimal
    tat: Decimal
    returnTime: Decimal
    east_std1: Optional[Decimal] = None
    north_std1: Optional[Decimal] = None
    up_std1: Optional[Decimal] = None