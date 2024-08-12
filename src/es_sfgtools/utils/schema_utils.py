"""
Author: Franklyn Dunbar
Date: 2024-05-01
Email: franklyn.dunbar@earthscope.org, franklyn.dunbar@umontana.edu
"""

import pyproj
from typing import List,Tuple
import numpy as np
import math
import sys
import logging
import pymap3d as pm

from ..schemas.generics import PositionLLH

logger = logging.getLogger(__name__)

ECEF = "epsg:4978"
LLH = "epsg:4326"

_yxz2llh = pyproj.Transformer.from_crs(ECEF, LLH)
_llh2yxz = pyproj.Transformer.from_crs(LLH, ECEF)


def xyz2llh(X,Y ,Z,**kwargs):
    lon, lat, z = _yxz2llh.transform(X,Y, Z)
    return {"lat": lat, "lon": lon, "hgt": z}


def llh2xyz(lat, lon, hgt,**kwargs):
    x,y,z = _llh2yxz.transform(lat, lon, hgt)

    return {"X": x, "Y": y, "Z": z}


def LLHDEG2DEC(lat: List[float], lon: List[float]) -> List[float]:
    """
    Convert lat and lon from degrees to decimal
        >>> lat
        [40,26,15]
    """
    # process lat
    degrees, minutes, seconds = (
        float(lat[0]),
        float(lat[1]) / 60,
        float(lat[2]) / (60**2),
    )
    lat_decimal = degrees + minutes + seconds

    # process lon
    degrees, minutes, seconds = (
        float(lon[0]),
        float(lon[1]) / 60,
        float(lon[2]) / (60**2),
    )
    lon_decimal = degrees + minutes + seconds

    return [lat_decimal, lon_decimal]


def __llh2xyz(lt, ln, hgt):
    """
    Convert lat, long, height in WGS84 to ECEF (X,Y,Z).
    lat and long given in decimal degrees.
    height should be given in meters

    Parameters
    ----------
    lt : float
            Latitude in degrees
    ln : float
            Longitude in degrees
    hgt : float
            Height in meters

    Returns
    -------
    X : float
            X (m) in ECEF
    Y : float
            Y (m) in ECEF
    Z : float
            Z (m) in ECEF
    """
    lat = lt * math.pi / 180.0
    lon = ln * math.pi / 180.0
    a = 6378137.0  # earth semimajor axis in meters
    f = 1.0 / 298.257223563  # reciprocal flattening
    e2 = 2.0 * f - f**2  # eccentricity squared

    chi = (1.0 - e2 * (math.sin(lat)) ** 2) ** 0.5
    b = a * (1.0 - e2)

    X = (a / chi + hgt) * math.cos(lat) * math.cos(lon)
    Y = (a / chi + hgt) * math.cos(lat) * math.sin(lon)
    Z = (b / chi + hgt) * math.sin(lat)

    return X, Y, Z


def xyz2enu(x, y, z, lat0, lon0, hgt0, inv=1, **kwargs):
    """
    Rotates the vector of positions XYZ and covariance to
    the local east-north-up system at latitude and longitude
    (or XYZ coordinates) specified in origin.
    if inv = -1. then enu -> xyz

    Parameters
    ----------
    x : float
    y : float
    z : float
            Position in ECEF (if inv=-1, in ENU)
    lat0 : float
    lon0 : float
    Hgt0 : float
            Origin for the local system in degrees.
    inv : 1 or -1
            Switch (1: XYZ -> ENU, -1: ENU -> XYZ)

    Returns
    -------
    e : float
    n : float
    u : float
            Position in ENU (if inv=-1, in ECEF)
    """

    if inv != 1 and inv != -1:
        print("error in xyz2enu : ", inv)
        sys.exit(1)

    lat = lat0 * math.pi / 180.0 * inv
    lon = lon0 * math.pi / 180.0 * inv

    sphi = math.sin(lat)
    cphi = math.cos(lat)
    slmb = math.sin(lon)
    clmb = math.cos(lon)

    T1 = [-slmb, clmb, 0]
    T2 = [-sphi * clmb, -sphi * slmb, cphi]
    T3 = [cphi * clmb, cphi * slmb, sphi]

    e = x * T1[0] + y * T1[1] + z * T1[2]
    n = x * T2[0] + y * T2[1] + z * T2[2]
    u = x * T3[0] + y * T3[1] + z * T3[2]

    return e, n, u


# class CoordTransformer:
#     def __init__(self, pos_llh):
#         # Define the origin of the local coordinate system
#         self.lat0 = pos_llh.latitude
#         self.lon0 = pos_llh.longitude
#         self.hgt0 = pos_llh.height
#         self.origin_ecef = llh2xyz(**{"lat": self.lat0, "lon": self.lon0, "hgt": self.hgt0})

#     def XYZ2ENU(self, X,Y,Z,**kwargs) -> Tuple[float, float, float]:
#         # X -= self.origin_ecef["X"]  # "X"
#         # Y -= self.origin_ecef["Y"]  # "Y"
#         # Z -= self.origin_ecef["Z"]  # "Z"

#         e,n,u = xyz2enu(**{"x": X, "y": Y, "z": Z, "lat0": self.lat0, "lon0": self.lon0, "hgt0": self.hgt0})

#         return e, n, u

#     def LLH2ENU(self, lat, lon, hgt) -> Tuple[float,float,float]:
#         xyz:dict = llh2xyz(**{"lat": lat, "lon": lon, "hgt": hgt})

#         return self.XYZ2ENU(**xyz)

#     @classmethod
#     def from_llh(cls,lat,lon,hgt):
#         return cls(PositionLLH(latitude=lat,longitude=lon,height=hgt))


class CoordTransformer:
    def __init__(self,pos_llh):
        if isinstance(pos_llh,list):
            self.lat0 = pos_llh[0]
            self.lon0 = pos_llh[1]
            self.hgt0 = pos_llh[2]
        else:
            self.lat0 = pos_llh.latitude
            self.lon0 = pos_llh.longitude
            self.hgt0 = pos_llh.height

        self.X0,self.Y0,self.Z0 = pm.geodetic2ecef(self.lat0,self.lon0,self.hgt0)
    def XYZ2ENU(self,X,Y,Z,**kwargs):
        dX,dY,dZ = X-self.X0,Y-self.Y0,Z-self.Z0
        e, n, u = xyz2enu(
            **{
                "x": dX,
                "y": dY,
                "z": dZ,
                "lat0": self.lat0,
                "lon0": self.lon0,
                "hgt0": self.hgt0,
            }
        )

        return e, n, u
    def LLH2ENU(self,lat,lon,hgt,**kwargs):
        X,Y,Z = pm.geodetic2ecef(lat,lon,hgt)
        dX,dY,dZ = X-self.X0,Y-self.Y0,Z-self.Z0
        e, n, u = xyz2enu(
            **{
                "x": dX,
                "y": dY,
                "z": dZ,
                "lat0": self.lat0,
                "lon0": self.lon0,
                "hgt0": self.hgt0,
            }
        )

        return e, n, u
