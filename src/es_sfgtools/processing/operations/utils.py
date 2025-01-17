# External imports
import numpy as np
from numpy import datetime64
from sklearn.gaussian_process import GaussianProcessRegressor
from sklearn.gaussian_process.kernels import RBF
from sklearn.neighbors import KDTree
import itertools
import time
import matplotlib.pyplot as plt
import logging
from typing import List,Tuple
# Local imports
from es_sfgtools.processing.assets.tiledb_temp import TDBPositionArray,TDBGNSSArray,TDBAcousticArray,TDBShotDataArray
from es_sfgtools.utils.loggers import GNSSLogger as logger


def interpolate_enu(
    tenu_l: np.ndarray,
    enu_l_sig: np.ndarray,
    tenu_r: np.ndarray,
    enu_r_sig: np.ndarray,
) -> np.ndarray:
    
    """
    Interpolate the enu values between the left and right enu values

    Args:
        tenu_l (np.ndarray): The left enu time values in unix epoch
        enu_l_sig (np.ndarray): The standard deviation of the left enu values in ECEF coordinates
        tenu_r (np.ndarray): The right enu time values in unix epoch
        enu_r_sig (np.ndarray): The standard deviation of the right enu values in ECEF coordinates

    Returns:
        np.ndarray: The interpolated enu values and the standard deviation of the interpolated enu values predicted at the time values from tenu_r
    """

    logger.loginfo("Interpolating ENU values")
    length_scale = 5.0  # seconds
    kernel = RBF(length_scale=length_scale)
    X_train = np.hstack((tenu_l[:, 0], tenu_r[:, 0])).T.astype(float).reshape(-1, 1)
    Y_train = np.vstack((tenu_l[:, 1:], tenu_r[:, 1:])).astype(float)
    var_train = np.vstack((enu_l_sig, enu_r_sig)).astype(float)
    # take the inverse of the variance to get the precision

    TS_TREE = KDTree(X_train)

    block_size = 200
    # neighbors = 5
    start = time.time()
    for i in range(0, tenu_r.shape[0], block_size):
        idx = np.s_[i : i + block_size]
        ind, dist = TS_TREE.query_radius(
            tenu_r[idx, 0].astype(float).reshape(-1, 1),
            r=length_scale,
            return_distance=True,
        )
        # dist,ind = TS_TREE.query(tenu_r[idx,0].astype(float).reshape(-1,1),k=neighbors,return_distance=True)
        dist, ind = list(itertools.chain.from_iterable(dist)), list(
            itertools.chain.from_iterable(ind)
        )
        ind = np.unique(ind).astype(int)
        dist = np.array(dist)
        if any(dist != 0):
            for j in range(3):
                gp = GaussianProcessRegressor(kernel=kernel)
                gpr = gp.fit(X_train[ind], Y_train[ind, j])
                y_mean, y_std = gpr.predict(
                    tenu_r[idx, 0].reshape(-1, 1), return_std=True
                )
                enu_r_sig[idx, j] = y_std
                tenu_r[idx, j + 1] = y_mean

    logger.loginfo(f"Interpolation took {time.time()-start:.3f} seconds for {tenu_r.shape[0]} x {tenu_r.shape[1]} points")
    return tenu_r.astype(float), enu_r_sig.astype(float)

def get_merge_signature_shotdata(shotdata: TDBShotDataArray, gnss: TDBGNSSArray) -> Tuple[List[str], List[np.datetime64]]:
    """
    Get the merge signature for the shotdata and gnss data
    
    Args:
        shotdata (TDBShotDataArray): The shotdata array
        gnss (TDBGNSSArray): The gnss array
        
    Returns:
        Tuple[List[str], List[np.datetime64]]: The merge signature and the dates to merge
    """
    
    merge_signature = []
    shotdata_dates: np.ndarray = shotdata.get_unique_dates(
        "triggerTime"
    )  # get the unique dates from the shotdata
    gnss_dates: np.ndarray = gnss.get_unique_dates(
        "time"
    )  # get the unique dates from the gnss

    # get the intersection of the dates
    dates = np.intersect1d(shotdata_dates, gnss_dates).tolist()
    if len(dates) == 0:
        error_message = "No common dates found between shotdata and gnss"   
        logger.logerr(error_message)
        raise ValueError(error_message)
    
    for date in dates:
        merge_signature.append(str(date))
    
    return merge_signature, dates

def merge_shotdata_gnss(shotdata: TDBShotDataArray, gnss: TDBGNSSArray,dates:List[datetime64],plot:bool=False) -> TDBShotDataArray:
    """
    Merge the shotdata and gnss data

    Args:
        shotdata (TDBShotDataArray): The shotdata array
        gnss (TDBGNSSArray): The TileDB gnss array
        dates (List[datetime64]): The dates to merge
        plot (bool, optional): Plot the interpolated values. Defaults to False.

    Returns:
        TDBShotDataArray: The shotdata array with the interpolated values
    """

    logger.loginfo("Merging shotdata and gnss data")
    for start,end in zip(dates,dates[1:]):
        logger.loginfo(f"Interpolating shotdata for date {str(start)}")
        
        shotdata_df = shotdata.read_df(start=start,end=end)
        gnss_df = gnss.read_df(start=start, end=end)
        
        if shotdata_df.empty or gnss_df.empty:
            continue
        
        shotdata_df_distilled = shotdata_df.drop_duplicates("triggerTime")
        delta_tenur = shotdata_df_distilled[['east1','north1','up1']].to_numpy() - shotdata_df_distilled[['east0','north0','up0']].to_numpy()
        tenu_l = gnss_df[['time','east','north','up']].to_numpy()
        tenu_l[:,0] = [x.timestamp() for x in tenu_l[:,0].tolist()]
        enu_l_sig = 0.05*np.ones_like(tenu_l[:,1:])
        tenu_r = shotdata_df_distilled[['triggerTime','east0','north0','up0']].to_numpy()
        tenu_r[:,0] = [x.timestamp() for x in tenu_r[:,0].tolist()]
        enu_r_sig = shotdata_df_distilled[["east_std","north_std","up_std"]].to_numpy()
        enu_r_sig[np.isnan(enu_r_sig)] = 1.0 # set the standard deviation to 1.0 meters if it is nan
        
        logger.loginfo(f"Interpolating {tenu_r.shape[0]} points")
        pred_mu,pred_std = interpolate_enu(tenu_l,enu_l_sig,tenu_r.copy(),enu_r_sig)
        # create filter that matches the undistiled triggerTime with the first column of pred_mu
        triggerTimePred = pred_mu[:,0]
        triggerTimeDF = shotdata_df["triggerTime"].apply(lambda x: x.timestamp()).to_numpy()
        shot_df_inds = np.searchsorted(triggerTimePred,triggerTimeDF,side="right") - 1
        
        for i,key in enumerate(["east0","north0","up0"]):
            shotdata_df.iloc[shot_df_inds][key] = pred_mu[shot_df_inds,i+1]
            shotdata_df.iloc[shot_df_inds][f"{key}_std"] = pred_std[shot_df_inds,i]
            if plot and i == 0:
                plt.scatter(
                    tenu_l[:, 0],
                    tenu_l[:, i + 1],
                    marker="o",
                    c="r",
                    linewidths=0.15,
                    label=f"{key} gnss",
                )
                plt.plot(pred_mu[:,0],pred_mu[:,i+1],label=f"{key} interpolated")
                plt.scatter(tenu_r[:,0],tenu_r[:,i+1],marker="o",c="b",linewidths=0.15,label=f"{key} original")
                plt.fill_between(pred_mu[:,0],pred_mu[:,i+1]-pred_std[:,i],pred_mu[:,i+1]+pred_std[:,i],alpha=0.5)
        
        if plot:
            plt.legend()
            plt.show()

        shotdata_df.iloc[shot_df_inds][['east1','north1','up1']] = shotdata_df.iloc[shot_df_inds][['east0','north0','up0']].to_numpy() - delta_tenur[shot_df_inds]

        shotdata.write_df(shotdata_df,validate=False)
