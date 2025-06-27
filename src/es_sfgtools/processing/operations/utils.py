# External imports
import numpy as np
from numpy import datetime64
from sklearn.gaussian_process import GaussianProcessRegressor
from sklearn.gaussian_process.kernels import RBF
from sklearn.neighbors import KDTree
from sklearn.kernel_ridge import KernelRidge
from sklearn.preprocessing import StandardScaler
import itertools
import time
import matplotlib.pyplot as plt
import logging
from typing import List,Tuple
# Local imports
from es_sfgtools.processing.assets.tiledb import TDBPositionArray,TDBGNSSArray,TDBAcousticArray,TDBShotDataArray
from es_sfgtools.utils.loggers import GNSSLogger as logger
from sklearn.preprocessing import StandardScaler,MaxAbsScaler


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
    length_scale = 3  # seconds
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

def interpolate_enu_kernalridge(
    gnss_data: np.ndarray,
    shot_data: np.ndarray,
    lengthscale: float = 0.5
    
) -> np.ndarray:
    """
    Interpolate the enu values between the left and right enu values using Kernel Ridge Regression.

    Args:
        tenu_l (np.ndarray): The left enu time values in unix epoch
        tenu_r (np.ndarray): The right enu time values in unix epoch
        lengthscale (float, optional): The length scale for the kernel. Defaults to 2.0 seconds.
    Returns:
        np.ndarray: The interpolated enu values at the time values from tenu_r
    """

    logger.loginfo("Interpolating ENU values using Kernel Ridge Regression")
    # First, we need to find the indices of tenu_l that are within the lengthscale of tenu_r
    # We will use a KDTree to find the indices efficiently
    GNSS_DATA_TREE = KDTree(gnss_data[:, 0].astype(float).reshape(-1, 1))

    shotdata_near_gnss_count = GNSS_DATA_TREE.query_radius(
        shot_data[:, 0].astype(float).reshape(-1, 1),
        r=lengthscale,  # seconds
        count_only=True,
    )
    shotdata_to_update_filter = shotdata_near_gnss_count > 0
    shotdata_to_update = shot_data[shotdata_to_update_filter,:]

    if shotdata_to_update.shape[0] == 0:
        logger.loginfo("No points to update, returning original shot_data")
        return shot_data

    gnss_training_data_inds = GNSS_DATA_TREE.query_radius(
        shotdata_to_update[:, 0].astype(float).reshape(-1, 1),
        r=lengthscale,  # seconds
        return_distance=False,
        count_only=False,
    )
    gnss_training_data_inds = np.unique(list(itertools.chain.from_iterable(gnss_training_data_inds))).astype(int)

    if len(gnss_training_data_inds) == 0:
        logger.loginfo("No points to update, returning original tenu_r")
        return shot_data

    scaler = StandardScaler(with_std=False)  # we do not want to scale the standard deviation
    scaler.fit(gnss_data[gnss_training_data_inds,:])

    XY_train = scaler.transform(gnss_data[gnss_training_data_inds, :])  # East, North, Up values
    XY_predict = scaler.transform(shotdata_to_update[:, 0:-1])  # East, North, Up values

    X_train = XY_train[:, 0][:, np.newaxis]  # timestamps
    Y_train = XY_train[:, 1:]  # East, North, Up values
    X_predict = XY_predict[:, 0][:, np.newaxis]  # timestamps for the points to update
    # create the kernel ridge regression model

    kernel = RBF(length_scale=lengthscale)
    kernal_ridge = KernelRidge(alpha=1,kernel=kernel)
    kernal_ridge = kernal_ridge.fit(X_train, Y_train)
    err = kernal_ridge.score(
        XY_predict[:,0][:,np.newaxis],
        XY_predict[:, 1:],
    )

    train_score = kernal_ridge.score(X_train, Y_train)

    logger.loginfo(f"Kernel Ridge Regression model score: {err:.3f}")
    logger.loginfo(f"Kernel Ridge Regression model training score: {train_score:.3f}")
    # predict the values at the tenu_r timestamps
    updated_shotdata_scaled = kernal_ridge.predict(X_predict)
    # create a merged array with each column being the East, North, Up values
    # and the first column being the timestamps


    updated_shotdata_scaled_merged = np.vstack(
        (
            (
                X_predict.T,
                updated_shotdata_scaled[:, 0][:, np.newaxis].T,
                updated_shotdata_scaled[:, 1][:, np.newaxis].T,
                updated_shotdata_scaled[:, 2][:, np.newaxis].T,
            )
        )
    ).T
    updated_shotdata = scaler.inverse_transform(updated_shotdata_scaled_merged)
    # compute the offset between the predicted values and the original values
    offset = np.abs(updated_shotdata - shotdata_to_update[:,:-1])
    if offset.max() > 1:
        logger.logwarn(f"Offset between predicted and original values is too high: {offset.max():.2f}. This may indicate a problem with the data or the model.")
    east_offset_max = offset[:, 0].max()
    north_offset_max = offset[:, 1].max()
    up_offset_max = offset[:, 2].max()
    logger.loginfo(f"Max offset for East: {east_offset_max}, North: {north_offset_max}, Up: {up_offset_max}")

    # update the tenu_r values with the predicted values
    shot_data[shotdata_to_update_filter, 1:-1] = updated_shotdata
    # set the isUpdated flag to True for the updated points
    shot_data[shotdata_to_update_filter, -1] = True
    # return the updated tenu_r values
    # if to_not_update.shape[0] > 0:
    #     tenu_r = np.vstack((tenu_r[to_update_filter], tenu_r[~to_update_filter]))
    # else:
    #     tenu_r = tenu_r[to_update_filter]
    logger.loginfo(f"Interpolated {updated_shotdata.shape[0]} points using Kernel Ridge Regression")
    logger.loginfo(f"Returning {shot_data.shape[0]} points")

    # # sort the tenu_r array by the first column (time)
    # tenu_r = tenu_r[np.argsort(tenu_r[:, 0])].astype(float)
    return shot_data

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
        "pingTime"
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

def merge_shotdata_gnss(
        shotdata_pre: TDBShotDataArray,
        shotdata: TDBShotDataArray, 
        gnss: TDBGNSSArray,
        dates:List[datetime64],
        lengthscale:float=0.5,
        plot:bool=False) -> TDBShotDataArray:

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

        shotdata_df = shotdata_pre.read_df(start=start,end=end)
        gnss_df = gnss.read_df(start=start, end=end)

        if shotdata_df.empty or gnss_df.empty:
            continue
        shotdata_df.returnTime = shotdata_df.returnTime.apply(lambda x:x.timestamp())
        shotdata_df.pingTime = shotdata_df.pingTime.apply(lambda x:x.timestamp())
        gnss_df.time = gnss_df.time.apply(lambda x:x.timestamp())
        # shotdata_df_distilled = shotdata_df.drop_duplicates("pingTime")
        delta_tenur = shotdata_df[['east1','north1','up1']].to_numpy() - shotdata_df[['east0','north0','up0']].to_numpy()
        gnss_data_interpolation = gnss_df[['time','east','north','up']].to_numpy()
 
        # enu_l_sig = 0.05*np.ones_like(tenu_l[:,1:])
        shotdata_interpolation = shotdata_df[['pingTime','east0','north0','up0','isUpdated']].to_numpy()
 
        # enu_r_sig = shotdata_df[["east_std","north_std","up_std"]].to_numpy()
        # enu_r_sig[np.isnan(enu_r_sig)] = 1.0 # set the standard deviation to 1.0 meters if it is nan

        logger.loginfo(f"Interpolating {shotdata_interpolation.shape[0]} points")
        # pred_mu,pred_std = interpolate_enu(tenu_l,enu_l_sig,tenu_r.copy(),enu_r_sig)
        tenu_r_updated = interpolate_enu_kernalridge(gnss_data=gnss_data_interpolation, shot_data=shotdata_interpolation.copy(), lengthscale=lengthscale)
        # create filter that matches the undistiled triggerTime with the first column of pred_mu

        shotdata_df[["pingTime","east0","north0","up0","isUpdated"]] = tenu_r_updated
        shotdata_df[["east1", "north1", "up1"]] = shotdata_df[["east0", "north0", "up0"]].to_numpy() + delta_tenur
        # for i,key in enumerate(["east0","north0","up0"]):
        #     shotdata_df.iloc[shot_df_inds][key] = pred_mu[shot_df_inds,i+1]
        #     #shotdata_df.iloc[shot_df_inds][f"{key}_std"] = pred_std[shot_df_inds,i]
        #     if plot and i == 0:
        #         plt.scatter(
        #             tenu_l[:, 0],
        #             tenu_l[:, i + 1],
        #             marker="o",
        #             c="r",
        #             linewidths=0.15,
        #             label=f"{key} gnss",
        #         )
        #         plt.plot(pred_mu[:,0],pred_mu[:,i+1],label=f"{key} interpolated")
        #         plt.scatter(tenu_r[:,0],tenu_r[:,i+1],marker="o",c="b",linewidths=0.15,label=f"{key} original")
        #         #plt.fill_between(pred_mu[:,0],pred_mu[:,i+1]-pred_std[:,i],pred_mu[:,i+1]+pred_std[:,i],alpha=0.5)

        # if plot:
        #     plt.legend()
        #     plt.show()

        # shotdata_df.iloc[shot_df_inds][['east1','north1','up1']] = shotdata_df.iloc[shot_df_inds][['east0','north0','up0']].to_numpy() - delta_tenur[shot_df_inds]

        shotdata.write_df(shotdata_df,validate=False)
