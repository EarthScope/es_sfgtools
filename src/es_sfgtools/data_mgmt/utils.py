# External imports
import itertools
import time
from typing import List, Tuple

import numpy as np
import pandas as pd
from numpy import datetime64
from sklearn.gaussian_process import GaussianProcessRegressor
from sklearn.gaussian_process.kernels import RBF
from sklearn.kernel_ridge import KernelRidge
from sklearn.neighbors import KDTree, RadiusNeighborsRegressor
from sklearn.preprocessing import StandardScaler

from ..logging import ProcessLogger as logger

# Local imports
from ..tiledb_tools.tiledb_schemas import TDBKinPositionArray, TDBShotDataArray


def interpolate_enu(
    tenu_l: np.ndarray,
    enu_l_sig: np.ndarray,
    tenu_r: np.ndarray,
    enu_r_sig: np.ndarray,
) -> np.ndarray:
    """Interpolate the enu values between the left and right enu values.

    Parameters
    ----------
    tenu_l : np.ndarray
        The left enu time values in unix epoch.
    enu_l_sig : np.ndarray
        The standard deviation of the left enu values in ECEF coordinates.
    tenu_r : np.ndarray
        The right enu time values in unix epoch.
    enu_r_sig : np.ndarray
        The standard deviation of the right enu values in ECEF coordinates.

    Returns
    -------
    np.ndarray
        The interpolated enu values and the standard deviation of the
        interpolated enu values predicted at the time values from tenu_r.
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
    kin_position_data: np.ndarray,
    shot_data: np.ndarray,
    lengthscale: float = 0.5
    
) -> np.ndarray:
    """Interpolate the enu values using Kernel Ridge Regression.

    Parameters
    ----------
    kin_position_data : np.ndarray
        The kinematic position data.
    shot_data : np.ndarray
        The shot data.
    lengthscale : float, optional
        The length scale for the kernel, by default 0.5.

    Returns
    -------
    np.ndarray
        The interpolated enu values at the time values from tenu_r.
    """

    logger.loginfo("Interpolating ENU values using Kernel Ridge Regression")
    # First, we need to find the indices of tenu_l that are within the lengthscale of tenu_r
    # We will use a KDTree to find the indices efficiently
    KIN_POSITION_DATA_TREE = KDTree(kin_position_data[:, 0].astype(float).reshape(-1, 1))

    shotdata_near_kin_position_count = KIN_POSITION_DATA_TREE.query_radius(
        shot_data[:, 0].astype(float).reshape(-1, 1),
        r=lengthscale,  # seconds
        count_only=True,
    )
    shotdata_to_update_filter = shotdata_near_kin_position_count > 0
    shotdata_to_update = shot_data[shotdata_to_update_filter,:]

    if shotdata_to_update.shape[0] == 0:
        logger.loginfo("No points to update, returning original shot_data")
        return shot_data

    kin_position_training_data_inds = KIN_POSITION_DATA_TREE.query_radius(
        shotdata_to_update[:, 0].astype(float).reshape(-1, 1),
        r=lengthscale,  # seconds
        return_distance=False,
        count_only=False,
    )
    kin_position_training_data_inds = np.unique(list(itertools.chain.from_iterable(kin_position_training_data_inds))).astype(int)

    if len(kin_position_training_data_inds) == 0:
        logger.loginfo("No points to update, returning original tenu_r")
        return shot_data

    scaler = StandardScaler(with_std=False)  # we do not want to scale the standard deviation
    scaler.fit(kin_position_data[kin_position_training_data_inds,:])

    XY_train = scaler.transform(kin_position_data[kin_position_training_data_inds, :])  # East, North, Up values
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

def interpolate_enu_radius_regression(
        kin_position_df:pd.DataFrame,
        shotdata_df:pd.DataFrame,
        lengthscale:float=0.1,
) -> pd.DataFrame:
    """Interpolate the enu values using Radius Neighbors Regression.

    Parameters
    ----------
    kin_position_df : pd.DataFrame
        The kinematic position data.
    shotdata_df : pd.DataFrame
        The shot data.
    lengthscale : float, optional
        The length scale for the kernel, by default 0.1.

    Returns
    -------
    pd.DataFrame
        The updated shotdata DataFrame.
    """
   
    X_train = kin_position_df[["time"]].to_numpy()
    Y_train = kin_position_df[["east", "north", "up"]].to_numpy()

    XY_predict_ping = shotdata_df[["pingTime", "east0", "north0", "up0"]].to_numpy()
    XY_predict_return = shotdata_df[["returnTime", "east1", "north1", "up1"]].to_numpy()
    isUpdated = shotdata_df["isUpdated"].to_numpy()[:, np.newaxis]

    X_train = np.vstack((X_train,XY_predict_ping[:, 0][:,np.newaxis],XY_predict_return[:, 0][:,np.newaxis]))
    Y_train = np.vstack((Y_train, XY_predict_ping[:, 1:], XY_predict_return[:, 1:]))
    KIN_POSITION_DATA_TREE = RadiusNeighborsRegressor(
        radius=lengthscale, weights="uniform", algorithm="kd_tree"
    )
    KIN_POSITION_DATA_TREE.fit(X_train, Y_train)
    train_score = KIN_POSITION_DATA_TREE.score(X_train, Y_train)
    logger.loginfo(f"Training Score: {train_score}")
    pred_ping = KIN_POSITION_DATA_TREE.predict(XY_predict_ping[:, 0][:, np.newaxis])
    pred_return = KIN_POSITION_DATA_TREE.predict(XY_predict_return[:, 0][:, np.newaxis])

    # Get offsets between predicted and original values
    offset_ping = np.abs(pred_ping - XY_predict_ping[:, 1:])
    offset_return = np.abs(pred_return - XY_predict_return[:, 1:])
    logger.loginfo(f"Max offset for ping: {offset_ping.max()}, return: {offset_return.max()}")

    # update isUpdated flag
    isUpdated_1 = np.logical_or(
        isUpdated, np.any((offset_ping > 1e-3), axis=1)[:, np.newaxis]
    )
    isUpdated_2 = np.logical_or(
        isUpdated_1, np.any((offset_return > 1e-3), axis=1)[:, np.newaxis]
    )
    percentage_updated = (np.sum(isUpdated_2) / shotdata_df.shape[0])* 100
    shotdata_df["isUpdated"] = isUpdated_2
    shotdata_df[["east0", "north0", "up0"]] = pred_ping
    shotdata_df[["east1", "north1", "up1"]] = pred_return
    logger.loginfo(f"Interpolated {percentage_updated:.2f}% points using Radius Neighbors Regression with lengthscale {lengthscale:.2f} seconds")
    return shotdata_df

def get_merge_signature_shotdata(shotdata: TDBShotDataArray, kin_position: TDBKinPositionArray) -> Tuple[List[str], List[np.datetime64]]:
    """Get the merge signature for the shotdata and kin_position data.

    Parameters
    ----------
    shotdata : TDBShotDataArray
        The shotdata array.
    kin_position : TDBKinPositionArray
        The kinposition array.

    Returns
    -------
    Tuple[List[str], List[np.datetime64]]
        The merge signature and the dates to merge.
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

def merge_shotdata_kinposition(
        shotdata_pre: TDBShotDataArray,
        shotdata: TDBShotDataArray, 
        kin_position: TDBKinPositionArray,
        dates:List[datetime64],
        lengthscale:float=0.1,
        plot:bool=False) -> TDBShotDataArray:

    """Merge the shotdata and kin_position data.

    Parameters
    ----------
    shotdata_pre : TDBShotDataArray
        The DFOP00 data.
    shotdata : TDBShotDataArray
        The shotdata array to write to.
    kin_position : TDBKinPositionArray
        The TileDB KinPosition array.
    dates : List[datetime64]
        The dates to merge.
    lengthscale : float, optional
        The length scale for the kernel, by default 0.1.
    plot : bool, optional
        Plot the interpolated values, by default False.

    Returns
    -------
    TDBShotDataArray
        The updated shotdata array.
    """

    logger.loginfo("Merging shotdata and kin_position data")
    for start,end in zip(dates,dates[1:]):
        logger.loginfo(f"Interpolating shotdata for date {str(start)}")

        shotdata_df = shotdata_pre.read_df(start=start,end=end)
        kin_position_df = kin_position.read_df(start=start, end=end)

        if shotdata_df.empty or kin_position_df.empty:
            continue

        kin_position_df.time = kin_position_df.time.apply(lambda x:x.timestamp())
       
        # interpolate the enu values
        shotdata_df_updated = interpolate_enu_radius_regression(
            kin_position_df=kin_position_df,
            shotdata_df=shotdata_df.copy(),
            lengthscale=lengthscale
        )


        shotdata.write_df(shotdata_df_updated,validate=False)