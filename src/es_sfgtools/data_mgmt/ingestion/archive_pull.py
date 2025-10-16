import os
import ssl
import urllib.request
from pathlib import Path
from typing import List, Optional
from collections import defaultdict
import boto3
from es_sfgtools.data_mgmt.assetcatalog.schemas import AssetType
import requests
from earthscope_cli.login import login as es_login
from earthscope_sdk import EarthScopeClient
from earthscope_sdk.config.settings import SdkSettings

from es_sfgtools.data_models.metadata import Site, Vessel, import_site, import_vessel

from ...logging import ProcessLogger as logger

from .config import ARCHIVE_PREFIX
from .datadiscovery import get_file_type_remote

ssl._create_default_https_context = ssl._create_stdlib_context


def retrieve_token(profile=None):
    """Retrieve or generate a token for the public archive.

    This uses the EarthScope SDK (new method).

    Parameters
    ----------
    profile : str, optional
        The profile to use for authentication (e.g., 'dev'), by default None (prod).

    Returns
    -------
    str
        The access token.
    """
    es = EarthScopeClient()
    settings = SdkSettings(profile_name=profile) if profile else SdkSettings()
    es = EarthScopeClient(settings=settings)

    try:
        es.ctx.auth_flow.refresh_if_necessary()
    except Exception as e:
        logger.logerr(f"Failed to refresh token: {e} Attempting to login...")
        es_login(sdk=es)

    token = es.ctx.auth_flow.access_token
    return token

def download_file_from_archive(url, 
                               dest_dir = "./", 
                               profile = None, 
                               show_details: bool = True) -> None:
    """Download a file from the public archive using the EarthScope SDK.

    Parameters
    ----------
    url : str
        The URL of the file to download.
    dest_dir : str, optional
        The directory to save the downloaded file, by default "./".
    profile : str, optional
        The profile to use for authentication (e.g., 'dev'), by default None (prod).
    show_details : bool, optional
        Log the file details, by default True.
    """

    # Make the destination directory if it doesn't exis
    if not os.path.exists(dest_dir):
        os.makedirs(dest_dir)

    # retrieve the token
    token = retrieve_token(profile=profile)

    file_name = Path(url).name
    destination_file = os.path.join(dest_dir, file_name)

    # Provide the token in the Authorization header
    r = requests.get(url, headers={"authorization": f"Bearer {token}"})
    if r.status_code == requests.codes.ok:
        if show_details:
            logger.loginfo(f"Downloading {url} to {destination_file}")
        with open(destination_file, "wb") as f:
            for data in r:
                f.write(data)
    else:
        raise Exception(
            f"Failed to download file from {url}, status code: {r.status_code}, reason: {r.reason}"
        )

def list_files_from_archive(url) -> list:
    """List files from the public archive using urllib.

    Parameters
    ----------
    url : str
        The URL of the directory to list. This must be a directory that
        contains files.

    Returns
    -------
    list
        A list of files.
    """

    token = retrieve_token()

    # Generate the list URL
    list_url = os.path.join(
        url, "?list&uris=1"
    )  # ?list returns a list of files and &uris=1 returns the full path

    # Create a request object
    req = urllib.request.Request(list_url)

    # Add the authorization header
    req.add_header("Authorization", "Bearer " + token)

    try:
        # Send the request and capture the response
        with urllib.request.urlopen(req) as response:
            result = response.read().decode("utf-8")
    except Exception as e:
        logger.logerr(e)
        return []

    return _parse_output(result)

def _parse_output(output) -> list:
    """Parse the output from the public archive.

    Parameters
    ----------
    output : str
        The file output from the public archive.

    Returns
    -------
    list
        A list of files.
    """

    lines = output.split("\n")
    files = []

    for line in lines:
        if not line.strip():  # Skip empty lines
            continue

        files.append(line)

    return files

def download_file_list_from_archive(file_urls: list, dest_dir="./files") -> None:
    """Download a list of files from the public archive.

    Parameters
    ----------
    file_urls : list
        A list of URLs to download.
    dest_dir : str, optional
        The directory to save the downloaded files, by default "./files".
    """

    successful_files = []
    failed_files = []

    logger.loginfo(f"Downloading {len(file_urls)} files to {dest_dir}")
    for url in file_urls:
        try:
            download_file_from_archive(url=url, 
                                       dest_dir=dest_dir)
            successful_files.append(url)
        except Exception as e:
            logger.logerr(f"Failed to download {url}: {e}")
            failed_files.append(url)

    logger.loginfo(f"Downloaded {len(successful_files)} files successfully.")
    if len(failed_files) > 0:
        logger.logwarn(f"Failed to download {len(failed_files)} files.")
        print(failed_files)

def generate_archive_campaign_url(network, station, campaign):
    """Generate a URL for a campaign in the public archive.

    Parameters
    ----------
    network : str
        The network name.
    station : str
        The station name.
    campaign : str
        The campaign name (e.g YYYY_A_WVGL).

    Returns
    -------
    str
        The URL of the campaign directory.
    """

    # Grab the year out of the campaign name
    year = campaign.split("_")[0]

    return f"{ARCHIVE_PREFIX}{network}/{year}/{station}/{campaign}/raw"

def generate_archive_campaign_metadata_url(network, station, campaign):
    """Generate a URL for campaign metadata in the public archive.

    Parameters
    ----------
    network : str
        The network name.
    station : str
        The station name.
    campaign : str
        The campaign name (e.g YYYY_A_WVGL).

    Returns
    -------
    str
        The URL of the campaign directory.
    """

    # Grab the year out of the campaign name
    year = campaign.split("_")[0]

    return f"{ARCHIVE_PREFIX}/{network}/{year}/{station}/{campaign}/metadata"

def generate_archive_site_json_url(network, station, profile: str = None) -> str:
    """Generate a URL for the site JSON file in the public archive.

    Parameters
    ----------
    network : str
        The network name.
    station : str
        The station name.
    profile : str, optional
        The profile to use for the archive (e.g., 'prod', 'dev'), by default None (prod).

    Returns
    -------
    str
        The URL of the site JSON file.
    """
    if profile == "prod" or profile is None:
        return f"{ARCHIVE_PREFIX}/metadata/{network}/{station}.json"
    elif profile == "dev":
        return f"{ARCHIVE_PREFIX}/metadata/{network}/{station}.json"
    else:
        raise ValueError("Invalid profile specified.")

def generate_archive_vessel_json_url(vessel_code, profile: str = None) -> str:
    """Generate a URL for the vessel JSON file in the public archive.

    Parameters
    ----------
    vessel_code : str
        The vessel code.
    profile : str, optional
        The profile to use for the archive (e.g., 'prod', 'dev'), by default None (prod).

    Returns
    -------
    str
        The URL of the vessel JSON file.
    """
    if profile == "prod" or profile is None:
        return f"{ARCHIVE_PREFIX}/metadata/vessels/{vessel_code}.json"
    elif profile == "dev":
        return f"{ARCHIVE_PREFIX}/metadata/vessels/{vessel_code}.json"
    else:
        raise ValueError("Invalid profile specified.")

def load_vessel_metadata(vessel_code: str, profile: str = None, local_path: Path|str = None) -> Vessel:
    """Load the vessel metadata from the s3 archive.

    Note
    ----
    To access the dev archive, you must:
    1. set up ~/.earthscope/config.toml
    2. run `es login --profile dev`
    3. be on the earthscope vpn

    Parameters
    ----------
    vessel_code : str
        The vessel code.
    profile : str, optional
        The profile to use for the archive (e.g., 'prod', 'dev'), by default
        None (prod).
    local_path : Path | str, optional
        Local path to a JSON file containing vessel metadata. If provided,
        this will be used instead of downloading from the archive.

    Returns
    -------
    Vessel
        An instance of the Vessel class with the metadata loaded.
    """
    if local_path is not None:
        # If a local path is provided, load the vessel metadata from the local file
        json_file_path = Path(local_path)
        if not json_file_path.exists():
            raise FileNotFoundError(f"Local vessel metadata file {json_file_path} does not exist.")
        vessel = import_vessel(json_file_path)
        return vessel
    else:
        vessel_json_url = generate_archive_vessel_json_url(vessel_code, profile)
        logger.loginfo(f"Loading vessel metadata from {vessel_json_url}")
        download_file_from_archive(vessel_json_url, dest_dir="./", profile=profile, show_details=False)
        # Load the vessel metadata from the downloaded JSON file
        vessel_file_path = Path(f"./{vessel_code}.json")
        vessel = import_vessel(vessel_file_path)
        
        vessel_file_path.unlink()  # Remove the JSON file after loading
        return vessel   

def load_site_metadata(network: str, station: str, profile: str = None, local_path: Path|str = None) -> Site:
    """Load the site metadata from the s3 archive.

    Note
    ----
    To access the dev archive, you must:
    1. set up ~/.earthscope/config.toml
    2. run `es login --profile dev`
    3. be on the earthscope vpn

    Parameters
    ----------
    network : str
        The network name.
    station : str
        The station name.
    profile : str, optional
        The profile to use for the archive (e.g., 'prod', 'dev'), by default
        None (prod).
    local_path : Path | str, optional
        Local path to a JSON file containing site metadata. If provided,
        this will be used instead of downloading from the archive.

    Returns
    -------
    Site
        An instance of the Site class with the metadata loaded.
    """
    if local_path is not None:
        # If a local path is provided, load the site metadata from the local file.
        # TODO: allow for local vessel metadata to be loaded as well
        json_file_path = Path(local_path)
        if not json_file_path.exists():
            raise FileNotFoundError(f"Local site metadata file {json_file_path} does not exist.")
        site = import_site(json_file_path)

    else:
        site_json_url = generate_archive_site_json_url(network, station, profile)
        logger.loginfo(f"Loading site metadata from {site_json_url}")
        download_file_from_archive(site_json_url, dest_dir="./", profile=profile, show_details=False)
        # Load the site metadata from the downloaded JSON file
        site_file_path = Path(f"./{station}.json")
        site = import_site(site_file_path)
        site_file_path.unlink()  # Remove the JSON file after loading

    for campaign in site.campaigns:
        try:
            campaign.vessel = load_vessel_metadata(campaign.vesselCode, profile=profile)
        except FileNotFoundError as e:
            logger.logerr(f"Vessel metadata file not found for campaign {campaign.name}: {e}")
            campaign.vessel = None
        except ValueError as e:
            logger.logerr(f"Invalid vessel metadata for campaign {campaign.name}: {e}")
            campaign.vessel = None
        except requests.exceptions.RequestException as e:
            logger.logerr(f"Network error while loading vessel metadata for campaign {campaign.name}: {e}")
            campaign.vessel = None
    return site

def list_file_counts_by_type(file_list: list, url: Optional[str] = None, show_logs=True) -> dict:
    """Counts files by type, and builds a dictionary.

    Parameters
    ----------
    file_list : list
        List of files from the archive.
    url : str, optional
        URL of where in the archive the files were found, by default None.
    show_logs : bool, optional
        Whether to show logs containing file counts, by default True.

    Returns
    -------
    dict
        Dictionary of files by type.
    """
    file_dict = defaultdict(list)
    for file in file_list:
        file_type:AssetType = get_file_type_remote(file)

        if file_type is not None:
            file_dict[file_type.value].append(file)

    if show_logs:
        if url is not None:
            logger.loginfo(f"Found under {url}:")
        else:
            logger.loginfo("Found:")
        for k, v in file_dict.items():
            logger.loginfo(f"    {len(v)} {k} file(s)")

    return file_dict

def get_campaign_file_dict(url: str) -> dict:
    """Get a dictionary of campaign files by type.

    Parameters
    ----------
    url : str
        Location in archive.

    Returns
    -------
    dict
        Dictionary of file locations by type.
    """

    file_list = list_files_from_archive(url)
    return list_file_counts_by_type(file_list, url)

def list_campaign_files(network: str, station: str, campaign: str) -> list:
    """Returns a list of files for a given campaign in the archive.

    Optionally displays a summary of file counts by type.

    Parameters
    ----------
    network : str
        Network name.
    station : str
        Station name.
    campaign : str
        Campaign name.

    Returns
    -------
    list
        List of file locations in archive.
    """

    # Generate the URLs for raw data & metadata
    raw_url = generate_archive_campaign_url(network, station, campaign)
    metadata_url = generate_archive_campaign_metadata_url(network, station, campaign)
    logger.loginfo(f"Listing raw campaign files from url {raw_url}")
    
    raw_file_list = list_files_from_archive(raw_url)
    list_file_counts_by_type(file_list=raw_file_list, url=raw_url)

    logger.loginfo(f"Listing metadata campaign files from url {metadata_url}")
    metadata_file_list = list_files_from_archive(metadata_url)
    metadata_file_list += list_files_from_archive(f"{metadata_url}/ctd")
    list_file_counts_by_type(file_list=metadata_file_list, url=metadata_url)

    # Concatenate the two lists
    file_list = raw_file_list + metadata_file_list

    return file_list

def list_campaign_files_by_type(network: str, station: str, campaign: str, show_logs: bool=True) -> dict:
    """List campaign files by type.

    Parameters
    ----------
    network : str
        Network name.
    station : str
        Station name.
    campaign : str
        Campaign name.
    show_logs : bool, optional
        Whether to show logs containing file counts, by default True.

    Returns
    -------
    dict
        Dictionary of file locations by type.
    """

    # Generate the URLs for raw data & metadata
    raw_url = generate_archive_campaign_url(network, station, campaign)
    metadata_url = generate_archive_campaign_metadata_url(network, station, campaign)

    if show_logs:
        logger.loginfo(f"Listing raw campaign files from url {raw_url}")
    raw_file_list = list_files_from_archive(raw_url)
    raw_file_dict = list_file_counts_by_type(file_list=raw_file_list, url=raw_url, show_logs=show_logs)

    if show_logs:
        logger.loginfo(f"Listing metadata campaign files from url {metadata_url}")
    metadata_file_list = list_files_from_archive(metadata_url)
    metadata_file_list += list_files_from_archive(f"{metadata_url}/ctd")
    metadata_file_dict = list_file_counts_by_type(file_list=metadata_file_list, url=metadata_url, show_logs=show_logs)

    # Concatenate the two lists
    file_dict = raw_file_dict | metadata_file_dict

    return file_dict

def list_s3_directory_files(bucket_name: str, prefix: str) -> List[str]:
    """Returns a list all files in a given S3 bucket.

    This is under a specified prefix and return absolute S3 paths.

    Parameters
    ----------
    bucket_name : str
        Name of the S3 bucket.
    prefix : str
        S3 prefix (folder path) to filter the files.

    Returns
    -------
    List[str]
        List of absolute S3 file paths.
    """
    s3_client = boto3.client("s3")
    file_paths = []

    paginator = s3_client.get_paginator("list_objects_v2")
    for page in paginator.paginate(Bucket=bucket_name, Prefix=prefix):
        if "Contents" in page:
            file_paths.extend(
                f"s3://{bucket_name}/{obj['Key']}" for obj in page["Contents"]
            )

    return file_paths


if __name__ == "__main__":
    # Example usage

    # files = list_campaign_files(network="alaska-shumagins", 
    #                     station="SPT1",
    #                     campaign="2022_A_1049")
    # print(files)

    # # Download file from public arhive
    url = "https://data.dev.earthscope.org/archive/seafloor/alaska-shumagins/SEM1.json"
    url="https://data.dev.earthscope.org/archive/seafloor/alaska-shumagins/IVB1/2018_A_SFG1/raw/bcnovatel_20180530184921.txt"
    # url = "https://gage-data.earthscope.org/archive/seafloor/alaska-shumagins/2018/IVB1/2018_A_SFG1/metadata/IVB1.master"
    download_file_from_archive(url, dest_dir=".")

    # # List files from public arhive
    # url = "https://data.earthscope.org/archive/gnss/rinex/met/2021/072"
    # file_list = list_files_from_archive(url)

    # # Download a list of files
    # download_file_list_from_archive(file_list)