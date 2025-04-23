import requests
from pathlib import Path
import os
import urllib.request
import ssl
from typing import List
import boto3

ssl._create_default_https_context = ssl._create_stdlib_context

from earthscope_sdk.auth.device_code_flow import DeviceCodeFlowSimple
from earthscope_sdk.auth.auth_flow import NoTokensError
from es_sfgtools.utils.loggers import ProcessLogger as logger


def retrieve_token(token_path="."):
    """
    Retrieve or generate a token for the public archive using the EarthScope SDK.

    Args:
        token_path (str): The path to the token file.
    """
    # instantiate the device code flow subclass
    device_flow = DeviceCodeFlowSimple(Path(token_path))
    try:
        # get access token from local path
        device_flow.get_access_token_refresh_if_necessary()
    except NoTokensError:
        # if no token was found locally, do the device code flow
        device_flow.do_flow()

    token = device_flow.access_token

    return token


def download_file_from_archive(
    url, dest_dir="./", token_path=".", show_details: bool = True
) -> None:
    """
    Download a file from the public archive using the EarthScope SDK.
    Args:
        url (str): The URL of the file to download.
        dest_dir (str): The directory to save the downloaded file.
        token_path (str): The path to the token file.
        logger (logging.Logger): The logger to use
        show_details (bool): log the file details
    """

    # Make the destination directory if it doesn't exis
    if not os.path.exists(dest_dir):
        os.makedirs(dest_dir)

    # generate a token
    token = retrieve_token(token_path)

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


def list_files_from_archive(url, token_path=".") -> list:
    """
    List files from the public archive using urllib

    Args:
        url (str): The URL of the directory to list. This must be a directory that contains files
        token_path (str): The path to the token file.
    """

    token = retrieve_token(token_path)

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
    """
    Parse the output from the public archive
    Args:
        output (str): The file output from the public archive

    Returns:
        list: A list of files
    """

    lines = output.split("\n")
    files = []

    for line in lines:
        if not line.strip():  # Skip empty lines
            continue

        files.append(line)

    return files


def download_file_list_from_archive(
    file_urls: list, dest_dir="./files", token_path="."
) -> None:
    """
    Download a list of files from the public archive

    Args:
        file_urls (list): A list of URLs to download
        dest_dir (str): The directory to save the downloaded files
        token_path (str): The path to the token file
    """

    successful_files = []
    failed_files = []

    logger.loginfo(f"Downloading {len(file_urls)} files to {dest_dir}")
    for url in file_urls:
        try:
            download_file_from_archive(
                url=url, dest_dir=dest_dir, token_path=token_path
            )
            successful_files.append(url)
        except Exception as e:
            logger.logerr(f"Failed to download {url}: {e}")
            failed_files.append(url)

    logger.loginfo(f"Downloaded {len(successful_files)} files successfully.")
    if len(failed_files) > 0:
        logger.logwarn(f"Failed to download {len(failed_files)} files.")
        print(failed_files)


def generate_archive_campaign_url(network, station, campaign):
    """
    Generate a URL for a campaign in the public archive

    Args:
        network (str): The network name
        station (str): The station name
        campaign (str): The campaign name (e.g YYYY_A_WVGL)

    Returns:
        str: The URL of the campaign directory
    """

    # Grab the year out of the campaign name
    year = campaign.split("_")[0]

    return f"https://data.earthscope.org/archive/seafloor/{network}/{year}/{station}/{campaign}/raw"

def generate_archive_campaign_metadata_url(network, station, campaign):
    """
    Generate a URL for campaign metadata in the public archive

    Args:
        network (str): The network name
        station (str): The station name
        campaign (str): The campaign name (e.g YYYY_A_WVGL)

    Returns:
        str: The URL of the campaign directory
    """

    # Grab the year out of the campaign name
    year = campaign.split("_")[0]

    return f"https://data.earthscope.org/archive/seafloor/{network}/{year}/{station}/{campaign}/metadata"


def list_file_counts_by_type(file_list: list, url: str = None) -> dict:
    """
    Counts files by type, and builds a dictionary.

    Args:
        file_list (list): list of files from the archive
        url (str): url of where in the archive the files were found

    Returns:
        dict: dictionary of files by type
    """
    file_dict = {}
    for file in file_list:
        if "master" in file:
            file_dict.setdefault("master", []).append(file)
        elif "lever_arms" in file:
            file_dict.setdefault("lever_arms", []).append(file)
        elif "bcsonardyne" in file:
            file_dict.setdefault("sonardyne", []).append(file)
        elif "bcnovatel" in file:
            file_dict.setdefault("novatel", []).append(file)
        elif "bcoffload" in file:
            file_dict.setdefault("offload", []).append(file)
        elif file.endswith("NOV770.raw"):
            file_dict.setdefault("NOV770", []).append(file)
        elif file.endswith("DFOP00.raw"):
            file_dict.setdefault("DFOP00", []).append(file)
        elif file.endswith("NOV000.bin"):
            file_dict.setdefault("NOV000", []).append(file)
        elif "ctd" in file:
            file_dict.setdefault("ctd", []).append(file)

    if url is not None:
        logger.loginfo(f"Found under {url}:")
    else:
        logger.loginfo("Found:")
    for k, v in file_dict.items():
        logger.loginfo(f"    {len(v)} {k} file(s)")

    return file_dict


def get_survey_file_dict(url: str) -> dict:
    """

    Args:
        url (str): location in archive

    Returns:
        dict: dictionary of file locations by type
    """

    file_list = list_files_from_archive(url)
    return list_file_counts_by_type(file_list, url)


def list_campaign_files(network: str, station: str, campaign: str) -> list:
    """
    Returns a list of files for a given campaign in the archive.  Optionally displays a summary of file counts by type

    Args:
        network (str): network name
        station (str): station name
        campaign (str): campaign name

    Returns:
        list: list of file locations in archive
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


def list_s3_directory_files(bucket_name: str, prefix: str) -> List[str]:
    """
    Returns a list all files in a given S3 bucket under a specified prefix and return absolute S3 paths.

    Args:
        bucket_name (str): Name of the S3 bucket.
        prefix (str): S3 prefix (folder path) to filter the files.
    Returns:
        List[str]: List of absolute S3 file paths.
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

    files = list_campaign_files(network="alaska-shumagins", 
                        station="SPT1",
                        campaign="2022_A_1049")
    print(files)

    # # Download file from public arhive
    # url = ""
    # download_file_from_archive(url, dest_dir="./files")

    # # List files from public arhive
    # url = "https://data.earthscope.org/archive/gnss/rinex/met/2021/072"
    # file_list = list_files_from_archive(url)

    # # Download a list of files
    # download_file_list_from_archive(file_list)
