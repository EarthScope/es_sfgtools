import requests
from pathlib import Path
import os
import urllib.request
import ssl
import logging

ssl._create_default_https_context = ssl._create_stdlib_context

from earthscope_sdk.auth.device_code_flow import DeviceCodeFlowSimple
from earthscope_sdk.auth.auth_flow import NoTokensError

logger = logging.getLogger(__name__)

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

def download_file_from_archive(url, 
                               dest_dir='./', 
                               token_path=".",
                               show_details: bool=True) -> None:
    """ 
    Download a file from the public archive using the EarthScope SDK.
    Args:
        url (str): The URL of the file to download.
        dest_dir (str): The directory to save the downloaded file.
        token_path (str): The path to the token file.
        show_details (bool): log the file details
    """ 

    # Make the destination directory if it doesn't exist
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
            logger.info(f"Downloading {url} to {destination_file}")
        with open(destination_file, 'wb') as f:
            for data in r:
                f.write(data)
    else:
        raise Exception(f"Failed to download file from {url}, status code: {r.status_code}, reason: {r.reason}")


def list_files_from_archive(url, token_path=".") -> list:
    """
    List files from the public archive using urllib

    Args:
        url (str): The URL of the directory to list. This must be a directory that contains files
        token_path (str): The path to the token file.
    """

    token = retrieve_token(token_path)

    # Generate the list URL
    list_url = os.path.join(url, "?list&uris=1")    # ?list returns a list of files and &uris=1 returns the full path

    # Create a request object
    req = urllib.request.Request(list_url)

    # Add the authorization header
    req.add_header('Authorization', 'Bearer ' + token)

    try:
        # Send the request and capture the response
        with urllib.request.urlopen(req) as response:
            result = response.read().decode('utf-8')
    except Exception as e:
        logger.error(e)
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

    lines = output.split('\n')
    files = []

    for line in lines:
        if not line.strip():    # Skip empty lines
            continue
        
        files.append(line)

    return files


def download_file_list_from_archive(file_urls: list, dest_dir='./files', token_path=".") -> None:
    """
    Download a list of files from the public archive

    Args:
        file_urls (list): A list of URLs to download
        dest_dir (str): The directory to save the downloaded files
        token_path (str): The path to the token file
    """

    successful_files = []
    failed_files = []
    
    logger.info(f"Downloading {len(file_urls)} files to {dest_dir}")
    for url in file_urls:
        try: 
            download_file_from_archive(url=url, 
                                    dest_dir=dest_dir, 
                                    token_path=token_path)
            successful_files.append(url)
        except Exception as e:
            logger.error(f"Failed to download {url}: {e}")
            failed_files.append(url)

    logger.info(f"Downloaded {len(successful_files)} files successfully.")
    if len(failed_files) > 0:
        logger.warning(f"Failed to download {len(failed_files)} files.")
        print(failed_files)

def generate_archive_survey_url(network, station, survey):
    """
    Generate a URL for a survey in the public archive

    Args:
        network (str): The network name
        station (str): The station name
        survey (str): The survey name

    Returns:
        str: The URL of the survey directory
    """ 

    return f'https://gage-data.earthscope.org/archive/seafloor/{network}/{station}/{survey}/raw'

def list_file_counts_by_type(file_list: list, url: str=None, show_details: bool=True) -> dict:
    """counts files by type, and builds a dictionary.

    :param file_list: a list of files from the archive
    :type file_list: list
    :param url: url of where in the archive the files were found, defaults to None
    :type url: str, optional
    :param show_details: log details, defaults to True
    :type show_details: bool, optional
    :return: dictionary of files by type
    :rtype: dict
    """
    file_dict = {}
    for file in file_list:
        if 'master' in file:
            file_dict.setdefault('master', []).append(file)
        elif 'lever_arms' in file:
            file_dict.setdefault('lever_arms', []).append(file)
        elif 'bcsonardyne' in file:
            file_dict.setdefault('sonardyne', []).append(file)
        elif 'bcnovatel' in file:
            file_dict.setdefault('novatel', []).append(file)
        elif 'bcoffload' in file:
            file_dict.setdefault('offload', []).append(file)
        elif file.endswith('NOV770.raw'):
            file_dict.setdefault('NOV770', []).append(file)
        elif file.endswith('DFOP00.raw'):
            file_dict.setdefault('DFOP00', []).append(file)
        elif file.endswith('NOV000.bin'):
            file_dict.setdefault('NOV000', []).append(file)
        elif "ctd" in file:
            file_dict.setdefault('ctd', []).append(file)

    if url is not None:
        logger.info(f'Found under {url}:')
    else:
        logger.info('Found:')
    for k,v in file_dict.items():
        logger.info(f'    {len(v)} {k} file(s)')

    if show_details:
        if url is not None:
            print(f'Found under {url}:')
        else:
            print('Found:')
        for k,v in file_dict.items():
            print(f'    {len(v)} {k} file(s)')
    return file_dict  

def get_survey_file_dict(url:str) -> dict:
    """gets

    :param url: location in archive
    :type url: str
    :return: dictionary of file locations by type
    :rtype: dict
    """
    
    file_list = list_files_from_archive(url)
    return list_file_counts_by_type(file_list, url)

def list_survey_files(network: str, 
                      station: str, 
                      survey: str,
                      show_details: bool=True) -> list:
    """returns a list of files for a given survey in the archive.  optionally displays a summary of file
    counts by type

    :param network: network name
    :type network: str
    :param station: station name
    :type station: str
    :param survey: survey name
    :type survey: str
    :param show_details: option to display summary of file counts by type, defaults to True
    :type show_details: bool, optional
    :return: list of file locations in archive
    :rtype: list
    """
    url = generate_archive_survey_url(network, station, survey)
    file_list = list_files_from_archive(url)
    file_list += list_files_from_archive(f'{url}/ctd')
    list_file_counts_by_type(file_list=file_list, url=url, show_details=show_details)
    return file_list  


if __name__ == "__main__":    
    # Example usage 

    # Download file from public arhive
    url = "https://gage-data.earthscope.org/archive/gnss/L1/rinex/1998/300/cal13000.98S"
    download_file_from_archive(url, dest_dir='./files')

    # List files from public arhive
    url = "https://gage-data.earthscope.org/archive/gnss/rinex/met/2021/072"
    file_list = list_files_from_archive(url)

    # Download a list of files
    download_file_list_from_archive(file_list)

