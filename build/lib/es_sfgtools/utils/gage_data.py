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
    Retrieve or generate a token for the GAGE data archive using the EarthScope SDK.

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

def get_file_from_gage_data(url, dest_dir='./', token_path=".") -> None:
    """ 
    Download a file from the GAGE data archive using the EarthScope SDK.
    Args:
        url (str): The URL of the file to download.
        dest_dir (str): The directory to save the downloaded file.
        token_path (str): The path to the token file.
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
        print(f"Downloading {url} to {destination_file}")
        with open(destination_file, 'wb') as f:
            for data in r:
                f.write(data)
    else:
        raise Exception(f"Failed to download file from {url}, status code: {r.status_code}, reason: {r.reason}")


def list_files_from_gage_data(url, token_path=".") -> list:
    """
    List files from the GAGE data archive using urllib

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
        print(e)
        return []

    return _parse_output(result)

def _parse_output(output) -> list:
    """
    Parse the output from the GAGE data archive
    Args:
        output (str): The file output from the GAGE data archive

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


def download_file_list_from_gage_data(file_urls: list, dest_dir='./files', token_path=".") -> None:
    """
    Download a list of files from the GAGE data archive

    Args:
        file_urls (list): A list of URLs to download
        dest_dir (str): The directory to save the downloaded files
        token_path (str): The path to the token file
    """

    successful_files = []
    failed_files = []
    
    print(f"Downloading {len(file_urls)} files to {dest_dir}")
    for url in file_urls:
        try: 
            get_file_from_gage_data(url=url, 
                                    dest_dir=dest_dir, 
                                    token_path=token_path)
            successful_files.append(url)
        except Exception as e:
            print(f"Failed to download {url}: {e}")
            failed_files.append(url)

    print(f"Downloaded {len(successful_files)} files successfully.")
    if len(failed_files) > 0:
        print(f"Failed to download {len(failed_files)} files.")
        print(failed_files)

def generate_gage_data_survey_url(network, station, survey, level='raw'):
    url = f'https://gage-data.earthscope.org/archive/seafloor/{network}/{station}/{survey}/{level}'
    return url

def list_file_counts_by_type(file_list, url=None):
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
    return file_dict  

def get_survey_file_dict(url):
    file_list = list_files_from_gage_data(url)
    return list_file_counts_by_type(file_list)

def list_survey_files(network: str, 
                      station: str, 
                      survey: str) -> list:
    url = generate_gage_data_survey_url(network, station, survey)
    file_list = list_files_from_gage_data(url)
    file_list += list_files_from_gage_data(f'{url}/ctd')
    list_file_counts_by_type(file_list=file_list, url=url)
    return file_list
    

if __name__ == "__main__":    
    # Example usage 

    # Download file from GAGE data
    url = "https://gage-data.earthscope.org/archive/gnss/L1/rinex/1998/300/cal13000.98S"
    get_file_from_gage_data(url, dest_dir='./files')

    # List files from GAGE data
    url = "https://gage-data.earthscope.org/archive/gnss/rinex/met/2021/072"
    file_list = list_files_from_gage_data(url)

    # Download a list of files
    download_file_list_from_gage_data(file_list)

