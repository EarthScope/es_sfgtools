# Poplate metadata with lever arms and master/config file

from datetime import datetime 
import os

from es_sfgtools.utils.archive_pull import list_files_from_archive, download_file_list_from_archive
from es_sfgtools.utils.metadata.site import Site
from es_sfgtools.utils.metadata.vessel import Vessel

base_url = "https://gage-data.earthscope.org/archive/seafloor/{network}/{station}/{campaign}/raw"


def get_metadata_from_gage_data(network: str, site: str):
    """
    # 1. list & find the site on gage-data
    # 2. List & find the campaigns on gage-data
    # 3. List & find the files within the campaigns
    # 4. Populate metadata site class with data 
    # 5. Populate metadata vessel class with data

    Args:
        network (str): network name station is located in
        site (str): name of site to parse 
    """

    # Create site class
    site_class = Site(names=[site])

    # Get list of campigns
    campaigns = get_campaigns()

    # Add each campaign to Site
    for campaign_name in campaigns:
        campaign_dict = {}
        site_class.run_campaign(campaign_name, campaign_dict=campaign_dict, add_new=True)

        # Get config files under each campaign
        # Add to benchmark transponder

        # Create vessel class if not exist yet
        yyyy, interval, vessel_name = campaign_name.split('_')
        vessel_class = Vessel(name=vessel_name)

        # Get lever arms file
        lever_file_path = ''
        # Parse level arms file
        x,y,z = read_lever_arms_file(file_path=lever_file_path)
        # TODO stored under transducer which we dont have... make up one?
        vessel_class.new_equipment(serial_number=1, equipment_type='atdOffsets', equipment_data={'x': x, 'y': y, 'z': z})


    pass

def get_campaigns():
    """ Get list of campaigns from gage data and parse any useful information (add to site class)
    Return campaign names
    """
    pass


# TODO: Add enum list of the 2 types of config files

def read_config_file(file_path: str, file_type): 
    """ 
    Read the SITE.master or config file and return the contents as a dictionary. 
    The master file contains apriori transponder positions & delay times.

    Parameters:
    file_path (str): The path to the SITE.master or config yaml file.
    """

    # Check if the master file exists
    if not os.path.exists(file_path):
        print(f'The config file does not exist: {file_path}')
        return

    # Open the master file and read the first line
    with open(file_path, 'r') as file:
        lines = file.readlines()
    
    # Extract the transponder positions and delay times
    start_date = datetime.strptime(lines[0].strip(), '%Y-%m-%d %H:%M:%S')
    num_of_transponders = int(lines[1].strip())

    for transponder_index in range(num_of_transponders):
        transponder_ID, _, latitude, longitude, height, turn_around_time, _ = lines[2 + transponder_index].strip().split()

        # transponder_name = ("{}-{}").format(site_name + 
        #                                     transponder_ID)
        
# TODO: this will get added to a vessel file (vessel name comes from campaign name)
def read_lever_arms_file(file_path):
    """ Read the lever arms file and return the contents as a dictionary.
    (The lever arms file contains the body frame offsets between antenna and transducer)
    """

    # Check if the lever arms file exists
    if not os.path.exists(file_path):
        print(f'The lever arms file does not exist: {file_path}')
        return

    # Open the lever arms file and read the first line
    with open(file_path, 'r') as file:
        line = file.readline()

    # Extract the lever arms
    lever_arms = {}
    lever_arms['X'], lever_arms['Y'], lever_arms['Z'] = line.split()

    return lever_arms