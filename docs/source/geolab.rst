GeoLab Usage Guide
===================

This guide provides comprehensive instructions for using es_sfgtools within the EarthScope GeoLab environment for seafloor geodesy data processing. 


Prerequesites
----------------
Accessing GeoLab requires an EarthScope account with appropriate permissions. Ensure you have the following:

- An active EarthScope account

- Requested access for the `s3-seafloor` role 


Accessing GeoLab
-----------------
To access GeoLab and our prebuilt Jupyter notebooks, follow these steps:   

1. Open `GeoLab with the Seafloor Geodesy Notebooks <https://geolab.earthscope.cloud/hub/user-redirect/git-pull?repo=https%3A%2F%2Fgithub.com%2FEarthScope%2Fseafloor_geodesy_notebooks.git&urlpath=lab%2Ftree%2Fseafloor_geodesy_notebooks.git%2Fnotebooks%2F&branch=main>`_ and click "Lauch GeoLab".  
    Note this link uses nbgitpuller to automatically clone the `seafloor geodesy notebooks repository <https://github.com/EarthScope/seafloor_geodesy_notebooks>`_ into your GeoLab environment.  

2. Login with your EarthScope credentials.

3. Select Environment -> Other, and use this custom image URL:

   `public.ecr.aws/earthscope/sfg-geolab:latest`

4. Select a resource allocation, suggest starting with 7GB RAM, 1 CPU and increasing as needed.

5. Once logged in, you will be presented with a Jupyter Notebook interface where you can run es_sfgtools workflows.  

6. Select the `notebooks/run_garpos.ipynb` notebook to get started with GARPOS processing.



Data Organization
~~~~~~~~~~~~~~~~~

es_sfgtools follows a hierarchical data organization to manage data from multiple seafloor networks, stations, and campaigns. The workflow module assumes a directory structure as follows:

.. code-block:: text

   Main Directory/
   ├── network_name/           # e.g., "cascadia-gorda"
   │   ├── station_name/       # e.g., "NCC1", "NBR1"
   │   │   ├── campaign_name/  # e.g., "2025_A_1126"
   │   │   │   ├── raw/        # Raw data files
   │   │   │   ├── processed/  # Processed products
   │   │   │   ├── logs/       # Processing logs
   │   │   │   └── results/    # Analysis results


Sample GeoLab Workflow
~~~~~~~~~~~~~~~~~~~~~~

The following is a minimal example showing the steps for loading preprocessed data and running GARPOS modeling using the default settings.  

The run_garpos.ipynb notebook contains more details about setting custom filters and modeling parameters.


.. code-block:: python

   #!/usr/bin/env python3
   """
   Seafloor Geodesy Data Processing Demo - GeoLab Environment
   
   Demonstrates mid-process workflow for preparing data for GARPOS modeling in GEOLAB.
   """
   
    import os

    from es_sfgtools.config.env_config import Environment
    from es_sfgtools.workflows.workflow_handler import WorkflowHandler
    from earthscope_sdk import AsyncEarthScopeClient
    from earthscope_sdk.config.settings import SdkSettings

    #this reads environment variables set in the image
    Environment.load_working_environment()
   
    # Create an EarthScope client 
    es = AsyncEarthScopeClient()
    # Set AWS credentials for access to preprocessed data in S3
    creds = await es.user.get_aws_credentials(role="s3-seafloor")
    os.environ['AWS_ACCESS_KEY_ID'] = creds.aws_access_key_id
    os.environ['AWS_SECRET_ACCESS_KEY'] = creds.aws_secret_access_key
    os.environ['AWS_SESSION_TOKEN']  = creds.aws_session_token

    # Initialize workflow handler
    workflow = WorkflowHandler()
   
    # Select Network, Station, Campaign
    NETWORK = "cascadia-gorda"
    STATION = "NTH1"
    CAMPAIGN = "2025_A_1126"
   
    workflow.set_network_station_campaign(
        network_id=NETWORK,
        station_id=station,
        campaign_id=CAMPAIGN,
    )
    
    # Load data and prepare GARPOS input files
    workflow.midprocess_prep_garpos()
    
    # Run GARPOS
    workflow.modeling_run_garpos()

    # Plot results
    workflow.modeling_plot_garpos_results()




Getting Help
------------

**Documentation**

- :doc:`api` - Complete API reference

- `GitHub Repository <https://github.com/EarthScope/es_sfgtools>`_ - Source code and issues

**Community Support**

- GitHub Issues for bug reports and feature requests

- EarthScope forums for scientific discussions

- Tutorial workshops and webinars

**Contributing**

Contributions are welcome! See the repository for development guidelines and coding standards. 