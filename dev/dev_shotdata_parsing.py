from pathlib import Path
from es_sfgtools.workflows.workflow_handler import WorkflowHandler

main_dir = Path("/Volumes/DunbarSSD/Project/SeafloorGeodesy/SFGMain")
wfh = WorkflowHandler(main_dir)
network = "cascadia-gorda"
station = "GCC1"
campaign = "2025_A_1126"
wfh.set_network_station_campaign(
    network_id=network, station_id=station, campaign_id=campaign
)
wfh.midprocess_parse_surveys(
    override=True,write_intermediate=True
)