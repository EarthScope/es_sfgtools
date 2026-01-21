from pathlib import Path
from es_sfgtools.workflows.workflow_handler import WorkflowHandler

if __name__ == "__main__":
    main_dir = Path("/Volumes/DunbarSSD/Project/SeafloorGeodesy/SFGMain")
    wfh = WorkflowHandler(main_dir)
    network = "cascadia-gorda"
    station = "NCC1"
    campaign = "2025_A_1126"
    wfh.set_network_station_campaign(
        network_id=network, station_id=station, campaign_id=campaign
    )
    # wfh.preprocess_run_pipeline_sv3(job='process_dfop00',primary_config={
    #     'dfop00_config': {'override': True}
    # })
    # wfh.preprocess_run_pipeline_sv3(job='refine_shotdata',primary_config={
    #     'position_update_config': {'override': True}
    # })

    wfh.midprocess_parse_surveys(
        override=True, write_intermediate=True
    )
    wfh.midprocess_prep_garpos(
        override=True, write_intermediate=True
    )