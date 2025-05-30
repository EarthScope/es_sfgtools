### NOTES:

to inject a cloned garpos repository as a dependency, use the env variable "GARPOS_PATH"


### Garpos Handler

```python
    from es_sfgtools.utils.loggers import BaseLogger
    BaseLogger.route_to_console()
    from sfg_metadata import META_DATA, GEOLAB_DATA

    main_dir = Path("/Path/to/data/SFGMain")

    network = "cascadia-gorda"
    station = "NCC1"
    campaign = "2024_A_1126"

    dh = DataHandler(main_dir,GEOLAB_DATA)
    dh.change_working_station(network=network, station=station, campaign=campaign)

    gp_handler_ncc1 = dh.get_garpos_handler(
        site_data=META_DATA.networks[network].stations[station]
    )
    gp_handler_ncc1.set_campaign(campaign)

    gp_handler_ncc1.prep_shotdata(overwrite=False)

    update_dict = {"rejectcriteria": 2.5,"log_lambda":[0]}

    gp_handler_ncc1.set_inversion_params(update_dict)

    gp_handler_ncc1.run_garpos(campaign_id='2024_A_1126',run_id=0,override=True)

    gp_handler_ncc1.plot_ts_results(campaign_name='2024_A_1126'survey_id="2024_A_1126_1",res_filter=20)
```
