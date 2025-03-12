import json
from datetime import datetime
from typing import Any, Optional, Union, Dict, List

from es_sfgtools.utils.metadata.benchmark import Benchmark, Transponder
from es_sfgtools.utils.metadata.campaign import Campaign, Survey
from es_sfgtools.utils.metadata.utils import (
    AttributeUpdater,
    check_dates,
    only_one_is_true,
    parse_datetime,
)
from pydantic import BaseModel, Field, field_validator

REFERENCE_FRAMES = "referenceFrames"
BENCHMARKS = "benchmarks"
CAMPAIGNS = "campaigns"

top_level_groups = [REFERENCE_FRAMES, BENCHMARKS, CAMPAIGNS]


def import_site(filepath: str):
    """Import site data from a JSON file."""
    with open(filepath, "r") as file:
        return Site(**json.load(file))


class ReferenceFrame(AttributeUpdater, BaseModel):
    # Required
    name: str

    #Optional
    start: Optional[datetime] = Field(default=None)
    end: Optional[datetime] = Field(default=None)

    _parse_datetime = field_validator('start', 'end', mode='before')(parse_datetime)
    _check_dates = field_validator('end', mode='after')(check_dates)


class ArrayCenter(BaseModel, AttributeUpdater):
    x: Optional[float] = Field(default=None)
    y: Optional[float] = Field(default=None)
    z: Optional[float] = Field(default=None)

class Site(BaseModel):
    # Required 
    names: List[str]
    networks: List[str]
    timeOrigin: datetime
    localGeoidHeight: float

    # Optional
    arrayCenter: Optional[ArrayCenter] = Field(default_factory=ArrayCenter)
    campaigns: List[Campaign] = Field(default_factory=list)
    benchmarks: List[Benchmark] = Field(default_factory=list)
    referenceFrames: List[ReferenceFrame] = Field(default_factory=list)

    def export_site(self, filepath: str): 
        with open(filepath, "w") as file:
            json.dump(self.model_dump_json(), file, indent=2)

    @classmethod
    def from_json(cls, filepath: str) -> 'Site':
        with open(filepath, "r") as file:
            return cls(**json.load(file))
        
    def print_json(self):
        print(self.model_dump_json(indent=2))


    def new_benchmark(self, benchmark_name: str, benchmark_data: dict):
        """Add a new benchmark to the site dictionary"""

        for benchmark in self.benchmarks:
            if benchmark.name == benchmark_name:
                print(
                    "ERROR: Benchmark already exists.. Choose to update or delete if needed."
                )
                return

        print("Adding new benchmark..")
        new_benchmark = Benchmark(benchmark_name, additional_data=benchmark_data)
        self.benchmarks.append(new_benchmark)
        print(json.dumps(new_benchmark.to_dict(), indent=2))
        print("Added benchmark.")

    def run_benchmark(
        self,
        benchmark_name: str,
        benchmark_data: dict,
        add_new: bool = False,
        update: bool = False,
        delete: bool = False,
    ):
        """Run a benchmark operation based on the provided flags"""


        if not only_one_is_true(add_new, update, delete):
            print("ERROR: Please select only one operation(Add/Update/Delete) to run..")
            return

        if add_new:
            self.new_benchmark(benchmark_name, benchmark_data)
        if update:
            self.update_existing_benchmark(benchmark_name, benchmark_data)
        if delete:
            self.delete_benchmark(benchmark_name)

    def update_existing_benchmark(self, benchmark_name: str, benchmark_data: dict):
        """Update an existing benchmark in the site dictionary"""

        print("Updating existing benchmark..")
        for benchmark in self.benchmarks:
            if benchmark.name == benchmark_name:
                benchmark.update_attributes(benchmark_data)
                print(json.dumps(benchmark.to_dict(), indent=2))
                print("Updated benchmark.")
                return

    def delete_benchmark(self, benchmark_name: str):
        """Delete a benchmark from the site dictionary"""

        print("Deleting benchmark {}..".format)
        for benchmark in self.benchmarks:
            if benchmark.name == benchmark_name:
                self.benchmarks.remove(benchmark)
                print("Deleted benchmark.")
                return

        print("ERROR: Benchmark {} not found to delete..".format(benchmark_name))

    def run_transponder(
        self,
        benchmark_name: str,
        transponder_address: str,
        transponder_data: dict,
        add_new: bool,
        update: bool,
        delete: bool,
    ):
        """Run a transponder operation based on the provided flags"""

        if not only_one_is_true(add_new, update, delete):
            print("ERROR: Please select only one operation(Add/Update/Delete) to run..")
            return

        if add_new:
            self.new_transponder(
                benchmark_name=benchmark_name,
                transponder_address=transponder_address,
                transponder_data=transponder_data,
            )
        if update:
            self.update_existing_transponder(
                benchmark_name=benchmark_name,
                transponder_address=transponder_address,
                transponder_data=transponder_data,
            )
        if delete:
            self.delete_transponder(
                benchmark_name=benchmark_name, transponder_address=transponder_address
            )

    def new_transponder(
        self, benchmark_name: str, transponder_address, transponder_data: dict
    ):
        """Add a new transponder to a benchmark in the site dictionary"""

        benchmark = next((b for b in self.benchmarks if b.name == benchmark_name), None)

        if benchmark is None:
            print(
                f"ERROR: Benchmark {benchmark_name} not found, ensure you have the correct benchmark name"
            )
            return

        if not transponder_address:
            print(
                "ERROR: Transponder address not provided, address is required for adding.."
            )
            return

        for transponder in benchmark.transponders:
            if transponder.address == transponder_address:
                print(
                    "ERROR: Transponder {} already exists in benchmark.. please update instead of adding.".format(
                        transponder_address
                    )
                )
                return

        print("Adding new transponder to benchmark {}..".format(benchmark_name))
        benchmark.transponders.append(
            Transponder(transponder_address, additional_data=transponder_data)
        )
        print(json.dumps(benchmark.to_dict(), indent=2))
        print("Added transponder to benchmark.")

    def add_sensor_to_transponder(
        self, benchmark_name: str, transponder_address: str, sensor_data: dict
    ):
        """Add a sensor to a transponder in a benchmark in the site dictionary"""

        benchmark = next((b for b in self.benchmarks if b.name == benchmark_name), None)

        if benchmark is None:
            print(
                f"ERROR: Benchmark {benchmark_name} not found, ensure you have the correct benchmark name"
            )
            return

        transponder = next(
            (t for t in benchmark.transponders if t.address == transponder_address),
            None,
        )

        if transponder is None:
            print(
                f"ERROR: Transponder with address {transponder_address} not found in benchmark {benchmark_name}"
            )
            return

        print(
            "Adding sensor to transponder {} in benchmark {}..".format(
                transponder_address, benchmark_name
            )
        )
        transponder.extraSensors.append(sensor_data)
        print(json.dumps(transponder.to_dict(), indent=2))
        print("Added sensor to transponder.")

    def add_battery_voltage_to_transponder(
        self, benchmark_name: str, transponder_address: str, battery_data: dict
    ):
        """Add a battery voltage to a transponder in a benchmark in the site dictionary"""
        benchmark = next((b for b in self.benchmarks if b.name == benchmark_name), None)

        if benchmark is None:
            print(
                f"ERROR: Benchmark {benchmark_name} not found, ensure you have the correct benchmark name"
            )
            return

        transponder = next(
            (t for t in benchmark.transponders if t.address == transponder_address),
            None,
        )

        if transponder is None:
            print(
                f"ERROR: Transponder with address {transponder_address} not found in benchmark {benchmark_name}"
            )
            return

        print(
            "Adding battery voltage to transponder {} in benchmark {}..".format(
                transponder_address, benchmark_name
            )
        )
        transponder.batteryVoltage.append(battery_data)
        print(json.dumps(transponder.to_dict(), indent=2))
        print("Added battery voltage to transponder.")

    def update_existing_transponder(
        self, benchmark_name: str, transponder_address: str, transponder_data: dict
    ):
        """Update an existing transponder in a benchmark in the site dictionary"""
        benchmark = next((b for b in self.benchmarks if b.name == benchmark_name), None)

        if benchmark is None:
            print(
                f"ERROR: Benchmark {benchmark_name} not found, ensure you have the correct benchmark name"
            )
            return

        transponder = next(
            (t for t in benchmark.transponders if t.address == transponder_address),
            None,
        )

        if transponder is None:
            print(
                f"ERROR: Transponder with address {transponder_address} not found in benchmark {benchmark_name}"
            )
            return

        print("Updating transponder in benchmark {}..".format(benchmark_name))
        transponder.update_attributes(transponder_data)
        print(json.dumps(benchmark.to_dict(), indent=2))
        print("Updated transponder.")

    def delete_transponder(self, benchmark_name: str, transponder_address: str):
        benchmark = next((b for b in self.benchmarks if b.name == benchmark_name), None)

        if benchmark is None:
            print(
                f"ERROR: Benchmark {benchmark_name} not found, ensure you have the correct benchmark name"
            )
            return

        transponder = next(
            (t for t in benchmark.transponders if t.address == transponder_address),
            None,
        )

        if transponder is None:
            print(
                f"ERROR: Transponder with address {transponder_address} not found in benchmark {benchmark_name}"
            )
            return

        print("Deleting transponder in benchmark {}..".format(benchmark_name))
        for transponder in benchmark.transponders:
            if transponder.address == transponder_address:
                benchmark.transponders.remove(transponder)
                print("Deleted transponder.")
                return

        print("ERROR: Transponder not found..")
        
    def run_campaign(
        self,
        campaign_name: str,
        campaign_data: dict,
        add_new: bool = False,
        update: bool = False,
        delete: bool = False,
    ):
        """Run a campaign operation based on the provided flags"""

        if not only_one_is_true(add_new, update, delete):
            print("ERROR: Please select only one operation(Add/Update/Delete) to run..")
            return

        if add_new:
            self.new_campaign(campaign_name, campaign_data)
        if update:
            self.update_existing_campaign(campaign_name, campaign_data)
        if delete:
            self.delete_campaign(campaign_name)

    def new_campaign(self, campaign_name: str, campaign_data: dict):
        """Add a new campaign to the site dictionary"""
        for campaign in self.campaigns:
            if campaign.name == campaign_name:
                print(
                    "ERROR: Campaign already exists.. Choose to update or delete if needed"
                )
                return

        print("Adding new campaign..")
        new_campaign = Campaign(campaign_name, additional_data=campaign_data)
        self.campaigns.append(new_campaign)
        print(json.dumps(new_campaign.to_dict(), indent=2))
        print("Added campaign.")

    def update_existing_campaign(self, campaign_name: str, campaign_data: dict):
        """Update an existing campaign in the site dictionary"""
        print("Updating existing campaign..")
        for campaign in self.campaigns:
            if campaign.name == campaign_name:
                campaign.update_attributes(campaign_data)
                print(json.dumps(campaign.to_dict(), indent=2))
                print("Updated campaign.")
                return

        print("ERROR: Campaign not found..")

    def delete_campaign(self, campaign_name: str):
        """Delete a campaign from the site dictionary"""
        print("Deleting campaign {}..".format(campaign_name))
        for campaign in self.campaigns:
            if campaign.name == campaign_name:
                self.campaigns.remove(campaign)
                print("Deleted campaign.")
                return

        print("ERROR: Campaign {} not found to be deleted..".format(campaign_name))

    def run_survey(
        self,
        campaign_name: str,
        survey_data: dict,
        add_new: bool = False,
        update: bool = False,
        delete: bool = False,
        survey_id: str = None,
    ):
        """Run a survey operation based on the provided flags"""


        if not only_one_is_true(add_new, update, delete):
            print("ERROR: Please select only one operation(Add/Update/Delete) to run..")
            return

        if add_new:
            self.new_survey(
                campaign_name=campaign_name,
                survey_data=survey_data,
                survey_id=survey_id,
            )
        if update:
            if not survey_id:
                print(
                    "ERROR: Survey ID not provided, please provide a survey ID to update.."
                )
                return
            self.update_existing_survey(
                campaign_name=campaign_name,
                survey_id=survey_id,
                survey_data=survey_data,
            )
        if delete:
            if not survey_id:
                print(
                    "ERROR: Survey ID not provided, please provide a survey ID to delete.."
                )
            self.delete_survey(campaign_name=campaign_name, survey_id=survey_id)

    def new_survey(self, campaign_name: str, survey_data: dict, survey_id: str = None):
        """Add a new survey to a campaign in the site dictionary"""
        campaign = next((c for c in self.campaigns if c.name == campaign_name), None)

        if campaign is None:
            print(
                f"ERROR: Campaign {campaign_name} not found, ensure you have the correct campaign name"
            )
            return

        if survey_id:
            campaign_survey = next(
                (
                    survey
                    for survey in campaign.surveys
                    if survey.survey_id == survey_id
                ),
                None,
            )
            if campaign_survey:
                print(
                    f"ERROR: Survey with ID {survey_id} already exists in campaign {campaign_name}"
                )
                return

        if not survey_id:
            survey_id = campaign_name + "_" + str(len(campaign.surveys) + 1)
            print("Generating new survey ID: " + survey_id)

        print("Adding survey to campaign {}..".format(campaign_name))
        campaign.surveys.append(
            Survey(survey_id=survey_id, additional_data=survey_data)
        )
        print(json.dumps(campaign.to_dict(), indent=2))
        print("Added survey to campaign..")

    def update_existing_survey(
        self, campaign_name: str, survey_id: str, survey_data: dict
    ):
        campaign = next((c for c in self.campaigns if c.name == campaign_name), None)
        if campaign is None:
            print(
                f"ERROR: Campaign {campaign_name} not found, ensure you have the correct campaign name"
            )
            return

        campaign_survey = next(
            (survey for survey in campaign.surveys if survey.survey_id == survey_id),
            None,
        )
        if campaign_survey is None:
            print(
                f"ERROR: Survey with ID {survey_id} not found in campaign {campaign_name}"
            )
            return

        print("Updating survey {} in campaign {}..".format(survey_id, campaign_name))
        campaign_survey.update_attributes(survey_data)
        print(json.dumps(campaign.to_dict(), indent=2))
        print("Updated survey {} in campaign {}.".format(survey_id, campaign_name))

    def delete_survey(self, campaign_name: str, survey_id: str):
        """Delete a survey from a campaign in the site/campaign dictionary"""

        campaign = next((c for c in self.campaigns if c.name == campaign_name), None)
        if campaign is None:
            print(
                f"ERROR: Campaign {campaign_name} not found, ensure you have the correct campaign name"
            )
            return

        campaign_survey = next(
            (survey for survey in campaign.surveys if survey.id == survey_id), None
        )
        if campaign_survey is None:
            print(
                f"ERROR: Survey with ID {survey_id} not found in campaign {campaign_name}"
            )
            return

        print("Deleting survey {} in campaign {}..".format(survey_id, campaign_name))
        for survey in campaign.surveys:
            if survey.id == survey_id:
                campaign.surveys.remove(survey)
                print("Deleted survey.")
                return
        print(
            "ERROR: Survey {} not found in campaign {}..".format(
                survey_id, campaign_name
            )
        )

    def run_reference_frame(
        self,
        reference_frame_name: str,
        reference_frame_data: dict,
        add_new: bool = False,
        update: bool = False,
        delete: bool = False,
    ):
        """Run a reference frame operation based on the provided flags"""


        if not only_one_is_true(add_new, update, delete):
            print("ERROR: Please select only one operation(Add/Update/Delete) to run..")
            return

        if add_new:
            self.new_reference_frame(reference_frame_name, reference_frame_data)
        if update:
            self.update_existing_reference_frame(
                reference_frame_name, reference_frame_data
            )
        if delete:
            self.delete_reference_frame(reference_frame_name)

    def new_reference_frame(
        self, reference_frame_name: str, reference_frame_data: dict
    ):
        """Add a new reference frame to the site dictionary"""

        for reference_frame in self.referenceFrames:
            if reference_frame.name == reference_frame_name:
                print(
                    "ERROR: Reference frame already exists.. Choose to update or delete if needed"
                )
                return

        print("Adding new reference frame..")
        new_reference_frame = ReferenceFrame(reference_frame_name)
        new_reference_frame.update_attributes(reference_frame_data)
        self.referenceFrames.append(new_reference_frame)
        print(json.dumps(new_reference_frame.to_dict(), indent=2))
        print("Added reference frame.")

    def update_existing_reference_frame(
        self, reference_frame_name: str, reference_frame_data: dict
    ):
        """Update an existing reference frame in the site dictionary"""

        print("Updating existing reference frame..")
        for reference_frame in self.referenceFrames:
            if reference_frame.name == reference_frame_name:
                reference_frame.update_attributes(reference_frame_data)
                print(json.dumps(reference_frame.to_dict(), indent=2))
                print("Updated reference frame.")
                return

        print("ERROR: Reference frame not found..")

    def delete_reference_frame(self, reference_frame_name: str):
        """Delete a reference frame from the site dictionary"""

        print("Deleting reference frame..")
        for reference_frame in self.referenceFrames:
            if reference_frame.name == reference_frame_name:
                self.referenceFrames.remove(reference_frame)
                print("Deleted reference frame..")
                return

        print("ERROR: Reference frame not found..")

    @classmethod
    def from_json(cls, file: str) -> "Site":
        with open(file, "r") as buf:
            data = json.load(buf)
        instance = Site()
        instance.import_existing_site(data)
        return instance
