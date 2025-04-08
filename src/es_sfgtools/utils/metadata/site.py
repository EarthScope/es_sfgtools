from enum import Enum
import json
from datetime import datetime
from typing import Any, ClassVar, Optional, Union, Dict, List

from es_sfgtools.utils.metadata.benchmark import TAT, Benchmark, Transponder
from es_sfgtools.utils.metadata.campaign import Campaign, Survey
from es_sfgtools.utils.metadata.utils import (
    AttributeUpdater,
    check_dates,
    only_one_is_true,
    parse_datetime,
    if_zero_than_none,
)
from pydantic import BaseModel, Field, ValidationError, field_validator


class TopLevelSiteGroups(str, Enum):
    REFERENCE_FRAMES = "referenceFrames"
    BENCHMARKS = "benchmarks"
    CAMPAIGNS = "campaigns"


class SubLevelSiteGroups(str, Enum):
    SURVEYS = "surveys"
    TRANSPONDERS = "transponders"


def import_site(filepath: str):
    """Import site data from a JSON file."""
    with open(filepath, "r") as file:
        return Site(**json.load(file))


class ReferenceFrame(AttributeUpdater, BaseModel):
    # Required
    name: str = Field(..., description="The name of the reference frame")

    # Optional
    start: Optional[datetime] = Field(
        default=None,
        description="The start date of the reference frame used for the site",
        ge=datetime(1901, 1, 1),
    )
    end: Optional[datetime] = Field(
        default=None,
        description="The end date of the reference frame used for the site",
        ge=datetime(1901, 1, 1),
    )

    _parse_datetime = field_validator("start", "end", mode="before")(parse_datetime)
    _check_dates = field_validator("end", mode="after")(check_dates)


class ArrayCenter(BaseModel, AttributeUpdater):
    x: Optional[float] = Field(
        default=None, description="The x coordinate of the array center"
    )
    y: Optional[float] = Field(
        default=None, description="The y coordinate of the array center"
    )
    z: Optional[float] = Field(
        default=None, description="The z coordinate of the array center"
    )

    _if_zero_than_none = field_validator("x", "y", "z")(if_zero_than_none)


class Site(BaseModel):
    # Required
    names: List[str] = Field(
        ..., description="The names of the site, including the 4 character ID"
    )
    networks: List[str] = Field(..., description="A list networks the site is part of")
    timeOrigin: datetime = Field(
        ..., description="The time origin of the site", ge=datetime(1901, 1, 1)
    )
    localGeoidHeight: float = Field(
        ..., description="The local geoid height of the site"
    )

    # Optional
    arrayCenter: Optional[ArrayCenter] = Field(
        default_factory=dict, description="The array center of the site"
    )
    campaigns: List[Campaign] = Field(
        default_factory=list, description="The campaigns associated with the site"
    )
    benchmarks: List[Benchmark] = Field(
        default_factory=list, description="The benchmarks associated with the site"
    )
    referenceFrames: List[ReferenceFrame] = Field(
        default_factory=list, description="The reference frames used for the site"
    )

    # Map of top-level groups to their respective lists and classes - used for adding, updating and deleting items
    top_level_map_components: ClassVar[Dict[str, Any]] = {
        TopLevelSiteGroups.REFERENCE_FRAMES: (
            lambda self: self.referenceFrames,
            ReferenceFrame,
        ),
        TopLevelSiteGroups.BENCHMARKS: (lambda self: self.benchmarks, Benchmark),
        TopLevelSiteGroups.CAMPAIGNS: (lambda self: self.campaigns, Campaign),
    }

    _parse_datetime = field_validator("timeOrigin")(parse_datetime)

    def export_site(self, filepath: str):
        with open(filepath, "w") as file:
            file.write(self.model_dump_json(indent=2))

    @classmethod
    def from_json(cls, filepath: str) -> "Site":
        with open(filepath, "r") as file:
            return cls(**json.load(file))

    def print_json(self):
        print(self.model_dump_json(indent=2))

    def validate_components(self):
        """
        If there are no benchmarks, transponders, campaigns, or surveys, print a warning.
        """
        num_of_invalid_components = 0
        if not self.benchmarks:
            num_of_invalid_components += 1
            print("WARNING: No benchmarks found in the site.")
        else:
            for benchmark in self.benchmarks:
                if not benchmark.transponders:
                    num_of_invalid_components += 1
                    print("WARNING: No transponders found in benchmark", benchmark.name)

        if not self.campaigns:
            num_of_invalid_components += 1
            print("WARNING: No campaigns found in the site.")
        else:
            for campaign in self.campaigns:
                if not campaign.surveys:
                    num_of_invalid_components += 1
                    print("WARNING: No surveys found in campaign", campaign.name)
                else:
                    try:
                        campaign.check_survey_times()
                    except ValueError as e:
                        print(e)
                        num_of_invalid_components += 1

        if num_of_invalid_components == 0:
            print("All components are valid.")
        else:
            print(
                f"Please check the {num_of_invalid_components} warnings above and add required information prior to submitting to Earthscope."
                + "\n You can still write out to JSON file and come back to work on the other components in the notebook later."
            )

    def return_tats_for_campaign(
        self, campaign_name: str
    ) -> Optional[List[Dict[str, Any]]]:
        """
        Return all TATs for a given campaign

        Args:
            campaign_name (str): The name of the campaign
        Returns:
            List[Dict[str, Any]]: A list of dictionaries containing Benchmark name, Transponder address, and TAT

        """

        tat_list = []
        tat_info = {}
        for campaign in self.campaigns:
            if campaign.name == campaign_name:

                for benchmark in self.benchmarks:
                    for transponder in benchmark.transponders:
                        TAT = transponder.get_tat_by_datetime(campaign.start)
                        tat_info["Benchmark"] = benchmark.name
                        tat_info["Transponder"] = transponder.address
                        tat_info["TAT"] = TAT
                        tat_list.append(tat_info)

                return tat_list

        print(f"ERROR: Campaign {campaign_name} not found..")
        return None

    def run_component(
        self,
        component_type: TopLevelSiteGroups,
        component_metadata: dict,
        add_new: bool = False,
        update: bool = False,
        delete: bool = False,
    ):
        """
        Generic add, update or delete equipment for the site
        """

        if not only_one_is_true(add_new, update, delete):
            print("ERROR: Please select only one operation(Add/Update/Delete) to run..")
            return

        if not component_metadata["name"]:
            print(
                "ERROR: Equipment name not provided, please provide an equipment ID to add, update or delete.."
            )
            return

        if add_new:
            self._new_component(component_type, component_metadata)
        if update:
            self._update_existing_component(component_type, component_metadata)
        if delete:
            self._delete_equipment(component_type, component_metadata["name"])

    def _new_component(
        self, component_type: TopLevelSiteGroups, component_metadata: dict
    ):
        """
        Add a new equipment to the site dictionary
        """

        equipment_list, equipment_class = self.top_level_map_components[component_type]
        equipment_list = equipment_list(self)

        for equipment in equipment_list:
            if equipment.name == component_metadata["name"]:
                print(
                    f"ERROR: {component_type} {component_metadata['name']} already exists.. Choose to update or delete if needed."
                )
                print(equipment.model_dump_json(indent=2))
                return

        try:
            new_equipment = equipment_class(**component_metadata)
        except ValidationError as e:
            print(f"Validation error for {component_type}: {e}")
            return

        equipment_list.append(new_equipment)
        print(new_equipment.model_dump_json(indent=2))
        print(f"New {component_type} added successfully.")

    def _update_existing_component(
        self, component_type: TopLevelSiteGroups, component_metadata: dict
    ):
        """
        Update an existing equipment in the site dictionary
        """

        print(f"Updating existing {component_type} {component_metadata['name']}..")

        equipment_list, _ = self.top_level_map_components[component_type]
        equipment_list = equipment_list(self)

        for equipment in equipment_list:
            if equipment.name == component_metadata["name"]:
                equipment.update_attributes(component_metadata)
                print(equipment.model_dump_json(indent=2))
                print(f"Updated {component_type} {component_metadata['name']}.")
                return

        print(f"ERROR: {component_type} {component_metadata['name']} not found..")

    def _delete_equipment(self, equipment_type: str, equipment_name: str):
        """Delete an equipment from the site dictionary"""

        print(f"Deleting {equipment_type} {equipment_name}..")

        equipment_list, _ = self.top_level_map_components[equipment_type]
        equipment_list = equipment_list(self)

        for equipment in equipment_list:
            if equipment.name == equipment_name:
                equipment_list.remove(equipment)
                print(f"Deleted {equipment_type} {equipment_name}.")
                return

        print(f"ERROR: {equipment_type} {equipment_name} not found..")

    def run_sub_component(
        self,
        component_type: TopLevelSiteGroups,
        component_name: str,
        sub_component_type: SubLevelSiteGroups,
        sub_component_metadata: dict,
        add_new: bool = False,
        update: bool = False,
        delete: bool = False,
    ):
        """
        Generic add, update or delete sub-components (e.g Transponder attached to Benchmark, Survey to campaign)
        for the site.
        """

        TRANSPONDER_UNIQUE_ID = "address"
        SURVEY_UNIQUE_ID = "id"

        if not only_one_is_true(add_new, update, delete):
            print("ERROR: Please select only one operation(Add/Update/Delete) to run..")
            return

        if sub_component_type == SubLevelSiteGroups.TRANSPONDERS:
            if not sub_component_metadata[TRANSPONDER_UNIQUE_ID]:
                print("ERROR: Required transponder address not provided")

        elif sub_component_type == SubLevelSiteGroups.SURVEYS:
            if not sub_component_metadata[SURVEY_UNIQUE_ID]:
                print("ERROR: Required survey ID not provided")

        else:
            print(
                f"ERROR: {sub_component_type} not recognised, please provide a valid type.."
            )
            return

        if add_new:
            self._new_sub_component(
                component_type=component_type,
                component_name=component_name,
                sub_component_type=sub_component_type,
                sub_component_metadata=sub_component_metadata,
            )
        if update:
            self._update_existing_sub_component(
                component_type=component_type,
                component_name=component_name,
                sub_component_type=sub_component_type,
                sub_component_metadata=sub_component_metadata,
            )
        if delete:
            if sub_component_type == SubLevelSiteGroups.TRANSPONDERS:
                name = sub_component_metadata["address"]
            elif sub_component_type == SubLevelSiteGroups.SURVEYS:
                name = sub_component_metadata["id"]

            self._delete_sub_component(
                component_type=component_type,
                component_name=component_name,
                sub_component_type=sub_component_type,
                sub_component_name=name,
            )

    def _new_sub_component(
        self,
        component_type: TopLevelSiteGroups,
        component_name: str,
        sub_component_type: SubLevelSiteGroups,
        sub_component_metadata: dict,
    ):
        """
        Add a new sub-component (Transponder, Survey) to the site dictionary
        """

        component_list, component_class = self.top_level_map_components[component_type]
        component_list = component_list(self)

        for component in component_list:
            if component.name == component_name:

                if sub_component_type == SubLevelSiteGroups.TRANSPONDERS:
                    for transponder in component.transponders:
                        if transponder.address == sub_component_metadata["address"]:
                            print(
                                f"ERROR: Transponder {sub_component_metadata['address']} already exists.. Choose to update or delete if needed."
                            )
                            print(transponder.model_dump_json(indent=2))
                            return

                    try:
                        new_transponder = Transponder(**sub_component_metadata)
                    except ValidationError as e:
                        print(f"Validation error for transponder: {e}")
                        return

                    component.transponders.append(new_transponder)
                    print(new_transponder.model_dump_json(indent=2))
                    print(f"New transponder added successfully.")
                    return

                elif sub_component_type == SubLevelSiteGroups.SURVEYS:
                    num_of_surveys = 0
                    for survey in component.surveys:
                        if survey.id == sub_component_metadata["id"]:
                            print(
                                f"ERROR: Survey {sub_component_metadata['id']} already exists.. Choose to update or delete if needed."
                            )
                            print(survey.model_dump_json(indent=2))
                            return
                        num_of_surveys = len(component.surveys)

                    if not sub_component_metadata["id"]:
                        # Generate a new survey ID if not provided
                        sub_component_metadata["id"] = (
                            f"{component_name}_{num_of_surveys + 1}"
                        )

                    try:
                        new_survey = Survey(**sub_component_metadata)
                    except ValidationError as e:
                        print(f"Validation error for survey: {e}")
                        return
                    component.surveys.append(new_survey)
                    print(new_survey.model_dump_json(indent=2))
                    print(f"New survey added successfully.")
                    return

        print(f"ERROR: {component_type} {component_name} not found..")

    def _update_existing_sub_component(
        self,
        component_type: TopLevelSiteGroups,
        component_name: str,
        sub_component_type: SubLevelSiteGroups,
        sub_component_metadata: dict,
    ):
        """
        Update an existing sub-component(Transponder, Survey) in the site dictionary
        """

        equipment_list, _ = self.top_level_map_components[component_type]
        equipment_list = equipment_list(self)

        for equipment in equipment_list:
            if equipment.name == component_name:
                if sub_component_type == SubLevelSiteGroups.TRANSPONDERS:
                    for transponder in equipment.transponders:
                        if transponder.address == sub_component_metadata["address"]:

                            if "extraSensors" in sub_component_metadata:
                                transponder.extraSensors.append(
                                    sub_component_metadata["extraSensors"]
                                )
                                print(
                                    f"Added sensor to transponder {transponder.address}."
                                )

                            elif "batteryVoltage" in sub_component_metadata:
                                transponder.batteryVoltage.append(
                                    sub_component_metadata["batteryVoltage"]
                                )
                                print(
                                    f"Added battery voltage to transponder {transponder.address}."
                                )

                            elif "new_tat" in sub_component_metadata:

                                new_tat: TAT = sub_component_metadata["new_tat"]
                                for tat in transponder.tat:
                                    if tat.value == new_tat.value:
                                        # Only add start and end times to original tat
                                        tat.timeIntervals.append(
                                            new_tat.timeIntervals[0]
                                        )
                                        print(
                                            f"Added time interval to TAT {tat.value} for transponder {transponder.address}."
                                        )
                                        return

                                transponder.tat.append(new_tat)
                                print(
                                    f"Added new TAT to transponder {transponder.address}."
                                )

                            else:
                                transponder.update_attributes(sub_component_metadata)
                                print(f"Updated Transponder {transponder.address}.")

                            print(transponder.model_dump_json(indent=2))
                            return

                elif sub_component_type == SubLevelSiteGroups.SURVEYS:
                    for survey in equipment.surveys:
                        if survey.id == sub_component_metadata["id"]:
                            survey.update_attributes(sub_component_metadata)
                            print(survey.model_dump_json(indent=2))
                            return

    def _delete_sub_component(
        self,
        component_type: TopLevelSiteGroups,
        component_name: str,
        sub_component_type: SubLevelSiteGroups,
        sub_component_name: str,
    ):
        """Delete a sub-component from the site dictionary"""

        equipment_list, _ = self.top_level_map_components[component_type]
        equipment_list = equipment_list(self)

        for equipment in equipment_list:
            if equipment.name == component_name:
                if sub_component_type == SubLevelSiteGroups.TRANSPONDERS:
                    for transponder in equipment.transponders:
                        if transponder.address == sub_component_name:
                            equipment.transponders.remove(transponder)
                            print(f"Deleted Transponder {sub_component_name}.")

                elif sub_component_type == SubLevelSiteGroups.SURVEYS:
                    for survey in equipment.surveys:
                        if survey.id == sub_component_name:
                            equipment.surveys.remove(survey)
                            print(f"Deleted survey {sub_component_name}.")


if __name__ == "__main__":
    example_json_filepath = "json_schemas/site_example.json"
    site = Site.from_json(example_json_filepath)
    site.print_json()
    site.validate_components()

    # test_time = datetime(year=2025, month=1, day=1, hour=1, minute=0, second=0)
    # for benchmark in site.benchmarks:
    #     for transponder in benchmark.transponders:
    #         tat = transponder.get_tat_by_datetime(test_time)
    #         print(tat)

    # for campaign in site.campaigns:
    #     survey = campaign.get_survey_by_datetime(test_time)
    #     print(survey)

    tat_list = site.return_tats_for_campaign("Campaign1")
    print(tat_list)
