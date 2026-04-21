from datetime import datetime
from pathlib import Path
from typing import Any

from pydantic import BaseModel, Field, field_serializer, field_validator, root_validator

from ...config.file_config import AssetType


class _AssetBase(BaseModel):
    local_path: str | Path | None = Field(default=None)
    remote_path: str | None = Field(default=None)
    remote_type: str | None = Field(default=None)
    type: AssetType | None = Field(default=None)
    id: int | None = Field(default=None)
    network: str | None = Field(default=None)
    station: str | None = Field(default=None)
    campaign: str | None = Field(default=None)
    is_processed: bool | None = Field(default=False)

    timestamp_data_start: datetime | None = Field(default=None)
    timestamp_data_end: datetime | None = Field(default=None)
    timestamp_created: datetime | None = Field(default=None)

    @field_validator("type", mode="before")
    def _check_type(cls, type_value: str | AssetType):
        if isinstance(type_value, str):
            type_value = AssetType(type_value)
        return type_value

    @field_serializer("type", when_used="always")
    def _serialize_type(self, type_value: str | AssetType):
        if isinstance(type_value, AssetType):
            return type_value.value
        return type_value

    @root_validator(pre=True)
    def _check_at_least_one(cls, values):
        if not values.get("local_path") and not values.get("remote_path"):
            raise ValueError("At least one of the following must be set: local_path, remote_path")

        if isinstance(values.get("local_path"), str):
            values["local_path"] = Path(values["local_path"])

        return values

    @field_serializer("local_path", when_used="always")
    def _serialize_local_path(self, local_path_value: str | Path):
        if isinstance(local_path_value, Path):
            return str(local_path_value)
        return local_path_value

    class Config:
        arbitrary_types_allowed = True


class AssetEntry(_AssetBase):
    parent_id: int | None = Field(default=None)

    def to_update_dict(self) -> dict[str, Any]:
        # Drop the id
        model_dict = self.model_dump()
        model_dict.pop("id")
        return model_dict
