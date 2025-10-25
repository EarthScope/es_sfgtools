import mmap
from datetime import datetime
from enum import Enum
from pathlib import Path
from typing import Any, Dict, Optional, Union, List

from pydantic import BaseModel, Field, field_serializer, field_validator, root_validator


from es_sfgtools.config.file_config import AssetType


class _AssetBase(BaseModel):
    local_path: Optional[Union[str, Path]] = Field(default=None)
    remote_path: Optional[str] = Field(default=None)
    remote_type: Optional[str] = Field(default=None)
    type: Optional[AssetType] = Field(default=None)
    id: Optional[int] = Field(default=None)
    network: Optional[str] = Field(default=None)
    station: Optional[str] = Field(default=None)
    campaign: Optional[str] = Field(default=None)
    is_processed: Optional[bool] = Field(default=False)

    timestamp_data_start: Optional[datetime] = Field(default=None)
    timestamp_data_end: Optional[datetime] = Field(default=None)
    timestamp_created: Optional[datetime] = Field(default=None)

    @field_validator("type", mode="before")
    def _check_type(cls, type_value: Union[str, AssetType]):
        if isinstance(type_value, str):
            type_value = AssetType(type_value)
        return type_value

    @field_serializer("type", when_used="always")
    def _serialize_type(self, type_value: Union[str, AssetType]):
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
    def _serialize_local_path(self, local_path_value: Union[str, Path]):
        if isinstance(local_path_value, Path):
            return str(local_path_value)
        return local_path_value

    class Config:
        arbitrary_types_allowed = True


class AssetEntry(_AssetBase):
    parent_id: Optional[int] = Field(default=None)

    def to_update_dict(self) -> Dict[str, Any]:
        # Drop the id  
        model_dict = self.model_dump()
        model_dict.pop("id")
        return model_dict
