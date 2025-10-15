from typing import Union,Dict,Any
from pydantic import BaseModel
from difflib import get_close_matches

def validate_keys_recursively(config_dict: dict, model_class: BaseModel, path: str = ""):
    """Recursively validate keys and suggest corrections for typos.

    Parameters
    ----------
    config_dict : dict
        The dictionary to validate.
    model_class : BaseModel
        The Pydantic model to validate against.
    path : str, optional
        The current path in the nested dictionary, for error reporting.
    
    Returns
    -------
    list
        A list of error messages.
    """
    valid_fields = set(model_class.model_fields.keys())
    errors = []

    for key, value in config_dict.items():
        if value is None:
            continue  # Skip None values
        current_path = f"{path}.{key}" if path else key

        if key not in valid_fields:
            # Find close matches for potential typos
            suggestions = get_close_matches(key, valid_fields, n=3, cutoff=0.6)
            if suggestions:
                suggestion_text = (
                    f" Did you mean: {', '.join(suggestions)}?" if suggestions else ""
                )
            else:
                suggestion_text = "No similar keys found."

            errors.append(
                f"Invalid key '{current_path}' in {model_class.__name__}. {suggestion_text}"
            )

        # If the value is a dict and the field exists, check nested structure
        elif isinstance(value, dict) and key in valid_fields:
            field_info = model_class.model_fields[key]
            # Get the annotation type
            if hasattr(field_info, "annotation"):
                annotation = field_info.annotation
                # Handle Optional types
                if (
                    hasattr(annotation, "__origin__")
                    and annotation.__origin__ is Union
                ):
                    annotation = annotation.__args__[0]  # Get first non-None type

                # If it's a BaseModel subclass, validate recursively
                if hasattr(annotation, "__bases__") and any(
                    issubclass(base, BaseModel)
                    for base in annotation.__bases__
                    if base != object
                ):
                    nested_errors = validate_keys_recursively(
                        value, annotation, current_path
                    )
                    errors.extend(nested_errors)

    return errors

def validate_and_merge_config(
     base_class: BaseModel, override_config: dict
) -> BaseModel:
    """Validates and merges override configuration with base config, checking for typos.

    Parameters
    ----------
    base_class : BaseModel
        The base configuration class instance (Pydantic model).
    override_config : dict
        The override configuration dictionary to update the base config.

    Returns
    -------
    BaseModel
        A new instance of the base_class with merged configuration.

    Raises
    ------
    ValueError
        If there are typos or invalid keys in the override_config.
    """

    # Check if the base class is a Pydantic model
    if not isinstance(base_class, BaseModel):
        raise TypeError("base_class must be an instance of a Pydantic BaseModel")
    if not isinstance(override_config, dict):
        raise TypeError("override_config must be a dictionary")


    # Check for typos and validate keys
    errors = validate_keys_recursively(override_config, base_class)
    if errors:
        raise ValueError("Configuration validation errors:\n" + "\n".join(errors))
    # If no errors, proceed to merge
   
    merged_config = base_class.model_copy(update=override_config)
    # create new instance to ensure validation
    updated_config = base_class.__class__(**dict(merged_config))
    return updated_config


if __name__ == "__main__":
    import os
    import sys
    from pathlib import Path

    os.environ["DYLD_LIBRARY_PATH"] = os.environ.get("CONDA_PREFIX", "") + "/lib"
    from es_sfgtools.pipelines.sv3_pipeline import SV3PipelineConfig
    # Validate keys and collect errors
    primary_config_true =  {
    "dfop00_config": {
      "override": False,
    },
    "novatel_config": {
      "n_processes": 14,
      "override": False
    },
    "position_update_config": {
      "override": False,
      "lengthscale": 0.1,
      "plot": False
    },
    "pride_config": {
      "cutoff_elevation": 1000,
      "end": None,
      "frequency": ["G12", "R12", "E15", "C26", "J12"],
      "high_ion": None,
      "interval": None,
      "local_pdp3_path": None,
      "loose_edit": True,
      "sample_frequency": 1,
      "start": None,
      "system": "GREC23J",
      "tides": "SOP",
      "override_products_download": False,
      "override": False
    },
    "rinex_config": {
      "n_processes": 5,
      "time_interval": 24,
      "override": False
    }
  }
    secondary_config_true = {
        "pride_config": {"cutoff_elevation": 10, "override": True}
    }

    sv3config = SV3PipelineConfig()
    errors = validate_keys_recursively(primary_config_true, SV3PipelineConfig)
    if errors:
        raise ValueError("Configuration validation errors:\n" + "\n".join(errors))

    sv3_config_new = validate_and_merge_config(sv3config, primary_config_true)
    print(sv3_config_new)

    sv3_config_new2 = validate_and_merge_config(sv3_config_new, secondary_config_true)
    print(sv3_config_new2)

    assert sv3_config_new2.pride_config.cutoff_elevation != sv3config.pride_config.cutoff_elevation != sv3_config_new.pride_config.cutoff_elevation

    update_dict_bogus = {
        "pride_config": {"cutoff_elevation": 10, "override": True, "bogus_key": 123}
    }
    try:
        sv3_config_bogus = validate_and_merge_config(sv3config, update_dict_bogus)
    except ValueError as e:
        print("Caught expected ValueError for bogus key:")
        print(e)
