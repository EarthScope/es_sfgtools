from pathlib import Path
import os
from typing import Callable,Tuple
import sys
from ...logging import GarposLogger as logger
import importlib.util
from types import ModuleType
from typing import Callable

def load_lib() -> Tuple[str,str]:

    """
    Loads the required library paths for GARPOS and validates their existence.
    This function retrieves the GARPOS_PATH environment variable, verifies its existence,
    and searches for the `f90lib` directory within it. It then ensures the presence of
    the `lib_raytrace.so` file inside the `f90lib` directory. If any of these components
    are missing, appropriate exceptions are raised.
    Returns:
        Tuple[str, str]: A tuple containing the paths to the `f90lib` directory and the
        `lib_raytrace.so` file as strings.
    Raises:
        FileNotFoundError: If GARPOS_PATH does not exist, the `f90lib` directory is not
        found, or the `lib_raytrace.so` file is missing.
    """

    garpos_path = os.getenv("GARPOS_PATH", None)
    if garpos_path is None or garpos_path == 'None':
        return
    garpos_path = Path(garpos_path)
    if not garpos_path.exists():
        raise FileNotFoundError(f"GARPOS_PATH {garpos_path} does not exist")
    logger.loginfo(
        f"Found GARPOS_PATH: {garpos_path}"
    )
    f90lib_path = None
    # find /f90lib dir
    for dirs in garpos_path.glob("**/f90lib"):
        if dirs.is_dir():
            f90lib_path = dirs
            logger.loginfo(
                f"Found f90lib directory: {f90lib_path}"
            )
            break
    if f90lib_path is None:
        raise FileNotFoundError("f90lib directory not found in GARPOS_PATH")
    
    # find libraytrace.so

    lib_raytrace = f90lib_path / "lib_raytrace.so"
    if not lib_raytrace.exists():
        raise FileNotFoundError("lib_raytrace.so not found in f90lib directory")
    logger.loginfo(
        f"Found lib_raytrace.so: {lib_raytrace}"
    )
    return str(f90lib_path), str(lib_raytrace)

def load_drive_garpos() -> Callable:
    """
    Loads the `drive_garpos` function from the `garpos_main.py` module located
    within the directory specified by the `GARPOS_PATH` environment variable.

    This function performs the following steps:
    1. Retrieves the `GARPOS_PATH` environment variable and validates its existence.
    2. Searches for the `garpos_main.py` file within the `GARPOS_PATH` directory.
    3. Dynamically imports the `garpos_main` module and retrieves the `drive_garpos` function.

    Returns:
        Callable: The `drive_garpos` function from the `garpos_main.py` module.

    Raises:
        FileNotFoundError: If the `GARPOS_PATH` environment variable is not set,
                           the path does not exist, or the `garpos_main.py` file
                           is not found within the directory.
        AttributeError: If the `drive_garpos` function is not found in the
                        `garpos_main` module.
    """

    garpos_path = Path(os.getenv("GARPOS_PATH"))
    if not garpos_path.exists():
        raise FileNotFoundError(f"GARPOS_PATH {garpos_path} does not exist")
    logger.loginfo(
        f"Found GARPOS_PATH: {garpos_path}"
    )

    # find the function drive_garpos in garpos_main.py
    garpos_main = None
    for file in list(garpos_path.rglob("*.py")):
        if "garpos_main" in file.name:
            garpos_main = file
            logger.loginfo(
                f"Found garpos_main.py: {str(garpos_main)}"
            )
            break

    if not garpos_main:
        raise FileNotFoundError(f"Garpos main module not found")

    # Setup module
    module_name = str(garpos_main.parent.stem)
    spec = importlib.util.spec_from_file_location(module_name, str(garpos_main.parent / "__init__.py"))
    module = importlib.util.module_from_spec(spec)
    sys.modules[module_name] = module
    spec.loader.exec_module(module)
    garpos_main_module = importlib.import_module(
        f"{garpos_main.parent.stem}.{garpos_main.stem}"
    )
    drive_garpos = getattr(garpos_main_module,"drive_garpos")
    return drive_garpos
