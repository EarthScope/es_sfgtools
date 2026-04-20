"""Workspace: unified environment configuration and directory tree management.

Three workspace types are supported:

- **LOCAL**  — developer laptop or on-prem server.  Directories are created
  locally, TileDB arrays are initialized on disk.
- **GEOLAB** — EarthScope JupyterHub (GeoLab).  Data lives primarily on S3;
  the hub syncs what it needs down before processing.
- **ECS**    — AWS ECS task.  Runs in a container; creates local scratch dirs
  inside the task, syncs results back to S3.

The ``Workspace`` class is the single object that flows through every layer of
the orchestration stack.  It owns both the deployment configuration *and* the
live directory tree, so callers never need to juggle a separate
``DirectoryHandler`` alongside a config object.

Usage
-----
Build explicitly with a factory classmethod::

    from es_sfgtools.config.workspace import Workspace

    ws = Workspace.local("/data/sfg")
    ws.build()                          # scan / create the on-disk tree
    ws.build_station_directory("cascadia", "NCC1", "2025_A")

    ws = Workspace.geolab("/efs/sfg", s3_sync_bucket="my-earthscope-bucket")
    ws = Workspace.ecs("/tmp/sfg",    s3_sync_bucket="my-earthscope-bucket")

Auto-detect from environment variables::

    ws = Workspace.from_environment()

Environment variables consumed by ``Workspace.from_environment``
----------------------------------------------------------------
WORKSPACE_TYPE        : local | geolab | ecs  (default: local)
MAIN_DIRECTORY        : root data directory path
S3_SYNC_BUCKET        : S3 bucket name (no s3:// prefix required)
AWS_PROFILE           : boto3 named profile (takes priority over key/secret)
AWS_ACCESS_KEY_ID     : explicit AWS key
AWS_SECRET_ACCESS_KEY : explicit AWS secret
AWS_SESSION_TOKEN     : STS session token
PRIDE_BINARY_DIR      : path to PRIDE-PPPAR binary directory
"""

from __future__ import annotations

import json
import os
import warnings
from copy import deepcopy
from enum import Enum
from pathlib import Path

import cloudpathlib
from cloudpathlib import S3Path

# ---------------------------------------------------------------------------
# Workspace type
# ---------------------------------------------------------------------------


class WorkspaceType(Enum):
    LOCAL = "local"
    GEOLAB = "geolab"
    ECS = "ecs"


# ---------------------------------------------------------------------------
# Environment variable keys
# ---------------------------------------------------------------------------

_ENV_WORKSPACE_TYPE = "WORKSPACE_TYPE"
_ENV_ROOT_DIRECTORY = "MAIN_DIRECTORY"
_ENV_S3_SYNC_BUCKET = "S3_SYNC_BUCKET"
_ENV_AWS_PROFILE = "AWS_PROFILE"
_ENV_AWS_ACCESS_KEY_ID = "AWS_ACCESS_KEY_ID"
_ENV_AWS_SECRET_ACCESS_KEY = "AWS_SECRET_ACCESS_KEY"
_ENV_AWS_SESSION_TOKEN = "AWS_SESSION_TOKEN"
_ENV_PRIDE_BINARY_DIR = "PRIDE_BINARY_DIR"

# Infrastructure filenames (mirrors directorymgmt.config constants)
_CATALOG_FILENAME = "directoryCatalog.json"
_ASSET_CATALOG = "assetCatalog.db"
_PRIDE_SUBDIR = "Pride"


# ---------------------------------------------------------------------------
# Workspace — unified config + directory tree
# ---------------------------------------------------------------------------


class Workspace:
    """Unified deployment configuration and directory-tree manager.

    A single ``Workspace`` instance replaces the previous pattern of carrying
    a ``WorkspaceConfig`` *and* a ``DirectoryHandler`` through the call stack.
    It knows what kind of environment it is running in (LOCAL / GEOLAB / ECS)
    and owns the live network -> station -> campaign -> survey directory tree.

    Attributes
    ----------
    workspace_type : WorkspaceType
        The deployment environment.
    location : Path | S3Path
        Root path of the data tree.  A ``pathlib.Path`` for LOCAL/ECS; an
        ``S3Path`` when projected onto S3 via :meth:`point_to_s3`.
    s3_sync_bucket : str, optional
        S3 bucket name (no ``s3://`` prefix) used for sync operations.
    pride_binary_dir : Path, optional
        Path to the PRIDE-PPPAR binary directory.  When ``None`` the binary
        must be on ``PATH``.
    aws_profile / aws_access_key_id / aws_secret_access_key / aws_session_token
        AWS credentials forwarded to ``cloudpathlib``.
    networks : dict[str, NetworkDir]
        Live directory tree, populated by :meth:`build` or the ``add_*``
        helpers.
    asset_catalog_db_path : Path | S3Path, optional
        Path to the SQLite asset catalog for this workspace.
    pride_directory : Path, optional
        Resolved path to the PRIDE-PPPAR binary directory inside the tree.
    """

    def __init__(
        self,
        workspace_type: WorkspaceType,
        location: Path | S3Path | str,
        s3_sync_bucket: str | None = None,
        pride_binary_dir: Path | str | None = None,
        aws_profile: str | None = None,
        aws_access_key_id: str | None = None,
        aws_secret_access_key: str | None = None,
        aws_session_token: str | None = None,
    ) -> None:
        # --- Deployment config ---
        self.workspace_type = workspace_type
        if isinstance(location, str):
            if location.startswith("s3://"):
                self.location: Path | S3Path = S3Path(location)
            else:
                self.location = Path(location).resolve()
        else:
            self.location = location

        self.s3_sync_bucket = s3_sync_bucket
        self.pride_binary_dir: Path | None = (
            Path(pride_binary_dir) if pride_binary_dir else None
        )
        self.aws_profile = aws_profile
        self.aws_access_key_id = aws_access_key_id
        self.aws_secret_access_key = aws_secret_access_key
        self.aws_session_token = aws_session_token

        # --- Directory tree (populated by build / add_* helpers) ---
        self.networks: dict = {}
        self.asset_catalog_db_path: Path | S3Path | None = None
        self.pride_directory: Path | None = None
        self._catalog_filepath: Path | S3Path | None = None

    # ------------------------------------------------------------------
    # Behaviour flags — derived from workspace_type
    # ------------------------------------------------------------------

    @property
    def creates_local_dirs(self) -> bool:
        """True for LOCAL and ECS; False for GEOLAB (tree lives on S3)."""
        return self.workspace_type in (WorkspaceType.LOCAL, WorkspaceType.ECS)

    @property
    def builds_tiledb_locally(self) -> bool:
        """True only for LOCAL; other modes use remote TileDB URIs."""
        return self.workspace_type == WorkspaceType.LOCAL

    @property
    def syncs_with_s3(self) -> bool:
        """True for GEOLAB and ECS when ``s3_sync_bucket`` is configured."""
        return (
            self.workspace_type in (WorkspaceType.GEOLAB, WorkspaceType.ECS)
            and self.s3_sync_bucket is not None
        )

    @property
    def s3_sync_bucket_uri(self) -> str | None:
        """Full ``s3://`` URI for the sync bucket, or ``None``."""
        if self.s3_sync_bucket is None:
            return None
        bucket = self.s3_sync_bucket.strip().rstrip("/")
        return bucket if bucket.startswith("s3://") else f"s3://{bucket}"

    @property
    def root_directory(self) -> Path | S3Path:
        """Alias for :attr:`location`."""
        return self.location

    # ------------------------------------------------------------------
    # AWS client
    # ------------------------------------------------------------------

    def make_s3_client(self) -> cloudpathlib.S3Client | None:
        """Return a ``cloudpathlib.S3Client`` built from stored credentials."""
        try:
            if self.aws_profile:
                return cloudpathlib.S3Client(profile_name=self.aws_profile)
            return cloudpathlib.S3Client(
                aws_access_key_id=self.aws_access_key_id,
                aws_secret_access_key=self.aws_secret_access_key,
                aws_session_token=self.aws_session_token,
            )
        except Exception:
            return None

    # ------------------------------------------------------------------
    # Directory tree — build / save
    # ------------------------------------------------------------------

    def build(self) -> None:
        """Initialize or reload the on-disk directory tree.

        Scans :attr:`location` for an existing tree and merges it into this
        instance, then creates any missing infrastructure directories/files
        (only when :attr:`creates_local_dirs` is ``True``).
        """
        existing = Workspace._scan(self.location, workspace_type=self.workspace_type)
        if existing.networks:
            self.networks = existing.networks
        if existing.pride_directory:
            self.pride_directory = existing.pride_directory
        if existing.asset_catalog_db_path:
            self.asset_catalog_db_path = existing.asset_catalog_db_path

        if not self._catalog_filepath:
            self._catalog_filepath = self.location / _CATALOG_FILENAME

        if not self.pride_directory:
            self.pride_directory = self.pride_binary_dir or (
                self.location / _PRIDE_SUBDIR
            )
            if self.creates_local_dirs and isinstance(self.pride_directory, Path):
                self.pride_directory.mkdir(parents=True, exist_ok=True)

        if not self.asset_catalog_db_path:
            self.asset_catalog_db_path = self.location / _ASSET_CATALOG
            if (
                self.creates_local_dirs
                and isinstance(self.asset_catalog_db_path, Path)
                and not self.asset_catalog_db_path.exists()
            ):
                self.asset_catalog_db_path.touch()

    def save(self) -> None:
        """Persist the directory tree to a JSON catalog on disk."""
        catalog_path = self._catalog_filepath or (self.location / _CATALOG_FILENAME)
        data = {
            "location": str(self.location),
            "pride_directory": str(self.pride_directory) if self.pride_directory else None,
            "asset_catalog_db_path": (
                str(self.asset_catalog_db_path) if self.asset_catalog_db_path else None
            ),
            "networks": {
                name: net.model_dump(mode="json")
                for name, net in self.networks.items()
            },
        }
        with open(catalog_path, "w") as fh:
            json.dump(data, fh, indent=4)

    # ------------------------------------------------------------------
    # Directory tree — mutations
    # ------------------------------------------------------------------

    def add_network(self, name: str):
        """Add a network to the tree and build its directory structure.

        Returns the new (or existing) ``NetworkDir``.
        """
        from es_sfgtools.data_mgmt.directorymgmt.schemas import NetworkDir  # deferred

        if name in self.networks:
            return self.networks[name]
        net = NetworkDir(name=name, main_directory=self.location)
        net.build(self)
        self.networks[name] = net
        return net

    def build_station_directory(
        self,
        network_name: str,
        station_name: str | None = None,
        campaign_name: str | None = None,
        survey_name: str | None = None,
    ):
        """Build up the network -> station -> campaign -> survey tree.

        Returns a 4-tuple ``(NetworkDir, StationDir, CampaignDir, SurveyDir)``
        with any levels not requested filled with ``None``.
        """
        if station_name and not network_name:
            print("Station name provided without network name.")
            return None, None, None, None
        if campaign_name and not station_name:
            print("Campaign name provided without station name.")
            return None, None, None, None
        if survey_name and not campaign_name:
            print("Survey name provided without campaign name.")
            return None, None, None, None

        network_dir = self.networks.get(network_name) or self.add_network(network_name)
        station_dir = campaign_dir = survey_dir = None

        if station_name:
            station_dir = (
                network_dir.stations.get(station_name)
                or network_dir.add_station(name=station_name, workspace=self)
            )
            if campaign_name:
                campaign_dir = (
                    station_dir.campaigns.get(campaign_name)
                    or station_dir.add_campaign(name=campaign_name, workspace=self)
                )
                if survey_name:
                    survey_dir = (
                        campaign_dir.surveys.get(survey_name)
                        or campaign_dir.add_survey(name=survey_name, workspace=self)
                    )

        return network_dir, station_dir, campaign_dir, survey_dir

    # ------------------------------------------------------------------
    # S3 projection
    # ------------------------------------------------------------------

    def point_to_s3(self, bucket_path: str | S3Path) -> Workspace:
        """Return a new ``Workspace`` with all paths remapped to S3.

        The workspace configuration (type, credentials, etc.) is preserved;
        only :attr:`location` and all nested path attributes are remapped from
        the local root to *bucket_path*.
        """
        s3_client = self.make_s3_client()
        if not isinstance(bucket_path, S3Path):
            bucket_str = str(bucket_path)
            if not bucket_str.startswith("s3://"):
                bucket_str = "s3://" + bucket_str
            bucket_path = S3Path(bucket_str, client=s3_client)

        s3_ws = Workspace(
            workspace_type=self.workspace_type,
            location=bucket_path,
            s3_sync_bucket=self.s3_sync_bucket,
            pride_binary_dir=self.pride_binary_dir,
            aws_profile=self.aws_profile,
            aws_access_key_id=self.aws_access_key_id,
            aws_secret_access_key=self.aws_secret_access_key,
            aws_session_token=self.aws_session_token,
        )
        new_networks = deepcopy(self.networks)
        _remap_paths(new_networks, old_root=self.location, new_root=bucket_path)
        s3_ws.networks = new_networks
        return s3_ws

    # ------------------------------------------------------------------
    # Subscript access
    # ------------------------------------------------------------------

    def __getitem__(self, key: str):
        try:
            return self.networks[key]
        except KeyError:
            print(f"Network {key!r} not found.")
            return None

    def __repr__(self) -> str:
        bucket = self.s3_sync_bucket_uri or "<none>"
        return (
            f"Workspace(type={self.workspace_type.value!r}, "
            f"location={self.location}, "
            f"s3={bucket})"
        )

    # ------------------------------------------------------------------
    # Factory classmethods
    # ------------------------------------------------------------------

    @classmethod
    def local(
        cls,
        location: Path | str,
        *,
        pride_binary_dir: Path | str | None = None,
        aws_profile: str | None = None,
        s3_sync_bucket: str | None = None,
    ) -> Workspace:
        """Create a ``Workspace`` for local file-system development."""
        return cls(
            workspace_type=WorkspaceType.LOCAL,
            location=Path(location).resolve(),
            pride_binary_dir=pride_binary_dir,
            aws_profile=aws_profile,
            s3_sync_bucket=s3_sync_bucket,
        )

    @classmethod
    def geolab(
        cls,
        location: Path | str,
        s3_sync_bucket: str,
        *,
        aws_profile: str | None = None,
        aws_access_key_id: str | None = None,
        aws_secret_access_key: str | None = None,
        aws_session_token: str | None = None,
        pride_binary_dir: Path | str | None = None,
    ) -> Workspace:
        """Create a ``Workspace`` for a GeoLab JupyterHub environment."""
        return cls(
            workspace_type=WorkspaceType.GEOLAB,
            location=Path(location),
            s3_sync_bucket=s3_sync_bucket,
            aws_profile=aws_profile,
            aws_access_key_id=aws_access_key_id,
            aws_secret_access_key=aws_secret_access_key,
            aws_session_token=aws_session_token,
            pride_binary_dir=pride_binary_dir,
        )

    @classmethod
    def ecs(
        cls,
        location: Path | str,
        s3_sync_bucket: str,
        *,
        aws_profile: str | None = None,
        aws_access_key_id: str | None = None,
        aws_secret_access_key: str | None = None,
        aws_session_token: str | None = None,
        pride_binary_dir: Path | str | None = None,
    ) -> Workspace:
        """Create a ``Workspace`` for an AWS ECS task container."""
        return cls(
            workspace_type=WorkspaceType.ECS,
            location=Path(location),
            s3_sync_bucket=s3_sync_bucket,
            aws_profile=aws_profile,
            aws_access_key_id=aws_access_key_id,
            aws_secret_access_key=aws_secret_access_key,
            aws_session_token=aws_session_token,
            pride_binary_dir=pride_binary_dir,
        )

    @classmethod
    def from_environment(cls) -> Workspace:
        """Build a ``Workspace`` by reading standard environment variables.

        Workspace type is determined by ``WORKSPACE_TYPE`` (local / geolab /
        ecs).  Falls back to ``WORKING_ENVIRONMENT`` (legacy key) then to
        ``local``.

        Raises
        ------
        ValueError
            If the workspace type is not LOCAL but ``MAIN_DIRECTORY`` is
            missing, or an unrecognised ``WORKSPACE_TYPE`` value is found.
        """
        raw_type = (
            os.environ.get(_ENV_WORKSPACE_TYPE)
            or os.environ.get("WORKING_ENVIRONMENT")
            or "local"
        ).lower().strip()

        match raw_type:
            case "local":
                wtype = WorkspaceType.LOCAL
            case "geolab":
                wtype = WorkspaceType.GEOLAB
            case "ecs":
                wtype = WorkspaceType.ECS
            case _:
                raise ValueError(
                    f"Unknown workspace type: {raw_type!r}. "
                    f"Set {_ENV_WORKSPACE_TYPE} to one of: local, geolab, ecs."
                )

        root_dir_str = (
            os.environ.get(_ENV_ROOT_DIRECTORY)
            or os.environ.get("MAIN_DIRECTORY_GEOLAB")
        )
        if root_dir_str is None:
            if wtype != WorkspaceType.LOCAL:
                raise ValueError(
                    f"{_ENV_ROOT_DIRECTORY} must be set for {wtype.value!r} workspace."
                )
            root_dir_str = os.getcwd()

        s3_bucket = os.environ.get(_ENV_S3_SYNC_BUCKET)
        if s3_bucket is None and wtype != WorkspaceType.LOCAL:
            warnings.warn(
                f"{_ENV_S3_SYNC_BUCKET} is not set; S3 sync will be disabled.",
                stacklevel=2,
            )

        aws_profile = os.environ.get(_ENV_AWS_PROFILE)
        aws_access_key_id: str | None = None
        aws_secret_access_key: str | None = None
        aws_session_token: str | None = None

        if not aws_profile:
            aws_access_key_id = os.environ.get(_ENV_AWS_ACCESS_KEY_ID)
            aws_secret_access_key = os.environ.get(_ENV_AWS_SECRET_ACCESS_KEY)
            aws_session_token = os.environ.get(_ENV_AWS_SESSION_TOKEN)
            if aws_access_key_id is None and wtype != WorkspaceType.LOCAL:
                warnings.warn(
                    "AWS credentials are not configured. "
                    f"Set {_ENV_AWS_PROFILE} or "
                    f"{_ENV_AWS_ACCESS_KEY_ID}/{_ENV_AWS_SECRET_ACCESS_KEY}.",
                    stacklevel=2,
                )

        pride_binary_dir_str = os.environ.get(_ENV_PRIDE_BINARY_DIR)

        return cls(
            workspace_type=wtype,
            location=Path(root_dir_str),
            s3_sync_bucket=s3_bucket,
            aws_profile=aws_profile,
            aws_access_key_id=aws_access_key_id,
            aws_secret_access_key=aws_secret_access_key,
            aws_session_token=aws_session_token,
            pride_binary_dir=Path(pride_binary_dir_str) if pride_binary_dir_str else None,
        )

    @classmethod
    def load_from_path(
        cls,
        path: str | Path | S3Path,
        workspace_type: WorkspaceType = WorkspaceType.LOCAL,
        **kwargs,
    ) -> Workspace:
        """Load a workspace by scanning an existing directory tree.

        Parameters
        ----------
        path :
            Local or S3 root to scan.
        workspace_type :
            Workspace type to assign to the returned instance.
        **kwargs :
            Forwarded to :class:`Workspace` (e.g. AWS credentials).
        """
        return cls._scan(path, workspace_type=workspace_type, **kwargs)

    @classmethod
    def load_from_s3(
        cls,
        bucket_path: str,
        workspace_type: WorkspaceType = WorkspaceType.GEOLAB,
        **kwargs,
    ) -> Workspace:
        """Load a workspace by scanning an existing S3 directory tree."""
        if not bucket_path.startswith("s3://"):
            bucket_path = "s3://" + bucket_path
        return cls._scan(bucket_path, workspace_type=workspace_type, **kwargs)

    # ------------------------------------------------------------------
    # Internal scan helper
    # ------------------------------------------------------------------

    @classmethod
    def _scan(
        cls,
        path: str | Path | S3Path,
        workspace_type: WorkspaceType = WorkspaceType.LOCAL,
        **kwargs,
    ) -> Workspace:
        """Scan *path* and return a populated ``Workspace`` (no disk writes)."""
        from es_sfgtools.data_mgmt.directorymgmt.schemas import NetworkDir  # deferred

        if isinstance(path, str):
            if path.startswith("s3://"):
                path = S3Path(path)
            else:
                path = Path(path)

        ws = cls(workspace_type=workspace_type, location=path, **kwargs)

        pride_dir = path / _PRIDE_SUBDIR
        if pride_dir.exists():
            ws.pride_directory = pride_dir

        asset_catalog = path / _ASSET_CATALOG
        if asset_catalog.exists():
            ws.asset_catalog_db_path = asset_catalog

        try:
            for sub_dir in path.iterdir():
                if NetworkDir.is_network_directory(sub_dir):
                    net = NetworkDir.load_from_path(path=sub_dir)
                    ws.networks[net.name] = net
        except (FileNotFoundError, NotADirectoryError):
            pass

        return ws


# ---------------------------------------------------------------------------
# Path-remapping helper (used by point_to_s3)
# ---------------------------------------------------------------------------


def _remap_paths(obj, old_root: Path, new_root: S3Path) -> None:
    """Recursively remap all ``Path`` attrs in a Pydantic model tree to S3Path."""
    if isinstance(obj, dict):
        for val in obj.values():
            _remap_paths(val, old_root, new_root)
