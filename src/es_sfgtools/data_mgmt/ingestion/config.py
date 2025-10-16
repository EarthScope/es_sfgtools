from ..assetcatalog.schemas import AssetType
import re

ARCHIVE_PREFIX = "https://data.earthscope.org/archive/seafloor"

pattern_map = {
    re.compile(r"\.\d{2}O$"): AssetType.RINEX,
    re.compile("sonardyne"): AssetType.SONARDYNE,
    re.compile(r"^(?=.*novatel)(?!.*pin).*$", re.IGNORECASE): AssetType.NOVATELPIN,
    re.compile("novatel"): AssetType.NOVATEL,
    re.compile("kin"): AssetType.KIN,
    re.compile("NOV000"): AssetType.NOVATEL000,
    # re.compile("rinex"): AssetType.RINEX,
    re.compile(r"\.\d{2}o$"): AssetType.RINEX,
    re.compile("NOV770"): AssetType.NOVATEL770,
    re.compile("DFOP00.raw"): AssetType.DFOP00,
    re.compile("lever_arms"): AssetType.LEVERARM,
    re.compile("master"): AssetType.MASTER,
    re.compile(r"\.pin$"): AssetType.QCPIN,
    re.compile("CTD"): AssetType.CTD,
    re.compile("svpavg"): AssetType.SEABIRD,
    re.compile(r"\.res$"): AssetType.KINRESIDUALS,
    re.compile("bcoffload"): AssetType.BCOFFLOAD,
    re.compile("seabird"): AssetType.SEABIRD,
}
