from pathlib import Path

from es_sfgtools.processing.assets.siteconfig import Site

# Need to take in a .json defining individual surveys and use that to
# Interface with GARPOS

site_path = Path(
    "/Users/franklyndunbar/Project/SeaFloorGeodesy/es_sfgtools/dev/NCC1_Dec3_move_around.json"
)

site = Site.from_json(site_path)

print(
    site.campaigns[0]
)

new = Site()