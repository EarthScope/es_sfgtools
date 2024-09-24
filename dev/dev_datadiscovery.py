from pathlib import Path
from es_sfgtools.pipeline.datadiscovery import scrape_directory

directory = Path("/Users/franklyndunbar/Project/SeaFloorGeodesy/Data/NCB1")
files = scrape_directory(directory)
for file in files:
    print(file)
