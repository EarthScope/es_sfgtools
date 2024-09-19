import sys
from pathlib import Path
import json
import es_sfgtools
import es_sfgtools.processing as sfg_proc
import es_sfgtools.modeling.garpos_tools as sfg_mod
from es_sfgtools.modeling.garpos_tools.schemas import GarposResults
import pandas as pd
import matplotlib.pyplot as plt

res_path_dir = Path("/Users/franklyndunbar/Project/SeaFloorGeodesy/Data/TestSV3")
days = ["07","08","09","10"]

path_pattern = lambda x: f"d1_2021-09-{x}-00-00-00_results.json"

res_path = res_path_dir / path_pattern(days[2])



with open(res_path,"r") as f:
    res = json.load(f)
    results = GarposResults(**res)
    print(results)

# plot seconds vs residuals (RT vs ResiRange)

results.shot_data.sort_values(by="RT",inplace=True)
results.shot_data = results.shot_data[results.shot_data.flag == False]
ids = results.shot_data.MT.unique().tolist()
colors = ['c','m','y','k','r','g','b']
fig,axs = plt.subplots(2,1,figsize=(10,10))
resid_ax = axs[0]
pos_ax = axs[1]
for idx,id in enumerate(ids):
    data = results.shot_data[results.shot_data.MT == id]
    resid_ax.plot(data.RT.values,data.ResiRange.values,label=id,c=colors[idx],linestyle="dashed")

for idx,transponder in enumerate(results.transponders):
    pos_ax.scatter(
        transponder.position_enu.east,transponder.position_enu.north,c=colors[idx],label=transponder.name)
   


pos_ax.plot(results.shot_data.ant_e1.values,results.shot_data.ant_n1.values,c="b")

# ax.legend()
resid_ax.set_xlabel("Seconds")
resid_ax.set_ylabel("Residuals [m]")
plt.suptitle(f"Residuals vs Time [s]")
plt.show()
print("stop")
