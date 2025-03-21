from es_sfgtools.processing.assets.observables import ShotDataFrame
import pandas as pd

data = "~/Downloads/NCC1_2023_shotdata_rectified.csv"
df = pd.read_csv(data)
print(df.head())
test = ShotDataFrame.validate(df,lazy=True)
