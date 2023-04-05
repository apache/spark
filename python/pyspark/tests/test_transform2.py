import numpy as np
import pandas as pd
from pyspark.pandas.spark import transform

path=input('Input path: ')
data_frame=pd.read_csv(path)
sample_data=transform.strd_to_numd(data_frame)
print(sample_data)
