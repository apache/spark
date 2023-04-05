import pyspark.pandas as pd
from pyspark.pandas.spark import transform

path=input('Input path:')
data_frame=pd.read_csv(path)
sample_data=data_frame.copy()
dictionary_values={}
for cols in sample_data.columns:
    if sample_data[cols].dtype=='O':
        sample_data[cols]=sample_data[cols].astype(str)
        values=transform.strc_to_numc(sample_data,cols)
        sample_data[cols]=values[0]
        dictionary_values[cols]=values[1]


print(sample_data,dictionary_values)
