from pyspark import *
from pyspark.sql import *
from pyspark.sql.functions import *

def convert_stuff(df):
    df.select(toRadians(df["cheese"]))
