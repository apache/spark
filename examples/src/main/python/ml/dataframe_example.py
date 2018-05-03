#
# Licensed to the Apache Software Foundation (ASF) under one or more
# contributor license agreements.  See the NOTICE file distributed with
# this work for additional information regarding copyright ownership.
# The ASF licenses this file to You under the Apache License, Version 2.0
# (the "License"); you may not use this file except in compliance with
# the License.  You may obtain a copy of the License at
#
#    http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#

"""
An example of how to use DataFrame for ML. Run with::
    bin/spark-submit examples/src/main/python/ml/dataframe_example.py <input_path>
"""
from __future__ import print_function

import os
import sys
import tempfile
import shutil

from pyspark.sql import SparkSession
from pyspark.mllib.stat import Statistics
from pyspark.mllib.util import MLUtils

if __name__ == "__main__":
    if len(sys.argv) > 2:
        print("Usage: dataframe_example.py <libsvm file>", file=sys.stderr)
        sys.exit(-1)
    elif len(sys.argv) == 2:
        input_path = sys.argv[1]
    else:
        input_path = "data/mllib/sample_libsvm_data.txt"

    spark = SparkSession \
        .builder \
        .appName("DataFrameExample") \
        .getOrCreate()

    # Load an input file
    print("Loading LIBSVM file with UDT from " + input_path + ".")
    df = spark.read.format("libsvm").load(input_path).cache()
    print("Schema from LIBSVM:")
    df.printSchema()
    print("Loaded training data as a DataFrame with " +
          str(df.count()) + " records.")

    # Show statistical summary of labels.
    labelSummary = df.describe("label")
    labelSummary.show()

    # Convert features column to an RDD of vectors.
    features = MLUtils.convertVectorColumnsFromML(df, "features") \
        .select("features").rdd.map(lambda r: r.features)
    summary = Statistics.colStats(features)
    print("Selected features column with average values:\n" +
          str(summary.mean()))

    # Save the records in a parquet file.
    tempdir = tempfile.NamedTemporaryFile(delete=False).name
    os.unlink(tempdir)
    print("Saving to " + tempdir + " as Parquet file.")
    df.write.parquet(tempdir)

    # Load the records back.
    print("Loading Parquet file with UDT from " + tempdir)
    newDF = spark.read.parquet(tempdir)
    print("Schema from Parquet:")
    newDF.printSchema()
    shutil.rmtree(tempdir)

    spark.stop()
