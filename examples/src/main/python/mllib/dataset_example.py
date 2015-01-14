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
An example of how to use SchemaRDD as a dataset for ML. Run with::
    bin/spark-submit examples/src/main/python/mllib/dataset_example.py
"""

import os
import sys
import tempfile
import shutil

from pyspark import SparkContext
from pyspark.sql import SQLContext
from pyspark.mllib.util import MLUtils
from pyspark.mllib.stat import Statistics


def summarize(dataset):
    print "schema: %s" % dataset.schema().json()
    labels = dataset.map(lambda r: r.label)
    print "label average: %f" % labels.mean()
    features = dataset.map(lambda r: r.features)
    summary = Statistics.colStats(features)
    print "features average: %r" % summary.mean()

if __name__ == "__main__":
    if len(sys.argv) > 2:
        print >> sys.stderr, "Usage: dataset_example.py <libsvm file>"
        exit(-1)
    sc = SparkContext(appName="DatasetExample")
    sqlCtx = SQLContext(sc)
    if len(sys.argv) == 2:
        input = sys.argv[1]
    else:
        input = "data/mllib/sample_libsvm_data.txt"
    points = MLUtils.loadLibSVMFile(sc, input)
    dataset0 = sqlCtx.inferSchema(points).setName("dataset0").cache()
    summarize(dataset0)
    tempdir = tempfile.NamedTemporaryFile(delete=False).name
    os.unlink(tempdir)
    print "Save dataset as a Parquet file to %s." % tempdir
    dataset0.saveAsParquetFile(tempdir)
    print "Load it back and summarize it again."
    dataset1 = sqlCtx.parquetFile(tempdir).setName("dataset1").cache()
    summarize(dataset1)
    shutil.rmtree(tempdir)
