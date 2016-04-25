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

from __future__ import print_function

from pyspark import SparkContext
# $example on$
from pyspark.mllib.linalg.distributed import RowMatrix
# $example off$

if __name__ == "__main__":

    sc = SparkContext(appName="RowMatrixExample")  # SparkContext

    # $example on$
    # Create an RDD of vectors.
    rows = sc.parallelize([[1.0, 10.0, 100.0], [2.0, 20.0, 200.0], [3.0, 30.0, 300.0]])

    # Create a RowMatrix from an RDD of vectors.
    mat = RowMatrix(rows)

    # Get its size.
    m = mat.numRows()  # 4
    n = mat.numCols()  # 3

    # Get the rows as an RDD of vectors again.
    rowsRDD = mat.rows
    # $example off$

    sc.stop()
