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
from pyspark.mllib.linalg.distributed import CoordinateMatrix, MatrixEntry
# $example off$

if __name__ == "__main__":

    sc = SparkContext(appName="CoordinateMatrixExample")  # SparkContext

    # $example on$
    # Create an RDD of coordinate entries.
    #   - This can be done explicitly with the MatrixEntry class:
    entries = sc.parallelize(
        [MatrixEntry(0, 0, 1.2), MatrixEntry(1, 0, 2.1), MatrixEntry(6, 1, 3.7)])
    #   - or using (long, long, float) tuples:
    entries = sc.parallelize([(0, 0, 1.2), (1, 0, 2.1), (2, 1, 3.7)])

    # Create an CoordinateMatrix from an RDD of MatrixEntries.
    mat = CoordinateMatrix(entries)

    # Get its size.
    m = mat.numRows()  # 3
    n = mat.numCols()  # 2

    # Get the entries as an RDD of MatrixEntries.
    entriesRDD = mat.entries

    # Convert to a RowMatrix.
    rowMat = mat.toRowMatrix()

    # Convert to an IndexedRowMatrix.
    indexedRowMat = mat.toIndexedRowMatrix()

    # Convert to a BlockMatrix.
    blockMat = mat.toBlockMatrix()
    # $example off$

    sc.stop()
