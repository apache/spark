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
from pyspark.mllib.linalg.distributed import IndexedRow, IndexedRowMatrix
# $example off$

if __name__ == "__main__":

    sc = SparkContext(appName="IndexedRowMatrixExample")  # SparkContext

    # $example on$
    # Create an RDD of indexed rows.
    #   - This can be done explicitly with the IndexedRow class:
    indexedRows = sc.parallelize([IndexedRow(0, [1, 2, 3]),
                                  IndexedRow(1, [4, 5, 6]),
                                  IndexedRow(2, [7, 8, 9]),
                                  IndexedRow(3, [10, 11, 12])])
    #   - or by using (long, vector) tuples:
    indexedRows = sc.parallelize([(0, [1, 2, 3]), (1, [4, 5, 6]),
                                  (2, [7, 8, 9]), (3, [10, 11, 12])])

    # Create an IndexedRowMatrix from an RDD of IndexedRows.
    mat = IndexedRowMatrix(indexedRows)

    # Get its size.
    m = mat.numRows()  # 4
    n = mat.numCols()  # 3

    # Get the rows as an RDD of IndexedRows.
    rowsRDD = mat.rows

    # Convert to a RowMatrix by dropping the row indices.
    rowMat = mat.toRowMatrix()

    # Convert to a CoordinateMatrix.
    coordinateMat = mat.toCoordinateMatrix()

    # Convert to a BlockMatrix.
    blockMat = mat.toBlockMatrix()
    # $example off$

    sc.stop()
