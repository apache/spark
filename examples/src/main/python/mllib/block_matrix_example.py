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
from pyspark.mllib.linalg import Matrices
from pyspark.mllib.linalg.distributed import BlockMatrix
# $example off$

if __name__ == "__main__":

    sc = SparkContext(appName="BlockMatrixExample")  # SparkContext

    # $example on$
    # Create an RDD of sub-matrix blocks.
    blocks = sc.parallelize([((0, 0), Matrices.dense(3, 2, [1, 2, 3, 4, 5, 6])),
                             ((1, 0), Matrices.dense(3, 2, [7, 8, 9, 10, 11, 12]))])

    # Create a BlockMatrix from an RDD of sub-matrix blocks.
    mat = BlockMatrix(blocks, 3, 2)

    # Get its size.
    m = mat.numRows()  # 6
    n = mat.numCols()  # 2

    # Get the blocks as an RDD of sub-matrix blocks.
    blocksRDD = mat.blocks

    # Convert to a LocalMatrix.
    localMat = mat.toLocalMatrix()

    # Convert to an IndexedRowMatrix.
    indexedRowMat = mat.toIndexedRowMatrix()

    # Convert to a CoordinateMatrix.
    coordinateMat = mat.toCoordinateMatrix()
    # $example off$

    sc.stop()
