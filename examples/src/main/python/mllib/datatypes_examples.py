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
from pyspark.sql import SQLContext


def __local_vector_example():
    # $example on:local_vector$
    import numpy as np
    import scipy.sparse as sps
    from pyspark.mllib.linalg import Vectors

    # Use a NumPy array as a dense vector.
    dv1 = np.array([1.0, 0.0, 3.0])
    # Use a Python list as a dense vector.
    dv2 = [1.0, 0.0, 3.0]
    # Create a SparseVector.
    sv1 = Vectors.sparse(3, [0, 2], [1.0, 3.0])
    # Use a single-column SciPy csc_matrix as a sparse vector.
    sv2 = sps.csc_matrix((np.array([1.0, 3.0]), np.array([0, 2]), np.array([0, 2])), shape=(3, 1))
    # $example off:local_vector$


def __labeled_point_example():
    # $example on:labeled_point$
    from pyspark.mllib.linalg import SparseVector
    from pyspark.mllib.regression import LabeledPoint

    # Create a labeled point with a positive label and a dense feature vector.
    pos = LabeledPoint(1.0, [1.0, 0.0, 3.0])

    # Create a labeled point with a negative label and a sparse feature vector.
    neg = LabeledPoint(0.0, SparseVector(3, [0, 2], [1.0, 3.0]))
    # $example off:labeled_point$


def __libsvm_example():
    sc = SparkContext.getOrCreate()

    # $example on:libsvm$
    from pyspark.mllib.util import MLUtils

    examples = MLUtils.loadLibSVMFile(sc, "data/mllib/sample_libsvm_data.txt")
    # $example off:libsvm$


def __local_matrix_example():
    # $example on:local_matrix$
    from pyspark.mllib.linalg import Matrix, Matrices

    # Create a dense matrix ((1.0, 2.0), (3.0, 4.0), (5.0, 6.0))
    dm2 = Matrices.dense(3, 2, [1, 2, 3, 4, 5, 6])

    # Create a sparse matrix ((9.0, 0.0), (0.0, 8.0), (0.0, 6.0))
    sm = Matrices.sparse(3, 2, [0, 1, 3], [0, 2, 1], [9, 6, 8])
    # $example off:local_matrix$


def __row_matrix_example():
    sc = SparkContext.getOrCreate()

    # $example on:row_matrix$
    from pyspark.mllib.linalg.distributed import RowMatrix

    # Create an RDD of vectors.
    rows = sc.parallelize([[1, 2, 3], [4, 5, 6], [7, 8, 9], [10, 11, 12]], 1)

    # Create a RowMatrix from an RDD of vectors.
    mat = RowMatrix(rows)

    # Get its size.
    m = mat.numRows()  # 4
    n = mat.numCols()  # 3

    # QR decomposition
    qrResult = mat.tallSkinnyQR(True)
    # $example off:row_matrix$


def __indexed_row_matrix_example():
    sc = SparkContext.getOrCreate()
    sqlContext = SQLContext.getOrCreate(sc)

    # $example on:indexed_row_matrix$
    from pyspark.mllib.linalg.distributed import IndexedRow, IndexedRowMatrix

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
    # $example off:indexed_row_matrix$


def __coordinate_matrix_example():
    sc = SparkContext.getOrCreate()

    # $example on:coordinate_matrix$
    from pyspark.mllib.linalg.distributed import CoordinateMatrix, MatrixEntry

    # Create an RDD of coordinate entries.
    #   - This can be done explicitly with the MatrixEntry class:
    entries =\
        sc.parallelize([MatrixEntry(0, 0, 1.2), MatrixEntry(1, 0, 2.1), MatrixEntry(6, 1, 3.7)])
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
    # $example off:coordinate_matrix$


def __block_matrix():
    sc = SparkContext.getOrCreate()

    # $example on:block_matrix$
    from pyspark.mllib.linalg import Matrices
    from pyspark.mllib.linalg.distributed import BlockMatrix

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
    # $example off:block_matrix$


if __name__ == "__main__":
    sc = SparkContext(appName="PythonDataTypesExamples")  # SparkContext

    __local_vector_example()
    __labeled_point_example()
    __libsvm_example()
    __local_matrix_example()
    __row_matrix_example()
    __indexed_row_matrix_example()
    __coordinate_matrix_example()
    __block_matrix()

    sc.stop()
