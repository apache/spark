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
Package for distributed linear algebra.
"""

import sys
from typing import Any, Generic, Optional, Tuple, TypeVar, Union, TYPE_CHECKING

from py4j.java_gateway import JavaObject

from pyspark import RDD, since
from pyspark.mllib.common import callMLlibFunc, JavaModelWrapper
from pyspark.mllib.linalg import _convert_to_vector, DenseMatrix, Matrix, QRDecomposition, Vector
from pyspark.mllib.stat import MultivariateStatisticalSummary
from pyspark.sql import DataFrame
from pyspark.storagelevel import StorageLevel

UT = TypeVar("UT", bound="DistributedMatrix")
VT = TypeVar("VT", bound="Matrix")

if TYPE_CHECKING:
    from pyspark.ml._typing import VectorLike

__all__ = [
    "BlockMatrix",
    "CoordinateMatrix",
    "DistributedMatrix",
    "IndexedRow",
    "IndexedRowMatrix",
    "MatrixEntry",
    "RowMatrix",
    "SingularValueDecomposition",
]


class DistributedMatrix:
    """
    Represents a distributively stored matrix backed by one or
    more RDDs.

    """

    def numRows(self) -> int:
        """Get or compute the number of rows."""
        raise NotImplementedError

    def numCols(self) -> int:
        """Get or compute the number of cols."""
        raise NotImplementedError


class RowMatrix(DistributedMatrix):
    """
    Represents a row-oriented distributed Matrix with no meaningful
    row indices.


    Parameters
    ----------
    rows : :py:class:`pyspark.RDD` or :py:class:`pyspark.sql.DataFrame`
        An RDD or DataFrame of vectors. If a DataFrame is provided, it must have a single
        vector typed column.
    numRows : int, optional
        Number of rows in the matrix. A non-positive
        value means unknown, at which point the number
        of rows will be determined by the number of
        records in the `rows` RDD.
    numCols : int, optional
        Number of columns in the matrix. A non-positive
        value means unknown, at which point the number
        of columns will be determined by the size of
        the first row.
    """

    def __init__(
        self,
        rows: Union[RDD[Vector], DataFrame],
        numRows: int = 0,
        numCols: int = 0,
    ):
        """
        Note: This docstring is not shown publicly.

        Create a wrapper over a Java RowMatrix.

        Publicly, we require that `rows` be an RDD or DataFrame.  However, for
        internal usage, `rows` can also be a Java RowMatrix
        object, in which case we can wrap it directly.  This
        assists in clean matrix conversions.

        Examples
        --------
        >>> rows = sc.parallelize([[1, 2, 3], [4, 5, 6]])
        >>> mat = RowMatrix(rows)

        >>> mat_diff = RowMatrix(rows)
        >>> (mat_diff._java_matrix_wrapper._java_model ==
        ...  mat._java_matrix_wrapper._java_model)
        False

        >>> mat_same = RowMatrix(mat._java_matrix_wrapper._java_model)
        >>> (mat_same._java_matrix_wrapper._java_model ==
        ...  mat._java_matrix_wrapper._java_model)
        True
        """
        if isinstance(rows, RDD):
            rows = rows.map(_convert_to_vector)
            java_matrix = callMLlibFunc("createRowMatrix", rows, int(numRows), int(numCols))
        elif isinstance(rows, DataFrame):
            java_matrix = callMLlibFunc("createRowMatrix", rows, int(numRows), int(numCols))
        elif isinstance(rows, JavaObject) and rows.getClass().getSimpleName() == "RowMatrix":
            java_matrix = rows
        else:
            raise TypeError("rows should be an RDD of vectors, got %s" % type(rows))

        self._java_matrix_wrapper = JavaModelWrapper(java_matrix)

    @property
    def rows(self) -> RDD[Vector]:
        """
        Rows of the RowMatrix stored as an RDD of vectors.

        Examples
        --------
        >>> mat = RowMatrix(sc.parallelize([[1, 2, 3], [4, 5, 6]]))
        >>> rows = mat.rows
        >>> rows.first()
        DenseVector([1.0, 2.0, 3.0])
        """
        return self._java_matrix_wrapper.call("rows")

    def numRows(self) -> int:
        """
        Get or compute the number of rows.

        Examples
        --------
        >>> rows = sc.parallelize([[1, 2, 3], [4, 5, 6],
        ...                        [7, 8, 9], [10, 11, 12]])

        >>> mat = RowMatrix(rows)
        >>> print(mat.numRows())
        4

        >>> mat = RowMatrix(rows, 7, 6)
        >>> print(mat.numRows())
        7
        """
        return self._java_matrix_wrapper.call("numRows")

    def numCols(self) -> int:
        """
        Get or compute the number of cols.

        Examples
        --------
        >>> rows = sc.parallelize([[1, 2, 3], [4, 5, 6],
        ...                        [7, 8, 9], [10, 11, 12]])

        >>> mat = RowMatrix(rows)
        >>> print(mat.numCols())
        3

        >>> mat = RowMatrix(rows, 7, 6)
        >>> print(mat.numCols())
        6
        """
        return self._java_matrix_wrapper.call("numCols")

    def computeColumnSummaryStatistics(self) -> MultivariateStatisticalSummary:
        """
        Computes column-wise summary statistics.

        .. versionadded:: 2.0.0

        Returns
        -------
        :py:class:`MultivariateStatisticalSummary`
            object containing column-wise summary statistics.

        Examples
        --------
        >>> rows = sc.parallelize([[1, 2, 3], [4, 5, 6]])
        >>> mat = RowMatrix(rows)

        >>> colStats = mat.computeColumnSummaryStatistics()
        >>> colStats.mean()
        array([ 2.5,  3.5,  4.5])
        """
        java_col_stats = self._java_matrix_wrapper.call("computeColumnSummaryStatistics")
        return MultivariateStatisticalSummary(java_col_stats)

    def computeCovariance(self) -> Matrix:
        """
        Computes the covariance matrix, treating each row as an
        observation.

        .. versionadded:: 2.0.0

        Notes
        -----
        This cannot be computed on matrices with more than 65535 columns.

        Examples
        --------
        >>> rows = sc.parallelize([[1, 2], [2, 1]])
        >>> mat = RowMatrix(rows)

        >>> mat.computeCovariance()
        DenseMatrix(2, 2, [0.5, -0.5, -0.5, 0.5], 0)
        """
        return self._java_matrix_wrapper.call("computeCovariance")

    def computeGramianMatrix(self) -> Matrix:
        """
        Computes the Gramian matrix `A^T A`.

        .. versionadded:: 2.0.0

        Notes
        -----
        This cannot be computed on matrices with more than 65535 columns.

        Examples
        --------
        >>> rows = sc.parallelize([[1, 2, 3], [4, 5, 6]])
        >>> mat = RowMatrix(rows)

        >>> mat.computeGramianMatrix()
        DenseMatrix(3, 3, [17.0, 22.0, 27.0, 22.0, 29.0, 36.0, 27.0, 36.0, 45.0], 0)
        """
        return self._java_matrix_wrapper.call("computeGramianMatrix")

    @since("2.0.0")
    def columnSimilarities(self, threshold: float = 0.0) -> "CoordinateMatrix":
        """
        Compute similarities between columns of this matrix.

        The threshold parameter is a trade-off knob between estimate
        quality and computational cost.

        The default threshold setting of 0 guarantees deterministically
        correct results, but uses the brute-force approach of computing
        normalized dot products.

        Setting the threshold to positive values uses a sampling
        approach and incurs strictly less computational cost than the
        brute-force approach. However the similarities computed will
        be estimates.

        The sampling guarantees relative-error correctness for those
        pairs of columns that have similarity greater than the given
        similarity threshold.

        To describe the guarantee, we set some notation:

        - Let A be the smallest in magnitude non-zero element of
          this matrix.
        - Let B be the largest in magnitude non-zero element of
          this matrix.
        - Let L be the maximum number of non-zeros per row.

        For example, for {0,1} matrices: A=B=1.
        Another example, for the Netflix matrix: A=1, B=5

        For those column pairs that are above the threshold, the
        computed similarity is correct to within 20% relative error
        with probability at least 1 - (0.981)^10/B^

        The shuffle size is bounded by the *smaller* of the following
        two expressions:

        - O(n log(n) L / (threshold * A))
        - O(m L^2^)

        The latter is the cost of the brute-force approach, so for
        non-zero thresholds, the cost is always cheaper than the
        brute-force approach.

        .. versionadded:: 2.0.0

        Parameters
        ----------
        threshold : float, optional
            Set to 0 for deterministic guaranteed
            correctness. Similarities above this
            threshold are estimated with the cost vs
            estimate quality trade-off described above.

        Returns
        -------
        :py:class:`CoordinateMatrix`
            An n x n sparse upper-triangular CoordinateMatrix of
            cosine similarities between columns of this matrix.

        Examples
        --------
        >>> rows = sc.parallelize([[1, 2], [1, 5]])
        >>> mat = RowMatrix(rows)

        >>> sims = mat.columnSimilarities()
        >>> sims.entries.first().value
        0.91914503...
        """
        java_sims_mat = self._java_matrix_wrapper.call("columnSimilarities", float(threshold))
        return CoordinateMatrix(java_sims_mat)

    def tallSkinnyQR(
        self, computeQ: bool = False
    ) -> QRDecomposition[Optional["RowMatrix"], Matrix]:
        """
        Compute the QR decomposition of this RowMatrix.

        The implementation is designed to optimize the QR decomposition
        (factorization) for the RowMatrix of a tall and skinny shape [1]_.

        .. [1] Paul G. Constantine, David F. Gleich. "Tall and skinny QR
            factorizations in MapReduce architectures"
            https://doi.org/10.1145/1996092.1996103

        .. versionadded:: 2.0.0

        Parameters
        ----------
        computeQ : bool, optional
            whether to computeQ

        Returns
        -------
        :py:class:`pyspark.mllib.linalg.QRDecomposition`
            QRDecomposition(Q: RowMatrix, R: Matrix), where
            Q = None if computeQ = false.

        Examples
        --------
        >>> rows = sc.parallelize([[3, -6], [4, -8], [0, 1]])
        >>> mat = RowMatrix(rows)
        >>> decomp = mat.tallSkinnyQR(True)
        >>> Q = decomp.Q
        >>> R = decomp.R

        >>> # Test with absolute values
        >>> absQRows = Q.rows.map(lambda row: abs(row.toArray()).tolist())
        >>> absQRows.collect()
        [[0.6..., 0.0], [0.8..., 0.0], [0.0, 1.0]]

        >>> # Test with absolute values
        >>> abs(R.toArray()).tolist()
        [[5.0, 10.0], [0.0, 1.0]]
        """
        decomp = JavaModelWrapper(self._java_matrix_wrapper.call("tallSkinnyQR", computeQ))
        if computeQ:
            java_Q = decomp.call("Q")
            Q = RowMatrix(java_Q)
        else:
            Q = None
        R = decomp.call("R")
        return QRDecomposition(Q, R)

    def computeSVD(
        self, k: int, computeU: bool = False, rCond: float = 1e-9
    ) -> "SingularValueDecomposition[RowMatrix, Matrix]":
        """
        Computes the singular value decomposition of the RowMatrix.

        The given row matrix A of dimension (m X n) is decomposed into
        U * s * V'T where

        - U: (m X k) (left singular vectors) is a RowMatrix whose
          columns are the eigenvectors of (A X A')
        - s: DenseVector consisting of square root of the eigenvalues
          (singular values) in descending order.
        - v: (n X k) (right singular vectors) is a Matrix whose columns
          are the eigenvectors of (A' X A)

        For more specific details on implementation, please refer
        the Scala documentation.

        .. versionadded:: 2.2.0

        Parameters
        ----------
        k : int
            Number of leading singular values to keep (`0 < k <= n`).
            It might return less than k if there are numerically zero singular values
            or there are not enough Ritz values converged before the maximum number of
            Arnoldi update iterations is reached (in case that matrix A is ill-conditioned).
        computeU : bool, optional
            Whether or not to compute U. If set to be
            True, then U is computed by A * V * s^-1
        rCond : float, optional
            Reciprocal condition number. All singular values
            smaller than rCond * s[0] are treated as zero
            where s[0] is the largest singular value.

        Returns
        -------
        :py:class:`SingularValueDecomposition`

        Examples
        --------
        >>> rows = sc.parallelize([[3, 1, 1], [-1, 3, 1]])
        >>> rm = RowMatrix(rows)

        >>> svd_model = rm.computeSVD(2, True)
        >>> svd_model.U.rows.collect()
        [DenseVector([-0.7071, 0.7071]), DenseVector([-0.7071, -0.7071])]
        >>> svd_model.s
        DenseVector([3.4641, 3.1623])
        >>> svd_model.V
        DenseMatrix(3, 2, [-0.4082, -0.8165, -0.4082, 0.8944, -0.4472, 0.0], 0)
        """
        j_model = self._java_matrix_wrapper.call("computeSVD", int(k), bool(computeU), float(rCond))
        return SingularValueDecomposition(j_model)

    def computePrincipalComponents(self, k: int) -> Matrix:
        """
        Computes the k principal components of the given row matrix

        .. versionadded:: 2.2.0

        Notes
        -----
        This cannot be computed on matrices with more than 65535 columns.

        Parameters
        ----------
        k : int
            Number of principal components to keep.

        Returns
        -------
        :py:class:`pyspark.mllib.linalg.DenseMatrix`

        Examples
        --------
        >>> rows = sc.parallelize([[1, 2, 3], [2, 4, 5], [3, 6, 1]])
        >>> rm = RowMatrix(rows)

        >>> # Returns the two principal components of rm
        >>> pca = rm.computePrincipalComponents(2)
        >>> pca
        DenseMatrix(3, 2, [-0.349, -0.6981, 0.6252, -0.2796, -0.5592, -0.7805], 0)

        >>> # Transform into new dimensions with the greatest variance.
        >>> rm.multiply(pca).rows.collect() # doctest: +NORMALIZE_WHITESPACE
        [DenseVector([0.1305, -3.7394]), DenseVector([-0.3642, -6.6983]), \
        DenseVector([-4.6102, -4.9745])]
        """
        return self._java_matrix_wrapper.call("computePrincipalComponents", k)

    def multiply(self, matrix: Matrix) -> "RowMatrix":
        """
        Multiply this matrix by a local dense matrix on the right.

        .. versionadded:: 2.2.0

        Parameters
        ----------
        matrix : :py:class:`pyspark.mllib.linalg.Matrix`
            a local dense matrix whose number of rows must match the number of columns
            of this matrix

        Returns
        -------
        :py:class:`RowMatrix`

        Examples
        --------
        >>> rm = RowMatrix(sc.parallelize([[0, 1], [2, 3]]))
        >>> rm.multiply(DenseMatrix(2, 2, [0, 2, 1, 3])).rows.collect()
        [DenseVector([2.0, 3.0]), DenseVector([6.0, 11.0])]
        """
        if not isinstance(matrix, DenseMatrix):
            raise TypeError("Only multiplication with DenseMatrix is supported.")
        j_model = self._java_matrix_wrapper.call("multiply", matrix)
        return RowMatrix(j_model)


class SingularValueDecomposition(JavaModelWrapper, Generic[UT, VT]):
    """
    Represents singular value decomposition (SVD) factors.

    .. versionadded:: 2.2.0
    """

    @property  # type: ignore[misc]
    @since("2.2.0")
    def U(self) -> Optional[UT]:  # type: ignore[return]
        """
        Returns a distributed matrix whose columns are the left
        singular vectors of the SingularValueDecomposition if computeU was set to be True.
        """
        u = self.call("U")
        if u is not None:
            mat_name = u.getClass().getSimpleName()
            if mat_name == "RowMatrix":
                return RowMatrix(u)  # type: ignore[return-value]
            elif mat_name == "IndexedRowMatrix":
                return IndexedRowMatrix(u)  # type: ignore[return-value]
            else:
                raise TypeError("Expected RowMatrix/IndexedRowMatrix got %s" % mat_name)

    @property  # type: ignore[misc]
    @since("2.2.0")
    def s(self) -> Vector:
        """
        Returns a DenseVector with singular values in descending order.
        """
        return self.call("s")

    @property  # type: ignore[misc]
    @since("2.2.0")
    def V(self) -> VT:
        """
        Returns a DenseMatrix whose columns are the right singular
        vectors of the SingularValueDecomposition.
        """
        return self.call("V")


class IndexedRow:
    """
    Represents a row of an IndexedRowMatrix.

    Just a wrapper over a (int, vector) tuple.

    Parameters
    ----------
    index : int
        The index for the given row.
    vector : :py:class:`pyspark.mllib.linalg.Vector` or convertible
        The row in the matrix at the given index.
    """

    def __init__(self, index: int, vector: "VectorLike") -> None:
        self.index = int(index)
        self.vector = _convert_to_vector(vector)

    def __repr__(self) -> str:
        return "IndexedRow(%s, %s)" % (self.index, self.vector)


def _convert_to_indexed_row(row: Any) -> IndexedRow:
    if isinstance(row, IndexedRow):
        return row
    elif isinstance(row, tuple) and len(row) == 2:
        return IndexedRow(*row)
    else:
        raise TypeError("Cannot convert type %s into IndexedRow" % type(row))


class IndexedRowMatrix(DistributedMatrix):
    """
    Represents a row-oriented distributed Matrix with indexed rows.

    Parameters
    ----------
    rows : :py:class:`pyspark.RDD`
        An RDD of IndexedRows or (int, vector) tuples or a DataFrame consisting of a
        int typed column of indices and a vector typed column.
    numRows : int, optional
        Number of rows in the matrix. A non-positive
        value means unknown, at which point the number
        of rows will be determined by the max row
        index plus one.
    numCols : int, optional
        Number of columns in the matrix. A non-positive
        value means unknown, at which point the number
        of columns will be determined by the size of
        the first row.
    """

    def __init__(
        self,
        rows: RDD[Union[Tuple[int, "VectorLike"], IndexedRow]],
        numRows: int = 0,
        numCols: int = 0,
    ):
        """
        Note: This docstring is not shown publicly.

        Create a wrapper over a Java IndexedRowMatrix.

        Publicly, we require that `rows` be an RDD or DataFrame.  However, for
        internal usage, `rows` can also be a Java IndexedRowMatrix
        object, in which case we can wrap it directly.  This
        assists in clean matrix conversions.

        Examples
        --------
        >>> rows = sc.parallelize([IndexedRow(0, [1, 2, 3]),
        ...                        IndexedRow(1, [4, 5, 6])])
        >>> mat = IndexedRowMatrix(rows)

        >>> mat_diff = IndexedRowMatrix(rows)
        >>> (mat_diff._java_matrix_wrapper._java_model ==
        ...  mat._java_matrix_wrapper._java_model)
        False

        >>> mat_same = IndexedRowMatrix(mat._java_matrix_wrapper._java_model)
        >>> (mat_same._java_matrix_wrapper._java_model ==
        ...  mat._java_matrix_wrapper._java_model)
        True
        """
        if isinstance(rows, RDD):
            rows = rows.map(_convert_to_indexed_row)
            # We use DataFrames for serialization of IndexedRows from
            # Python, so first convert the RDD to a DataFrame on this
            # side. This will convert each IndexedRow to a Row
            # containing the 'index' and 'vector' values, which can
            # both be easily serialized.  We will convert back to
            # IndexedRows on the Scala side.
            java_matrix = callMLlibFunc(
                "createIndexedRowMatrix", rows.toDF(), int(numRows), int(numCols)
            )
        elif isinstance(rows, DataFrame):
            java_matrix = callMLlibFunc("createIndexedRowMatrix", rows, int(numRows), int(numCols))
        elif isinstance(rows, JavaObject) and rows.getClass().getSimpleName() == "IndexedRowMatrix":
            java_matrix = rows
        else:
            raise TypeError(
                "rows should be an RDD of IndexedRows or (int, vector) tuples, "
                "got %s" % type(rows)
            )

        self._java_matrix_wrapper = JavaModelWrapper(java_matrix)

    @property
    def rows(self) -> RDD[IndexedRow]:
        """
        Rows of the IndexedRowMatrix stored as an RDD of IndexedRows.

        Examples
        --------
        >>> mat = IndexedRowMatrix(sc.parallelize([IndexedRow(0, [1, 2, 3]),
        ...                                        IndexedRow(1, [4, 5, 6])]))
        >>> rows = mat.rows
        >>> rows.first()
        IndexedRow(0, [1.0,2.0,3.0])
        """
        # We use DataFrames for serialization of IndexedRows from
        # Java, so we first convert the RDD of rows to a DataFrame
        # on the Scala/Java side. Then we map each Row in the
        # DataFrame back to an IndexedRow on this side.
        rows_df = callMLlibFunc("getIndexedRows", self._java_matrix_wrapper._java_model)
        rows = rows_df.rdd.map(lambda row: IndexedRow(row[0], row[1]))
        return rows

    def numRows(self) -> int:
        """
        Get or compute the number of rows.

        Examples
        --------
        >>> rows = sc.parallelize([IndexedRow(0, [1, 2, 3]),
        ...                        IndexedRow(1, [4, 5, 6]),
        ...                        IndexedRow(2, [7, 8, 9]),
        ...                        IndexedRow(3, [10, 11, 12])])

        >>> mat = IndexedRowMatrix(rows)
        >>> print(mat.numRows())
        4

        >>> mat = IndexedRowMatrix(rows, 7, 6)
        >>> print(mat.numRows())
        7
        """
        return self._java_matrix_wrapper.call("numRows")

    def numCols(self) -> int:
        """
        Get or compute the number of cols.

        Examples
        --------
        >>> rows = sc.parallelize([IndexedRow(0, [1, 2, 3]),
        ...                        IndexedRow(1, [4, 5, 6]),
        ...                        IndexedRow(2, [7, 8, 9]),
        ...                        IndexedRow(3, [10, 11, 12])])

        >>> mat = IndexedRowMatrix(rows)
        >>> print(mat.numCols())
        3

        >>> mat = IndexedRowMatrix(rows, 7, 6)
        >>> print(mat.numCols())
        6
        """
        return self._java_matrix_wrapper.call("numCols")

    def columnSimilarities(self) -> "CoordinateMatrix":
        """
        Compute all cosine similarities between columns.

        Examples
        --------
        >>> rows = sc.parallelize([IndexedRow(0, [1, 2, 3]),
        ...                        IndexedRow(6, [4, 5, 6])])
        >>> mat = IndexedRowMatrix(rows)
        >>> cs = mat.columnSimilarities()
        >>> print(cs.numCols())
        3
        """
        java_coordinate_matrix = self._java_matrix_wrapper.call("columnSimilarities")
        return CoordinateMatrix(java_coordinate_matrix)

    def computeGramianMatrix(self) -> Matrix:
        """
        Computes the Gramian matrix `A^T A`.

        .. versionadded:: 2.0.0

        Notes
        -----
        This cannot be computed on matrices with more than 65535 columns.

        Examples
        --------
        >>> rows = sc.parallelize([IndexedRow(0, [1, 2, 3]),
        ...                        IndexedRow(1, [4, 5, 6])])
        >>> mat = IndexedRowMatrix(rows)

        >>> mat.computeGramianMatrix()
        DenseMatrix(3, 3, [17.0, 22.0, 27.0, 22.0, 29.0, 36.0, 27.0, 36.0, 45.0], 0)
        """
        return self._java_matrix_wrapper.call("computeGramianMatrix")

    def toRowMatrix(self) -> RowMatrix:
        """
        Convert this matrix to a RowMatrix.

        Examples
        --------
        >>> rows = sc.parallelize([IndexedRow(0, [1, 2, 3]),
        ...                        IndexedRow(6, [4, 5, 6])])
        >>> mat = IndexedRowMatrix(rows).toRowMatrix()
        >>> mat.rows.collect()
        [DenseVector([1.0, 2.0, 3.0]), DenseVector([4.0, 5.0, 6.0])]
        """
        java_row_matrix = self._java_matrix_wrapper.call("toRowMatrix")
        return RowMatrix(java_row_matrix)

    def toCoordinateMatrix(self) -> "CoordinateMatrix":
        """
        Convert this matrix to a CoordinateMatrix.

        Examples
        --------
        >>> rows = sc.parallelize([IndexedRow(0, [1, 0]),
        ...                        IndexedRow(6, [0, 5])])
        >>> mat = IndexedRowMatrix(rows).toCoordinateMatrix()
        >>> mat.entries.take(3)
        [MatrixEntry(0, 0, 1.0), MatrixEntry(0, 1, 0.0), MatrixEntry(6, 0, 0.0)]
        """
        java_coordinate_matrix = self._java_matrix_wrapper.call("toCoordinateMatrix")
        return CoordinateMatrix(java_coordinate_matrix)

    def toBlockMatrix(self, rowsPerBlock: int = 1024, colsPerBlock: int = 1024) -> "BlockMatrix":
        """
        Convert this matrix to a BlockMatrix.

        Parameters
        ----------
        rowsPerBlock : int, optional
            Number of rows that make up each block.
            The blocks forming the final rows are not
            required to have the given number of rows.
        colsPerBlock : int, optional
            Number of columns that make up each block.
            The blocks forming the final columns are not
            required to have the given number of columns.

        Examples
        --------
        >>> rows = sc.parallelize([IndexedRow(0, [1, 2, 3]),
        ...                        IndexedRow(6, [4, 5, 6])])
        >>> mat = IndexedRowMatrix(rows).toBlockMatrix()

        >>> # This IndexedRowMatrix will have 7 effective rows, due to
        >>> # the highest row index being 6, and the ensuing
        >>> # BlockMatrix will have 7 rows as well.
        >>> print(mat.numRows())
        7

        >>> print(mat.numCols())
        3
        """
        java_block_matrix = self._java_matrix_wrapper.call(
            "toBlockMatrix", rowsPerBlock, colsPerBlock
        )
        return BlockMatrix(java_block_matrix, rowsPerBlock, colsPerBlock)

    def computeSVD(
        self, k: int, computeU: bool = False, rCond: float = 1e-9
    ) -> SingularValueDecomposition["IndexedRowMatrix", Matrix]:
        """
        Computes the singular value decomposition of the IndexedRowMatrix.

        The given row matrix A of dimension (m X n) is decomposed into
        U * s * V'T where

        * U: (m X k) (left singular vectors) is a IndexedRowMatrix
             whose columns are the eigenvectors of (A X A')
        * s: DenseVector consisting of square root of the eigenvalues
             (singular values) in descending order.
        * v: (n X k) (right singular vectors) is a Matrix whose columns
             are the eigenvectors of (A' X A)

        For more specific details on implementation, please refer
        the scala documentation.

        .. versionadded:: 2.2.0

        Parameters
        ----------
        k : int
            Number of leading singular values to keep (`0 < k <= n`).
            It might return less than k if there are numerically zero singular values
            or there are not enough Ritz values converged before the maximum number of
            Arnoldi update iterations is reached (in case that matrix A is ill-conditioned).
        computeU : bool, optional
            Whether or not to compute U. If set to be
            True, then U is computed by A * V * s^-1
        rCond : float, optional
            Reciprocal condition number. All singular values
            smaller than rCond * s[0] are treated as zero
            where s[0] is the largest singular value.

        Returns
        -------
        :py:class:`SingularValueDecomposition`

        Examples
        --------
        >>> rows = [(0, (3, 1, 1)), (1, (-1, 3, 1))]
        >>> irm = IndexedRowMatrix(sc.parallelize(rows))
        >>> svd_model = irm.computeSVD(2, True)
        >>> svd_model.U.rows.collect() # doctest: +NORMALIZE_WHITESPACE
        [IndexedRow(0, [-0.707106781187,0.707106781187]),\
        IndexedRow(1, [-0.707106781187,-0.707106781187])]
        >>> svd_model.s
        DenseVector([3.4641, 3.1623])
        >>> svd_model.V
        DenseMatrix(3, 2, [-0.4082, -0.8165, -0.4082, 0.8944, -0.4472, 0.0], 0)
        """
        j_model = self._java_matrix_wrapper.call("computeSVD", int(k), bool(computeU), float(rCond))
        return SingularValueDecomposition(j_model)

    def multiply(self, matrix: Matrix) -> "IndexedRowMatrix":
        """
        Multiply this matrix by a local dense matrix on the right.

        .. versionadded:: 2.2.0

        Parameters
        ----------
        matrix : :py:class:`pyspark.mllib.linalg.Matrix`
            a local dense matrix whose number of rows must match the number of columns
            of this matrix

        Returns
        -------
        :py:class:`IndexedRowMatrix`

        Examples
        --------
        >>> mat = IndexedRowMatrix(sc.parallelize([(0, (0, 1)), (1, (2, 3))]))
        >>> mat.multiply(DenseMatrix(2, 2, [0, 2, 1, 3])).rows.collect()
        [IndexedRow(0, [2.0,3.0]), IndexedRow(1, [6.0,11.0])]
        """
        if not isinstance(matrix, DenseMatrix):
            raise TypeError("Only multiplication with DenseMatrix is supported.")
        return IndexedRowMatrix(self._java_matrix_wrapper.call("multiply", matrix))


class MatrixEntry:
    """
    Represents an entry of a CoordinateMatrix.

    Just a wrapper over a (int, int, float) tuple.

    Parameters
    ----------
    i : int
        The row index of the matrix.
    j : int
        The column index of the matrix.
    value : float
        The (i, j)th entry of the matrix, as a float.
    """

    def __init__(self, i: int, j: int, value: float) -> None:
        self.i = int(i)
        self.j = int(j)
        self.value = float(value)

    def __repr__(self) -> str:
        return "MatrixEntry(%s, %s, %s)" % (self.i, self.j, self.value)


def _convert_to_matrix_entry(entry: Any) -> MatrixEntry:
    if isinstance(entry, MatrixEntry):
        return entry
    elif isinstance(entry, tuple) and len(entry) == 3:
        return MatrixEntry(*entry)
    else:
        raise TypeError("Cannot convert type %s into MatrixEntry" % type(entry))


class CoordinateMatrix(DistributedMatrix):
    """
    Represents a matrix in coordinate format.

    Parameters
    ----------
    entries : :py:class:`pyspark.RDD`
        An RDD of MatrixEntry inputs or
        (int, int, float) tuples.
    numRows : int, optional
        Number of rows in the matrix. A non-positive
        value means unknown, at which point the number
        of rows will be determined by the max row
        index plus one.
    numCols : int, optional
        Number of columns in the matrix. A non-positive
        value means unknown, at which point the number
        of columns will be determined by the max row
        index plus one.
    """

    def __init__(
        self,
        entries: RDD[Union[Tuple[int, int, float], MatrixEntry]],
        numRows: int = 0,
        numCols: int = 0,
    ):
        """
        Note: This docstring is not shown publicly.

        Create a wrapper over a Java CoordinateMatrix.

        Publicly, we require that `rows` be an RDD.  However, for
        internal usage, `rows` can also be a Java CoordinateMatrix
        object, in which case we can wrap it directly.  This
        assists in clean matrix conversions.

        Examples
        --------
        >>> entries = sc.parallelize([MatrixEntry(0, 0, 1.2),
        ...                           MatrixEntry(6, 4, 2.1)])
        >>> mat = CoordinateMatrix(entries)

        >>> mat_diff = CoordinateMatrix(entries)
        >>> (mat_diff._java_matrix_wrapper._java_model ==
        ...  mat._java_matrix_wrapper._java_model)
        False

        >>> mat_same = CoordinateMatrix(mat._java_matrix_wrapper._java_model)
        >>> (mat_same._java_matrix_wrapper._java_model ==
        ...  mat._java_matrix_wrapper._java_model)
        True
        """
        if isinstance(entries, RDD):
            entries = entries.map(_convert_to_matrix_entry)
            # We use DataFrames for serialization of MatrixEntry entries
            # from Python, so first convert the RDD to a DataFrame on
            # this side. This will convert each MatrixEntry to a Row
            # containing the 'i', 'j', and 'value' values, which can
            # each be easily serialized. We will convert back to
            # MatrixEntry inputs on the Scala side.
            java_matrix = callMLlibFunc(
                "createCoordinateMatrix", entries.toDF(), int(numRows), int(numCols)
            )
        elif (
            isinstance(entries, JavaObject)
            and entries.getClass().getSimpleName() == "CoordinateMatrix"
        ):
            java_matrix = entries
        else:
            raise TypeError(
                "entries should be an RDD of MatrixEntry entries or "
                "(int, int, float) tuples, got %s" % type(entries)
            )

        self._java_matrix_wrapper = JavaModelWrapper(java_matrix)

    @property
    def entries(self) -> RDD[MatrixEntry]:
        """
        Entries of the CoordinateMatrix stored as an RDD of
        MatrixEntries.

        Examples
        --------
        >>> mat = CoordinateMatrix(sc.parallelize([MatrixEntry(0, 0, 1.2),
        ...                                        MatrixEntry(6, 4, 2.1)]))
        >>> entries = mat.entries
        >>> entries.first()
        MatrixEntry(0, 0, 1.2)
        """
        # We use DataFrames for serialization of MatrixEntry entries
        # from Java, so we first convert the RDD of entries to a
        # DataFrame on the Scala/Java side. Then we map each Row in
        # the DataFrame back to a MatrixEntry on this side.
        entries_df = callMLlibFunc("getMatrixEntries", self._java_matrix_wrapper._java_model)
        entries = entries_df.rdd.map(lambda row: MatrixEntry(row[0], row[1], row[2]))
        return entries

    def numRows(self) -> int:
        """
        Get or compute the number of rows.

        Examples
        --------
        >>> entries = sc.parallelize([MatrixEntry(0, 0, 1.2),
        ...                           MatrixEntry(1, 0, 2),
        ...                           MatrixEntry(2, 1, 3.7)])

        >>> mat = CoordinateMatrix(entries)
        >>> print(mat.numRows())
        3

        >>> mat = CoordinateMatrix(entries, 7, 6)
        >>> print(mat.numRows())
        7
        """
        return self._java_matrix_wrapper.call("numRows")

    def numCols(self) -> int:
        """
        Get or compute the number of cols.

        Examples
        --------
        >>> entries = sc.parallelize([MatrixEntry(0, 0, 1.2),
        ...                           MatrixEntry(1, 0, 2),
        ...                           MatrixEntry(2, 1, 3.7)])

        >>> mat = CoordinateMatrix(entries)
        >>> print(mat.numCols())
        2

        >>> mat = CoordinateMatrix(entries, 7, 6)
        >>> print(mat.numCols())
        6
        """
        return self._java_matrix_wrapper.call("numCols")

    def transpose(self) -> "CoordinateMatrix":
        """
        Transpose this CoordinateMatrix.

        .. versionadded:: 2.0.0

        Examples
        --------
        >>> entries = sc.parallelize([MatrixEntry(0, 0, 1.2),
        ...                           MatrixEntry(1, 0, 2),
        ...                           MatrixEntry(2, 1, 3.7)])
        >>> mat = CoordinateMatrix(entries)
        >>> mat_transposed = mat.transpose()

        >>> print(mat_transposed.numRows())
        2

        >>> print(mat_transposed.numCols())
        3
        """
        java_transposed_matrix = self._java_matrix_wrapper.call("transpose")
        return CoordinateMatrix(java_transposed_matrix)

    def toRowMatrix(self) -> RowMatrix:
        """
        Convert this matrix to a RowMatrix.

        Examples
        --------
        >>> entries = sc.parallelize([MatrixEntry(0, 0, 1.2),
        ...                           MatrixEntry(6, 4, 2.1)])
        >>> mat = CoordinateMatrix(entries).toRowMatrix()

        >>> # This CoordinateMatrix will have 7 effective rows, due to
        >>> # the highest row index being 6, but the ensuing RowMatrix
        >>> # will only have 2 rows since there are only entries on 2
        >>> # unique rows.
        >>> print(mat.numRows())
        2

        >>> # This CoordinateMatrix will have 5 columns, due to the
        >>> # highest column index being 4, and the ensuing RowMatrix
        >>> # will have 5 columns as well.
        >>> print(mat.numCols())
        5
        """
        java_row_matrix = self._java_matrix_wrapper.call("toRowMatrix")
        return RowMatrix(java_row_matrix)

    def toIndexedRowMatrix(self) -> IndexedRowMatrix:
        """
        Convert this matrix to an IndexedRowMatrix.

        Examples
        --------
        >>> entries = sc.parallelize([MatrixEntry(0, 0, 1.2),
        ...                           MatrixEntry(6, 4, 2.1)])
        >>> mat = CoordinateMatrix(entries).toIndexedRowMatrix()

        >>> # This CoordinateMatrix will have 7 effective rows, due to
        >>> # the highest row index being 6, and the ensuing
        >>> # IndexedRowMatrix will have 7 rows as well.
        >>> print(mat.numRows())
        7

        >>> # This CoordinateMatrix will have 5 columns, due to the
        >>> # highest column index being 4, and the ensuing
        >>> # IndexedRowMatrix will have 5 columns as well.
        >>> print(mat.numCols())
        5
        """
        java_indexed_row_matrix = self._java_matrix_wrapper.call("toIndexedRowMatrix")
        return IndexedRowMatrix(java_indexed_row_matrix)

    def toBlockMatrix(self, rowsPerBlock: int = 1024, colsPerBlock: int = 1024) -> "BlockMatrix":
        """
        Convert this matrix to a BlockMatrix.

        Parameters
        ----------
        rowsPerBlock : int, optional
            Number of rows that make up each block.
            The blocks forming the final rows are not
            required to have the given number of rows.
        colsPerBlock : int, optional
            Number of columns that make up each block.
            The blocks forming the final columns are not
            required to have the given number of columns.

        Examples
        --------
        >>> entries = sc.parallelize([MatrixEntry(0, 0, 1.2),
        ...                           MatrixEntry(6, 4, 2.1)])
        >>> mat = CoordinateMatrix(entries).toBlockMatrix()

        >>> # This CoordinateMatrix will have 7 effective rows, due to
        >>> # the highest row index being 6, and the ensuing
        >>> # BlockMatrix will have 7 rows as well.
        >>> print(mat.numRows())
        7

        >>> # This CoordinateMatrix will have 5 columns, due to the
        >>> # highest column index being 4, and the ensuing
        >>> # BlockMatrix will have 5 columns as well.
        >>> print(mat.numCols())
        5
        """
        java_block_matrix = self._java_matrix_wrapper.call(
            "toBlockMatrix", rowsPerBlock, colsPerBlock
        )
        return BlockMatrix(java_block_matrix, rowsPerBlock, colsPerBlock)


def _convert_to_matrix_block_tuple(block: Any) -> Tuple[Tuple[int, int], Matrix]:
    if (
        isinstance(block, tuple)
        and len(block) == 2
        and isinstance(block[0], tuple)
        and len(block[0]) == 2
        and isinstance(block[1], Matrix)
    ):
        blockRowIndex = int(block[0][0])
        blockColIndex = int(block[0][1])
        subMatrix = block[1]
        return ((blockRowIndex, blockColIndex), subMatrix)
    else:
        raise TypeError("Cannot convert type %s into a sub-matrix block tuple" % type(block))


class BlockMatrix(DistributedMatrix):
    """
    Represents a distributed matrix in blocks of local matrices.

    Parameters
    ----------
    blocks : :py:class:`pyspark.RDD`
        An RDD of sub-matrix blocks
        ((blockRowIndex, blockColIndex), sub-matrix) that
        form this distributed matrix. If multiple blocks
        with the same index exist, the results for
        operations like add and multiply will be
        unpredictable.
    rowsPerBlock : int
        Number of rows that make up each block.
        The blocks forming the final rows are not
        required to have the given number of rows.
    colsPerBlock : int
        Number of columns that make up each block.
        The blocks forming the final columns are not
        required to have the given number of columns.
    numRows : int, optional
        Number of rows of this matrix. If the supplied
        value is less than or equal to zero, the number
        of rows will be calculated when `numRows` is
        invoked.
    numCols : int, optional
        Number of columns of this matrix. If the supplied
        value is less than or equal to zero, the number
        of columns will be calculated when `numCols` is
        invoked.
    """

    def __init__(
        self,
        blocks: RDD[Tuple[Tuple[int, int], Matrix]],
        rowsPerBlock: int,
        colsPerBlock: int,
        numRows: int = 0,
        numCols: int = 0,
    ):
        """
        Note: This docstring is not shown publicly.

        Create a wrapper over a Java BlockMatrix.

        Publicly, we require that `blocks` be an RDD.  However, for
        internal usage, `blocks` can also be a Java BlockMatrix
        object, in which case we can wrap it directly.  This
        assists in clean matrix conversions.

        Examples
        --------
        >>> blocks = sc.parallelize([((0, 0), Matrices.dense(3, 2, [1, 2, 3, 4, 5, 6])),
        ...                          ((1, 0), Matrices.dense(3, 2, [7, 8, 9, 10, 11, 12]))])
        >>> mat = BlockMatrix(blocks, 3, 2)

        >>> mat_diff = BlockMatrix(blocks, 3, 2)
        >>> (mat_diff._java_matrix_wrapper._java_model ==
        ...  mat._java_matrix_wrapper._java_model)
        False

        >>> mat_same = BlockMatrix(mat._java_matrix_wrapper._java_model, 3, 2)
        >>> (mat_same._java_matrix_wrapper._java_model ==
        ...  mat._java_matrix_wrapper._java_model)
        True
        """
        if isinstance(blocks, RDD):
            blocks = blocks.map(_convert_to_matrix_block_tuple)
            # We use DataFrames for serialization of sub-matrix blocks
            # from Python, so first convert the RDD to a DataFrame on
            # this side. This will convert each sub-matrix block
            # tuple to a Row containing the 'blockRowIndex',
            # 'blockColIndex', and 'subMatrix' values, which can
            # each be easily serialized.  We will convert back to
            # ((blockRowIndex, blockColIndex), sub-matrix) tuples on
            # the Scala side.
            java_matrix = callMLlibFunc(
                "createBlockMatrix",
                blocks.toDF(),
                int(rowsPerBlock),
                int(colsPerBlock),
                int(numRows),
                int(numCols),
            )
        elif isinstance(blocks, JavaObject) and blocks.getClass().getSimpleName() == "BlockMatrix":
            java_matrix = blocks
        else:
            raise TypeError(
                "blocks should be an RDD of sub-matrix blocks as "
                "((int, int), matrix) tuples, got %s" % type(blocks)
            )

        self._java_matrix_wrapper = JavaModelWrapper(java_matrix)

    @property
    def blocks(self) -> RDD[Tuple[Tuple[int, int], Matrix]]:
        """
        The RDD of sub-matrix blocks
        ((blockRowIndex, blockColIndex), sub-matrix) that form this
        distributed matrix.

        Examples
        --------
        >>> mat = BlockMatrix(
        ...     sc.parallelize([((0, 0), Matrices.dense(3, 2, [1, 2, 3, 4, 5, 6])),
        ...                     ((1, 0), Matrices.dense(3, 2, [7, 8, 9, 10, 11, 12]))]), 3, 2)
        >>> blocks = mat.blocks
        >>> blocks.first()
        ((0, 0), DenseMatrix(3, 2, [1.0, 2.0, 3.0, 4.0, 5.0, 6.0], 0))

        """
        # We use DataFrames for serialization of sub-matrix blocks
        # from Java, so we first convert the RDD of blocks to a
        # DataFrame on the Scala/Java side. Then we map each Row in
        # the DataFrame back to a sub-matrix block on this side.
        blocks_df = callMLlibFunc("getMatrixBlocks", self._java_matrix_wrapper._java_model)
        blocks = blocks_df.rdd.map(lambda row: ((row[0][0], row[0][1]), row[1]))
        return blocks

    @property
    def rowsPerBlock(self) -> int:
        """
        Number of rows that make up each block.

        Examples
        --------
        >>> blocks = sc.parallelize([((0, 0), Matrices.dense(3, 2, [1, 2, 3, 4, 5, 6])),
        ...                          ((1, 0), Matrices.dense(3, 2, [7, 8, 9, 10, 11, 12]))])
        >>> mat = BlockMatrix(blocks, 3, 2)
        >>> mat.rowsPerBlock
        3
        """
        return self._java_matrix_wrapper.call("rowsPerBlock")

    @property
    def colsPerBlock(self) -> int:
        """
        Number of columns that make up each block.

        Examples
        --------
        >>> blocks = sc.parallelize([((0, 0), Matrices.dense(3, 2, [1, 2, 3, 4, 5, 6])),
        ...                          ((1, 0), Matrices.dense(3, 2, [7, 8, 9, 10, 11, 12]))])
        >>> mat = BlockMatrix(blocks, 3, 2)
        >>> mat.colsPerBlock
        2
        """
        return self._java_matrix_wrapper.call("colsPerBlock")

    @property
    def numRowBlocks(self) -> int:
        """
        Number of rows of blocks in the BlockMatrix.

        Examples
        --------
        >>> blocks = sc.parallelize([((0, 0), Matrices.dense(3, 2, [1, 2, 3, 4, 5, 6])),
        ...                          ((1, 0), Matrices.dense(3, 2, [7, 8, 9, 10, 11, 12]))])
        >>> mat = BlockMatrix(blocks, 3, 2)
        >>> mat.numRowBlocks
        2
        """
        return self._java_matrix_wrapper.call("numRowBlocks")

    @property
    def numColBlocks(self) -> int:
        """
        Number of columns of blocks in the BlockMatrix.

        Examples
        --------
        >>> blocks = sc.parallelize([((0, 0), Matrices.dense(3, 2, [1, 2, 3, 4, 5, 6])),
        ...                          ((1, 0), Matrices.dense(3, 2, [7, 8, 9, 10, 11, 12]))])
        >>> mat = BlockMatrix(blocks, 3, 2)
        >>> mat.numColBlocks
        1
        """
        return self._java_matrix_wrapper.call("numColBlocks")

    def numRows(self) -> int:
        """
        Get or compute the number of rows.

        Examples
        --------
        >>> blocks = sc.parallelize([((0, 0), Matrices.dense(3, 2, [1, 2, 3, 4, 5, 6])),
        ...                          ((1, 0), Matrices.dense(3, 2, [7, 8, 9, 10, 11, 12]))])

        >>> mat = BlockMatrix(blocks, 3, 2)
        >>> print(mat.numRows())
        6

        >>> mat = BlockMatrix(blocks, 3, 2, 7, 6)
        >>> print(mat.numRows())
        7
        """
        return self._java_matrix_wrapper.call("numRows")

    def numCols(self) -> int:
        """
        Get or compute the number of cols.

        Examples
        --------
        >>> blocks = sc.parallelize([((0, 0), Matrices.dense(3, 2, [1, 2, 3, 4, 5, 6])),
        ...                          ((1, 0), Matrices.dense(3, 2, [7, 8, 9, 10, 11, 12]))])

        >>> mat = BlockMatrix(blocks, 3, 2)
        >>> print(mat.numCols())
        2

        >>> mat = BlockMatrix(blocks, 3, 2, 7, 6)
        >>> print(mat.numCols())
        6
        """
        return self._java_matrix_wrapper.call("numCols")

    @since("2.0.0")
    def cache(self) -> "BlockMatrix":
        """
        Caches the underlying RDD.
        """
        self._java_matrix_wrapper.call("cache")
        return self

    @since("2.0.0")
    def persist(self, storageLevel: StorageLevel) -> "BlockMatrix":
        """
        Persists the underlying RDD with the specified storage level.
        """
        if not isinstance(storageLevel, StorageLevel):
            raise TypeError("`storageLevel` should be a StorageLevel, got %s" % type(storageLevel))
        javaStorageLevel = self._java_matrix_wrapper._sc._getJavaStorageLevel(storageLevel)
        self._java_matrix_wrapper.call("persist", javaStorageLevel)
        return self

    @since("2.0.0")
    def validate(self) -> None:
        """
        Validates the block matrix info against the matrix data (`blocks`)
        and throws an exception if any error is found.
        """
        self._java_matrix_wrapper.call("validate")

    def add(self, other: "BlockMatrix") -> "BlockMatrix":
        """
        Adds two block matrices together. The matrices must have the
        same size and matching `rowsPerBlock` and `colsPerBlock` values.
        If one of the sub matrix blocks that are being added is a
        SparseMatrix, the resulting sub matrix block will also be a
        SparseMatrix, even if it is being added to a DenseMatrix. If
        two dense sub matrix blocks are added, the output block will
        also be a DenseMatrix.

        Examples
        --------
        >>> dm1 = Matrices.dense(3, 2, [1, 2, 3, 4, 5, 6])
        >>> dm2 = Matrices.dense(3, 2, [7, 8, 9, 10, 11, 12])
        >>> sm = Matrices.sparse(3, 2, [0, 1, 3], [0, 1, 2], [7, 11, 12])
        >>> blocks1 = sc.parallelize([((0, 0), dm1), ((1, 0), dm2)])
        >>> blocks2 = sc.parallelize([((0, 0), dm1), ((1, 0), dm2)])
        >>> blocks3 = sc.parallelize([((0, 0), sm), ((1, 0), dm2)])
        >>> mat1 = BlockMatrix(blocks1, 3, 2)
        >>> mat2 = BlockMatrix(blocks2, 3, 2)
        >>> mat3 = BlockMatrix(blocks3, 3, 2)

        >>> mat1.add(mat2).toLocalMatrix()
        DenseMatrix(6, 2, [2.0, 4.0, 6.0, 14.0, 16.0, 18.0, 8.0, 10.0, 12.0, 20.0, 22.0, 24.0], 0)

        >>> mat1.add(mat3).toLocalMatrix()
        DenseMatrix(6, 2, [8.0, 2.0, 3.0, 14.0, 16.0, 18.0, 4.0, 16.0, 18.0, 20.0, 22.0, 24.0], 0)
        """
        if not isinstance(other, BlockMatrix):
            raise TypeError("Other should be a BlockMatrix, got %s" % type(other))

        other_java_block_matrix = other._java_matrix_wrapper._java_model
        java_block_matrix = self._java_matrix_wrapper.call("add", other_java_block_matrix)
        return BlockMatrix(java_block_matrix, self.rowsPerBlock, self.colsPerBlock)

    def subtract(self, other: "BlockMatrix") -> "BlockMatrix":
        """
        Subtracts the given block matrix `other` from this block matrix:
        `this - other`. The matrices must have the same size and
        matching `rowsPerBlock` and `colsPerBlock` values.  If one of
        the sub matrix blocks that are being subtracted is a
        SparseMatrix, the resulting sub matrix block will also be a
        SparseMatrix, even if it is being subtracted from a DenseMatrix.
        If two dense sub matrix blocks are subtracted, the output block
        will also be a DenseMatrix.

        .. versionadded:: 2.0.0

        Examples
        --------
        >>> dm1 = Matrices.dense(3, 2, [3, 1, 5, 4, 6, 2])
        >>> dm2 = Matrices.dense(3, 2, [7, 8, 9, 10, 11, 12])
        >>> sm = Matrices.sparse(3, 2, [0, 1, 3], [0, 1, 2], [1, 2, 3])
        >>> blocks1 = sc.parallelize([((0, 0), dm1), ((1, 0), dm2)])
        >>> blocks2 = sc.parallelize([((0, 0), dm2), ((1, 0), dm1)])
        >>> blocks3 = sc.parallelize([((0, 0), sm), ((1, 0), dm2)])
        >>> mat1 = BlockMatrix(blocks1, 3, 2)
        >>> mat2 = BlockMatrix(blocks2, 3, 2)
        >>> mat3 = BlockMatrix(blocks3, 3, 2)

        >>> mat1.subtract(mat2).toLocalMatrix()
        DenseMatrix(6, 2, [-4.0, -7.0, -4.0, 4.0, 7.0, 4.0, -6.0, -5.0, -10.0, 6.0, 5.0, 10.0], 0)

        >>> mat2.subtract(mat3).toLocalMatrix()
        DenseMatrix(6, 2, [6.0, 8.0, 9.0, -4.0, -7.0, -4.0, 10.0, 9.0, 9.0, -6.0, -5.0, -10.0], 0)
        """
        if not isinstance(other, BlockMatrix):
            raise TypeError("Other should be a BlockMatrix, got %s" % type(other))

        other_java_block_matrix = other._java_matrix_wrapper._java_model
        java_block_matrix = self._java_matrix_wrapper.call("subtract", other_java_block_matrix)
        return BlockMatrix(java_block_matrix, self.rowsPerBlock, self.colsPerBlock)

    def multiply(self, other: "BlockMatrix") -> "BlockMatrix":
        """
        Left multiplies this BlockMatrix by `other`, another
        BlockMatrix. The `colsPerBlock` of this matrix must equal the
        `rowsPerBlock` of `other`. If `other` contains any SparseMatrix
        blocks, they will have to be converted to DenseMatrix blocks.
        The output BlockMatrix will only consist of DenseMatrix blocks.
        This may cause some performance issues until support for
        multiplying two sparse matrices is added.

        Examples
        --------
        >>> dm1 = Matrices.dense(2, 3, [1, 2, 3, 4, 5, 6])
        >>> dm2 = Matrices.dense(2, 3, [7, 8, 9, 10, 11, 12])
        >>> dm3 = Matrices.dense(3, 2, [1, 2, 3, 4, 5, 6])
        >>> dm4 = Matrices.dense(3, 2, [7, 8, 9, 10, 11, 12])
        >>> sm = Matrices.sparse(3, 2, [0, 1, 3], [0, 1, 2], [7, 11, 12])
        >>> blocks1 = sc.parallelize([((0, 0), dm1), ((0, 1), dm2)])
        >>> blocks2 = sc.parallelize([((0, 0), dm3), ((1, 0), dm4)])
        >>> blocks3 = sc.parallelize([((0, 0), sm), ((1, 0), dm4)])
        >>> mat1 = BlockMatrix(blocks1, 2, 3)
        >>> mat2 = BlockMatrix(blocks2, 3, 2)
        >>> mat3 = BlockMatrix(blocks3, 3, 2)

        >>> mat1.multiply(mat2).toLocalMatrix()
        DenseMatrix(2, 2, [242.0, 272.0, 350.0, 398.0], 0)

        >>> mat1.multiply(mat3).toLocalMatrix()
        DenseMatrix(2, 2, [227.0, 258.0, 394.0, 450.0], 0)
        """
        if not isinstance(other, BlockMatrix):
            raise TypeError("Other should be a BlockMatrix, got %s" % type(other))

        other_java_block_matrix = other._java_matrix_wrapper._java_model
        java_block_matrix = self._java_matrix_wrapper.call("multiply", other_java_block_matrix)
        return BlockMatrix(java_block_matrix, self.rowsPerBlock, self.colsPerBlock)

    def transpose(self) -> "BlockMatrix":
        """
        Transpose this BlockMatrix. Returns a new BlockMatrix
        instance sharing the same underlying data. Is a lazy operation.

        .. versionadded:: 2.0.0

        Examples
        --------
        >>> blocks = sc.parallelize([((0, 0), Matrices.dense(3, 2, [1, 2, 3, 4, 5, 6])),
        ...                          ((1, 0), Matrices.dense(3, 2, [7, 8, 9, 10, 11, 12]))])
        >>> mat = BlockMatrix(blocks, 3, 2)

        >>> mat_transposed = mat.transpose()
        >>> mat_transposed.toLocalMatrix()
        DenseMatrix(2, 6, [1.0, 4.0, 2.0, 5.0, 3.0, 6.0, 7.0, 10.0, 8.0, 11.0, 9.0, 12.0], 0)
        """
        java_transposed_matrix = self._java_matrix_wrapper.call("transpose")
        return BlockMatrix(java_transposed_matrix, self.colsPerBlock, self.rowsPerBlock)

    def toLocalMatrix(self) -> Matrix:
        """
        Collect the distributed matrix on the driver as a DenseMatrix.

        Examples
        --------
        >>> blocks = sc.parallelize([((0, 0), Matrices.dense(3, 2, [1, 2, 3, 4, 5, 6])),
        ...                          ((1, 0), Matrices.dense(3, 2, [7, 8, 9, 10, 11, 12]))])
        >>> mat = BlockMatrix(blocks, 3, 2).toLocalMatrix()

        >>> # This BlockMatrix will have 6 effective rows, due to
        >>> # having two sub-matrix blocks stacked, each with 3 rows.
        >>> # The ensuing DenseMatrix will also have 6 rows.
        >>> print(mat.numRows)
        6

        >>> # This BlockMatrix will have 2 effective columns, due to
        >>> # having two sub-matrix blocks stacked, each with 2
        >>> # columns. The ensuing DenseMatrix will also have 2 columns.
        >>> print(mat.numCols)
        2
        """
        return self._java_matrix_wrapper.call("toLocalMatrix")

    def toIndexedRowMatrix(self) -> IndexedRowMatrix:
        """
        Convert this matrix to an IndexedRowMatrix.

        Examples
        --------
        >>> blocks = sc.parallelize([((0, 0), Matrices.dense(3, 2, [1, 2, 3, 4, 5, 6])),
        ...                          ((1, 0), Matrices.dense(3, 2, [7, 8, 9, 10, 11, 12]))])
        >>> mat = BlockMatrix(blocks, 3, 2).toIndexedRowMatrix()

        >>> # This BlockMatrix will have 6 effective rows, due to
        >>> # having two sub-matrix blocks stacked, each with 3 rows.
        >>> # The ensuing IndexedRowMatrix will also have 6 rows.
        >>> print(mat.numRows())
        6

        >>> # This BlockMatrix will have 2 effective columns, due to
        >>> # having two sub-matrix blocks stacked, each with 2 columns.
        >>> # The ensuing IndexedRowMatrix will also have 2 columns.
        >>> print(mat.numCols())
        2
        """
        java_indexed_row_matrix = self._java_matrix_wrapper.call("toIndexedRowMatrix")
        return IndexedRowMatrix(java_indexed_row_matrix)

    def toCoordinateMatrix(self) -> CoordinateMatrix:
        """
        Convert this matrix to a CoordinateMatrix.

        Examples
        --------
        >>> blocks = sc.parallelize([((0, 0), Matrices.dense(1, 2, [1, 2])),
        ...                          ((1, 0), Matrices.dense(1, 2, [7, 8]))])
        >>> mat = BlockMatrix(blocks, 1, 2).toCoordinateMatrix()
        >>> mat.entries.take(3)
        [MatrixEntry(0, 0, 1.0), MatrixEntry(0, 1, 2.0), MatrixEntry(1, 0, 7.0)]
        """
        java_coordinate_matrix = self._java_matrix_wrapper.call("toCoordinateMatrix")
        return CoordinateMatrix(java_coordinate_matrix)


def _test() -> None:
    import doctest
    import numpy
    from pyspark.sql import SparkSession
    from pyspark.mllib.linalg import Matrices
    import pyspark.mllib.linalg.distributed

    try:
        # Numpy 1.14+ changed it's string format.
        numpy.set_printoptions(legacy="1.13")
    except TypeError:
        pass
    globs = pyspark.mllib.linalg.distributed.__dict__.copy()
    spark = (
        SparkSession.builder.master("local[2]")
        .appName("mllib.linalg.distributed tests")
        .getOrCreate()
    )
    globs["sc"] = spark.sparkContext
    globs["Matrices"] = Matrices
    (failure_count, test_count) = doctest.testmod(globs=globs, optionflags=doctest.ELLIPSIS)
    spark.stop()
    if failure_count:
        sys.exit(-1)


if __name__ == "__main__":
    _test()
