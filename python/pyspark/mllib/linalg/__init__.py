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
MLlib utilities for linear algebra. For dense vectors, MLlib
uses the NumPy C{array} type, so you can simply pass NumPy arrays
around. For sparse vectors, users can construct a L{SparseVector}
object from MLlib or pass SciPy C{scipy.sparse} column vectors if
SciPy is available in their environment.
"""

import sys
import array
import struct

if sys.version >= '3':
    basestring = str
    xrange = range
    import copyreg as copy_reg
    long = int
else:
    from itertools import izip as zip
    import copy_reg

import numpy as np

from pyspark import since
from pyspark.ml import linalg as newlinalg
from pyspark.ml.linalg import _convert_to_vector, _format_float, _have_scipy, _vector_size, \
    _double_to_long_bits, _format_float_list
from pyspark.sql.types import UserDefinedType, StructField, StructType, ArrayType, DoubleType, \
    IntegerType, ByteType, BooleanType


__all__ = ['Vector', 'DenseVector', 'SparseVector', 'Vectors',
           'Matrix', 'DenseMatrix', 'SparseMatrix', 'Matrices',
           'QRDecomposition']


if sys.version_info[:2] == (2, 7):
    # speed up pickling array in Python 2.7
    def fast_pickle_array(ar):
        return array.array, (ar.typecode, ar.tostring())
    copy_reg.pickle(array.array, fast_pickle_array)


# Check whether we have SciPy. MLlib works without it too, but if we have it, some methods,
# such as _dot and _serialize_double_vector, start to support scipy.sparse matrices.

try:
    import scipy.sparse
    _have_scipy = True
except:
    # No SciPy in environment, but that's okay
    _have_scipy = False


class VectorUDT(newlinalg.VectorUDT):
    """
    SQL user-defined type (UDT) for Vector.
    """

    @classmethod
    def module(cls):
        return "pyspark.mllib.linalg"

    @classmethod
    def scalaUDT(cls):
        return "org.apache.spark.mllib.linalg.VectorUDT"


class MatrixUDT(newlinalg.MatrixUDT):
    """
    SQL user-defined type (UDT) for Matrix.
    """

    @classmethod
    def module(cls):
        return "pyspark.mllib.linalg"

    @classmethod
    def scalaUDT(cls):
        return "org.apache.spark.mllib.linalg.MatrixUDT"


class Vector(object):

    __UDT__ = VectorUDT()

    """
    Abstract class for DenseVector and SparseVector
    """
    def toArray(self):
        """
        Convert the vector into an numpy.ndarray

        :return: numpy.ndarray
        """
        raise NotImplementedError

    def asML(self):
        """
        Convert this vector to the new mllib-local representation.
        This does NOT copy the data; it copies references.

        :return: :py:class:`pyspark.ml.linalg.Vector`
        """
        raise NotImplementedError


class DenseVector(Vector, newlinalg.DenseVector):
    """
    A dense vector represented by a value array. We use numpy array for
    storage and arithmetics will be delegated to the underlying numpy
    array.

    >>> v = Vectors.dense([1.0, 2.0])
    >>> u = Vectors.dense([3.0, 4.0])
    >>> v + u
    DenseVector([4.0, 6.0])
    >>> 2 - v
    DenseVector([1.0, 0.0])
    >>> v / 2
    DenseVector([0.5, 1.0])
    >>> v * u
    DenseVector([3.0, 8.0])
    >>> u / v
    DenseVector([3.0, 2.0])
    >>> u % 2
    DenseVector([1.0, 0.0])
    """
    def __reduce__(self):
        return DenseVector, (self.array.tostring(),)

    @staticmethod
    def parse(s):
        """
        Parse string representation back into the DenseVector.

        >>> DenseVector.parse(' [ 0.0,1.0,2.0,  3.0]')
        DenseVector([0.0, 1.0, 2.0, 3.0])
        """
        start = s.find('[')
        if start == -1:
            raise ValueError("Array should start with '['.")
        end = s.find(']')
        if end == -1:
            raise ValueError("Array should end with ']'.")
        s = s[start + 1: end]

        try:
            values = [float(val) for val in s.split(',') if val]
        except ValueError:
            raise ValueError("Unable to parse values from %s" % s)
        return DenseVector(values)

    def asML(self):
        """
        Convert this vector to the new mllib-local representation.
        This does NOT copy the data; it copies references.

        :return: :py:class:`pyspark.ml.linalg.DenseVector`

        .. versionadded:: 2.0.0
        """
        return newlinalg.DenseVector(self.array)


class SparseVector(Vector, newlinalg.SparseVector):
    """
    A simple sparse vector class for passing data to MLlib. Users may
    alternatively pass SciPy's {scipy.sparse} data types.
    """
    def __reduce__(self):
        return (
            SparseVector,
            (self.size, self.indices.tostring(), self.values.tostring()))

    @staticmethod
    def parse(s):
        """
        Parse string representation back into the SparseVector.

        >>> SparseVector.parse(' (4, [0,1 ],[ 4.0,5.0] )')
        SparseVector(4, {0: 4.0, 1: 5.0})
        """
        start = s.find('(')
        if start == -1:
            raise ValueError("Tuple should start with '('")
        end = s.find(')')
        if end == -1:
            raise ValueError("Tuple should end with ')'")
        s = s[start + 1: end].strip()

        size = s[: s.find(',')]
        try:
            size = int(size)
        except ValueError:
            raise ValueError("Cannot parse size %s." % size)

        ind_start = s.find('[')
        if ind_start == -1:
            raise ValueError("Indices array should start with '['.")
        ind_end = s.find(']')
        if ind_end == -1:
            raise ValueError("Indices array should end with ']'")
        new_s = s[ind_start + 1: ind_end]
        ind_list = new_s.split(',')
        try:
            indices = [int(ind) for ind in ind_list if ind]
        except ValueError:
            raise ValueError("Unable to parse indices from %s." % new_s)
        s = s[ind_end + 1:].strip()

        val_start = s.find('[')
        if val_start == -1:
            raise ValueError("Values array should start with '['.")
        val_end = s.find(']')
        if val_end == -1:
            raise ValueError("Values array should end with ']'.")
        val_list = s[val_start + 1: val_end].split(',')
        try:
            values = [float(val) for val in val_list if val]
        except ValueError:
            raise ValueError("Unable to parse values from %s." % s)
        return SparseVector(size, indices, values)

    def asML(self):
        """
        Convert this vector to the new mllib-local representation.
        This does NOT copy the data; it copies references.

        :return: :py:class:`pyspark.ml.linalg.SparseVector`

        .. versionadded:: 2.0.0
        """
        return newlinalg.SparseVector(self.size, self.indices, self.values)


class Vectors(object):

    """
    Factory methods for working with vectors. Note that dense vectors
    are simply represented as NumPy array objects, so there is no need
    to covert them for use in MLlib. For sparse vectors, the factory
    methods in this class create an MLlib-compatible type, or users
    can pass in SciPy's C{scipy.sparse} column vectors.
    """

    @staticmethod
    def sparse(size, *args):
        """
        Create a sparse vector, using either a dictionary, a list of
        (index, value) pairs, or two separate arrays of indices and
        values (sorted by index).

        :param size: Size of the vector.
        :param args: Non-zero entries, as a dictionary, list of tuples,
                     or two sorted lists containing indices and values.

        >>> Vectors.sparse(4, {1: 1.0, 3: 5.5})
        SparseVector(4, {1: 1.0, 3: 5.5})
        >>> Vectors.sparse(4, [(1, 1.0), (3, 5.5)])
        SparseVector(4, {1: 1.0, 3: 5.5})
        >>> Vectors.sparse(4, [1, 3], [1.0, 5.5])
        SparseVector(4, {1: 1.0, 3: 5.5})
        """
        return SparseVector(size, *args)

    @staticmethod
    def dense(*elements):
        """
        Create a dense vector of 64-bit floats from a Python list or numbers.

        >>> Vectors.dense([1, 2, 3])
        DenseVector([1.0, 2.0, 3.0])
        >>> Vectors.dense(1.0, 2.0)
        DenseVector([1.0, 2.0])
        """
        if len(elements) == 1 and not isinstance(elements[0], (float, int, long)):
            # it's list, numpy.array or other iterable object.
            elements = elements[0]
        return DenseVector(elements)

    @staticmethod
    def fromML(vec):
        """
        Convert a vector from the new mllib-local representation.
        This does NOT copy the data; it copies references.

        :param vec: a :py:class:`pyspark.ml.linalg.Vector`
        :return: a :py:class:`pyspark.mllib.linalg.Vector`

        .. versionadded:: 2.0.0
        """
        if isinstance(vec, newlinalg.DenseVector):
            return DenseVector(vec.array)
        elif isinstance(vec, newlinalg.SparseVector):
            return SparseVector(vec.size, vec.indices, vec.values)
        else:
            raise TypeError("Unsupported vector type %s" % type(vec))

    @staticmethod
    def stringify(vector):
        """
        Converts a vector into a string, which can be recognized by
        Vectors.parse().

        >>> Vectors.stringify(Vectors.sparse(2, [1], [1.0]))
        '(2,[1],[1.0])'
        >>> Vectors.stringify(Vectors.dense([0.0, 1.0]))
        '[0.0,1.0]'
        """
        return str(vector)

    @staticmethod
    def squared_distance(v1, v2):
        """
        Squared distance between two vectors.
        a and b can be of type SparseVector, DenseVector, np.ndarray
        or array.array.

        >>> a = Vectors.sparse(4, [(0, 1), (3, 4)])
        >>> b = Vectors.dense([2, 5, 4, 1])
        >>> a.squared_distance(b)
        51.0
        """
        v1, v2 = _convert_to_vector(v1), _convert_to_vector(v2)
        return v1.squared_distance(v2)

    @staticmethod
    def norm(vector, p):
        """
        Find norm of the given vector.
        """
        return _convert_to_vector(vector).norm(p)

    @staticmethod
    def parse(s):
        """Parse a string representation back into the Vector.

        >>> Vectors.parse('[2,1,2 ]')
        DenseVector([2.0, 1.0, 2.0])
        >>> Vectors.parse(' ( 100,  [0],  [2])')
        SparseVector(100, {0: 2.0})
        """
        if s.find('(') == -1 and s.find('[') != -1:
            return DenseVector.parse(s)
        elif s.find('(') != -1:
            return SparseVector.parse(s)
        else:
            raise ValueError(
                "Cannot find tokens '[' or '(' from the input string.")

    @staticmethod
    def zeros(size):
        return DenseVector(np.zeros(size))

    @staticmethod
    def _equals(v1_indices, v1_values, v2_indices, v2_values):
        """
        Check equality between sparse/dense vectors,
        v1_indices and v2_indices assume to be strictly increasing.
        """
        v1_size = len(v1_values)
        v2_size = len(v2_values)
        k1 = 0
        k2 = 0
        all_equal = True
        while all_equal:
            while k1 < v1_size and v1_values[k1] == 0:
                k1 += 1
            while k2 < v2_size and v2_values[k2] == 0:
                k2 += 1

            if k1 >= v1_size or k2 >= v2_size:
                return k1 >= v1_size and k2 >= v2_size

            all_equal = v1_indices[k1] == v2_indices[k2] and v1_values[k1] == v2_values[k2]
            k1 += 1
            k2 += 1
        return all_equal


class Matrix(object):

    __UDT__ = MatrixUDT()

    """
    Represents a local matrix.
    """
    def __init__(self, numRows, numCols, isTransposed=False):
        self.numRows = numRows
        self.numCols = numCols
        self.isTransposed = isTransposed

    def toArray(self):
        """
        Returns its elements in a NumPy ndarray.
        """
        raise NotImplementedError

    def asML(self):
        """
        Convert this matrix to the new mllib-local representation.
        This does NOT copy the data; it copies references.
        """
        raise NotImplementedError

    @staticmethod
    def _convert_to_array(array_like, dtype):
        """
        Convert Matrix attributes which are array-like or buffer to array.
        """
        if isinstance(array_like, bytes):
            return np.frombuffer(array_like, dtype=dtype)
        return np.asarray(array_like, dtype=dtype)


class DenseMatrix(Matrix):
    """
    Column-major dense matrix.
    """
    def __init__(self, numRows, numCols, values, isTransposed=False):
        Matrix.__init__(self, numRows, numCols, isTransposed)
        values = self._convert_to_array(values, np.float64)
        assert len(values) == numRows * numCols
        self.values = values

    def __reduce__(self):
        return DenseMatrix, (
            self.numRows, self.numCols, self.values.tostring(),
            int(self.isTransposed))

    def __str__(self):
        """
        Pretty printing of a DenseMatrix

        >>> dm = DenseMatrix(2, 2, range(4))
        >>> print(dm)
        DenseMatrix([[ 0.,  2.],
                     [ 1.,  3.]])
        >>> dm = DenseMatrix(2, 2, range(4), isTransposed=True)
        >>> print(dm)
        DenseMatrix([[ 0.,  1.],
                     [ 2.,  3.]])
        """
        # Inspired by __repr__ in scipy matrices.
        array_lines = repr(self.toArray()).splitlines()

        # We need to adjust six spaces which is the difference in number
        # of letters between "DenseMatrix" and "array"
        x = '\n'.join([(" " * 6 + line) for line in array_lines[1:]])
        return array_lines[0].replace("array", "DenseMatrix") + "\n" + x

    def __repr__(self):
        """
        Representation of a DenseMatrix

        >>> dm = DenseMatrix(2, 2, range(4))
        >>> dm
        DenseMatrix(2, 2, [0.0, 1.0, 2.0, 3.0], False)
        """
        # If the number of values are less than seventeen then return as it is.
        # Else return first eight values and last eight values.
        if len(self.values) < 17:
            entries = _format_float_list(self.values)
        else:
            entries = (
                _format_float_list(self.values[:8]) +
                ["..."] +
                _format_float_list(self.values[-8:])
            )

        entries = ", ".join(entries)
        return "DenseMatrix({0}, {1}, [{2}], {3})".format(
            self.numRows, self.numCols, entries, self.isTransposed)

    def toArray(self):
        """
        Return an numpy.ndarray

        >>> m = DenseMatrix(2, 2, range(4))
        >>> m.toArray()
        array([[ 0.,  2.],
               [ 1.,  3.]])
        """
        if self.isTransposed:
            return np.asfortranarray(
                self.values.reshape((self.numRows, self.numCols)))
        else:
            return self.values.reshape((self.numRows, self.numCols), order='F')

    def toSparse(self):
        """Convert to SparseMatrix"""
        if self.isTransposed:
            values = np.ravel(self.toArray(), order='F')
        else:
            values = self.values
        indices = np.nonzero(values)[0]
        colCounts = np.bincount(indices // self.numRows)
        colPtrs = np.cumsum(np.hstack(
            (0, colCounts, np.zeros(self.numCols - colCounts.size))))
        values = values[indices]
        rowIndices = indices % self.numRows

        return SparseMatrix(self.numRows, self.numCols, colPtrs, rowIndices, values)

    def asML(self):
        """
        Convert this matrix to the new mllib-local representation.
        This does NOT copy the data; it copies references.

        :return: :py:class:`pyspark.ml.linalg.DenseMatrix`

        .. versionadded:: 2.0.0
        """
        return newlinalg.DenseMatrix(self.numRows, self.numCols, self.values, self.isTransposed)

    def __getitem__(self, indices):
        i, j = indices
        if i < 0 or i >= self.numRows:
            raise ValueError("Row index %d is out of range [0, %d)"
                             % (i, self.numRows))
        if j >= self.numCols or j < 0:
            raise ValueError("Column index %d is out of range [0, %d)"
                             % (j, self.numCols))

        if self.isTransposed:
            return self.values[i * self.numCols + j]
        else:
            return self.values[i + j * self.numRows]

    def __eq__(self, other):
        if (not isinstance(other, DenseMatrix) or
                self.numRows != other.numRows or
                self.numCols != other.numCols):
            return False

        self_values = np.ravel(self.toArray(), order='F')
        other_values = np.ravel(other.toArray(), order='F')
        return all(self_values == other_values)


class SparseMatrix(Matrix):
    """Sparse Matrix stored in CSC format."""
    def __init__(self, numRows, numCols, colPtrs, rowIndices, values,
                 isTransposed=False):
        Matrix.__init__(self, numRows, numCols, isTransposed)
        self.colPtrs = self._convert_to_array(colPtrs, np.int32)
        self.rowIndices = self._convert_to_array(rowIndices, np.int32)
        self.values = self._convert_to_array(values, np.float64)

        if self.isTransposed:
            if self.colPtrs.size != numRows + 1:
                raise ValueError("Expected colPtrs of size %d, got %d."
                                 % (numRows + 1, self.colPtrs.size))
        else:
            if self.colPtrs.size != numCols + 1:
                raise ValueError("Expected colPtrs of size %d, got %d."
                                 % (numCols + 1, self.colPtrs.size))
        if self.rowIndices.size != self.values.size:
            raise ValueError("Expected rowIndices of length %d, got %d."
                             % (self.rowIndices.size, self.values.size))

    def __str__(self):
        """
        Pretty printing of a SparseMatrix

        >>> sm1 = SparseMatrix(2, 2, [0, 2, 3], [0, 1, 1], [2, 3, 4])
        >>> print(sm1)
        2 X 2 CSCMatrix
        (0,0) 2.0
        (1,0) 3.0
        (1,1) 4.0
        >>> sm1 = SparseMatrix(2, 2, [0, 2, 3], [0, 1, 1], [2, 3, 4], True)
        >>> print(sm1)
        2 X 2 CSRMatrix
        (0,0) 2.0
        (0,1) 3.0
        (1,1) 4.0
        """
        spstr = "{0} X {1} ".format(self.numRows, self.numCols)
        if self.isTransposed:
            spstr += "CSRMatrix\n"
        else:
            spstr += "CSCMatrix\n"

        cur_col = 0
        smlist = []

        # Display first 16 values.
        if len(self.values) <= 16:
            zipindval = zip(self.rowIndices, self.values)
        else:
            zipindval = zip(self.rowIndices[:16], self.values[:16])
        for i, (rowInd, value) in enumerate(zipindval):
            if self.colPtrs[cur_col + 1] <= i:
                cur_col += 1
            if self.isTransposed:
                smlist.append('({0},{1}) {2}'.format(
                    cur_col, rowInd, _format_float(value)))
            else:
                smlist.append('({0},{1}) {2}'.format(
                    rowInd, cur_col, _format_float(value)))
        spstr += "\n".join(smlist)

        if len(self.values) > 16:
            spstr += "\n.." * 2
        return spstr

    def __repr__(self):
        """
        Representation of a SparseMatrix

        >>> sm1 = SparseMatrix(2, 2, [0, 2, 3], [0, 1, 1], [2, 3, 4])
        >>> sm1
        SparseMatrix(2, 2, [0, 2, 3], [0, 1, 1], [2.0, 3.0, 4.0], False)
        """
        rowIndices = list(self.rowIndices)
        colPtrs = list(self.colPtrs)

        if len(self.values) <= 16:
            values = _format_float_list(self.values)

        else:
            values = (
                _format_float_list(self.values[:8]) +
                ["..."] +
                _format_float_list(self.values[-8:])
            )
            rowIndices = rowIndices[:8] + ["..."] + rowIndices[-8:]

        if len(self.colPtrs) > 16:
            colPtrs = colPtrs[:8] + ["..."] + colPtrs[-8:]

        values = ", ".join(values)
        rowIndices = ", ".join([str(ind) for ind in rowIndices])
        colPtrs = ", ".join([str(ptr) for ptr in colPtrs])
        return "SparseMatrix({0}, {1}, [{2}], [{3}], [{4}], {5})".format(
            self.numRows, self.numCols, colPtrs, rowIndices,
            values, self.isTransposed)

    def __reduce__(self):
        return SparseMatrix, (
            self.numRows, self.numCols, self.colPtrs.tostring(),
            self.rowIndices.tostring(), self.values.tostring(),
            int(self.isTransposed))

    def __getitem__(self, indices):
        i, j = indices
        if i < 0 or i >= self.numRows:
            raise ValueError("Row index %d is out of range [0, %d)"
                             % (i, self.numRows))
        if j < 0 or j >= self.numCols:
            raise ValueError("Column index %d is out of range [0, %d)"
                             % (j, self.numCols))

        # If a CSR matrix is given, then the row index should be searched
        # for in ColPtrs, and the column index should be searched for in the
        # corresponding slice obtained from rowIndices.
        if self.isTransposed:
            j, i = i, j

        colStart = self.colPtrs[j]
        colEnd = self.colPtrs[j + 1]
        nz = self.rowIndices[colStart: colEnd]
        ind = np.searchsorted(nz, i) + colStart
        if ind < colEnd and self.rowIndices[ind] == i:
            return self.values[ind]
        else:
            return 0.0

    def toArray(self):
        """
        Return an numpy.ndarray
        """
        A = np.zeros((self.numRows, self.numCols), dtype=np.float64, order='F')
        for k in xrange(self.colPtrs.size - 1):
            startptr = self.colPtrs[k]
            endptr = self.colPtrs[k + 1]
            if self.isTransposed:
                A[k, self.rowIndices[startptr:endptr]] = self.values[startptr:endptr]
            else:
                A[self.rowIndices[startptr:endptr], k] = self.values[startptr:endptr]
        return A

    def toDense(self):
        densevals = np.ravel(self.toArray(), order='F')
        return DenseMatrix(self.numRows, self.numCols, densevals)

    def asML(self):
        """
        Convert this matrix to the new mllib-local representation.
        This does NOT copy the data; it copies references.

        :return: :py:class:`pyspark.ml.linalg.SparseMatrix`

        .. versionadded:: 2.0.0
        """
        return newlinalg.SparseMatrix(self.numRows, self.numCols, self.colPtrs, self.rowIndices,
                                      self.values, self.isTransposed)

    # TODO: More efficient implementation:
    def __eq__(self, other):
        return np.all(self.toArray() == other.toArray())


class Matrices(object):
    @staticmethod
    def dense(numRows, numCols, values):
        """
        Create a DenseMatrix
        """
        return DenseMatrix(numRows, numCols, values)

    @staticmethod
    def sparse(numRows, numCols, colPtrs, rowIndices, values):
        """
        Create a SparseMatrix
        """
        return SparseMatrix(numRows, numCols, colPtrs, rowIndices, values)

    @staticmethod
    def fromML(mat):
        """
        Convert a matrix from the new mllib-local representation.
        This does NOT copy the data; it copies references.

        :param mat: a :py:class:`pyspark.ml.linalg.Matrix`
        :return: a :py:class:`pyspark.mllib.linalg.Matrix`

        .. versionadded:: 2.0.0
        """
        if isinstance(mat, newlinalg.DenseMatrix):
            return DenseMatrix(mat.numRows, mat.numCols, mat.values, mat.isTransposed)
        elif isinstance(mat, newlinalg.SparseMatrix):
            return SparseMatrix(mat.numRows, mat.numCols, mat.colPtrs, mat.rowIndices,
                                mat.values, mat.isTransposed)
        else:
            raise TypeError("Unsupported matrix type %s" % type(mat))


class QRDecomposition(object):
    """
    Represents QR factors.
    """
    def __init__(self, Q, R):
        self._Q = Q
        self._R = R

    @property
    @since('2.0.0')
    def Q(self):
        """
        An orthogonal matrix Q in a QR decomposition.
        May be null if not computed.
        """
        return self._Q

    @property
    @since('2.0.0')
    def R(self):
        """
        An upper triangular matrix R in a QR decomposition.
        """
        return self._R


def _test():
    import doctest
    (failure_count, test_count) = doctest.testmod(optionflags=doctest.ELLIPSIS)
    if failure_count:
        exit(-1)

if __name__ == "__main__":
    _test()
