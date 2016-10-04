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
from pyspark.ml.linalg import _convert_to_vector as _new_convert_to_vector, \
    _format_float, _have_scipy, _vector_size, _double_to_long_bits, _format_float_list
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


def _convert_to_vector(l):
    return Vectors.fromML(_new_convert_to_vector(l))


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

    def deserialize(self, datum):
        return Vectors.fromML(super(VectorUDT, self).deserialize(datum))


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

    def deserialize(self, datum):
        return Matrices.fromML(super(MatrixUDT, self).deserialize(datum))


class Vector(newlinalg.Vector):

    __UDT__ = VectorUDT()

    """
    Abstract class for DenseVector and SparseVector
    """
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


class Vectors(newlinalg.Vectors):

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


class Matrix(newlinalg.Matrix):

    __UDT__ = MatrixUDT()

    """
    Represents a local matrix.
    """
    def asML(self):
        """
        Convert this matrix to the new mllib-local representation.
        This does NOT copy the data; it copies references.
        """
        raise NotImplementedError


class DenseMatrix(Matrix, newlinalg.DenseMatrix):
    """
    Column-major dense matrix.
    """
    def __reduce__(self):
        return DenseMatrix, (
            self.numRows, self.numCols, self.values.tostring(),
            int(self.isTransposed))

    def asML(self):
        """
        Convert this matrix to the new mllib-local representation.
        This does NOT copy the data; it copies references.

        :return: :py:class:`pyspark.ml.linalg.DenseMatrix`

        .. versionadded:: 2.0.0
        """
        return newlinalg.DenseMatrix(self.numRows, self.numCols, self.values, self.isTransposed)


class SparseMatrix(Matrix, newlinalg.SparseMatrix):
    """Sparse Matrix stored in CSC format."""
    def __reduce__(self):
        return SparseMatrix, (
            self.numRows, self.numCols, self.colPtrs.tostring(),
            self.rowIndices.tostring(), self.values.tostring(),
            int(self.isTransposed))

    def asML(self):
        """
        Convert this matrix to the new mllib-local representation.
        This does NOT copy the data; it copies references.

        :return: :py:class:`pyspark.ml.linalg.SparseMatrix`

        .. versionadded:: 2.0.0
        """
        return newlinalg.SparseMatrix(self.numRows, self.numCols, self.colPtrs, self.rowIndices,
                                      self.values, self.isTransposed)


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
