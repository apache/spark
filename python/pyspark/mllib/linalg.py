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
import copy_reg

import numpy as np

__all__ = ['Vector', 'DenseVector', 'SparseVector', 'Vectors']


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


def _convert_to_vector(l):
    if isinstance(l, Vector):
        return l
    elif type(l) in (array.array, np.array, np.ndarray, list, tuple):
        return DenseVector(l)
    elif _have_scipy and scipy.sparse.issparse(l):
        assert l.shape[1] == 1, "Expected column vector"
        csc = l.tocsc()
        return SparseVector(l.shape[0], csc.indices, csc.data)
    else:
        raise TypeError("Cannot convert type %s into Vector" % type(l))


def _vector_size(v):
    """
    Returns the size of the vector.

    >>> _vector_size([1., 2., 3.])
    3
    >>> _vector_size((1., 2., 3.))
    3
    >>> _vector_size(array.array('d', [1., 2., 3.]))
    3
    >>> _vector_size(np.zeros(3))
    3
    >>> _vector_size(np.zeros((3, 1)))
    3
    >>> _vector_size(np.zeros((1, 3)))
    Traceback (most recent call last):
        ...
    ValueError: Cannot treat an ndarray of shape (1, 3) as a vector
    """
    if isinstance(v, Vector):
        return len(v)
    elif type(v) in (array.array, list, tuple):
        return len(v)
    elif type(v) == np.ndarray:
        if v.ndim == 1 or (v.ndim == 2 and v.shape[1] == 1):
            return len(v)
        else:
            raise ValueError("Cannot treat an ndarray of shape %s as a vector" % str(v.shape))
    elif _have_scipy and scipy.sparse.issparse(v):
        assert v.shape[1] == 1, "Expected column vector"
        return v.shape[0]
    else:
        raise TypeError("Cannot treat type %s as a vector" % type(v))


class Vector(object):
    """
    Abstract class for DenseVector and SparseVector
    """
    def toArray(self):
        """
        Convert the vector into an numpy.ndarray
        :return: numpy.ndarray
        """
        raise NotImplementedError


class DenseVector(Vector):
    """
    A dense vector represented by a value array.
    """
    def __init__(self, ar):
        if not isinstance(ar, array.array):
            ar = array.array('d', ar)
        self.array = ar

    def __reduce__(self):
        return DenseVector, (self.array,)

    def dot(self, other):
        """
        Compute the dot product of two Vectors. We support
        (Numpy array, list, SparseVector, or SciPy sparse)
        and a target NumPy array that is either 1- or 2-dimensional.
        Equivalent to calling numpy.dot of the two vectors.

        >>> dense = DenseVector(array.array('d', [1., 2.]))
        >>> dense.dot(dense)
        5.0
        >>> dense.dot(SparseVector(2, [0, 1], [2., 1.]))
        4.0
        >>> dense.dot(range(1, 3))
        5.0
        >>> dense.dot(np.array(range(1, 3)))
        5.0
        >>> dense.dot([1.,])
        Traceback (most recent call last):
            ...
        AssertionError: dimension mismatch
        >>> dense.dot(np.reshape([1., 2., 3., 4.], (2, 2), order='F'))
        array([  5.,  11.])
        >>> dense.dot(np.reshape([1., 2., 3.], (3, 1), order='F'))
        Traceback (most recent call last):
            ...
        AssertionError: dimension mismatch
        """
        if type(other) == np.ndarray and other.ndim > 1:
            assert len(self) == other.shape[0], "dimension mismatch"
            return np.dot(self.toArray(), other)
        elif _have_scipy and scipy.sparse.issparse(other):
            assert len(self) == other.shape[0], "dimension mismatch"
            return other.transpose().dot(self.toArray())
        else:
            assert len(self) == _vector_size(other), "dimension mismatch"
            if isinstance(other, SparseVector):
                return other.dot(self)
            elif isinstance(other, Vector):
                return np.dot(self.toArray(), other.toArray())
            else:
                return np.dot(self.toArray(), other)

    def squared_distance(self, other):
        """
        Squared distance of two Vectors.

        >>> dense1 = DenseVector(array.array('d', [1., 2.]))
        >>> dense1.squared_distance(dense1)
        0.0
        >>> dense2 = np.array([2., 1.])
        >>> dense1.squared_distance(dense2)
        2.0
        >>> dense3 = [2., 1.]
        >>> dense1.squared_distance(dense3)
        2.0
        >>> sparse1 = SparseVector(2, [0, 1], [2., 1.])
        >>> dense1.squared_distance(sparse1)
        2.0
        >>> dense1.squared_distance([1.,])
        Traceback (most recent call last):
            ...
        AssertionError: dimension mismatch
        >>> dense1.squared_distance(SparseVector(1, [0,], [1.,]))
        Traceback (most recent call last):
            ...
        AssertionError: dimension mismatch
        """
        assert len(self) == _vector_size(other), "dimension mismatch"
        if isinstance(other, SparseVector):
            return other.squared_distance(self)
        elif _have_scipy and scipy.sparse.issparse(other):
            return _convert_to_vector(other).squared_distance(self)

        if isinstance(other, Vector):
            other = other.toArray()
        elif not isinstance(other, np.ndarray):
            other = np.array(other)
        diff = self.toArray() - other
        return np.dot(diff, diff)

    def toArray(self):
        return np.array(self.array)

    def __getitem__(self, item):
        return self.array[item]

    def __len__(self):
        return len(self.array)

    def __str__(self):
        return "[" + ",".join([str(v) for v in self.array]) + "]"

    def __repr__(self):
        return "DenseVector(%r)" % self.array

    def __eq__(self, other):
        return isinstance(other, DenseVector) and self.array == other.array

    def __ne__(self, other):
        return not self == other

    def __getattr__(self, item):
        return getattr(self.array, item)


class SparseVector(Vector):
    """
    A simple sparse vector class for passing data to MLlib. Users may
    alternatively pass SciPy's {scipy.sparse} data types.
    """
    def __init__(self, size, *args):
        """
        Create a sparse vector, using either a dictionary, a list of
        (index, value) pairs, or two separate arrays of indices and
        values (sorted by index).

        :param size: Size of the vector.
        :param args: Non-zero entries, as a dictionary, list of tupes,
               or two sorted lists containing indices and values.

        >>> print SparseVector(4, {1: 1.0, 3: 5.5})
        (4,[1,3],[1.0,5.5])
        >>> print SparseVector(4, [(1, 1.0), (3, 5.5)])
        (4,[1,3],[1.0,5.5])
        >>> print SparseVector(4, [1, 3], [1.0, 5.5])
        (4,[1,3],[1.0,5.5])
        """
        self.size = int(size)
        assert 1 <= len(args) <= 2, "must pass either 2 or 3 arguments"
        if len(args) == 1:
            pairs = args[0]
            if type(pairs) == dict:
                pairs = pairs.items()
            pairs = sorted(pairs)
            self.indices = array.array('i', [p[0] for p in pairs])
            self.values = array.array('d', [p[1] for p in pairs])
        else:
            assert len(args[0]) == len(args[1]), "index and value arrays not same length"
            self.indices = array.array('i', args[0])
            self.values = array.array('d', args[1])
            for i in xrange(len(self.indices) - 1):
                if self.indices[i] >= self.indices[i + 1]:
                    raise TypeError("indices array must be sorted")

    def __reduce__(self):
        return (SparseVector, (self.size, self.indices, self.values))

    def dot(self, other):
        """
        Dot product with a SparseVector or 1- or 2-dimensional Numpy array.

        >>> a = SparseVector(4, [1, 3], [3.0, 4.0])
        >>> a.dot(a)
        25.0
        >>> a.dot(array.array('d', [1., 2., 3., 4.]))
        22.0
        >>> b = SparseVector(4, [2, 4], [1.0, 2.0])
        >>> a.dot(b)
        0.0
        >>> a.dot(np.array([[1, 1], [2, 2], [3, 3], [4, 4]]))
        array([ 22.,  22.])
        >>> a.dot([1., 2., 3.])
        Traceback (most recent call last):
            ...
        AssertionError: dimension mismatch
        >>> a.dot(np.array([1., 2.]))
        Traceback (most recent call last):
            ...
        AssertionError: dimension mismatch
        >>> a.dot(DenseVector([1., 2.]))
        Traceback (most recent call last):
            ...
        AssertionError: dimension mismatch
        >>> a.dot(np.zeros((3, 2)))
        Traceback (most recent call last):
            ...
        AssertionError: dimension mismatch
        """
        if type(other) == np.ndarray:
            if other.ndim == 2:
                results = [self.dot(other[:, i]) for i in xrange(other.shape[1])]
                return np.array(results)
            elif other.ndim > 2:
                raise ValueError("Cannot call dot with %d-dimensional array" % other.ndim)

        assert len(self) == _vector_size(other), "dimension mismatch"

        if type(other) in (np.ndarray, array.array, DenseVector):
            result = 0.0
            for i in xrange(len(self.indices)):
                result += self.values[i] * other[self.indices[i]]
            return result

        elif type(other) is SparseVector:
            result = 0.0
            i, j = 0, 0
            while i < len(self.indices) and j < len(other.indices):
                if self.indices[i] == other.indices[j]:
                    result += self.values[i] * other.values[j]
                    i += 1
                    j += 1
                elif self.indices[i] < other.indices[j]:
                    i += 1
                else:
                    j += 1
            return result

        else:
            return self.dot(_convert_to_vector(other))

    def squared_distance(self, other):
        """
        Squared distance from a SparseVector or 1-dimensional NumPy array.

        >>> a = SparseVector(4, [1, 3], [3.0, 4.0])
        >>> a.squared_distance(a)
        0.0
        >>> a.squared_distance(array.array('d', [1., 2., 3., 4.]))
        11.0
        >>> a.squared_distance(np.array([1., 2., 3., 4.]))
        11.0
        >>> b = SparseVector(4, [2, 4], [1.0, 2.0])
        >>> a.squared_distance(b)
        30.0
        >>> b.squared_distance(a)
        30.0
        >>> b.squared_distance([1., 2.])
        Traceback (most recent call last):
            ...
        AssertionError: dimension mismatch
        >>> b.squared_distance(SparseVector(3, [1,], [1.0,]))
        Traceback (most recent call last):
            ...
        AssertionError: dimension mismatch
        """
        assert len(self) == _vector_size(other), "dimension mismatch"
        if type(other) in (list, array.array, DenseVector, np.array, np.ndarray):
            if type(other) is np.array and other.ndim != 1:
                raise Exception("Cannot call squared_distance with %d-dimensional array" %
                                other.ndim)
            result = 0.0
            j = 0   # index into our own array
            for i in xrange(len(other)):
                if j < len(self.indices) and self.indices[j] == i:
                    diff = self.values[j] - other[i]
                    result += diff * diff
                    j += 1
                else:
                    result += other[i] * other[i]
            return result

        elif type(other) is SparseVector:
            result = 0.0
            i, j = 0, 0
            while i < len(self.indices) and j < len(other.indices):
                if self.indices[i] == other.indices[j]:
                    diff = self.values[i] - other.values[j]
                    result += diff * diff
                    i += 1
                    j += 1
                elif self.indices[i] < other.indices[j]:
                    result += self.values[i] * self.values[i]
                    i += 1
                else:
                    result += other.values[j] * other.values[j]
                    j += 1
            while i < len(self.indices):
                result += self.values[i] * self.values[i]
                i += 1
            while j < len(other.indices):
                result += other.values[j] * other.values[j]
                j += 1
            return result
        else:
            return self.squared_distance(_convert_to_vector(other))

    def toArray(self):
        """
        Returns a copy of this SparseVector as a 1-dimensional NumPy array.
        """
        arr = np.zeros((self.size,), dtype=np.float64)
        for i in xrange(self.indices.size):
            arr[self.indices[i]] = self.values[i]
        return arr

    def __len__(self):
        return self.size

    def __str__(self):
        inds = "[" + ",".join([str(i) for i in self.indices]) + "]"
        vals = "[" + ",".join([str(v) for v in self.values]) + "]"
        return "(" + ",".join((str(self.size), inds, vals)) + ")"

    def __repr__(self):
        inds = self.indices
        vals = self.values
        entries = ", ".join(["{0}: {1}".format(inds[i], vals[i]) for i in xrange(len(inds))])
        return "SparseVector({0}, {{{1}}})".format(self.size, entries)

    def __eq__(self, other):
        """
        Test SparseVectors for equality.

        >>> v1 = SparseVector(4, [(1, 1.0), (3, 5.5)])
        >>> v2 = SparseVector(4, [(1, 1.0), (3, 5.5)])
        >>> v1 == v2
        True
        >>> v1 != v2
        False
        """
        return (isinstance(other, self.__class__)
                and other.size == self.size
                and other.indices == self.indices
                and other.values == self.values)

    def __ne__(self, other):
        return not self.__eq__(other)


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
        :param args: Non-zero entries, as a dictionary, list of tupes,
                     or two sorted lists containing indices and values.

        >>> print Vectors.sparse(4, {1: 1.0, 3: 5.5})
        (4,[1,3],[1.0,5.5])
        >>> print Vectors.sparse(4, [(1, 1.0), (3, 5.5)])
        (4,[1,3],[1.0,5.5])
        >>> print Vectors.sparse(4, [1, 3], [1.0, 5.5])
        (4,[1,3],[1.0,5.5])
        """
        return SparseVector(size, *args)

    @staticmethod
    def dense(elements):
        """
        Create a dense vector of 64-bit floats from a Python list. Always
        returns a NumPy array.

        >>> Vectors.dense([1, 2, 3])
        DenseVector(array('d', [1.0, 2.0, 3.0]))
        """
        return DenseVector(elements)

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


class Matrix(object):
    """
    Represents a local matrix.
    """

    def __init__(self, numRows, numCols):
        self.numRows = numRows
        self.numCols = numCols

    def toArray(self):
        """
        Returns its elements in a NumPy ndarray.
        """
        raise NotImplementedError


class DenseMatrix(Matrix):
    """
    Column-major dense matrix.
    """
    def __init__(self, numRows, numCols, values):
        Matrix.__init__(self, numRows, numCols)
        assert len(values) == numRows * numCols
        self.values = values

    def __reduce__(self):
        return DenseMatrix, (self.numRows, self.numCols, self.values)

    def toArray(self):
        """
        Return an numpy.ndarray

        >>> arr = array.array('d', [float(i) for i in range(4)])
        >>> m = DenseMatrix(2, 2, arr)
        >>> m.toArray()
        array([[ 0.,  2.],
               [ 1.,  3.]])
        """
        return np.reshape(self.values, (self.numRows, self.numCols), order='F')


def _test():
    import doctest
    (failure_count, test_count) = doctest.testmod(optionflags=doctest.ELLIPSIS)
    if failure_count:
        exit(-1)

if __name__ == "__main__":
    # remove current path from list of search paths to avoid importing mllib.random
    # for C{import random}, which is done in an external dependency of pyspark during doctests.
    import sys
    sys.path.pop(0)
    _test()
