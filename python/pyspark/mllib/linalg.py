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

if sys.version >= '3':
    basestring = str
    xrange = range
    import copyreg as copy_reg
else:
    import copy_reg

import numpy as np

from pyspark.sql.types import UserDefinedType, StructField, StructType, ArrayType, DoubleType, \
    IntegerType, ByteType


__all__ = ['Vector', 'DenseVector', 'SparseVector', 'Vectors',
           'Matrix', 'DenseMatrix', 'SparseMatrix', 'Matrices']


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
    elif type(l) in (array.array, np.array, np.ndarray, list, tuple, xrange):
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
    elif type(v) in (array.array, list, tuple, xrange):
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


def _format_float(f, digits=4):
    s = str(round(f, digits))
    if '.' in s:
        s = s[:s.index('.') + 1 + digits]
    return s


class VectorUDT(UserDefinedType):
    """
    SQL user-defined type (UDT) for Vector.
    """

    @classmethod
    def sqlType(cls):
        return StructType([
            StructField("type", ByteType(), False),
            StructField("size", IntegerType(), True),
            StructField("indices", ArrayType(IntegerType(), False), True),
            StructField("values", ArrayType(DoubleType(), False), True)])

    @classmethod
    def module(cls):
        return "pyspark.mllib.linalg"

    @classmethod
    def scalaUDT(cls):
        return "org.apache.spark.mllib.linalg.VectorUDT"

    def serialize(self, obj):
        if isinstance(obj, SparseVector):
            indices = [int(i) for i in obj.indices]
            values = [float(v) for v in obj.values]
            return (0, obj.size, indices, values)
        elif isinstance(obj, DenseVector):
            values = [float(v) for v in obj]
            return (1, None, None, values)
        else:
            raise TypeError("cannot serialize %r of type %r" % (obj, type(obj)))

    def deserialize(self, datum):
        assert len(datum) == 4, \
            "VectorUDT.deserialize given row with length %d but requires 4" % len(datum)
        tpe = datum[0]
        if tpe == 0:
            return SparseVector(datum[1], datum[2], datum[3])
        elif tpe == 1:
            return DenseVector(datum[3])
        else:
            raise ValueError("do not recognize type %r" % tpe)

    def simpleString(self):
        return "vector"


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


class DenseVector(Vector):
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
    def __init__(self, ar):
        if isinstance(ar, bytes):
            ar = np.frombuffer(ar, dtype=np.float64)
        elif not isinstance(ar, np.ndarray):
            ar = np.array(ar, dtype=np.float64)
        if ar.dtype != np.float64:
            ar = ar.astype(np.float64)
        self.array = ar

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
            values = [float(val) for val in s.split(',')]
        except ValueError:
            raise ValueError("Unable to parse values from %s" % s)
        return DenseVector(values)

    def __reduce__(self):
        return DenseVector, (self.array.tostring(),)

    def numNonzeros(self):
        return np.count_nonzero(self.array)

    def norm(self, p):
        """
        Calculte the norm of a DenseVector.

        >>> a = DenseVector([0, -1, 2, -3])
        >>> a.norm(2)
        3.7...
        >>> a.norm(1)
        6.0
        """
        return np.linalg.norm(self.array, p)

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
        if type(other) == np.ndarray:
            if other.ndim > 1:
                assert len(self) == other.shape[0], "dimension mismatch"
            return np.dot(self.array, other)
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
        return self.array

    def __getitem__(self, item):
        return self.array[item]

    def __len__(self):
        return len(self.array)

    def __str__(self):
        return "[" + ",".join([str(v) for v in self.array]) + "]"

    def __repr__(self):
        return "DenseVector([%s])" % (', '.join(_format_float(i) for i in self.array))

    def __eq__(self, other):
        return isinstance(other, DenseVector) and np.array_equal(self.array, other.array)

    def __ne__(self, other):
        return not self == other

    def __getattr__(self, item):
        return getattr(self.array, item)

    def _delegate(op):
        def func(self, other):
            if isinstance(other, DenseVector):
                other = other.array
            return DenseVector(getattr(self.array, op)(other))
        return func

    __neg__ = _delegate("__neg__")
    __add__ = _delegate("__add__")
    __sub__ = _delegate("__sub__")
    __mul__ = _delegate("__mul__")
    __div__ = _delegate("__div__")
    __truediv__ = _delegate("__truediv__")
    __mod__ = _delegate("__mod__")
    __radd__ = _delegate("__radd__")
    __rsub__ = _delegate("__rsub__")
    __rmul__ = _delegate("__rmul__")
    __rdiv__ = _delegate("__rdiv__")
    __rtruediv__ = _delegate("__rtruediv__")
    __rmod__ = _delegate("__rmod__")


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

        >>> SparseVector(4, {1: 1.0, 3: 5.5})
        SparseVector(4, {1: 1.0, 3: 5.5})
        >>> SparseVector(4, [(1, 1.0), (3, 5.5)])
        SparseVector(4, {1: 1.0, 3: 5.5})
        >>> SparseVector(4, [1, 3], [1.0, 5.5])
        SparseVector(4, {1: 1.0, 3: 5.5})
        """
        self.size = int(size)
        assert 1 <= len(args) <= 2, "must pass either 2 or 3 arguments"
        if len(args) == 1:
            pairs = args[0]
            if type(pairs) == dict:
                pairs = pairs.items()
            pairs = sorted(pairs)
            self.indices = np.array([p[0] for p in pairs], dtype=np.int32)
            self.values = np.array([p[1] for p in pairs], dtype=np.float64)
        else:
            if isinstance(args[0], bytes):
                assert isinstance(args[1], bytes), "values should be string too"
                if args[0]:
                    self.indices = np.frombuffer(args[0], np.int32)
                    self.values = np.frombuffer(args[1], np.float64)
                else:
                    # np.frombuffer() doesn't work well with empty string in older version
                    self.indices = np.array([], dtype=np.int32)
                    self.values = np.array([], dtype=np.float64)
            else:
                self.indices = np.array(args[0], dtype=np.int32)
                self.values = np.array(args[1], dtype=np.float64)
            assert len(self.indices) == len(self.values), "index and value arrays not same length"
            for i in xrange(len(self.indices) - 1):
                if self.indices[i] >= self.indices[i + 1]:
                    raise TypeError("indices array must be sorted")

    def numNonzeros(self):
        return np.count_nonzero(self.values)

    def norm(self, p):
        """
        Calculte the norm of a SparseVector.

        >>> a = SparseVector(4, [0, 1], [3., -4.])
        >>> a.norm(1)
        7.0
        >>> a.norm(2)
        5.0
        """
        return np.linalg.norm(self.values, p)

    def __reduce__(self):
        return (
            SparseVector,
            (self.size, self.indices.tostring(), self.values.tostring()))

    @staticmethod
    def parse(s):
        """
        Parse string representation back into the DenseVector.

        >>> SparseVector.parse(' (4, [0,1 ],[ 4.0,5.0] )')
        SparseVector(4, {0: 4.0, 1: 5.0})
        """
        start = s.find('(')
        if start == -1:
            raise ValueError("Tuple should start with '('")
        end = s.find(')')
        if start == -1:
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
            indices = [int(ind) for ind in ind_list]
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
            values = [float(val) for val in val_list]
        except ValueError:
            raise ValueError("Unable to parse values from %s." % s)
        return SparseVector(size, indices, values)

    def dot(self, other):
        """
        Dot product with a SparseVector or 1- or 2-dimensional Numpy array.

        >>> a = SparseVector(4, [1, 3], [3.0, 4.0])
        >>> a.dot(a)
        25.0
        >>> a.dot(array.array('d', [1., 2., 3., 4.]))
        22.0
        >>> b = SparseVector(4, [2], [1.0])
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
        >>> b = SparseVector(4, [2], [1.0])
        >>> a.squared_distance(b)
        26.0
        >>> b.squared_distance(a)
        26.0
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
        arr[self.indices] = self.values
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
        entries = ", ".join(["{0}: {1}".format(inds[i], _format_float(vals[i]))
                             for i in xrange(len(inds))])
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
                and np.array_equal(other.indices, self.indices)
                and np.array_equal(other.values, self.values))

    def __getitem__(self, index):
        inds = self.indices
        vals = self.values
        if not isinstance(index, int):
            raise TypeError(
                "Indices must be of type integer, got type %s" % type(index))
        if index < 0:
            index += self.size
        if index >= self.size or index < 0:
            raise ValueError("Index %d out of bounds." % index)

        insert_index = np.searchsorted(inds, index)
        row_ind = inds[insert_index]
        if row_ind == index:
            return vals[insert_index]
        return 0.

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

        >>> Vectors.sparse(4, {1: 1.0, 3: 5.5})
        SparseVector(4, {1: 1.0, 3: 5.5})
        >>> Vectors.sparse(4, [(1, 1.0), (3, 5.5)])
        SparseVector(4, {1: 1.0, 3: 5.5})
        >>> Vectors.sparse(4, [1, 3], [1.0, 5.5])
        SparseVector(4, {1: 1.0, 3: 5.5})
        """
        return SparseVector(size, *args)

    @staticmethod
    def dense(elements):
        """
        Create a dense vector of 64-bit floats from a Python list. Always
        returns a NumPy array.

        >>> Vectors.dense([1, 2, 3])
        DenseVector([1.0, 2.0, 3.0])
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


class Matrix(object):
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


def _test():
    import doctest
    (failure_count, test_count) = doctest.testmod(optionflags=doctest.ELLIPSIS)
    if failure_count:
        exit(-1)

if __name__ == "__main__":
    _test()
