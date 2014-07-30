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

import struct
import numpy
from numpy import ndarray, float64, int64, int32, array_equal, array
from pyspark import SparkContext, RDD
from pyspark.mllib.linalg import SparseVector
from pyspark.serializers import Serializer


"""
Common utilities shared throughout MLlib, primarily for dealing with
different data types. These include:
- Serialization utilities to / from byte arrays that Java can handle
- Serializers for other data types, like ALS Rating objects
- Common methods for linear models
- Methods to deal with the different vector types we support, such as
  SparseVector and scipy.sparse matrices.
"""


# Check whether we have SciPy. MLlib works without it too, but if we have it, some methods,
# such as _dot and _serialize_double_vector, start to support scipy.sparse matrices.

_have_scipy = False
_scipy_issparse = None
try:
    import scipy.sparse
    _have_scipy = True
    _scipy_issparse = scipy.sparse.issparse
except:
    # No SciPy in environment, but that's okay
    pass


# Serialization functions to and from Scala. These use the following formats, understood
# by the PythonMLLibAPI class in Scala:
#
# Dense double vector format:
#
# [1-byte 1] [4-byte length] [length*8 bytes of data]
#
# Sparse double vector format:
#
# [1-byte 2] [4-byte length] [4-byte nonzeros] [nonzeros*4 bytes of indices] \
# [nonzeros*8 bytes of values]
#
# Double matrix format:
#
# [1-byte 3] [4-byte rows] [4-byte cols] [rows*cols*8 bytes of data]
#
# LabeledPoint format:
#
# [1-byte 4] [8-byte label] [dense or sparse vector]
#
# This is all in machine-endian.  That means that the Java interpreter and the
# Python interpreter must agree on what endian the machine is.


DENSE_VECTOR_MAGIC  = 1
SPARSE_VECTOR_MAGIC = 2
DENSE_MATRIX_MAGIC  = 3
LABELED_POINT_MAGIC = 4


def _deserialize_numpy_array(shape, ba, offset, dtype=float64):
    """
    Deserialize a numpy array of the given type from an offset in
    bytearray ba, assigning it the given shape.

    >>> x = array([1.0, 2.0, 3.0, 4.0, 5.0])
    >>> array_equal(x, _deserialize_numpy_array(x.shape, x.data, 0))
    True
    >>> x = array([1.0, 2.0, 3.0, 4.0]).reshape(2,2)
    >>> array_equal(x, _deserialize_numpy_array(x.shape, x.data, 0))
    True
    >>> x = array([1, 2, 3], dtype=int32)
    >>> array_equal(x, _deserialize_numpy_array(x.shape, x.data, 0, dtype=int32))
    True
    """
    ar = ndarray(shape=shape, buffer=ba, offset=offset, dtype=dtype, order='C')
    return ar.copy()


def _serialize_double(d):
    """
    Serialize a double (float or numpy.float64) into a mutually understood format.
    """
    if type(d) == float or type(d) == float64:
        d = float64(d)
        ba = bytearray(8)
        _copyto(d, buffer=ba, offset=0, shape=[1], dtype=float64)
        return ba
    else:
        raise TypeError("_serialize_double called on non-float input")


def _serialize_double_vector(v):
    """
    Serialize a double vector into a mutually understood format.

    Note: we currently do not use a magic byte for double for storage
    efficiency. This should be reconsidered when we add Ser/De for other
    8-byte types (e.g. Long), for safety. The corresponding deserializer,
    _deserialize_double, needs to be modified as well if the serialization
    scheme changes.

    >>> x = array([1,2,3])
    >>> y = _deserialize_double_vector(_serialize_double_vector(x))
    >>> array_equal(y, array([1.0, 2.0, 3.0]))
    True
    """
    v = _convert_vector(v)
    if type(v) == ndarray:
        return _serialize_dense_vector(v)
    elif type(v) == SparseVector:
        return _serialize_sparse_vector(v)
    else:
        raise TypeError("_serialize_double_vector called on a %s; "
                        "wanted ndarray or SparseVector" % type(v))


def _serialize_dense_vector(v):
    """Serialize a dense vector given as a NumPy array."""
    if v.ndim != 1:
        raise TypeError("_serialize_double_vector called on a %ddarray; "
                        "wanted a 1darray" % v.ndim)
    if v.dtype != float64:
        if numpy.issubdtype(v.dtype, numpy.complex):
            raise TypeError("_serialize_double_vector called on an ndarray of %s; "
                            "wanted ndarray of float64" % v.dtype)
        v = v.astype(float64)
    length = v.shape[0]
    ba = bytearray(5 + 8 * length)
    ba[0] = DENSE_VECTOR_MAGIC
    length_bytes = ndarray(shape=[1], buffer=ba, offset=1, dtype=int32)
    length_bytes[0] = length
    _copyto(v, buffer=ba, offset=5, shape=[length], dtype=float64)
    return ba


def _serialize_sparse_vector(v):
    """Serialize a pyspark.mllib.linalg.SparseVector."""
    nonzeros = len(v.indices)
    ba = bytearray(9 + 12 * nonzeros)
    ba[0] = SPARSE_VECTOR_MAGIC
    header = ndarray(shape=[2], buffer=ba, offset=1, dtype=int32)
    header[0] = v.size
    header[1] = nonzeros
    _copyto(v.indices, buffer=ba, offset=9, shape=[nonzeros], dtype=int32)
    values_offset = 9 + 4 * nonzeros
    _copyto(v.values, buffer=ba, offset=values_offset, shape=[nonzeros], dtype=float64)
    return ba


def _deserialize_double(ba, offset=0):
    """Deserialize a double from a mutually understood format.

    >>> import sys
    >>> _deserialize_double(_serialize_double(123.0)) == 123.0
    True
    >>> _deserialize_double(_serialize_double(float64(0.0))) == 0.0
    True
    >>> x = sys.float_info.max
    >>> _deserialize_double(_serialize_double(sys.float_info.max)) == x
    True
    >>> y = float64(sys.float_info.max)
    >>> _deserialize_double(_serialize_double(sys.float_info.max)) == y
    True
    """
    if type(ba) != bytearray:
        raise TypeError("_deserialize_double called on a %s; wanted bytearray" % type(ba))
    if len(ba) - offset != 8:
        raise TypeError("_deserialize_double called on a %d-byte array; wanted 8 bytes." % nb)
    return struct.unpack("d", ba[offset:])[0]


def _deserialize_double_vector(ba, offset=0):
    """Deserialize a double vector from a mutually understood format.

    >>> x = array([1.0, 2.0, 3.0, 4.0, -1.0, 0.0, -0.0])
    >>> array_equal(x, _deserialize_double_vector(_serialize_double_vector(x)))
    True
    >>> s = SparseVector(4, [1, 3], [3.0, 5.5])
    >>> s == _deserialize_double_vector(_serialize_double_vector(s))
    True
    """
    if type(ba) != bytearray:
        raise TypeError("_deserialize_double_vector called on a %s; "
                        "wanted bytearray" % type(ba))
    nb = len(ba) - offset
    if nb < 5:
        raise TypeError("_deserialize_double_vector called on a %d-byte array, "
                        "which is too short" % nb)
    if ba[offset] == DENSE_VECTOR_MAGIC:
        return _deserialize_dense_vector(ba, offset)
    elif ba[offset] == SPARSE_VECTOR_MAGIC:
        return _deserialize_sparse_vector(ba, offset)
    else:
        raise TypeError("_deserialize_double_vector called on bytearray "
                        "with wrong magic")


def _deserialize_dense_vector(ba, offset=0):
    """Deserialize a dense vector into a numpy array."""
    nb = len(ba) - offset
    if nb < 5:
        raise TypeError("_deserialize_dense_vector called on a %d-byte array, "
                        "which is too short" % nb)
    length = ndarray(shape=[1], buffer=ba, offset=offset + 1, dtype=int32)[0]
    if nb < 8 * length + 5:
        raise TypeError("_deserialize_dense_vector called on bytearray "
                        "with wrong length")
    return _deserialize_numpy_array([length], ba, offset + 5)


def _deserialize_sparse_vector(ba, offset=0):
    """Deserialize a sparse vector into a MLlib SparseVector object."""
    nb = len(ba) - offset
    if nb < 9:
        raise TypeError("_deserialize_sparse_vector called on a %d-byte array, "
                        "which is too short" % nb)
    header = ndarray(shape=[2], buffer=ba, offset=offset + 1, dtype=int32)
    size = header[0]
    nonzeros = header[1]
    if nb < 9 + 12 * nonzeros:
        raise TypeError("_deserialize_sparse_vector called on bytearray "
                        "with wrong length")
    indices = _deserialize_numpy_array([nonzeros], ba, offset + 9, dtype=int32)
    values = _deserialize_numpy_array([nonzeros], ba, offset + 9 + 4 * nonzeros, dtype=float64)
    return SparseVector(int(size), indices, values)


def _serialize_double_matrix(m):
    """Serialize a double matrix into a mutually understood format."""
    if (type(m) == ndarray and m.ndim == 2):
        if m.dtype != float64:
            if numpy.issubdtype(m.dtype, numpy.complex):
                raise TypeError("_serialize_double_matrix called on an ndarray of %s; "
                                "wanted ndarray of float64" % m.dtype)
            m = m.astype(float64)
        rows = m.shape[0]
        cols = m.shape[1]
        ba = bytearray(9 + 8 * rows * cols)
        ba[0] = DENSE_MATRIX_MAGIC
        lengths = ndarray(shape=[3], buffer=ba, offset=1, dtype=int32)
        lengths[0] = rows
        lengths[1] = cols
        _copyto(m, buffer=ba, offset=9, shape=[rows, cols], dtype=float64)
        return ba
    else:
        raise TypeError("_serialize_double_matrix called on a "
                        "non-double-matrix")


def _deserialize_double_matrix(ba):
    """Deserialize a double matrix from a mutually understood format."""
    if type(ba) != bytearray:
        raise TypeError("_deserialize_double_matrix called on a %s; "
                        "wanted bytearray" % type(ba))
    if len(ba) < 9:
        raise TypeError("_deserialize_double_matrix called on a %d-byte array, "
                        "which is too short" % len(ba))
    if ba[0] != DENSE_MATRIX_MAGIC:
        raise TypeError("_deserialize_double_matrix called on bytearray "
                        "with wrong magic")
    lengths = ndarray(shape=[2], buffer=ba, offset=1, dtype=int32)
    rows = lengths[0]
    cols = lengths[1]
    if (len(ba) != 8 * rows * cols + 9):
        raise TypeError("_deserialize_double_matrix called on bytearray "
                        "with wrong length")
    return _deserialize_numpy_array([rows, cols], ba, 9)


def _serialize_labeled_point(p):
    """
    Serialize a LabeledPoint with a features vector of any type.

    >>> from pyspark.mllib.regression import LabeledPoint
    >>> dp0 = LabeledPoint(0.5, array([1.0, 2.0, 3.0, 4.0, -1.0, 0.0, -0.0]))
    >>> dp1 = _deserialize_labeled_point(_serialize_labeled_point(dp0))
    >>> dp1.label == dp0.label
    True
    >>> array_equal(dp1.features, dp0.features)
    True
    >>> sp0 = LabeledPoint(0.0, SparseVector(4, [1, 3], [3.0, 5.5]))
    >>> sp1 = _deserialize_labeled_point(_serialize_labeled_point(sp0))
    >>> sp1.label == sp1.label
    True
    >>> sp1.features == sp0.features
    True
    """
    from pyspark.mllib.regression import LabeledPoint
    serialized_features = _serialize_double_vector(p.features)
    header = bytearray(9)
    header[0] = LABELED_POINT_MAGIC
    header_float = ndarray(shape=[1], buffer=header, offset=1, dtype=float64)
    header_float[0] = p.label
    return header + serialized_features


def _deserialize_labeled_point(ba, offset=0):
    """Deserialize a LabeledPoint from a mutually understood format."""
    from pyspark.mllib.regression import LabeledPoint
    if type(ba) != bytearray:
        raise TypeError("Expecting a bytearray but got %s" % type(ba))
    if ba[offset] != LABELED_POINT_MAGIC:
        raise TypeError("Expecting magic number %d but got %d" % (LABELED_POINT_MAGIC, ba[0]))
    label = ndarray(shape=[1], buffer=ba, offset=offset + 1, dtype=float64)[0]
    features = _deserialize_double_vector(ba, offset + 9)
    return LabeledPoint(label, features)


def _copyto(array, buffer, offset, shape, dtype):
    """
    Copy the contents of a vector to a destination bytearray at the
    given offset.

    TODO: In the future this could use numpy.copyto on NumPy 1.7+, but
    we should benchmark that to see whether it provides a benefit.
    """
    temp_array = ndarray(shape=shape, buffer=buffer, offset=offset, dtype=dtype, order='C')
    temp_array[...] = array


def _get_unmangled_rdd(data, serializer):
    dataBytes = data.map(serializer)
    dataBytes._bypass_serializer = True
    dataBytes.cache()  # TODO: users should unpersist() this later!
    return dataBytes


# Map a pickled Python RDD of Python dense or sparse vectors to a Java RDD of
# _serialized_double_vectors
def _get_unmangled_double_vector_rdd(data):
    return _get_unmangled_rdd(data, _serialize_double_vector)


# Map a pickled Python RDD of LabeledPoint to a Java RDD of _serialized_labeled_points
def _get_unmangled_labeled_point_rdd(data):
    return _get_unmangled_rdd(data, _serialize_labeled_point)


# Common functions for dealing with and training linear models

def _linear_predictor_typecheck(x, coeffs):
    """
    Check that x is a one-dimensional vector of the right shape.
    This is a temporary hackaround until we actually implement bulk predict.
    """
    x = _convert_vector(x)
    if type(x) == ndarray:
        if x.ndim == 1:
            if x.shape != coeffs.shape:
                raise RuntimeError("Got array of %d elements; wanted %d" % (
                    numpy.shape(x)[0], coeffs.shape[0]))
        else:
            raise RuntimeError("Bulk predict not yet supported.")
    elif type(x) == SparseVector:
        if x.size != coeffs.shape[0]:
            raise RuntimeError("Got sparse vector of size %d; wanted %d" % (
                x.size, coeffs.shape[0]))
    elif (type(x) == RDD):
        raise RuntimeError("Bulk predict not yet supported.")
    else:
        raise TypeError("Argument of type " + type(x).__name__ + " unsupported")


# If we weren't given initial weights, take a zero vector of the appropriate
# length.
def _get_initial_weights(initial_weights, data):
    if initial_weights is None:
        initial_weights = _convert_vector(data.first().features)
        if type(initial_weights) == ndarray:
            if initial_weights.ndim != 1:
                raise TypeError("At least one data element has "
                                + initial_weights.ndim + " dimensions, which is not 1")
            initial_weights = numpy.zeros([initial_weights.shape[0]])
        elif type(initial_weights) == SparseVector:
            initial_weights = numpy.zeros([initial_weights.size])
    return initial_weights


# train_func should take two parameters, namely data and initial_weights, and
# return the result of a call to the appropriate JVM stub.
# _regression_train_wrapper is responsible for setup and error checking.
def _regression_train_wrapper(sc, train_func, klass, data, initial_weights):
    initial_weights = _get_initial_weights(initial_weights, data)
    dataBytes = _get_unmangled_labeled_point_rdd(data)
    ans = train_func(dataBytes, _serialize_double_vector(initial_weights))
    if len(ans) != 2:
        raise RuntimeError("JVM call result had unexpected length")
    elif type(ans[0]) != bytearray:
        raise RuntimeError("JVM call result had first element of type "
                           + type(ans[0]).__name__ + " which is not bytearray")
    elif type(ans[1]) != float:
        raise RuntimeError("JVM call result had second element of type "
                           + type(ans[0]).__name__ + " which is not float")
    return klass(_deserialize_double_vector(ans[0]), ans[1])


# Functions for serializing ALS Rating objects and tuples

def _serialize_rating(r):
    ba = bytearray(16)
    intpart = ndarray(shape=[2], buffer=ba, dtype=int32)
    doublepart = ndarray(shape=[1], buffer=ba, dtype=float64, offset=8)
    intpart[0], intpart[1], doublepart[0] = r
    return ba


class RatingDeserializer(Serializer):
    def loads(self, stream):
        length = struct.unpack("!i", stream.read(4))[0]
        ba = stream.read(length)
        res = ndarray(shape=(3, ), buffer=ba, dtype=float64, offset=4)
        return int(res[0]), int(res[1]), res[2]

    def load_stream(self, stream):
        while True:
            try:
                yield self.loads(stream)
            except struct.error:
                return
            except EOFError:
                return


def _serialize_tuple(t):
    ba = bytearray(8)
    intpart = ndarray(shape=[2], buffer=ba, dtype=int32)
    intpart[0], intpart[1] = t
    return ba


# Vector math functions that support all of our vector types

def _convert_vector(vec):
    """
    Convert a vector to a format we support internally. This does
    the following:

    * For dense NumPy vectors (ndarray), returns them as is
    * For our SparseVector class, returns that as is
    * For Python lists, converts them to NumPy vectors
    * For scipy.sparse.*_matrix column vectors, converts them to
      our own SparseVector type.

    This should be called before passing any data to our algorithms
    or attempting to serialize it to Java.
    """
    if type(vec) == ndarray or type(vec) == SparseVector:
        return vec
    elif type(vec) == list:
        return array(vec, dtype=float64)
    elif _have_scipy:
        if _scipy_issparse(vec):
            assert vec.shape[1] == 1, "Expected column vector"
            csc = vec.tocsc()
            return SparseVector(vec.shape[0], csc.indices, csc.data)
    raise TypeError("Expected NumPy array, SparseVector, or scipy.sparse matrix")


def _squared_distance(v1, v2):
    """
    Squared distance of two NumPy or sparse vectors.

    >>> dense1 = array([1., 2.])
    >>> sparse1 = SparseVector(2, [0, 1], [1., 2.])
    >>> dense2 = array([2., 1.])
    >>> sparse2 = SparseVector(2, [0, 1], [2., 1.])
    >>> _squared_distance(dense1, dense2)
    2.0
    >>> _squared_distance(dense1, sparse2)
    2.0
    >>> _squared_distance(sparse1, dense2)
    2.0
    >>> _squared_distance(sparse1, sparse2)
    2.0
    """
    v1 = _convert_vector(v1)
    v2 = _convert_vector(v2)
    if type(v1) == ndarray and type(v2) == ndarray:
        diff = v1 - v2
        return numpy.dot(diff, diff)
    elif type(v1) == ndarray:
        return v2.squared_distance(v1)
    else:
        return v1.squared_distance(v2)


def _dot(vec, target):
    """
    Compute the dot product of a vector of the types we support
    (Numpy array, list, SparseVector, or SciPy sparse) and a target
    NumPy array that is either 1- or 2-dimensional. Equivalent to
    calling numpy.dot of the two vectors, but for SciPy ones, we
    have to transpose them because they're column vectors.
    """
    if type(vec) == ndarray:
        return numpy.dot(vec, target)
    elif type(vec) == SparseVector:
        return vec.dot(target)
    elif type(vec) == list:
        return numpy.dot(_convert_vector(vec), target)
    else:
        return vec.transpose().dot(target)[0]


def _test():
    import doctest
    globs = globals().copy()
    globs['sc'] = SparkContext('local[4]', 'PythonTest', batchSize=2)
    (failure_count, test_count) = doctest.testmod(globs=globs, optionflags=doctest.ELLIPSIS)
    globs['sc'].stop()
    if failure_count:
        exit(-1)


if __name__ == "__main__":
    _test()
