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
from numpy import ndarray, float64, int64, int32, ones, array_equal, array, dot, shape, complex, issubdtype
from pyspark import SparkContext, RDD
from pyspark.mllib.linalg import SparseVector
from pyspark.serializers import Serializer

# Dense double vector format:
#
# [1-byte 1] [4-byte length] [length*8 bytes of data]
#
# Sparse double vector format:
#
# [1-byte 2] [4-byte length] [4-byte nonzeros] [nonzeros*4 bytes of indices] [nonzeros*8 bytes of values]
#
# Double matrix format:
#
# [1-byte 3] [4-byte rows] [4-byte cols] [rows*cols*8 bytes of data]
#
# This is all in machine-endian.  That means that the Java interpreter and the
# Python interpreter must agree on what endian the machine is.

DENSE_VECTOR_MAGIC = 1
SPARSE_VECTOR_MAGIC = 2
DENSE_MATRIX_MAGIC = 3

def _deserialize_numpy_array(shape, ba, offset, dtype='float64'):
    """
    Deserialize a numpy array of the given type from an offset in
    bytearray ba, assigning it the given shape.

    >>> x = array([1.0, 2.0, 3.0, 4.0, 5.0])
    >>> array_equal(x, _deserialize_numpy_array(x.shape, x.data, 0))
    True
    >>> x = array([1.0, 2.0, 3.0, 4.0]).reshape(2,2)
    >>> array_equal(x, _deserialize_numpy_array(x.shape, x.data, 0))
    True
    >>> x = array([1, 2, 3], dtype='int32')
    >>> array_equal(x, _deserialize_numpy_array(x.shape, x.data, 0, dtype='int32'))
    True
    """
    ar = ndarray(shape=shape, buffer=ba, offset=offset, dtype=dtype, order='C')
    return ar.copy()

def _serialize_double_vector(v):
    """Serialize a double vector into a mutually understood format.

    >>> x = array([1,2,3])
    >>> y = _deserialize_double_vector(_serialize_double_vector(x))
    >>> array_equal(y, array([1.0, 2.0, 3.0]))
    True
    """
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
        v = v.astype('float64')
    length = v.shape[0]
    ba = bytearray(5 + 8 * length)
    ba[0] = DENSE_VECTOR_MAGIC
    length_bytes = ndarray(shape=[1], buffer=ba, offset=1, dtype="int32")
    length_bytes[0] = length
    arr_mid = ndarray(shape=[length], buffer=ba, offset=5, dtype="float64")
    arr_mid[...] = v
    return ba

def _serialize_sparse_vector(v):
    """Serialize a pyspark.mllib.linalg.SparseVector."""
    nonzeros = len(v.indices)
    ba = bytearray(9 + 12 * nonzeros)
    ba[0] = SPARSE_VECTOR_MAGIC
    header = ndarray(shape=[2], buffer=ba, offset=1, dtype="int32")
    header[0] = v.size
    header[1] = nonzeros
    copyto(ndarray(shape=[nonzeros], buffer=ba, offset=9, dtype="int32"), v.indices)
    values_offset = 9 + 4 * nonzeros
    copyto(ndarray(shape=[nonzeros], buffer=ba, offset=values_offset, dtype="float64"), v.values)
    return ba

def _deserialize_double_vector(ba):
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
    if len(ba) < 5:
        raise TypeError("_deserialize_double_vector called on a %d-byte array, "
                "which is too short" % len(ba))
    if ba[0] == DENSE_VECTOR_MAGIC:
        return _deserialize_dense_vector(ba)
    elif ba[0] == SPARSE_VECTOR_MAGIC:
        return _deserialize_sparse_vector(ba)
    else:
        raise TypeError("_deserialize_double_vector called on bytearray "
                        "with wrong magic")

def _deserialize_dense_vector(ba):
    """Deserialize a dense vector into a numpy array."""
    if len(ba) < 5:
        raise TypeError("_deserialize_dense_vector called on a %d-byte array, "
                "which is too short" % len(ba))
    length = ndarray(shape=[1], buffer=ba, offset=1, dtype="int32")[0]
    if len(ba) != 8 * length + 5:
        raise TypeError("_deserialize_dense_vector called on bytearray "
                        "with wrong length")
    return _deserialize_numpy_array([length], ba, 5)

def _deserialize_sparse_vector(ba):
    """Deserialize a sparse vector into a MLlib SparseVector object."""
    if len(ba) < 9:
        raise TypeError("_deserialize_sparse_vector called on a %d-byte array, "
                "which is too short" % len(ba))
    header = ndarray(shape=[2], buffer=ba, offset=1, dtype="int32")
    size = header[0]
    nonzeros = header[1]
    if len(ba) != 9 + 12 * nonzeros:
        raise TypeError("_deserialize_sparse_vector called on bytearray "
                        "with wrong length")
    indices = _deserialize_numpy_array([nonzeros], ba, 9, dtype='int32')
    values = _deserialize_numpy_array([nonzeros], ba, 9 + 4 * nonzeros, dtype='float64')
    return SparseVector(int(size), indices, values)

def _serialize_double_matrix(m):
    """Serialize a double matrix into a mutually understood format."""
    if (type(m) == ndarray and m.ndim == 2):
        if m.dtype != float64:
            if numpy.issubdtype(m.dtype, numpy.complex):
                raise TypeError("_serialize_double_matrix called on an ndarray of %s; "
                        "wanted ndarray of float64" % m.dtype)
            m = m.astype('float64')
        rows = m.shape[0]
        cols = m.shape[1]
        ba = bytearray(9 + 8 * rows * cols)
        ba[0] = DENSE_MATRIX_MAGIC
        lengths = ndarray(shape=[3], buffer=ba, offset=1, dtype="int32")
        lengths[0] = rows
        lengths[1] = cols
        arr_mid = ndarray(shape=[rows, cols], buffer=ba, offset=9, dtype="float64", order='C')
        arr_mid[...] = m
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
    lengths = ndarray(shape=[2], buffer=ba, offset=1, dtype="int32")
    rows = lengths[0]
    cols = lengths[1]
    if (len(ba) != 8 * rows * cols + 9):
        raise TypeError("_deserialize_double_matrix called on bytearray "
                        "with wrong length")
    return _deserialize_numpy_array([rows, cols], ba, 9)

def _linear_predictor_typecheck(x, coeffs):
    """Check that x is a one-dimensional vector of the right shape.
    This is a temporary hackaround until I actually implement bulk predict."""
    if type(x) == ndarray:
        if x.ndim == 1:
            if x.shape != coeffs.shape:
                raise RuntimeError("Got array of %d elements; wanted %d"
                        % (numpy.shape(x)[0], coeffs.shape[0]))
        else:
            raise RuntimeError("Bulk predict not yet supported.")
    elif type(x) == SparseVector:
        if x.size != coeffs.shape[0]:
           raise RuntimeError("Got sparse vector of size %d; wanted %d"
                   % (x.size, coeffs.shape[0]))
    elif (type(x) == RDD):
        raise RuntimeError("Bulk predict not yet supported.")
    else:
        raise TypeError("Argument of type " + type(x).__name__ + " unsupported")

def _get_unmangled_rdd(data, serializer):
    dataBytes = data.map(serializer)
    dataBytes._bypass_serializer = True
    dataBytes.cache()
    return dataBytes

# Map a pickled Python RDD of numpy double vectors to a Java RDD of
# _serialized_double_vectors
def _get_unmangled_double_vector_rdd(data):
    return _get_unmangled_rdd(data, _serialize_double_vector)

class LinearModel(object):
    """Something that has a vector of coefficients and an intercept."""
    def __init__(self, coeff, intercept):
        self._coeff = coeff
        self._intercept = intercept

class LinearRegressionModelBase(LinearModel):
    """A linear regression model.

    >>> lrmb = LinearRegressionModelBase(array([1.0, 2.0]), 0.1)
    >>> abs(lrmb.predict(array([-1.03, 7.777])) - 14.624) < 1e-6
    True
    """
    def predict(self, x):
        """Predict the value of the dependent variable given a vector x"""
        """containing values for the independent variables."""
        _linear_predictor_typecheck(x, self._coeff)
        return x.dot(self._coeff) + self._intercept

# If we weren't given initial weights, take a zero vector of the appropriate
# length.
def _get_initial_weights(initial_weights, data):
    if initial_weights is None:
        initial_weights = data.first()
        if type(initial_weights) == ndarray:
            if initial_weights.ndim != 1:
                raise TypeError("At least one data element has "
                        + initial_weights.ndim + " dimensions, which is not 1")
            initial_weights = numpy.ones([initial_weights.shape[0] - 1])
        elif type(initial_weights) == SparseVector:
            initial_weights = numpy.ones(initial_weights.size - 1)
        else:
            raise TypeError("At least one data element has type "
                    + type(initial_weights).__name__ + " which is not a vector")
    return initial_weights

# train_func should take two parameters, namely data and initial_weights, and
# return the result of a call to the appropriate JVM stub.
# _regression_train_wrapper is responsible for setup and error checking.
def _regression_train_wrapper(sc, train_func, klass, data, initial_weights):
    initial_weights = _get_initial_weights(initial_weights, data)
    dataBytes = _get_unmangled_double_vector_rdd(data)
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
        res = ndarray(shape=(3, ), buffer=ba, dtype="float64", offset=4)
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

def _test():
    import doctest
    globs = globals().copy()
    globs['sc'] = SparkContext('local[4]', 'PythonTest', batchSize=2)
    (failure_count, test_count) = doctest.testmod(globs=globs,
            optionflags=doctest.ELLIPSIS)
    globs['sc'].stop()
    if failure_count:
        exit(-1)

if __name__ == "__main__":
    _test()
