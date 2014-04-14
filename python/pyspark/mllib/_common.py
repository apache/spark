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

from numpy import ndarray, copyto, float64, int64, int32, ones, array_equal, array, dot, shape
from pyspark import SparkContext, RDD

from pyspark.serializers import Serializer
import struct

# Double vector format:
#
# [8-byte 1] [8-byte length] [length*8 bytes of data]
#
# Double matrix format:
#
# [8-byte 2] [8-byte rows] [8-byte cols] [rows*cols*8 bytes of data]
#
# This is all in machine-endian.  That means that the Java interpreter and the
# Python interpreter must agree on what endian the machine is.

def _deserialize_byte_array(shape, ba, offset):
    """Wrapper around ndarray aliasing hack.

    >>> x = array([1.0, 2.0, 3.0, 4.0, 5.0])
    >>> array_equal(x, _deserialize_byte_array(x.shape, x.data, 0))
    True
    >>> x = array([1.0, 2.0, 3.0, 4.0]).reshape(2,2)
    >>> array_equal(x, _deserialize_byte_array(x.shape, x.data, 0))
    True
    """
    ar = ndarray(shape=shape, buffer=ba, offset=offset, dtype="float64",
            order='C')
    return ar.copy()

def _serialize_double_vector(v):
    """Serialize a double vector into a mutually understood format."""
    if type(v) != ndarray:
        raise TypeError("_serialize_double_vector called on a %s; "
                "wanted ndarray" % type(v))
    if v.dtype != float64:
        raise TypeError("_serialize_double_vector called on an ndarray of %s; "
                "wanted ndarray of float64" % v.dtype)
    if v.ndim != 1:
        raise TypeError("_serialize_double_vector called on a %ddarray; "
                "wanted a 1darray" % v.ndim)
    length = v.shape[0]
    ba = bytearray(16 + 8*length)
    header = ndarray(shape=[2], buffer=ba, dtype="int64")
    header[0] = 1
    header[1] = length
    copyto(ndarray(shape=[length], buffer=ba, offset=16,
            dtype="float64"), v)
    return ba

def _deserialize_double_vector(ba):
    """Deserialize a double vector from a mutually understood format.

    >>> x = array([1.0, 2.0, 3.0, 4.0, -1.0, 0.0, -0.0])
    >>> array_equal(x, _deserialize_double_vector(_serialize_double_vector(x)))
    True
    """
    if type(ba) != bytearray:
        raise TypeError("_deserialize_double_vector called on a %s; "
                "wanted bytearray" % type(ba))
    if len(ba) < 16:
        raise TypeError("_deserialize_double_vector called on a %d-byte array, "
                "which is too short" % len(ba))
    if (len(ba) & 7) != 0:
        raise TypeError("_deserialize_double_vector called on a %d-byte array, "
                "which is not a multiple of 8" % len(ba))
    header = ndarray(shape=[2], buffer=ba, dtype="int64")
    if header[0] != 1:
        raise TypeError("_deserialize_double_vector called on bytearray "
                        "with wrong magic")
    length = header[1]
    if len(ba) != 8*length + 16:
        raise TypeError("_deserialize_double_vector called on bytearray "
                        "with wrong length")
    return _deserialize_byte_array([length], ba, 16)

def _serialize_double_matrix(m):
    """Serialize a double matrix into a mutually understood format."""
    if (type(m) == ndarray and m.dtype == float64 and m.ndim == 2):
        rows = m.shape[0]
        cols = m.shape[1]
        ba = bytearray(24 + 8 * rows * cols)
        header = ndarray(shape=[3], buffer=ba, dtype="int64")
        header[0] = 2
        header[1] = rows
        header[2] = cols
        copyto(ndarray(shape=[rows, cols], buffer=ba, offset=24,
                       dtype="float64", order='C'), m)
        return ba
    else:
        raise TypeError("_serialize_double_matrix called on a "
                        "non-double-matrix")

def _deserialize_double_matrix(ba):
    """Deserialize a double matrix from a mutually understood format."""
    if type(ba) != bytearray:
        raise TypeError("_deserialize_double_matrix called on a %s; "
                "wanted bytearray" % type(ba))
    if len(ba) < 24:
        raise TypeError("_deserialize_double_matrix called on a %d-byte array, "
                "which is too short" % len(ba))
    if (len(ba) & 7) != 0:
        raise TypeError("_deserialize_double_matrix called on a %d-byte array, "
                "which is not a multiple of 8" % len(ba))
    header = ndarray(shape=[3], buffer=ba, dtype="int64")
    if (header[0] != 2):
        raise TypeError("_deserialize_double_matrix called on bytearray "
                        "with wrong magic")
    rows = header[1]
    cols = header[2]
    if (len(ba) != 8*rows*cols + 24):
        raise TypeError("_deserialize_double_matrix called on bytearray "
                        "with wrong length")
    return _deserialize_byte_array([rows, cols], ba, 24)

def _linear_predictor_typecheck(x, coeffs):
    """Check that x is a one-dimensional vector of the right shape.
    This is a temporary hackaround until I actually implement bulk predict."""
    if type(x) == ndarray:
        if x.ndim == 1:
            if x.shape == coeffs.shape:
                pass
            else:
                raise RuntimeError("Got array of %d elements; wanted %d"
                        % (shape(x)[0], shape(coeffs)[0]))
        else:
            raise RuntimeError("Bulk predict not yet supported.")
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
        return dot(self._coeff, x) + self._intercept

# If we weren't given initial weights, take a zero vector of the appropriate
# length.
def _get_initial_weights(initial_weights, data):
    if initial_weights is None:
        initial_weights = data.first()
        if type(initial_weights) != ndarray:
            raise TypeError("At least one data element has type "
                    + type(initial_weights).__name__ + " which is not ndarray")
        if initial_weights.ndim != 1:
            raise TypeError("At least one data element has "
                    + initial_weights.ndim + " dimensions, which is not 1")
        initial_weights = ones([initial_weights.shape[0] - 1])
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
