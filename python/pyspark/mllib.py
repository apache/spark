from numpy import *

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
    ar = ndarray(shape=shape, buffer=ba, offset=offset, dtype="float64",
            order='C')
    return ar.copy()

def _serialize_double_vector(v):
    if (type(v) == ndarray and v.dtype == float64 and v.ndim == 1):
        length = v.shape[0]
        ba = bytearray(16 + 8*length)
        header = ndarray(shape=[2], buffer=ba, dtype="int64")
        header[0] = 1
        header[1] = length
        copyto(ndarray(shape=[length], buffer=ba, offset=16,
                dtype="float64"), v)
        return ba
    else:
        raise TypeError("_serialize_double_vector called on a "
                        "non-double-vector")

def _deserialize_double_vector(ba):
    if (type(ba) == bytearray and len(ba) >= 16 and (len(ba) & 7 == 0)):
        header = ndarray(shape=[2], buffer=ba, dtype="int64")
        if (header[0] != 1):
            raise TypeError("_deserialize_double_vector called on bytearray "
                            "with wrong magic")
        length = header[1]
        if (len(ba) != 8*length + 16):
            raise TypeError("_deserialize_double_vector called on bytearray "
                            "with wrong length")
        return _deserialize_byte_array([length], ba, 16)
    else:
        raise TypeError("_deserialize_double_vector called on a non-bytearray")

def _serialize_double_matrix(m):
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
    if (type(ba) == bytearray and len(ba) >= 24 and (len(ba) & 7 == 0)):
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
    else:
        raise TypeError("_deserialize_double_matrix called on a non-bytearray")

class LinearModel(object):
    def __init__(self, coeff, intercept):
        self._coeff = coeff
        self._intercept = intercept

    def predict(self, x):
        if (type(x) == ndarray):
            if (x.ndim == 1):
                return dot(_coeff, x) + _intercept
            else:
                raise RuntimeError("Bulk predict not yet supported.")
        elif (type(x) == RDD):
            raise RuntimeError("Bulk predict not yet supported.")
        else:
            raise TypeError("Bad type argument to "
                            "LinearRegressionModel::predict")

# Map a pickled Python RDD of numpy double vectors to a Java RDD of
# _serialized_double_vectors
def _get_unmangled_double_vector_rdd(data):
    dataBytes = data.map(_serialize_double_vector)
    dataBytes._bypass_serializer = True
    dataBytes.cache()
    return dataBytes;

# If we weren't given initial weights, take a zero vector of the appropriate
# length.
def _get_initial_weights(initial_weights, data):
    if initial_weights is None:
        initial_weights = data.first()
        if type(initial_weights) != ndarray:
            raise TypeError("At least one data element has type "
                    + type(initial_weights) + " which is not ndarray")
        if initial_weights.ndim != 1:
            raise TypeError("At least one data element has "
                    + initial_weights.ndim + " dimensions, which is not 1")
        initial_weights = zeros([initial_weights.shape[0] - 1]);
    return initial_weights;

# train_func should take two parameters, namely data and initial_weights, and
# return the result of a call to the appropriate JVM stub.
# _regression_train_wrapper is responsible for setup and error checking.
def _regression_train_wrapper(sc, train_func, klass, data, initial_weights):
    initial_weights = _get_initial_weights(initial_weights, data)
    dataBytes = _get_unmangled_double_vector_rdd(data)
    ans = train_func(dataBytes, _serialize_double_vector(initial_weights))
    if len(ans) != 2:
        raise RuntimeError("JVM call result had unexpected length");
    elif type(ans[0]) != bytearray:
        raise RuntimeError("JVM call result had first element of type "
                + type(ans[0]) + " which is not bytearray");
    elif type(ans[1]) != float:
        raise RuntimeError("JVM call result had second element of type "
                + type(ans[0]) + " which is not float");
    return klass(_deserialize_double_vector(ans[0]), ans[1]);

class LinearRegressionModel(LinearModel):
    @classmethod
    def train(cls, sc, data, iterations=100, step=1.0,
              mini_batch_fraction=1.0, initial_weights=None):
        """Train a linear regression model on the given data."""
        return _regression_train_wrapper(sc, lambda d, i:
                sc._jvm.PythonMLLibAPI().trainLinearRegressionModel(
                        d._jrdd, iterations, step, mini_batch_fraction, i),
                LinearRegressionModel, data, initial_weights)

class LassoModel(LinearModel):
    @classmethod
    def train(cls, sc, data, iterations=100, step=1.0, reg_param=1.0,
              mini_batch_fraction=1.0, initial_weights=None):
        """Train a Lasso regression model on the given data."""
        return _regression_train_wrapper(sc, lambda d, i:
                sc._jvm.PythonMLLibAPI().trainLassoModel(d._jrdd,
                        iterations, step, reg_param, mini_batch_fraction, i),
                LassoModel, data, initial_weights)

class RidgeRegressionModel(LinearModel):
    @classmethod
    def train(cls, sc, data, iterations=100, step=1.0, reg_param=1.0,
              mini_batch_fraction=1.0, initial_weights=None):
        """Train a ridge regression model on the given data."""
        return _regression_train_wrapper(sc, lambda d, i:
                sc._jvm.PythonMLLibAPI().trainRidgeModel(d._jrdd,
                        iterations, step, reg_param, mini_batch_fraction, i),
                RidgeRegressionModel, data, initial_weights)
