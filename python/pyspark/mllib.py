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

class LinearRegressionModel(object):
    def __init__(self, coeff, intercept):
        self._coeff = coeff
        self._intercept = intercept

    def predict(self, x):
        if (type(x) == ndarray):
            if (x.ndim == 1):
                return dot(_coeff, x) - _intercept
            else:
                raise RuntimeError("Bulk predict not yet supported.")
        elif (type(x) == RDD):
            raise RuntimeError("Bulk predict not yet supported.")
        else:
            raise TypeError("Bad type argument to "
                            "LinearRegressionModel::predict")

    @classmethod
    def train(cls, sc, data):
        """Train a linear regression model on the given data."""
        dataBytes = data.map(_serialize_double_vector)
        dataBytes._bypass_serializer = True
        dataBytes.cache()
        api = sc._jvm.PythonMLLibAPI()
        ans = api.trainLinearRegressionModel(dataBytes._jrdd)
        if (len(ans) != 2 or type(ans[0]) != bytearray
                or type(ans[1]) != float):
            raise RuntimeError("train_linear_regression_model received "
                               "garbage from JVM")
        return LinearRegressionModel(_deserialize_double_vector(ans[0]), ans[1])
