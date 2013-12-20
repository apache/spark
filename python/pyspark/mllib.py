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

def _linear_predictor_typecheck(x, coeffs):
    """Predict the class of the vector x."""
    if type(x) == ndarray:
        if x.ndim == 1:
            if x.shape == coeffs.shape:
                pass
            else:
                raise RuntimeError("Got array of %d elements; wanted %d"
                        % shape(x)[0] % shape(coeffs)[0])
        else:
            raise RuntimeError("Bulk predict not yet supported.")
    elif (type(x) == RDD):
        raise RuntimeError("Bulk predict not yet supported.")
    else:
        raise TypeError("Argument of type " + type(x) + " unsupported");

class LinearModel(object):
    """Something containing a vector of coefficients and an intercept."""
    def __init__(self, coeff, intercept):
        self._coeff = coeff
        self._intercept = intercept

class LinearRegressionModelBase(LinearModel):
    """A linear regression model."""
    def predict(self, x):
        """Predict the value of the dependent variable given a vector x"""
        """containing values for the independent variables."""
        _linear_predictor_typecheck(x, _coeff)
        return dot(_coeff, x) + _intercept

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

class LinearRegressionModel(LinearRegressionModelBase):
    """A linear regression model derived from a least-squares fit."""
    @classmethod
    def train(cls, sc, data, iterations=100, step=1.0,
              mini_batch_fraction=1.0, initial_weights=None):
        """Train a linear regression model on the given data."""
        return _regression_train_wrapper(sc, lambda d, i:
                sc._jvm.PythonMLLibAPI().trainLinearRegressionModel(
                        d._jrdd, iterations, step, mini_batch_fraction, i),
                LinearRegressionModel, data, initial_weights)

class LassoModel(LinearRegressionModelBase):
    """A linear regression model derived from a least-squares fit with an """
    """l_1 penalty term."""
    @classmethod
    def train(cls, sc, data, iterations=100, step=1.0, reg_param=1.0,
              mini_batch_fraction=1.0, initial_weights=None):
        """Train a Lasso regression model on the given data."""
        return _regression_train_wrapper(sc, lambda d, i:
                sc._jvm.PythonMLLibAPI().trainLassoModel(d._jrdd,
                        iterations, step, reg_param, mini_batch_fraction, i),
                LassoModel, data, initial_weights)

class RidgeRegressionModel(LinearRegressionModelBase):
    """A linear regression model derived from a least-squares fit with an """
    """l_2 penalty term."""
    @classmethod
    def train(cls, sc, data, iterations=100, step=1.0, reg_param=1.0,
              mini_batch_fraction=1.0, initial_weights=None):
        """Train a ridge regression model on the given data."""
        return _regression_train_wrapper(sc, lambda d, i:
                sc._jvm.PythonMLLibAPI().trainRidgeModel(d._jrdd,
                        iterations, step, reg_param, mini_batch_fraction, i),
                RidgeRegressionModel, data, initial_weights)

class LogisticRegressionModel(LinearModel):
    """A linear binary classification model derived from logistic regression."""
    def predict(self, x):
        _linear_predictor_typecheck(x, _coeff)
        margin = dot(x, _coeff) + intercept
        prob = 1/(1 + exp(-margin))
        return 1 if prob > 0.5 else 0

    @classmethod
    def train(cls, sc, data, iterations=100, step=1.0,
              mini_batch_fraction=1.0, initial_weights=None):
        """Train a logistic regression model on the given data."""
        return _regression_train_wrapper(sc, lambda d, i:
                sc._jvm.PythonMLLibAPI().trainLogisticRegressionModel(d._jrdd,
                        iterations, step, mini_batch_fraction, i),
                LogisticRegressionModel, data, initial_weights)

class SVMModel(LinearModel):
    """A support vector machine."""
    def predict(self, x):
        _linear_predictor_typecheck(x, _coeff)
        margin = dot(x, _coeff) + intercept
        return 1 if margin >= 0 else 0
    @classmethod
    def train(cls, sc, data, iterations=100, step=1.0, reg_param=1.0,
              mini_batch_fraction=1.0, initial_weights=None):
        """Train a support vector machine on the given data."""
        return _regression_train_wrapper(sc, lambda d, i:
                sc._jvm.PythonMLLibAPI().trainSVMModel(d._jrdd,
                        iterations, step, reg_param, mini_batch_fraction, i),
                SVMModel, data, initial_weights)

class KMeansModel(object):
    """A clustering model derived from the k-means method."""
    def __init__(self, centers_):
        self.centers = centers_

    def predict(self, x):
        best = 0
        best_distance = 1e75
        for i in range(0, centers.shape[0]):
            diff = x - centers[i]
            distance = sqrt(dot(diff, diff))
            if distance < best_distance:
                best = i
                best_distance = distance
        return best

    @classmethod
    def train(cls, sc, data, k, maxIterations = 100, runs = 1,
            initialization_mode="k-means||"):
        dataBytes = _get_unmangled_double_vector_rdd(data)
        ans = sc._jvm.PythonMLLibAPI().trainKMeansModel(dataBytes._jrdd,
                k, maxIterations, runs, initialization_mode)
        if len(ans) != 1:
            raise RuntimeError("JVM call result had unexpected length");
        elif type(ans[0]) != bytearray:
            raise RuntimeError("JVM call result had first element of type "
                    + type(ans[0]) + " which is not bytearray");
        return KMeansModel(_deserialize_double_matrix(ans[0]));
