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

import sys
import numpy as np
import warnings

if sys.version > '3':
    xrange = range
    basestring = str

from pyspark import SparkContext, since
from pyspark.mllib.common import callMLlibFunc, inherit_doc
from pyspark.mllib.linalg import Vectors, SparseVector, _convert_to_vector


class MLUtils(object):

    """
    Helper methods to load, save and pre-process data used in MLlib.

    .. versionadded:: 1.0.0
    """

    @staticmethod
    def _parse_libsvm_line(line, multiclass=None):
        """
        Parses a line in LIBSVM format into (label, indices, values).
        """
        if multiclass is not None:
            warnings.warn("deprecated", DeprecationWarning)
        items = line.split(None)
        label = float(items[0])
        nnz = len(items) - 1
        indices = np.zeros(nnz, dtype=np.int32)
        values = np.zeros(nnz)
        for i in xrange(nnz):
            index, value = items[1 + i].split(":")
            indices[i] = int(index) - 1
            values[i] = float(value)
        return label, indices, values

    @staticmethod
    def _convert_labeled_point_to_libsvm(p):
        """Converts a LabeledPoint to a string in LIBSVM format."""
        from pyspark.mllib.regression import LabeledPoint
        assert isinstance(p, LabeledPoint)
        items = [str(p.label)]
        v = _convert_to_vector(p.features)
        if isinstance(v, SparseVector):
            nnz = len(v.indices)
            for i in xrange(nnz):
                items.append(str(v.indices[i] + 1) + ":" + str(v.values[i]))
        else:
            for i in xrange(len(v)):
                items.append(str(i + 1) + ":" + str(v[i]))
        return " ".join(items)

    @staticmethod
    @since("1.0.0")
    def loadLibSVMFile(sc, path, numFeatures=-1, minPartitions=None, multiclass=None):
        """
        Loads labeled data in the LIBSVM format into an RDD of
        LabeledPoint. The LIBSVM format is a text-based format used by
        LIBSVM and LIBLINEAR. Each line represents a labeled sparse
        feature vector using the following format:

        label index1:value1 index2:value2 ...

        where the indices are one-based and in ascending order. This
        method parses each line into a LabeledPoint, where the feature
        indices are converted to zero-based.

        :param sc: Spark context
        :param path: file or directory path in any Hadoop-supported file
                     system URI
        :param numFeatures: number of features, which will be determined
                            from the input data if a nonpositive value
                            is given. This is useful when the dataset is
                            already split into multiple files and you
                            want to load them separately, because some
                            features may not present in certain files,
                            which leads to inconsistent feature
                            dimensions.
        :param minPartitions: min number of partitions
        @return: labeled data stored as an RDD of LabeledPoint

        >>> from tempfile import NamedTemporaryFile
        >>> from pyspark.mllib.util import MLUtils
        >>> from pyspark.mllib.regression import LabeledPoint
        >>> tempFile = NamedTemporaryFile(delete=True)
        >>> _ = tempFile.write(b"+1 1:1.0 3:2.0 5:3.0\\n-1\\n-1 2:4.0 4:5.0 6:6.0")
        >>> tempFile.flush()
        >>> examples = MLUtils.loadLibSVMFile(sc, tempFile.name).collect()
        >>> tempFile.close()
        >>> examples[0]
        LabeledPoint(1.0, (6,[0,2,4],[1.0,2.0,3.0]))
        >>> examples[1]
        LabeledPoint(-1.0, (6,[],[]))
        >>> examples[2]
        LabeledPoint(-1.0, (6,[1,3,5],[4.0,5.0,6.0]))
        """
        from pyspark.mllib.regression import LabeledPoint
        if multiclass is not None:
            warnings.warn("deprecated", DeprecationWarning)

        lines = sc.textFile(path, minPartitions)
        parsed = lines.map(lambda l: MLUtils._parse_libsvm_line(l))
        if numFeatures <= 0:
            parsed.cache()
            numFeatures = parsed.map(lambda x: -1 if x[1].size == 0 else x[1][-1]).reduce(max) + 1
        return parsed.map(lambda x: LabeledPoint(x[0], Vectors.sparse(numFeatures, x[1], x[2])))

    @staticmethod
    @since("1.0.0")
    def saveAsLibSVMFile(data, dir):
        """
        Save labeled data in LIBSVM format.

        :param data: an RDD of LabeledPoint to be saved
        :param dir: directory to save the data

        >>> from tempfile import NamedTemporaryFile
        >>> from fileinput import input
        >>> from pyspark.mllib.regression import LabeledPoint
        >>> from glob import glob
        >>> from pyspark.mllib.util import MLUtils
        >>> examples = [LabeledPoint(1.1, Vectors.sparse(3, [(0, 1.23), (2, 4.56)])), \
                        LabeledPoint(0.0, Vectors.dense([1.01, 2.02, 3.03]))]
        >>> tempFile = NamedTemporaryFile(delete=True)
        >>> tempFile.close()
        >>> MLUtils.saveAsLibSVMFile(sc.parallelize(examples), tempFile.name)
        >>> ''.join(sorted(input(glob(tempFile.name + "/part-0000*"))))
        '0.0 1:1.01 2:2.02 3:3.03\\n1.1 1:1.23 3:4.56\\n'
        """
        lines = data.map(lambda p: MLUtils._convert_labeled_point_to_libsvm(p))
        lines.saveAsTextFile(dir)

    @staticmethod
    @since("1.1.0")
    def loadLabeledPoints(sc, path, minPartitions=None):
        """
        Load labeled points saved using RDD.saveAsTextFile.

        :param sc: Spark context
        :param path: file or directory path in any Hadoop-supported file
                     system URI
        :param minPartitions: min number of partitions
        @return: labeled data stored as an RDD of LabeledPoint

        >>> from tempfile import NamedTemporaryFile
        >>> from pyspark.mllib.util import MLUtils
        >>> from pyspark.mllib.regression import LabeledPoint
        >>> examples = [LabeledPoint(1.1, Vectors.sparse(3, [(0, -1.23), (2, 4.56e-7)])), \
                        LabeledPoint(0.0, Vectors.dense([1.01, 2.02, 3.03]))]
        >>> tempFile = NamedTemporaryFile(delete=True)
        >>> tempFile.close()
        >>> sc.parallelize(examples, 1).saveAsTextFile(tempFile.name)
        >>> MLUtils.loadLabeledPoints(sc, tempFile.name).collect()
        [LabeledPoint(1.1, (3,[0,2],[-1.23,4.56e-07])), LabeledPoint(0.0, [1.01,2.02,3.03])]
        """
        minPartitions = minPartitions or min(sc.defaultParallelism, 2)
        return callMLlibFunc("loadLabeledPoints", sc, path, minPartitions)

    @staticmethod
    @since("1.5.0")
    def appendBias(data):
        """
        Returns a new vector with `1.0` (bias) appended to
        the end of the input vector.
        """
        vec = _convert_to_vector(data)
        if isinstance(vec, SparseVector):
            newIndices = np.append(vec.indices, len(vec))
            newValues = np.append(vec.values, 1.0)
            return SparseVector(len(vec) + 1, newIndices, newValues)
        else:
            return _convert_to_vector(np.append(vec.toArray(), 1.0))

    @staticmethod
    @since("1.5.0")
    def loadVectors(sc, path):
        """
        Loads vectors saved using `RDD[Vector].saveAsTextFile`
        with the default number of partitions.
        """
        return callMLlibFunc("loadVectors", sc, path)


class Saveable(object):
    """
    Mixin for models and transformers which may be saved as files.

    .. versionadded:: 1.3.0
    """

    def save(self, sc, path):
        """
        Save this model to the given path.

        This saves:
         * human-readable (JSON) model metadata to path/metadata/
         * Parquet formatted data to path/data/

        The model may be loaded using py:meth:`Loader.load`.

        :param sc: Spark context used to save model data.
        :param path: Path specifying the directory in which to save
                     this model. If the directory already exists,
                     this method throws an exception.
        """
        raise NotImplementedError


@inherit_doc
class JavaSaveable(Saveable):
    """
    Mixin for models that provide save() through their Scala
    implementation.

    .. versionadded:: 1.3.0
    """

    @since("1.3.0")
    def save(self, sc, path):
        """Save this model to the given path."""
        if not isinstance(sc, SparkContext):
            raise TypeError("sc should be a SparkContext, got type %s" % type(sc))
        if not isinstance(path, basestring):
            raise TypeError("path should be a basestring, got type %s" % type(path))
        self._java_model.save(sc._jsc.sc(), path)


class Loader(object):
    """
    Mixin for classes which can load saved models from files.

    .. versionadded:: 1.3.0
    """

    @classmethod
    def load(cls, sc, path):
        """
        Load a model from the given path. The model should have been
        saved using py:meth:`Saveable.save`.

        :param sc: Spark context used for loading model files.
        :param path: Path specifying the directory to which the model
                     was saved.
        :return: model instance
        """
        raise NotImplemented


@inherit_doc
class JavaLoader(Loader):
    """
    Mixin for classes which can load saved models using its Scala
    implementation.

    .. versionadded:: 1.3.0
    """

    @classmethod
    def _java_loader_class(cls):
        """
        Returns the full class name of the Java loader. The default
        implementation replaces "pyspark" by "org.apache.spark" in
        the Python full class name.
        """
        java_package = cls.__module__.replace("pyspark", "org.apache.spark")
        return ".".join([java_package, cls.__name__])

    @classmethod
    def _load_java(cls, sc, path):
        """
        Load a Java model from the given path.
        """
        java_class = cls._java_loader_class()
        java_obj = sc._jvm
        for name in java_class.split("."):
            java_obj = getattr(java_obj, name)
        return java_obj.load(sc._jsc.sc(), path)

    @classmethod
    @since("1.3.0")
    def load(cls, sc, path):
        """Load a model from the given path."""
        java_model = cls._load_java(sc, path)
        return cls(java_model)


class LinearDataGenerator(object):
    """Utils for generating linear data.

    .. versionadded:: 1.5.0
    """

    @staticmethod
    @since("1.5.0")
    def generateLinearInput(intercept, weights, xMean, xVariance,
                            nPoints, seed, eps):
        """
        :param: intercept bias factor, the term c in X'w + c
        :param: weights   feature vector, the term w in X'w + c
        :param: xMean     Point around which the data X is centered.
        :param: xVariance Variance of the given data
        :param: nPoints   Number of points to be generated
        :param: seed      Random Seed
        :param: eps       Used to scale the noise. If eps is set high,
                          the amount of gaussian noise added is more.

        Returns a list of LabeledPoints of length nPoints
        """
        weights = [float(weight) for weight in weights]
        xMean = [float(mean) for mean in xMean]
        xVariance = [float(var) for var in xVariance]
        return list(callMLlibFunc(
            "generateLinearInputWrapper", float(intercept), weights, xMean,
            xVariance, int(nPoints), int(seed), float(eps)))

    @staticmethod
    @since("1.5.0")
    def generateLinearRDD(sc, nexamples, nfeatures, eps,
                          nParts=2, intercept=0.0):
        """
        Generate a RDD of LabeledPoints.
        """
        return callMLlibFunc(
            "generateLinearRDDWrapper", sc, int(nexamples), int(nfeatures),
            float(eps), int(nParts), float(intercept))


def _test():
    import doctest
    from pyspark.context import SparkContext
    globs = globals().copy()
    # The small batch size here ensures that we see multiple batches,
    # even in these small test examples:
    globs['sc'] = SparkContext('local[2]', 'PythonTest', batchSize=2)
    (failure_count, test_count) = doctest.testmod(globs=globs, optionflags=doctest.ELLIPSIS)
    globs['sc'].stop()
    if failure_count:
        exit(-1)


if __name__ == "__main__":
    _test()
