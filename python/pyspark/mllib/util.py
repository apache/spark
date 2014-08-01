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

import numpy as np

from pyspark.mllib.linalg import Vectors, SparseVector
from pyspark.mllib.regression import LabeledPoint
from pyspark.mllib._common import _convert_vector, _deserialize_labeled_point
from pyspark.rdd import RDD
from pyspark.serializers import NoOpSerializer


class MLUtils:
    """
    Helper methods to load, save and pre-process data used in MLlib.
    """

    @deprecated
    @staticmethod
    def _parse_libsvm_line(line, multiclass):
        return _parse_libsvm_line(line)

    @staticmethod
    def _parse_libsvm_line(line):
        """
        Parses a line in LIBSVM format into (label, indices, values).
        """
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
        items = [str(p.label)]
        v = _convert_vector(p.features)
        if type(v) == np.ndarray:
            for i in xrange(len(v)):
                items.append(str(i + 1) + ":" + str(v[i]))
        elif type(v) == SparseVector:
            nnz = len(v.indices)
            for i in xrange(nnz):
                items.append(str(v.indices[i] + 1) + ":" + str(v.values[i]))
        else:
            raise TypeError("_convert_labeled_point_to_libsvm needs either ndarray or SparseVector"
                            " but got " % type(v))
        return " ".join(items)

    @deprecated
    @staticmethod
    def loadLibSVMFile(sc, path, multiclass=False, numFeatures=-1, minPartitions=None):
        return loadLibSVMFile(sc, path, numFeatures, minPartitions)

    @staticmethod
    def loadLibSVMFile(sc, path, numFeatures=-1, minPartitions=None):
        """
        Loads labeled data in the LIBSVM format into an RDD of
        LabeledPoint. The LIBSVM format is a text-based format used by
        LIBSVM and LIBLINEAR. Each line represents a labeled sparse
        feature vector using the following format:

        label index1:value1 index2:value2 ...

        where the indices are one-based and in ascending order. This
        method parses each line into a LabeledPoint, where the feature
        indices are converted to zero-based.

        @param sc: Spark context
        @param path: file or directory path in any Hadoop-supported file
                     system URI
        @param numFeatures: number of features, which will be determined
                            from the input data if a nonpositive value
                            is given. This is useful when the dataset is
                            already split into multiple files and you
                            want to load them separately, because some
                            features may not present in certain files,
                            which leads to inconsistent feature
                            dimensions.
        @param minPartitions: min number of partitions
        @return: labeled data stored as an RDD of LabeledPoint

        >>> from tempfile import NamedTemporaryFile
        >>> from pyspark.mllib.util import MLUtils
        >>> tempFile = NamedTemporaryFile(delete=True)
        >>> tempFile.write("+1 1:1.0 3:2.0 5:3.0\\n-1\\n-1 2:4.0 4:5.0 6:6.0")
        >>> tempFile.flush()
        >>> examples = MLUtils.loadLibSVMFile(sc, tempFile.name).collect()
        >>> multiclass_examples = MLUtils.loadLibSVMFile(sc, tempFile.name).collect()
        >>> tempFile.close()
        >>> type(examples[0]) == LabeledPoint
        True
        >>> print examples[0]
        (1.0,(6,[0,2,4],[1.0,2.0,3.0]))
        >>> type(examples[1]) == LabeledPoint
        True
        >>> print examples[1]
        (0.0,(6,[],[]))
        >>> type(examples[2]) == LabeledPoint
        True
        >>> print examples[2]
        (0.0,(6,[1,3,5],[4.0,5.0,6.0]))
        >>> multiclass_examples[1].label
        -1.0
        """

        lines = sc.textFile(path, minPartitions)
        parsed = lines.map(lambda l: MLUtils._parse_libsvm_line(l))
        if numFeatures <= 0:
            parsed.cache()
            numFeatures = parsed.map(lambda x: 0 if x[1].size == 0 else x[1][-1]).reduce(max) + 1
        return parsed.map(lambda x: LabeledPoint(x[0], Vectors.sparse(numFeatures, x[1], x[2])))

    @staticmethod
    def saveAsLibSVMFile(data, dir):
        """
        Save labeled data in LIBSVM format.

        @param data: an RDD of LabeledPoint to be saved
        @param dir: directory to save the data

        >>> from tempfile import NamedTemporaryFile
        >>> from fileinput import input
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
    def loadLabeledPoints(sc, path, minPartitions=None):
        """
        Load labeled points saved using RDD.saveAsTextFile.

        @param sc: Spark context
        @param path: file or directory path in any Hadoop-supported file
                     system URI
        @param minPartitions: min number of partitions
        @return: labeled data stored as an RDD of LabeledPoint

        >>> from tempfile import NamedTemporaryFile
        >>> from pyspark.mllib.util import MLUtils
        >>> examples = [LabeledPoint(1.1, Vectors.sparse(3, [(0, -1.23), (2, 4.56e-7)])), \
                        LabeledPoint(0.0, Vectors.dense([1.01, 2.02, 3.03]))]
        >>> tempFile = NamedTemporaryFile(delete=True)
        >>> tempFile.close()
        >>> sc.parallelize(examples, 1).saveAsTextFile(tempFile.name)
        >>> loaded = MLUtils.loadLabeledPoints(sc, tempFile.name).collect()
        >>> type(loaded[0]) == LabeledPoint
        True
        >>> print examples[0]
        (1.1,(3,[0,2],[-1.23,4.56e-07]))
        >>> type(examples[1]) == LabeledPoint
        True
        >>> print examples[1]
        (0.0,[1.01,2.02,3.03])
        """
        minPartitions = minPartitions or min(sc.defaultParallelism, 2)
        jSerialized = sc._jvm.PythonMLLibAPI().loadLabeledPoints(sc._jsc, path, minPartitions)
        serialized = RDD(jSerialized, sc, NoOpSerializer())
        return serialized.map(lambda bytes: _deserialize_labeled_point(bytearray(bytes)))


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
