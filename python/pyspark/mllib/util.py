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
from pyspark.mllib._common import _convert_vector


class MLUtils:
    """
    Helper methods to load, save and pre-process data used in MLlib.
    """

    @staticmethod
    def _parse_libsvm_line(line, multiclass):
        """
        Parses a line in LIBSVM format into (label, indices, values).
        """
        items = line.split(None)
        label = float(items[0])
        if not multiclass:
            label = 1.0 if label > 0.5 else 0.0
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

    @staticmethod
    def loadLibSVMFile(sc, path, multiclass=False, numFeatures=-1, minPartitions=None):
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
        @param multiclass: whether the input labels contain more than
                           two classes. If false, any label with value
                           greater than 0.5 will be mapped to 1.0, or
                           0.0 otherwise. So it works for both +1/-1 and
                           1/0 cases. If true, the double value parsed
                           directly from the label string will be used
                           as the label value.
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
        >>> multiclass_examples = MLUtils.loadLibSVMFile(sc, tempFile.name, True).collect()
        >>> tempFile.close()
        >>> examples[0].label
        1.0
        >>> examples[0].features.size
        6
        >>> print examples[0].features
        [0: 1.0, 2: 2.0, 4: 3.0]
        >>> examples[1].label
        0.0
        >>> examples[1].features.size
        6
        >>> print examples[1].features
        []
        >>> examples[2].label
        0.0
        >>> examples[2].features.size
        6
        >>> print examples[2].features
        [1: 4.0, 3: 5.0, 5: 6.0]
        >>> multiclass_examples[1].label
        -1.0
        """

        lines = sc.textFile(path, minPartitions)
        parsed = lines.map(lambda l: MLUtils._parse_libsvm_line(l, multiclass))
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
