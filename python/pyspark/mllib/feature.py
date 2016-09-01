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
Python package for feature in MLlib.
"""
from __future__ import absolute_import

import sys
import warnings
import random
import binascii
if sys.version >= '3':
    basestring = str
    unicode = str

from py4j.protocol import Py4JJavaError

from pyspark import since
from pyspark.rdd import RDD, ignore_unicode_prefix
from pyspark.mllib.common import callMLlibFunc, JavaModelWrapper
from pyspark.mllib.linalg import (
    Vector, Vectors, DenseVector, SparseVector, _convert_to_vector)
from pyspark.mllib.regression import LabeledPoint
from pyspark.mllib.util import JavaLoader, JavaSaveable

__all__ = ['Normalizer', 'StandardScalerModel', 'StandardScaler',
           'HashingTF', 'IDFModel', 'IDF', 'Word2Vec', 'Word2VecModel',
           'ChiSqSelector', 'ChiSqSelectorModel', 'ElementwiseProduct']


class VectorTransformer(object):
    """
    .. note:: DeveloperApi

    Base class for transformation of a vector or RDD of vector
    """
    def transform(self, vector):
        """
        Applies transformation on a vector.

        :param vector: vector to be transformed.
        """
        raise NotImplementedError


class Normalizer(VectorTransformer):
    """
    Normalizes samples individually to unit L\ :sup:`p`\  norm

    For any 1 <= `p` < float('inf'), normalizes samples using
    sum(abs(vector) :sup:`p`) :sup:`(1/p)` as norm.

    For `p` = float('inf'), max(abs(vector)) will be used as norm for
    normalization.

    :param p: Normalization in L^p^ space, p = 2 by default.

    >>> v = Vectors.dense(range(3))
    >>> nor = Normalizer(1)
    >>> nor.transform(v)
    DenseVector([0.0, 0.3333, 0.6667])

    >>> rdd = sc.parallelize([v])
    >>> nor.transform(rdd).collect()
    [DenseVector([0.0, 0.3333, 0.6667])]

    >>> nor2 = Normalizer(float("inf"))
    >>> nor2.transform(v)
    DenseVector([0.0, 0.5, 1.0])

    .. versionadded:: 1.2.0
    """
    def __init__(self, p=2.0):
        assert p >= 1.0, "p should be greater than 1.0"
        self.p = float(p)

    @since('1.2.0')
    def transform(self, vector):
        """
        Applies unit length normalization on a vector.

        :param vector: vector or RDD of vector to be normalized.
        :return: normalized vector. If the norm of the input is zero, it
                 will return the input vector.
        """
        if isinstance(vector, RDD):
            vector = vector.map(_convert_to_vector)
        else:
            vector = _convert_to_vector(vector)
        return callMLlibFunc("normalizeVector", self.p, vector)


class JavaVectorTransformer(JavaModelWrapper, VectorTransformer):
    """
    Wrapper for the model in JVM
    """

    def transform(self, vector):
        """
        Applies transformation on a vector or an RDD[Vector].

        Note: In Python, transform cannot currently be used within
              an RDD transformation or action.
              Call transform directly on the RDD instead.

        :param vector: Vector or RDD of Vector to be transformed.
        """
        if isinstance(vector, RDD):
            vector = vector.map(_convert_to_vector)
        else:
            vector = _convert_to_vector(vector)
        return self.call("transform", vector)


class StandardScalerModel(JavaVectorTransformer):
    """
    Represents a StandardScaler model that can transform vectors.

    .. versionadded:: 1.2.0
    """

    @since('1.2.0')
    def transform(self, vector):
        """
        Applies standardization transformation on a vector.

        Note: In Python, transform cannot currently be used within
              an RDD transformation or action.
              Call transform directly on the RDD instead.

        :param vector: Vector or RDD of Vector to be standardized.
        :return: Standardized vector. If the variance of a column is
                 zero, it will return default `0.0` for the column with
                 zero variance.
        """
        return JavaVectorTransformer.transform(self, vector)

    @since('1.4.0')
    def setWithMean(self, withMean):
        """
        Setter of the boolean which decides
        whether it uses mean or not
        """
        self.call("setWithMean", withMean)
        return self

    @since('1.4.0')
    def setWithStd(self, withStd):
        """
        Setter of the boolean which decides
        whether it uses std or not
        """
        self.call("setWithStd", withStd)
        return self

    @property
    @since('2.0.0')
    def withStd(self):
        """
        Returns if the model scales the data to unit standard deviation.
        """
        return self.call("withStd")

    @property
    @since('2.0.0')
    def withMean(self):
        """
        Returns if the model centers the data before scaling.
        """
        return self.call("withMean")

    @property
    @since('2.0.0')
    def std(self):
        """
        Return the column standard deviation values.
        """
        return self.call("std")

    @property
    @since('2.0.0')
    def mean(self):
        """
        Return the column mean values.
        """
        return self.call("mean")


class StandardScaler(object):
    """
    Standardizes features by removing the mean and scaling to unit
    variance using column summary statistics on the samples in the
    training set.

    :param withMean: False by default. Centers the data with mean
                     before scaling. It will build a dense output, so take
                     care when applying to sparse input.
    :param withStd: True by default. Scales the data to unit
                    standard deviation.

    >>> vs = [Vectors.dense([-2.0, 2.3, 0]), Vectors.dense([3.8, 0.0, 1.9])]
    >>> dataset = sc.parallelize(vs)
    >>> standardizer = StandardScaler(True, True)
    >>> model = standardizer.fit(dataset)
    >>> result = model.transform(dataset)
    >>> for r in result.collect(): r
    DenseVector([-0.7071, 0.7071, -0.7071])
    DenseVector([0.7071, -0.7071, 0.7071])
    >>> int(model.std[0])
    4
    >>> int(model.mean[0]*10)
    9
    >>> model.withStd
    True
    >>> model.withMean
    True

    .. versionadded:: 1.2.0
    """
    def __init__(self, withMean=False, withStd=True):
        if not (withMean or withStd):
            warnings.warn("Both withMean and withStd are false. The model does nothing.")
        self.withMean = withMean
        self.withStd = withStd

    @since('1.2.0')
    def fit(self, dataset):
        """
        Computes the mean and variance and stores as a model to be used
        for later scaling.

        :param dataset: The data used to compute the mean and variance
                     to build the transformation model.
        :return: a StandardScalarModel
        """
        dataset = dataset.map(_convert_to_vector)
        jmodel = callMLlibFunc("fitStandardScaler", self.withMean, self.withStd, dataset)
        return StandardScalerModel(jmodel)


class ChiSqSelectorModel(JavaVectorTransformer):
    """
    Represents a Chi Squared selector model.

    .. versionadded:: 1.4.0
    """

    @since('1.4.0')
    def transform(self, vector):
        """
        Applies transformation on a vector.

        :param vector: Vector or RDD of Vector to be transformed.
        :return: transformed vector.
        """
        return JavaVectorTransformer.transform(self, vector)


class ChiSqSelector(object):
    """
    Creates a ChiSquared feature selector.

    :param numTopFeatures: number of features that selector will select.

    >>> data = [
    ...     LabeledPoint(0.0, SparseVector(3, {0: 8.0, 1: 7.0})),
    ...     LabeledPoint(1.0, SparseVector(3, {1: 9.0, 2: 6.0})),
    ...     LabeledPoint(1.0, [0.0, 9.0, 8.0]),
    ...     LabeledPoint(2.0, [8.0, 9.0, 5.0])
    ... ]
    >>> model = ChiSqSelector(1).fit(sc.parallelize(data))
    >>> model.transform(SparseVector(3, {1: 9.0, 2: 6.0}))
    SparseVector(1, {0: 6.0})
    >>> model.transform(DenseVector([8.0, 9.0, 5.0]))
    DenseVector([5.0])

    .. versionadded:: 1.4.0
    """
    def __init__(self, numTopFeatures):
        self.numTopFeatures = int(numTopFeatures)

    @since('1.4.0')
    def fit(self, data):
        """
        Returns a ChiSquared feature selector.

        :param data: an `RDD[LabeledPoint]` containing the labeled dataset
                     with categorical features. Real-valued features will be
                     treated as categorical for each distinct value.
                     Apply feature discretizer before using this function.
        """
        jmodel = callMLlibFunc("fitChiSqSelector", self.numTopFeatures, data)
        return ChiSqSelectorModel(jmodel)


class PCAModel(JavaVectorTransformer):
    """
    Model fitted by [[PCA]] that can project vectors to a low-dimensional space using PCA.

    .. versionadded:: 1.5.0
    """


class PCA(object):
    """
    A feature transformer that projects vectors to a low-dimensional space using PCA.

    >>> data = [Vectors.sparse(5, [(1, 1.0), (3, 7.0)]),
    ...     Vectors.dense([2.0, 0.0, 3.0, 4.0, 5.0]),
    ...     Vectors.dense([4.0, 0.0, 0.0, 6.0, 7.0])]
    >>> model = PCA(2).fit(sc.parallelize(data))
    >>> pcArray = model.transform(Vectors.sparse(5, [(1, 1.0), (3, 7.0)])).toArray()
    >>> pcArray[0]
    1.648...
    >>> pcArray[1]
    -4.013...

    .. versionadded:: 1.5.0
    """
    def __init__(self, k):
        """
        :param k: number of principal components.
        """
        self.k = int(k)

    @since('1.5.0')
    def fit(self, data):
        """
        Computes a [[PCAModel]] that contains the principal components of the input vectors.
        :param data: source vectors
        """
        jmodel = callMLlibFunc("fitPCA", self.k, data)
        return PCAModel(jmodel)


class HashingTF(object):
    """
    Maps a sequence of terms to their term frequencies using the hashing
    trick.

    Note: the terms must be hashable (can not be dict/set/list...).

    :param numFeatures: number of features (default: 2^20)

    >>> htf = HashingTF(100)
    >>> doc = "a a b b c d".split(" ")
    >>> htf.transform(doc)
    SparseVector(100, {...})

    .. versionadded:: 1.2.0
    """
    def __init__(self, numFeatures=1 << 20):
        self.numFeatures = numFeatures
        self.binary = False

    @since("2.0.0")
    def setBinary(self, value):
        """
        If True, term frequency vector will be binary such that non-zero
        term counts will be set to 1
        (default: False)
        """
        self.binary = value
        return self

    @since('1.2.0')
    def indexOf(self, term):
        """ Returns the index of the input term. """
        return hash(term) % self.numFeatures

    @since('1.2.0')
    def transform(self, document):
        """
        Transforms the input document (list of terms) to term frequency
        vectors, or transform the RDD of document to RDD of term
        frequency vectors.
        """
        if isinstance(document, RDD):
            return document.map(self.transform)

        freq = {}
        for term in document:
            i = self.indexOf(term)
            freq[i] = 1.0 if self.binary else freq.get(i, 0) + 1.0
        return Vectors.sparse(self.numFeatures, freq.items())


class IDFModel(JavaVectorTransformer):
    """
    Represents an IDF model that can transform term frequency vectors.

    .. versionadded:: 1.2.0
    """
    @since('1.2.0')
    def transform(self, x):
        """
        Transforms term frequency (TF) vectors to TF-IDF vectors.

        If `minDocFreq` was set for the IDF calculation,
        the terms which occur in fewer than `minDocFreq`
        documents will have an entry of 0.

        Note: In Python, transform cannot currently be used within
              an RDD transformation or action.
              Call transform directly on the RDD instead.

        :param x: an RDD of term frequency vectors or a term frequency
                  vector
        :return: an RDD of TF-IDF vectors or a TF-IDF vector
        """
        return JavaVectorTransformer.transform(self, x)

    @since('1.4.0')
    def idf(self):
        """
        Returns the current IDF vector.
        """
        return self.call('idf')


class IDF(object):
    """
    Inverse document frequency (IDF).

    The standard formulation is used: `idf = log((m + 1) / (d(t) + 1))`,
    where `m` is the total number of documents and `d(t)` is the number
    of documents that contain term `t`.

    This implementation supports filtering out terms which do not appear
    in a minimum number of documents (controlled by the variable
    `minDocFreq`). For terms that are not in at least `minDocFreq`
    documents, the IDF is found as 0, resulting in TF-IDFs of 0.

    :param minDocFreq: minimum of documents in which a term
                       should appear for filtering

    >>> n = 4
    >>> freqs = [Vectors.sparse(n, (1, 3), (1.0, 2.0)),
    ...          Vectors.dense([0.0, 1.0, 2.0, 3.0]),
    ...          Vectors.sparse(n, [1], [1.0])]
    >>> data = sc.parallelize(freqs)
    >>> idf = IDF()
    >>> model = idf.fit(data)
    >>> tfidf = model.transform(data)
    >>> for r in tfidf.collect(): r
    SparseVector(4, {1: 0.0, 3: 0.5754})
    DenseVector([0.0, 0.0, 1.3863, 0.863])
    SparseVector(4, {1: 0.0})
    >>> model.transform(Vectors.dense([0.0, 1.0, 2.0, 3.0]))
    DenseVector([0.0, 0.0, 1.3863, 0.863])
    >>> model.transform([0.0, 1.0, 2.0, 3.0])
    DenseVector([0.0, 0.0, 1.3863, 0.863])
    >>> model.transform(Vectors.sparse(n, (1, 3), (1.0, 2.0)))
    SparseVector(4, {1: 0.0, 3: 0.5754})

    .. versionadded:: 1.2.0
    """
    def __init__(self, minDocFreq=0):
        self.minDocFreq = minDocFreq

    @since('1.2.0')
    def fit(self, dataset):
        """
        Computes the inverse document frequency.

        :param dataset: an RDD of term frequency vectors
        """
        if not isinstance(dataset, RDD):
            raise TypeError("dataset should be an RDD of term frequency vectors")
        jmodel = callMLlibFunc("fitIDF", self.minDocFreq, dataset.map(_convert_to_vector))
        return IDFModel(jmodel)


class Word2VecModel(JavaVectorTransformer, JavaSaveable, JavaLoader):
    """
    class for Word2Vec model

    .. versionadded:: 1.2.0
    """
    @since('1.2.0')
    def transform(self, word):
        """
        Transforms a word to its vector representation

        Note: local use only

        :param word: a word
        :return: vector representation of word(s)
        """
        try:
            return self.call("transform", word)
        except Py4JJavaError:
            raise ValueError("%s not found" % word)

    @since('1.2.0')
    def findSynonyms(self, word, num):
        """
        Find synonyms of a word

        :param word: a word or a vector representation of word
        :param num: number of synonyms to find
        :return: array of (word, cosineSimilarity)

        Note: local use only
        """
        if not isinstance(word, basestring):
            word = _convert_to_vector(word)
        words, similarity = self.call("findSynonyms", word, num)
        return zip(words, similarity)

    @since('1.4.0')
    def getVectors(self):
        """
        Returns a map of words to their vector representations.
        """
        return self.call("getVectors")

    @classmethod
    @since('1.5.0')
    def load(cls, sc, path):
        """
        Load a model from the given path.
        """
        jmodel = sc._jvm.org.apache.spark.mllib.feature \
            .Word2VecModel.load(sc._jsc.sc(), path)
        model = sc._jvm.org.apache.spark.mllib.api.python.Word2VecModelWrapper(jmodel)
        return Word2VecModel(model)


@ignore_unicode_prefix
class Word2Vec(object):
    """
    Word2Vec creates vector representation of words in a text corpus.
    The algorithm first constructs a vocabulary from the corpus
    and then learns vector representation of words in the vocabulary.
    The vector representation can be used as features in
    natural language processing and machine learning algorithms.

    We used skip-gram model in our implementation and hierarchical
    softmax method to train the model. The variable names in the
    implementation matches the original C implementation.

    For original C implementation,
    see https://code.google.com/p/word2vec/
    For research papers, see
    Efficient Estimation of Word Representations in Vector Space
    and Distributed Representations of Words and Phrases and their
    Compositionality.

    >>> sentence = "a b " * 100 + "a c " * 10
    >>> localDoc = [sentence, sentence]
    >>> doc = sc.parallelize(localDoc).map(lambda line: line.split(" "))
    >>> model = Word2Vec().setVectorSize(10).setSeed(42).fit(doc)

    >>> syms = model.findSynonyms("a", 2)
    >>> [s[0] for s in syms]
    [u'b', u'c']
    >>> vec = model.transform("a")
    >>> syms = model.findSynonyms(vec, 2)
    >>> [s[0] for s in syms]
    [u'b', u'c']

    >>> import os, tempfile
    >>> path = tempfile.mkdtemp()
    >>> model.save(sc, path)
    >>> sameModel = Word2VecModel.load(sc, path)
    >>> model.transform("a") == sameModel.transform("a")
    True
    >>> syms = sameModel.findSynonyms("a", 2)
    >>> [s[0] for s in syms]
    [u'b', u'c']
    >>> from shutil import rmtree
    >>> try:
    ...     rmtree(path)
    ... except OSError:
    ...     pass

    .. versionadded:: 1.2.0
    """
    def __init__(self):
        """
        Construct Word2Vec instance
        """
        self.vectorSize = 100
        self.learningRate = 0.025
        self.numPartitions = 1
        self.numIterations = 1
        self.seed = random.randint(0, sys.maxsize)
        self.minCount = 5
        self.windowSize = 5

    @since('1.2.0')
    def setVectorSize(self, vectorSize):
        """
        Sets vector size (default: 100).
        """
        self.vectorSize = vectorSize
        return self

    @since('1.2.0')
    def setLearningRate(self, learningRate):
        """
        Sets initial learning rate (default: 0.025).
        """
        self.learningRate = learningRate
        return self

    @since('1.2.0')
    def setNumPartitions(self, numPartitions):
        """
        Sets number of partitions (default: 1). Use a small number for
        accuracy.
        """
        self.numPartitions = numPartitions
        return self

    @since('1.2.0')
    def setNumIterations(self, numIterations):
        """
        Sets number of iterations (default: 1), which should be smaller
        than or equal to number of partitions.
        """
        self.numIterations = numIterations
        return self

    @since('1.2.0')
    def setSeed(self, seed):
        """
        Sets random seed.
        """
        self.seed = seed
        return self

    @since('1.4.0')
    def setMinCount(self, minCount):
        """
        Sets minCount, the minimum number of times a token must appear
        to be included in the word2vec model's vocabulary (default: 5).
        """
        self.minCount = minCount
        return self

    @since('2.0.0')
    def setWindowSize(self, windowSize):
        """
        Sets window size (default: 5).
        """
        self.windowSize = windowSize
        return self

    @since('1.2.0')
    def fit(self, data):
        """
        Computes the vector representation of each word in vocabulary.

        :param data: training data. RDD of list of string
        :return: Word2VecModel instance
        """
        if not isinstance(data, RDD):
            raise TypeError("data should be an RDD of list of string")
        jmodel = callMLlibFunc("trainWord2VecModel", data, int(self.vectorSize),
                               float(self.learningRate), int(self.numPartitions),
                               int(self.numIterations), self.seed,
                               int(self.minCount), int(self.windowSize))
        return Word2VecModel(jmodel)


class ElementwiseProduct(VectorTransformer):
    """
    Scales each column of the vector, with the supplied weight vector.
    i.e the elementwise product.

    >>> weight = Vectors.dense([1.0, 2.0, 3.0])
    >>> eprod = ElementwiseProduct(weight)
    >>> a = Vectors.dense([2.0, 1.0, 3.0])
    >>> eprod.transform(a)
    DenseVector([2.0, 2.0, 9.0])
    >>> b = Vectors.dense([9.0, 3.0, 4.0])
    >>> rdd = sc.parallelize([a, b])
    >>> eprod.transform(rdd).collect()
    [DenseVector([2.0, 2.0, 9.0]), DenseVector([9.0, 6.0, 12.0])]

    .. versionadded:: 1.5.0
    """
    def __init__(self, scalingVector):
        self.scalingVector = _convert_to_vector(scalingVector)

    @since('1.5.0')
    def transform(self, vector):
        """
        Computes the Hadamard product of the vector.
        """
        if isinstance(vector, RDD):
            vector = vector.map(_convert_to_vector)

        else:
            vector = _convert_to_vector(vector)
        return callMLlibFunc("elementwiseProductVector", self.scalingVector, vector)


def _test():
    import doctest
    from pyspark.sql import SparkSession
    globs = globals().copy()
    spark = SparkSession.builder\
        .master("local[4]")\
        .appName("mllib.feature tests")\
        .getOrCreate()
    globs['sc'] = spark.sparkContext
    (failure_count, test_count) = doctest.testmod(globs=globs, optionflags=doctest.ELLIPSIS)
    spark.stop()
    if failure_count:
        exit(-1)

if __name__ == "__main__":
    sys.path.pop(0)
    _test()
