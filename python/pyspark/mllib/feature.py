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
import sys
import warnings

from py4j.protocol import Py4JJavaError

from pyspark import RDD, SparkContext
from pyspark.mllib.common import callMLlibFunc, JavaModelWrapper
from pyspark.mllib.linalg import Vectors

__all__ = ['Normalizer', 'StandardScalerModel', 'StandardScaler',
           'HashingTF', 'IDFModel', 'IDF', 'Word2Vec', 'Word2VecModel']


class VectorTransformer(object):
    """
    :: DeveloperApi ::

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
    :: Experimental ::

    Normalizes samples individually to unit L\ :sup:`p`\ norm

    For any 1 <= `p` <= float('inf'), normalizes samples using
    sum(abs(vector). :sup:`p`) :sup:`(1/p)` as norm.

    For `p` = float('inf'), max(abs(vector)) will be used as norm for normalization.

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
    """
    def __init__(self, p=2.0):
        """
        :param p: Normalization in L^p^ space, p = 2 by default.
        """
        assert p >= 1.0, "p should be greater than 1.0"
        self.p = float(p)

    def transform(self, vector):
        """
        Applies unit length normalization on a vector.

        :param vector: vector to be normalized.
        :return: normalized vector. If the norm of the input is zero, it
                will return the input vector.
        """
        sc = SparkContext._active_spark_context
        assert sc is not None, "SparkContext should be initialized first"
        return callMLlibFunc("normalizeVector", self.p, vector)


class JavaVectorTransformer(JavaModelWrapper, VectorTransformer):
    """
    Wrapper for the model in JVM
    """

    def transform(self, dataset):
        return self.call("transform", dataset)


class StandardScalerModel(JavaVectorTransformer):
    """
    :: Experimental ::

    Represents a StandardScaler model that can transform vectors.
    """
    def transform(self, vector):
        """
        Applies standardization transformation on a vector.

        :param vector: Vector to be standardized.
        :return: Standardized vector. If the variance of a column is zero,
                it will return default `0.0` for the column with zero variance.
        """
        return JavaVectorTransformer.transform(self, vector)


class StandardScaler(object):
    """
    :: Experimental ::

    Standardizes features by removing the mean and scaling to unit
    variance using column summary statistics on the samples in the
    training set.

    >>> vs = [Vectors.dense([-2.0, 2.3, 0]), Vectors.dense([3.8, 0.0, 1.9])]
    >>> dataset = sc.parallelize(vs)
    >>> standardizer = StandardScaler(True, True)
    >>> model = standardizer.fit(dataset)
    >>> result = model.transform(dataset)
    >>> for r in result.collect(): r
    DenseVector([-0.7071, 0.7071, -0.7071])
    DenseVector([0.7071, -0.7071, 0.7071])
    """
    def __init__(self, withMean=False, withStd=True):
        """
        :param withMean: False by default. Centers the data with mean
                 before scaling. It will build a dense output, so this
                 does not work on sparse input and will raise an exception.
        :param withStd: True by default. Scales the data to unit standard
                 deviation.
        """
        if not (withMean or withStd):
            warnings.warn("Both withMean and withStd are false. The model does nothing.")
        self.withMean = withMean
        self.withStd = withStd

    def fit(self, dataset):
        """
        Computes the mean and variance and stores as a model to be used for later scaling.

        :param data: The data used to compute the mean and variance to build
                    the transformation model.
        :return: a StandardScalarModel
        """
        jmodel = callMLlibFunc("fitStandardScaler", self.withMean, self.withStd, dataset)
        return StandardScalerModel(jmodel)


class HashingTF(object):
    """
    :: Experimental ::

    Maps a sequence of terms to their term frequencies using the hashing trick.

    Note: the terms must be hashable (can not be dict/set/list...).

    >>> htf = HashingTF(100)
    >>> doc = "a a b b c d".split(" ")
    >>> htf.transform(doc)
    SparseVector(100, {1: 1.0, 14: 1.0, 31: 2.0, 44: 2.0})
    """
    def __init__(self, numFeatures=1 << 20):
        """
        :param numFeatures: number of features (default: 2^20)
        """
        self.numFeatures = numFeatures

    def indexOf(self, term):
        """ Returns the index of the input term. """
        return hash(term) % self.numFeatures

    def transform(self, document):
        """
        Transforms the input document (list of terms) to term frequency vectors,
        or transform the RDD of document to RDD of term frequency vectors.
        """
        if isinstance(document, RDD):
            return document.map(self.transform)

        freq = {}
        for term in document:
            i = self.indexOf(term)
            freq[i] = freq.get(i, 0) + 1.0
        return Vectors.sparse(self.numFeatures, freq.items())


class IDFModel(JavaVectorTransformer):
    """
    Represents an IDF model that can transform term frequency vectors.
    """
    def transform(self, dataset):
        """
        Transforms term frequency (TF) vectors to TF-IDF vectors.

        If `minDocFreq` was set for the IDF calculation,
        the terms which occur in fewer than `minDocFreq`
        documents will have an entry of 0.

        :param dataset: an RDD of term frequency vectors
        :return: an RDD of TF-IDF vectors
        """
        return JavaVectorTransformer.transform(self, dataset)


class IDF(object):
    """
    :: Experimental ::

    Inverse document frequency (IDF).

    The standard formulation is used: `idf = log((m + 1) / (d(t) + 1))`,
    where `m` is the total number of documents and `d(t)` is the number
    of documents that contain term `t`.

    This implementation supports filtering out terms which do not appear
    in a minimum number of documents (controlled by the variable `minDocFreq`).
    For terms that are not in at least `minDocFreq` documents, the IDF is
    found as 0, resulting in TF-IDFs of 0.

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
    """
    def __init__(self, minDocFreq=0):
        """
        :param minDocFreq: minimum of documents in which a term
                           should appear for filtering
        """
        self.minDocFreq = minDocFreq

    def fit(self, dataset):
        """
        Computes the inverse document frequency.

        :param dataset: an RDD of term frequency vectors
        """
        jmodel = callMLlibFunc("fitIDF", self.minDocFreq, dataset)
        return IDFModel(jmodel)


class Word2VecModel(JavaVectorTransformer):
    """
    class for Word2Vec model
    """
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

    def findSynonyms(self, word, num):
        """
        Find synonyms of a word

        :param word: a word or a vector representation of word
        :param num: number of synonyms to find
        :return: array of (word, cosineSimilarity)

        Note: local use only
        """
        words, similarity = self.call("findSynonyms", word, num)
        return zip(words, similarity)


class Word2Vec(object):
    """
    Word2Vec creates vector representation of words in a text corpus.
    The algorithm first constructs a vocabulary from the corpus
    and then learns vector representation of words in the vocabulary.
    The vector representation can be used as features in
    natural language processing and machine learning algorithms.

    We used skip-gram model in our implementation and hierarchical softmax
    method to train the model. The variable names in the implementation
    matches the original C implementation.

    For original C implementation, see https://code.google.com/p/word2vec/
    For research papers, see
    Efficient Estimation of Word Representations in Vector Space
    and
    Distributed Representations of Words and Phrases and their Compositionality.

    >>> sentence = "a b " * 100 + "a c " * 10
    >>> localDoc = [sentence, sentence]
    >>> doc = sc.parallelize(localDoc).map(lambda line: line.split(" "))
    >>> model = Word2Vec().setVectorSize(10).setSeed(42L).fit(doc)

    >>> syms = model.findSynonyms("a", 2)
    >>> [s[0] for s in syms]
    [u'b', u'c']
    >>> vec = model.transform("a")
    >>> syms = model.findSynonyms(vec, 2)
    >>> [s[0] for s in syms]
    [u'b', u'c']
    """
    def __init__(self):
        """
        Construct Word2Vec instance
        """
        import random  # this can't be on the top because of mllib.random

        self.vectorSize = 100
        self.learningRate = 0.025
        self.numPartitions = 1
        self.numIterations = 1
        self.seed = random.randint(0, sys.maxint)

    def setVectorSize(self, vectorSize):
        """
        Sets vector size (default: 100).
        """
        self.vectorSize = vectorSize
        return self

    def setLearningRate(self, learningRate):
        """
        Sets initial learning rate (default: 0.025).
        """
        self.learningRate = learningRate
        return self

    def setNumPartitions(self, numPartitions):
        """
        Sets number of partitions (default: 1). Use a small number for accuracy.
        """
        self.numPartitions = numPartitions
        return self

    def setNumIterations(self, numIterations):
        """
        Sets number of iterations (default: 1), which should be smaller than or equal to number of
        partitions.
        """
        self.numIterations = numIterations
        return self

    def setSeed(self, seed):
        """
        Sets random seed.
        """
        self.seed = seed
        return self

    def fit(self, data):
        """
        Computes the vector representation of each word in vocabulary.

        :param data: training data. RDD of subtype of Iterable[String]
        :return: Word2VecModel instance
        """
        jmodel = callMLlibFunc("trainWord2Vec", data, int(self.vectorSize),
                               float(self.learningRate), int(self.numPartitions),
                               int(self.numIterations), long(self.seed))
        return Word2VecModel(jmodel)


def _test():
    import doctest
    from pyspark import SparkContext
    globs = globals().copy()
    globs['sc'] = SparkContext('local[4]', 'PythonTest', batchSize=2)
    (failure_count, test_count) = doctest.testmod(globs=globs, optionflags=doctest.ELLIPSIS)
    globs['sc'].stop()
    if failure_count:
        exit(-1)

if __name__ == "__main__":
    # remove current path from list of search paths to avoid importing mllib.random
    # for C{import random}, which is done in an external dependency of pyspark during doctests.
    import sys
    sys.path.pop(0)
    _test()
