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
Python package for Word2Vec in MLlib.
"""
from numpy import random

from sys import maxint

from pyspark.serializers import PickleSerializer, AutoBatchedSerializer

from pyspark.mllib.linalg import _convert_to_vector

__all__ = ['Word2Vec', 'Word2VecModel']


class Word2VecModel(object):
    """
    class for Word2Vec model
    """
    def __init__(self, sc, java_model):
        """
        :param sc:  Spark context
        :param java_model:  Handle to Java model object
        """
        self._sc = sc
        self._java_model = java_model

    def __del__(self):
        self._sc._gateway.detach(self._java_model)

    def transform(self, word):
        """
        local use only
        TODO: make transform usable in RDD operations from python side
        """
        result = self._java_model.transform(word)
        return PickleSerializer().loads(str(self._sc._jvm.SerDe.dumps(result)))

    def findSynonyms(self, x, num):
        """
        local use only
        TODO: make findSynonyms usable in RDD operations from python side
        """
        jlist = self._java_model.findSynonyms(x, num)
        words, similarity = PickleSerializer().loads(str(self._sc._jvm.SerDe.dumps(jlist)))
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
    >>> str(syms[0][0])
    'b'
    >>> str(syms[1][0])
    'c'
    >>> len(syms)
    2
    >>> vec = model.transform("a")
    >>> len(vec)
    10
    """
    def __init__(self):
        self.vectorSize = 100
        self.startingAlpha = 0.025
        self.numPartitions = 1
        self.numIterations = 1
        self.seed = random.randint(0, high=maxint)

    def setVectorSize(self, vectorSize):
        self.vectorSize = vectorSize
        return self

    def setLearningRate(self, learningRate):
        self.startingAlpha = learningRate
        return self

    def setNumPartitions(self, numPartitions):
        self.numPartitions = numPartitions
        return self

    def setNumIterations(self, numIterations):
        self.numIterations = numIterations
        return self

    def setSeed(self, seed):
        self.seed = seed
        return self

    def fit(self, data):
        sc = data.context
        ser = PickleSerializer()
        vectorSize = self.vectorSize
        startingAlpha = self.startingAlpha
        numPartitions = self.numPartitions
        numIterations = self.numIterations
        seed = self.seed

        # cached = data._reserialize(AutoBatchedSerializer(ser)).cache()
        model = sc._jvm.PythonMLLibAPI().trainWord2Vec(
            data._to_java_object_rdd(), vectorSize,
            startingAlpha, numPartitions, numIterations, seed)
        return Word2VecModel(sc, model)


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
    _test()
