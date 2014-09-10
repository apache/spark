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

from pyspark.mllib._common import \
    _get_unmangled_double_vector_rdd, _get_unmangled_rdd, \
    _serialize_double, _deserialize_double_matrix, _deserialize_double_vector, \
    _deserialize_string_seq, \
    _get_unmangled_string_seq_rdd

__all__ = ['Word2Vec', 'Word2VecModel']

class Word2VecModel(object):

    def __init__(self, sc, java_model):
        """
        :param sc:  Spark context
        :param java_model:  Handle to Java model object
        """
        self._sc = sc
        self._java_model = java_model

    def __del__(self):
        self._sc._gateway.detach(self._java_model)

    #def transform(self, word):

    #def findSynonyms(self, vector, num):
         
    def findSynonyms(self, word, num): 
        pythonAPI = self._sc._jvm.PythonMLLibAPI()
        result = pythonAPI.Word2VecSynonynms(self._java_model, word, num)
        similarity = _deserialize_double_vector(result[1])
        words = _deserialize_string_seq(result[0])
        ret = []
        for w,s in zip(words, similarity):
            ret.append((w,s))
        return ret

class Word2Vec(object):
    """
    data:RDD[Array[String]]
    """
    def __init__(self):
        self.vectorSize = 100
        self.startingAlpha = 0.025
        self.numPartitions = 1
        self.numIterations = 1

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

    def fit(self, data):
        sc = data.context
        dataBytes = _get_unmangled_string_seq_rdd(data)
        model = sc._jvm.PythonMLLibAPI().trainWord2Vec(dataBytes._jrdd)
        return Word2VecModel(sc, model)

