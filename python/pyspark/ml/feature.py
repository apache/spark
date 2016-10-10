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
if sys.version > '3':
    basestring = str

from pyspark import since, keyword_only
from pyspark.rdd import ignore_unicode_prefix
from pyspark.ml.linalg import _convert_to_vector
from pyspark.ml.param.shared import *
from pyspark.ml.util import JavaMLReadable, JavaMLWritable
from pyspark.ml.wrapper import JavaEstimator, JavaModel, JavaTransformer, _jvm
from pyspark.ml.common import inherit_doc

__all__ = ['Binarizer',
           'Bucketizer',
           'ChiSqSelector', 'ChiSqSelectorModel',
           'CountVectorizer', 'CountVectorizerModel',
           'DCT',
           'ElementwiseProduct',
           'HashingTF',
           'IDF', 'IDFModel',
           'IndexToString',
           'MaxAbsScaler', 'MaxAbsScalerModel',
           'MinMaxScaler', 'MinMaxScalerModel',
           'NGram',
           'Normalizer',
           'OneHotEncoder',
           'PCA', 'PCAModel',
           'PolynomialExpansion',
           'QuantileDiscretizer',
           'RegexTokenizer',
           'RFormula', 'RFormulaModel',
           'SQLTransformer',
           'StandardScaler', 'StandardScalerModel',
           'StopWordsRemover',
           'StringIndexer', 'StringIndexerModel',
           'Tokenizer',
           'VectorAssembler',
           'VectorIndexer', 'VectorIndexerModel',
           'VectorSlicer',
           'Word2Vec', 'Word2VecModel']


@inherit_doc
class Binarizer(JavaTransformer, HasInputCol, HasOutputCol, JavaMLReadable, JavaMLWritable):
    """
    Binarize a column of continuous features given a threshold.

    >>> df = spark.createDataFrame([(0.5,)], ["values"])
    >>> binarizer = Binarizer(threshold=1.0, inputCol="values", outputCol="features")
    >>> binarizer.transform(df).head().features
    0.0
    >>> binarizer.setParams(outputCol="freqs").transform(df).head().freqs
    0.0
    >>> params = {binarizer.threshold: -0.5, binarizer.outputCol: "vector"}
    >>> binarizer.transform(df, params).head().vector
    1.0
    >>> binarizerPath = temp_path + "/binarizer"
    >>> binarizer.save(binarizerPath)
    >>> loadedBinarizer = Binarizer.load(binarizerPath)
    >>> loadedBinarizer.getThreshold() == binarizer.getThreshold()
    True

    .. versionadded:: 1.4.0
    """

    threshold = Param(Params._dummy(), "threshold",
                      "threshold in binary classification prediction, in range [0, 1]",
                      typeConverter=TypeConverters.toFloat)

    @keyword_only
    def __init__(self, threshold=0.0, inputCol=None, outputCol=None):
        """
        __init__(self, threshold=0.0, inputCol=None, outputCol=None)
        """
        super(Binarizer, self).__init__()
        self._java_obj = self._new_java_obj("org.apache.spark.ml.feature.Binarizer", self.uid)
        self._setDefault(threshold=0.0)
        kwargs = self.__init__._input_kwargs
        self.setParams(**kwargs)

    @keyword_only
    @since("1.4.0")
    def setParams(self, threshold=0.0, inputCol=None, outputCol=None):
        """
        setParams(self, threshold=0.0, inputCol=None, outputCol=None)
        Sets params for this Binarizer.
        """
        kwargs = self.setParams._input_kwargs
        return self._set(**kwargs)

    @since("1.4.0")
    def setThreshold(self, value):
        """
        Sets the value of :py:attr:`threshold`.
        """
        return self._set(threshold=value)

    @since("1.4.0")
    def getThreshold(self):
        """
        Gets the value of threshold or its default value.
        """
        return self.getOrDefault(self.threshold)


@inherit_doc
class Bucketizer(JavaTransformer, HasInputCol, HasOutputCol, JavaMLReadable, JavaMLWritable):
    """
    Maps a column of continuous features to a column of feature buckets.

    >>> df = spark.createDataFrame([(0.1,), (0.4,), (1.2,), (1.5,)], ["values"])
    >>> bucketizer = Bucketizer(splits=[-float("inf"), 0.5, 1.4, float("inf")],
    ...     inputCol="values", outputCol="buckets")
    >>> bucketed = bucketizer.transform(df).collect()
    >>> bucketed[0].buckets
    0.0
    >>> bucketed[1].buckets
    0.0
    >>> bucketed[2].buckets
    1.0
    >>> bucketed[3].buckets
    2.0
    >>> bucketizer.setParams(outputCol="b").transform(df).head().b
    0.0
    >>> bucketizerPath = temp_path + "/bucketizer"
    >>> bucketizer.save(bucketizerPath)
    >>> loadedBucketizer = Bucketizer.load(bucketizerPath)
    >>> loadedBucketizer.getSplits() == bucketizer.getSplits()
    True

    .. versionadded:: 1.4.0
    """

    splits = \
        Param(Params._dummy(), "splits",
              "Split points for mapping continuous features into buckets. With n+1 splits, " +
              "there are n buckets. A bucket defined by splits x,y holds values in the " +
              "range [x,y) except the last bucket, which also includes y. The splits " +
              "should be of length >= 3 and strictly increasing. Values at -inf, inf must be " +
              "explicitly provided to cover all Double values; otherwise, values outside the " +
              "splits specified will be treated as errors.",
              typeConverter=TypeConverters.toListFloat)

    @keyword_only
    def __init__(self, splits=None, inputCol=None, outputCol=None):
        """
        __init__(self, splits=None, inputCol=None, outputCol=None)
        """
        super(Bucketizer, self).__init__()
        self._java_obj = self._new_java_obj("org.apache.spark.ml.feature.Bucketizer", self.uid)
        kwargs = self.__init__._input_kwargs
        self.setParams(**kwargs)

    @keyword_only
    @since("1.4.0")
    def setParams(self, splits=None, inputCol=None, outputCol=None):
        """
        setParams(self, splits=None, inputCol=None, outputCol=None)
        Sets params for this Bucketizer.
        """
        kwargs = self.setParams._input_kwargs
        return self._set(**kwargs)

    @since("1.4.0")
    def setSplits(self, value):
        """
        Sets the value of :py:attr:`splits`.
        """
        return self._set(splits=value)

    @since("1.4.0")
    def getSplits(self):
        """
        Gets the value of threshold or its default value.
        """
        return self.getOrDefault(self.splits)


@inherit_doc
class CountVectorizer(JavaEstimator, HasInputCol, HasOutputCol, JavaMLReadable, JavaMLWritable):
    """
    Extracts a vocabulary from document collections and generates a :py:attr:`CountVectorizerModel`.

    >>> df = spark.createDataFrame(
    ...    [(0, ["a", "b", "c"]), (1, ["a", "b", "b", "c", "a"])],
    ...    ["label", "raw"])
    >>> cv = CountVectorizer(inputCol="raw", outputCol="vectors")
    >>> model = cv.fit(df)
    >>> model.transform(df).show(truncate=False)
    +-----+---------------+-------------------------+
    |label|raw            |vectors                  |
    +-----+---------------+-------------------------+
    |0    |[a, b, c]      |(3,[0,1,2],[1.0,1.0,1.0])|
    |1    |[a, b, b, c, a]|(3,[0,1,2],[2.0,2.0,1.0])|
    +-----+---------------+-------------------------+
    ...
    >>> sorted(model.vocabulary) == ['a', 'b', 'c']
    True
    >>> countVectorizerPath = temp_path + "/count-vectorizer"
    >>> cv.save(countVectorizerPath)
    >>> loadedCv = CountVectorizer.load(countVectorizerPath)
    >>> loadedCv.getMinDF() == cv.getMinDF()
    True
    >>> loadedCv.getMinTF() == cv.getMinTF()
    True
    >>> loadedCv.getVocabSize() == cv.getVocabSize()
    True
    >>> modelPath = temp_path + "/count-vectorizer-model"
    >>> model.save(modelPath)
    >>> loadedModel = CountVectorizerModel.load(modelPath)
    >>> loadedModel.vocabulary == model.vocabulary
    True

    .. versionadded:: 1.6.0
    """

    minTF = Param(
        Params._dummy(), "minTF", "Filter to ignore rare words in" +
        " a document. For each document, terms with frequency/count less than the given" +
        " threshold are ignored. If this is an integer >= 1, then this specifies a count (of" +
        " times the term must appear in the document); if this is a double in [0,1), then this " +
        "specifies a fraction (out of the document's token count). Note that the parameter is " +
        "only used in transform of CountVectorizerModel and does not affect fitting. Default 1.0",
        typeConverter=TypeConverters.toFloat)
    minDF = Param(
        Params._dummy(), "minDF", "Specifies the minimum number of" +
        " different documents a term must appear in to be included in the vocabulary." +
        " If this is an integer >= 1, this specifies the number of documents the term must" +
        " appear in; if this is a double in [0,1), then this specifies the fraction of documents." +
        " Default 1.0", typeConverter=TypeConverters.toFloat)
    vocabSize = Param(
        Params._dummy(), "vocabSize", "max size of the vocabulary. Default 1 << 18.",
        typeConverter=TypeConverters.toInt)
    binary = Param(
        Params._dummy(), "binary", "Binary toggle to control the output vector values." +
        " If True, all nonzero counts (after minTF filter applied) are set to 1. This is useful" +
        " for discrete probabilistic models that model binary events rather than integer counts." +
        " Default False", typeConverter=TypeConverters.toBoolean)

    @keyword_only
    def __init__(self, minTF=1.0, minDF=1.0, vocabSize=1 << 18, binary=False, inputCol=None,
                 outputCol=None):
        """
        __init__(self, minTF=1.0, minDF=1.0, vocabSize=1 << 18, binary=False, inputCol=None,\
                 outputCol=None)
        """
        super(CountVectorizer, self).__init__()
        self._java_obj = self._new_java_obj("org.apache.spark.ml.feature.CountVectorizer",
                                            self.uid)
        self._setDefault(minTF=1.0, minDF=1.0, vocabSize=1 << 18, binary=False)
        kwargs = self.__init__._input_kwargs
        self.setParams(**kwargs)

    @keyword_only
    @since("1.6.0")
    def setParams(self, minTF=1.0, minDF=1.0, vocabSize=1 << 18, binary=False, inputCol=None,
                  outputCol=None):
        """
        setParams(self, minTF=1.0, minDF=1.0, vocabSize=1 << 18, binary=False, inputCol=None,\
                  outputCol=None)
        Set the params for the CountVectorizer
        """
        kwargs = self.setParams._input_kwargs
        return self._set(**kwargs)

    @since("1.6.0")
    def setMinTF(self, value):
        """
        Sets the value of :py:attr:`minTF`.
        """
        return self._set(minTF=value)

    @since("1.6.0")
    def getMinTF(self):
        """
        Gets the value of minTF or its default value.
        """
        return self.getOrDefault(self.minTF)

    @since("1.6.0")
    def setMinDF(self, value):
        """
        Sets the value of :py:attr:`minDF`.
        """
        return self._set(minDF=value)

    @since("1.6.0")
    def getMinDF(self):
        """
        Gets the value of minDF or its default value.
        """
        return self.getOrDefault(self.minDF)

    @since("1.6.0")
    def setVocabSize(self, value):
        """
        Sets the value of :py:attr:`vocabSize`.
        """
        return self._set(vocabSize=value)

    @since("1.6.0")
    def getVocabSize(self):
        """
        Gets the value of vocabSize or its default value.
        """
        return self.getOrDefault(self.vocabSize)

    @since("2.0.0")
    def setBinary(self, value):
        """
        Sets the value of :py:attr:`binary`.
        """
        return self._set(binary=value)

    @since("2.0.0")
    def getBinary(self):
        """
        Gets the value of binary or its default value.
        """
        return self.getOrDefault(self.binary)

    def _create_model(self, java_model):
        return CountVectorizerModel(java_model)


class CountVectorizerModel(JavaModel, JavaMLReadable, JavaMLWritable):
    """
    Model fitted by :py:class:`CountVectorizer`.

    .. versionadded:: 1.6.0
    """

    @property
    @since("1.6.0")
    def vocabulary(self):
        """
        An array of terms in the vocabulary.
        """
        return self._call_java("vocabulary")


@inherit_doc
class DCT(JavaTransformer, HasInputCol, HasOutputCol, JavaMLReadable, JavaMLWritable):
    """
    A feature transformer that takes the 1D discrete cosine transform
    of a real vector. No zero padding is performed on the input vector.
    It returns a real vector of the same length representing the DCT.
    The return vector is scaled such that the transform matrix is
    unitary (aka scaled DCT-II).

    .. seealso:: `More information on Wikipedia \
    <https://en.wikipedia.org/wiki/Discrete_cosine_transform#DCT-II Wikipedia>`_.

    >>> from pyspark.ml.linalg import Vectors
    >>> df1 = spark.createDataFrame([(Vectors.dense([5.0, 8.0, 6.0]),)], ["vec"])
    >>> dct = DCT(inverse=False, inputCol="vec", outputCol="resultVec")
    >>> df2 = dct.transform(df1)
    >>> df2.head().resultVec
    DenseVector([10.969..., -0.707..., -2.041...])
    >>> df3 = DCT(inverse=True, inputCol="resultVec", outputCol="origVec").transform(df2)
    >>> df3.head().origVec
    DenseVector([5.0, 8.0, 6.0])
    >>> dctPath = temp_path + "/dct"
    >>> dct.save(dctPath)
    >>> loadedDtc = DCT.load(dctPath)
    >>> loadedDtc.getInverse()
    False

    .. versionadded:: 1.6.0
    """

    inverse = Param(Params._dummy(), "inverse", "Set transformer to perform inverse DCT, " +
                    "default False.", typeConverter=TypeConverters.toBoolean)

    @keyword_only
    def __init__(self, inverse=False, inputCol=None, outputCol=None):
        """
        __init__(self, inverse=False, inputCol=None, outputCol=None)
        """
        super(DCT, self).__init__()
        self._java_obj = self._new_java_obj("org.apache.spark.ml.feature.DCT", self.uid)
        self._setDefault(inverse=False)
        kwargs = self.__init__._input_kwargs
        self.setParams(**kwargs)

    @keyword_only
    @since("1.6.0")
    def setParams(self, inverse=False, inputCol=None, outputCol=None):
        """
        setParams(self, inverse=False, inputCol=None, outputCol=None)
        Sets params for this DCT.
        """
        kwargs = self.setParams._input_kwargs
        return self._set(**kwargs)

    @since("1.6.0")
    def setInverse(self, value):
        """
        Sets the value of :py:attr:`inverse`.
        """
        return self._set(inverse=value)

    @since("1.6.0")
    def getInverse(self):
        """
        Gets the value of inverse or its default value.
        """
        return self.getOrDefault(self.inverse)


@inherit_doc
class ElementwiseProduct(JavaTransformer, HasInputCol, HasOutputCol, JavaMLReadable,
                         JavaMLWritable):
    """
    Outputs the Hadamard product (i.e., the element-wise product) of each input vector
    with a provided "weight" vector. In other words, it scales each column of the dataset
    by a scalar multiplier.

    >>> from pyspark.ml.linalg import Vectors
    >>> df = spark.createDataFrame([(Vectors.dense([2.0, 1.0, 3.0]),)], ["values"])
    >>> ep = ElementwiseProduct(scalingVec=Vectors.dense([1.0, 2.0, 3.0]),
    ...     inputCol="values", outputCol="eprod")
    >>> ep.transform(df).head().eprod
    DenseVector([2.0, 2.0, 9.0])
    >>> ep.setParams(scalingVec=Vectors.dense([2.0, 3.0, 5.0])).transform(df).head().eprod
    DenseVector([4.0, 3.0, 15.0])
    >>> elementwiseProductPath = temp_path + "/elementwise-product"
    >>> ep.save(elementwiseProductPath)
    >>> loadedEp = ElementwiseProduct.load(elementwiseProductPath)
    >>> loadedEp.getScalingVec() == ep.getScalingVec()
    True

    .. versionadded:: 1.5.0
    """

    scalingVec = Param(Params._dummy(), "scalingVec", "Vector for hadamard product.",
                       typeConverter=TypeConverters.toVector)

    @keyword_only
    def __init__(self, scalingVec=None, inputCol=None, outputCol=None):
        """
        __init__(self, scalingVec=None, inputCol=None, outputCol=None)
        """
        super(ElementwiseProduct, self).__init__()
        self._java_obj = self._new_java_obj("org.apache.spark.ml.feature.ElementwiseProduct",
                                            self.uid)
        kwargs = self.__init__._input_kwargs
        self.setParams(**kwargs)

    @keyword_only
    @since("1.5.0")
    def setParams(self, scalingVec=None, inputCol=None, outputCol=None):
        """
        setParams(self, scalingVec=None, inputCol=None, outputCol=None)
        Sets params for this ElementwiseProduct.
        """
        kwargs = self.setParams._input_kwargs
        return self._set(**kwargs)

    @since("2.0.0")
    def setScalingVec(self, value):
        """
        Sets the value of :py:attr:`scalingVec`.
        """
        return self._set(scalingVec=value)

    @since("2.0.0")
    def getScalingVec(self):
        """
        Gets the value of scalingVec or its default value.
        """
        return self.getOrDefault(self.scalingVec)


@inherit_doc
class HashingTF(JavaTransformer, HasInputCol, HasOutputCol, HasNumFeatures, JavaMLReadable,
                JavaMLWritable):
    """
    Maps a sequence of terms to their term frequencies using the hashing trick.
    Currently we use Austin Appleby's MurmurHash 3 algorithm (MurmurHash3_x86_32)
    to calculate the hash code value for the term object.
    Since a simple modulo is used to transform the hash function to a column index,
    it is advisable to use a power of two as the numFeatures parameter;
    otherwise the features will not be mapped evenly to the columns.

    >>> df = spark.createDataFrame([(["a", "b", "c"],)], ["words"])
    >>> hashingTF = HashingTF(numFeatures=10, inputCol="words", outputCol="features")
    >>> hashingTF.transform(df).head().features
    SparseVector(10, {0: 1.0, 1: 1.0, 2: 1.0})
    >>> hashingTF.setParams(outputCol="freqs").transform(df).head().freqs
    SparseVector(10, {0: 1.0, 1: 1.0, 2: 1.0})
    >>> params = {hashingTF.numFeatures: 5, hashingTF.outputCol: "vector"}
    >>> hashingTF.transform(df, params).head().vector
    SparseVector(5, {0: 1.0, 1: 1.0, 2: 1.0})
    >>> hashingTFPath = temp_path + "/hashing-tf"
    >>> hashingTF.save(hashingTFPath)
    >>> loadedHashingTF = HashingTF.load(hashingTFPath)
    >>> loadedHashingTF.getNumFeatures() == hashingTF.getNumFeatures()
    True

    .. versionadded:: 1.3.0
    """

    binary = Param(Params._dummy(), "binary", "If True, all non zero counts are set to 1. " +
                   "This is useful for discrete probabilistic models that model binary events " +
                   "rather than integer counts. Default False.",
                   typeConverter=TypeConverters.toBoolean)

    @keyword_only
    def __init__(self, numFeatures=1 << 18, binary=False, inputCol=None, outputCol=None):
        """
        __init__(self, numFeatures=1 << 18, binary=False, inputCol=None, outputCol=None)
        """
        super(HashingTF, self).__init__()
        self._java_obj = self._new_java_obj("org.apache.spark.ml.feature.HashingTF", self.uid)
        self._setDefault(numFeatures=1 << 18, binary=False)
        kwargs = self.__init__._input_kwargs
        self.setParams(**kwargs)

    @keyword_only
    @since("1.3.0")
    def setParams(self, numFeatures=1 << 18, binary=False, inputCol=None, outputCol=None):
        """
        setParams(self, numFeatures=1 << 18, binary=False, inputCol=None, outputCol=None)
        Sets params for this HashingTF.
        """
        kwargs = self.setParams._input_kwargs
        return self._set(**kwargs)

    @since("2.0.0")
    def setBinary(self, value):
        """
        Sets the value of :py:attr:`binary`.
        """
        return self._set(binary=value)

    @since("2.0.0")
    def getBinary(self):
        """
        Gets the value of binary or its default value.
        """
        return self.getOrDefault(self.binary)


@inherit_doc
class IDF(JavaEstimator, HasInputCol, HasOutputCol, JavaMLReadable, JavaMLWritable):
    """
    Compute the Inverse Document Frequency (IDF) given a collection of documents.

    >>> from pyspark.ml.linalg import DenseVector
    >>> df = spark.createDataFrame([(DenseVector([1.0, 2.0]),),
    ...     (DenseVector([0.0, 1.0]),), (DenseVector([3.0, 0.2]),)], ["tf"])
    >>> idf = IDF(minDocFreq=3, inputCol="tf", outputCol="idf")
    >>> model = idf.fit(df)
    >>> model.idf
    DenseVector([0.0, 0.0])
    >>> model.transform(df).head().idf
    DenseVector([0.0, 0.0])
    >>> idf.setParams(outputCol="freqs").fit(df).transform(df).collect()[1].freqs
    DenseVector([0.0, 0.0])
    >>> params = {idf.minDocFreq: 1, idf.outputCol: "vector"}
    >>> idf.fit(df, params).transform(df).head().vector
    DenseVector([0.2877, 0.0])
    >>> idfPath = temp_path + "/idf"
    >>> idf.save(idfPath)
    >>> loadedIdf = IDF.load(idfPath)
    >>> loadedIdf.getMinDocFreq() == idf.getMinDocFreq()
    True
    >>> modelPath = temp_path + "/idf-model"
    >>> model.save(modelPath)
    >>> loadedModel = IDFModel.load(modelPath)
    >>> loadedModel.transform(df).head().idf == model.transform(df).head().idf
    True

    .. versionadded:: 1.4.0
    """

    minDocFreq = Param(Params._dummy(), "minDocFreq",
                       "minimum number of documents in which a term should appear for filtering",
                       typeConverter=TypeConverters.toInt)

    @keyword_only
    def __init__(self, minDocFreq=0, inputCol=None, outputCol=None):
        """
        __init__(self, minDocFreq=0, inputCol=None, outputCol=None)
        """
        super(IDF, self).__init__()
        self._java_obj = self._new_java_obj("org.apache.spark.ml.feature.IDF", self.uid)
        self._setDefault(minDocFreq=0)
        kwargs = self.__init__._input_kwargs
        self.setParams(**kwargs)

    @keyword_only
    @since("1.4.0")
    def setParams(self, minDocFreq=0, inputCol=None, outputCol=None):
        """
        setParams(self, minDocFreq=0, inputCol=None, outputCol=None)
        Sets params for this IDF.
        """
        kwargs = self.setParams._input_kwargs
        return self._set(**kwargs)

    @since("1.4.0")
    def setMinDocFreq(self, value):
        """
        Sets the value of :py:attr:`minDocFreq`.
        """
        return self._set(minDocFreq=value)

    @since("1.4.0")
    def getMinDocFreq(self):
        """
        Gets the value of minDocFreq or its default value.
        """
        return self.getOrDefault(self.minDocFreq)

    def _create_model(self, java_model):
        return IDFModel(java_model)


class IDFModel(JavaModel, JavaMLReadable, JavaMLWritable):
    """
    Model fitted by :py:class:`IDF`.

    .. versionadded:: 1.4.0
    """

    @property
    @since("2.0.0")
    def idf(self):
        """
        Returns the IDF vector.
        """
        return self._call_java("idf")


@inherit_doc
class MaxAbsScaler(JavaEstimator, HasInputCol, HasOutputCol, JavaMLReadable, JavaMLWritable):
    """
    .. note:: Experimental

    Rescale each feature individually to range [-1, 1] by dividing through the largest maximum
    absolute value in each feature. It does not shift/center the data, and thus does not destroy
    any sparsity.

    >>> from pyspark.ml.linalg import Vectors
    >>> df = spark.createDataFrame([(Vectors.dense([1.0]),), (Vectors.dense([2.0]),)], ["a"])
    >>> maScaler = MaxAbsScaler(inputCol="a", outputCol="scaled")
    >>> model = maScaler.fit(df)
    >>> model.transform(df).show()
    +-----+------+
    |    a|scaled|
    +-----+------+
    |[1.0]| [0.5]|
    |[2.0]| [1.0]|
    +-----+------+
    ...
    >>> scalerPath = temp_path + "/max-abs-scaler"
    >>> maScaler.save(scalerPath)
    >>> loadedMAScaler = MaxAbsScaler.load(scalerPath)
    >>> loadedMAScaler.getInputCol() == maScaler.getInputCol()
    True
    >>> loadedMAScaler.getOutputCol() == maScaler.getOutputCol()
    True
    >>> modelPath = temp_path + "/max-abs-scaler-model"
    >>> model.save(modelPath)
    >>> loadedModel = MaxAbsScalerModel.load(modelPath)
    >>> loadedModel.maxAbs == model.maxAbs
    True

    .. versionadded:: 2.0.0
    """

    @keyword_only
    def __init__(self, inputCol=None, outputCol=None):
        """
        __init__(self, inputCol=None, outputCol=None)
        """
        super(MaxAbsScaler, self).__init__()
        self._java_obj = self._new_java_obj("org.apache.spark.ml.feature.MaxAbsScaler", self.uid)
        self._setDefault()
        kwargs = self.__init__._input_kwargs
        self.setParams(**kwargs)

    @keyword_only
    @since("2.0.0")
    def setParams(self, inputCol=None, outputCol=None):
        """
        setParams(self, inputCol=None, outputCol=None)
        Sets params for this MaxAbsScaler.
        """
        kwargs = self.setParams._input_kwargs
        return self._set(**kwargs)

    def _create_model(self, java_model):
        return MaxAbsScalerModel(java_model)


class MaxAbsScalerModel(JavaModel, JavaMLReadable, JavaMLWritable):
    """
    .. note:: Experimental

    Model fitted by :py:class:`MaxAbsScaler`.

    .. versionadded:: 2.0.0
    """

    @property
    @since("2.0.0")
    def maxAbs(self):
        """
        Max Abs vector.
        """
        return self._call_java("maxAbs")


@inherit_doc
class MinMaxScaler(JavaEstimator, HasInputCol, HasOutputCol, JavaMLReadable, JavaMLWritable):
    """
    Rescale each feature individually to a common range [min, max] linearly using column summary
    statistics, which is also known as min-max normalization or Rescaling. The rescaled value for
    feature E is calculated as,

    Rescaled(e_i) = (e_i - E_min) / (E_max - E_min) * (max - min) + min

    For the case E_max == E_min, Rescaled(e_i) = 0.5 * (max + min)

    Note that since zero values will probably be transformed to non-zero values, output of the
    transformer will be DenseVector even for sparse input.

    >>> from pyspark.ml.linalg import Vectors
    >>> df = spark.createDataFrame([(Vectors.dense([0.0]),), (Vectors.dense([2.0]),)], ["a"])
    >>> mmScaler = MinMaxScaler(inputCol="a", outputCol="scaled")
    >>> model = mmScaler.fit(df)
    >>> model.originalMin
    DenseVector([0.0])
    >>> model.originalMax
    DenseVector([2.0])
    >>> model.transform(df).show()
    +-----+------+
    |    a|scaled|
    +-----+------+
    |[0.0]| [0.0]|
    |[2.0]| [1.0]|
    +-----+------+
    ...
    >>> minMaxScalerPath = temp_path + "/min-max-scaler"
    >>> mmScaler.save(minMaxScalerPath)
    >>> loadedMMScaler = MinMaxScaler.load(minMaxScalerPath)
    >>> loadedMMScaler.getMin() == mmScaler.getMin()
    True
    >>> loadedMMScaler.getMax() == mmScaler.getMax()
    True
    >>> modelPath = temp_path + "/min-max-scaler-model"
    >>> model.save(modelPath)
    >>> loadedModel = MinMaxScalerModel.load(modelPath)
    >>> loadedModel.originalMin == model.originalMin
    True
    >>> loadedModel.originalMax == model.originalMax
    True

    .. versionadded:: 1.6.0
    """

    min = Param(Params._dummy(), "min", "Lower bound of the output feature range",
                typeConverter=TypeConverters.toFloat)
    max = Param(Params._dummy(), "max", "Upper bound of the output feature range",
                typeConverter=TypeConverters.toFloat)

    @keyword_only
    def __init__(self, min=0.0, max=1.0, inputCol=None, outputCol=None):
        """
        __init__(self, min=0.0, max=1.0, inputCol=None, outputCol=None)
        """
        super(MinMaxScaler, self).__init__()
        self._java_obj = self._new_java_obj("org.apache.spark.ml.feature.MinMaxScaler", self.uid)
        self._setDefault(min=0.0, max=1.0)
        kwargs = self.__init__._input_kwargs
        self.setParams(**kwargs)

    @keyword_only
    @since("1.6.0")
    def setParams(self, min=0.0, max=1.0, inputCol=None, outputCol=None):
        """
        setParams(self, min=0.0, max=1.0, inputCol=None, outputCol=None)
        Sets params for this MinMaxScaler.
        """
        kwargs = self.setParams._input_kwargs
        return self._set(**kwargs)

    @since("1.6.0")
    def setMin(self, value):
        """
        Sets the value of :py:attr:`min`.
        """
        return self._set(min=value)

    @since("1.6.0")
    def getMin(self):
        """
        Gets the value of min or its default value.
        """
        return self.getOrDefault(self.min)

    @since("1.6.0")
    def setMax(self, value):
        """
        Sets the value of :py:attr:`max`.
        """
        return self._set(max=value)

    @since("1.6.0")
    def getMax(self):
        """
        Gets the value of max or its default value.
        """
        return self.getOrDefault(self.max)

    def _create_model(self, java_model):
        return MinMaxScalerModel(java_model)


class MinMaxScalerModel(JavaModel, JavaMLReadable, JavaMLWritable):
    """
    Model fitted by :py:class:`MinMaxScaler`.

    .. versionadded:: 1.6.0
    """

    @property
    @since("2.0.0")
    def originalMin(self):
        """
        Min value for each original column during fitting.
        """
        return self._call_java("originalMin")

    @property
    @since("2.0.0")
    def originalMax(self):
        """
        Max value for each original column during fitting.
        """
        return self._call_java("originalMax")


@inherit_doc
@ignore_unicode_prefix
class NGram(JavaTransformer, HasInputCol, HasOutputCol, JavaMLReadable, JavaMLWritable):
    """
    A feature transformer that converts the input array of strings into an array of n-grams. Null
    values in the input array are ignored.
    It returns an array of n-grams where each n-gram is represented by a space-separated string of
    words.
    When the input is empty, an empty array is returned.
    When the input array length is less than n (number of elements per n-gram), no n-grams are
    returned.

    >>> df = spark.createDataFrame([Row(inputTokens=["a", "b", "c", "d", "e"])])
    >>> ngram = NGram(n=2, inputCol="inputTokens", outputCol="nGrams")
    >>> ngram.transform(df).head()
    Row(inputTokens=[u'a', u'b', u'c', u'd', u'e'], nGrams=[u'a b', u'b c', u'c d', u'd e'])
    >>> # Change n-gram length
    >>> ngram.setParams(n=4).transform(df).head()
    Row(inputTokens=[u'a', u'b', u'c', u'd', u'e'], nGrams=[u'a b c d', u'b c d e'])
    >>> # Temporarily modify output column.
    >>> ngram.transform(df, {ngram.outputCol: "output"}).head()
    Row(inputTokens=[u'a', u'b', u'c', u'd', u'e'], output=[u'a b c d', u'b c d e'])
    >>> ngram.transform(df).head()
    Row(inputTokens=[u'a', u'b', u'c', u'd', u'e'], nGrams=[u'a b c d', u'b c d e'])
    >>> # Must use keyword arguments to specify params.
    >>> ngram.setParams("text")
    Traceback (most recent call last):
        ...
    TypeError: Method setParams forces keyword arguments.
    >>> ngramPath = temp_path + "/ngram"
    >>> ngram.save(ngramPath)
    >>> loadedNGram = NGram.load(ngramPath)
    >>> loadedNGram.getN() == ngram.getN()
    True

    .. versionadded:: 1.5.0
    """

    n = Param(Params._dummy(), "n", "number of elements per n-gram (>=1)",
              typeConverter=TypeConverters.toInt)

    @keyword_only
    def __init__(self, n=2, inputCol=None, outputCol=None):
        """
        __init__(self, n=2, inputCol=None, outputCol=None)
        """
        super(NGram, self).__init__()
        self._java_obj = self._new_java_obj("org.apache.spark.ml.feature.NGram", self.uid)
        self._setDefault(n=2)
        kwargs = self.__init__._input_kwargs
        self.setParams(**kwargs)

    @keyword_only
    @since("1.5.0")
    def setParams(self, n=2, inputCol=None, outputCol=None):
        """
        setParams(self, n=2, inputCol=None, outputCol=None)
        Sets params for this NGram.
        """
        kwargs = self.setParams._input_kwargs
        return self._set(**kwargs)

    @since("1.5.0")
    def setN(self, value):
        """
        Sets the value of :py:attr:`n`.
        """
        return self._set(n=value)

    @since("1.5.0")
    def getN(self):
        """
        Gets the value of n or its default value.
        """
        return self.getOrDefault(self.n)


@inherit_doc
class Normalizer(JavaTransformer, HasInputCol, HasOutputCol, JavaMLReadable, JavaMLWritable):
    """
     Normalize a vector to have unit norm using the given p-norm.

    >>> from pyspark.ml.linalg import Vectors
    >>> svec = Vectors.sparse(4, {1: 4.0, 3: 3.0})
    >>> df = spark.createDataFrame([(Vectors.dense([3.0, -4.0]), svec)], ["dense", "sparse"])
    >>> normalizer = Normalizer(p=2.0, inputCol="dense", outputCol="features")
    >>> normalizer.transform(df).head().features
    DenseVector([0.6, -0.8])
    >>> normalizer.setParams(inputCol="sparse", outputCol="freqs").transform(df).head().freqs
    SparseVector(4, {1: 0.8, 3: 0.6})
    >>> params = {normalizer.p: 1.0, normalizer.inputCol: "dense", normalizer.outputCol: "vector"}
    >>> normalizer.transform(df, params).head().vector
    DenseVector([0.4286, -0.5714])
    >>> normalizerPath = temp_path + "/normalizer"
    >>> normalizer.save(normalizerPath)
    >>> loadedNormalizer = Normalizer.load(normalizerPath)
    >>> loadedNormalizer.getP() == normalizer.getP()
    True

    .. versionadded:: 1.4.0
    """

    p = Param(Params._dummy(), "p", "the p norm value.",
              typeConverter=TypeConverters.toFloat)

    @keyword_only
    def __init__(self, p=2.0, inputCol=None, outputCol=None):
        """
        __init__(self, p=2.0, inputCol=None, outputCol=None)
        """
        super(Normalizer, self).__init__()
        self._java_obj = self._new_java_obj("org.apache.spark.ml.feature.Normalizer", self.uid)
        self._setDefault(p=2.0)
        kwargs = self.__init__._input_kwargs
        self.setParams(**kwargs)

    @keyword_only
    @since("1.4.0")
    def setParams(self, p=2.0, inputCol=None, outputCol=None):
        """
        setParams(self, p=2.0, inputCol=None, outputCol=None)
        Sets params for this Normalizer.
        """
        kwargs = self.setParams._input_kwargs
        return self._set(**kwargs)

    @since("1.4.0")
    def setP(self, value):
        """
        Sets the value of :py:attr:`p`.
        """
        return self._set(p=value)

    @since("1.4.0")
    def getP(self):
        """
        Gets the value of p or its default value.
        """
        return self.getOrDefault(self.p)


@inherit_doc
class OneHotEncoder(JavaTransformer, HasInputCol, HasOutputCol, JavaMLReadable, JavaMLWritable):
    """
    A one-hot encoder that maps a column of category indices to a
    column of binary vectors, with at most a single one-value per row
    that indicates the input category index.
    For example with 5 categories, an input value of 2.0 would map to
    an output vector of `[0.0, 0.0, 1.0, 0.0]`.
    The last category is not included by default (configurable via
    :py:attr:`dropLast`) because it makes the vector entries sum up to
    one, and hence linearly dependent.
    So an input value of 4.0 maps to `[0.0, 0.0, 0.0, 0.0]`.
    Note that this is different from scikit-learn's OneHotEncoder,
    which keeps all categories.
    The output vectors are sparse.

    .. seealso::

       :py:class:`StringIndexer` for converting categorical values into
       category indices

    >>> stringIndexer = StringIndexer(inputCol="label", outputCol="indexed")
    >>> model = stringIndexer.fit(stringIndDf)
    >>> td = model.transform(stringIndDf)
    >>> encoder = OneHotEncoder(inputCol="indexed", outputCol="features")
    >>> encoder.transform(td).head().features
    SparseVector(2, {0: 1.0})
    >>> encoder.setParams(outputCol="freqs").transform(td).head().freqs
    SparseVector(2, {0: 1.0})
    >>> params = {encoder.dropLast: False, encoder.outputCol: "test"}
    >>> encoder.transform(td, params).head().test
    SparseVector(3, {0: 1.0})
    >>> onehotEncoderPath = temp_path + "/onehot-encoder"
    >>> encoder.save(onehotEncoderPath)
    >>> loadedEncoder = OneHotEncoder.load(onehotEncoderPath)
    >>> loadedEncoder.getDropLast() == encoder.getDropLast()
    True

    .. versionadded:: 1.4.0
    """

    dropLast = Param(Params._dummy(), "dropLast", "whether to drop the last category",
                     typeConverter=TypeConverters.toBoolean)

    @keyword_only
    def __init__(self, dropLast=True, inputCol=None, outputCol=None):
        """
        __init__(self, includeFirst=True, inputCol=None, outputCol=None)
        """
        super(OneHotEncoder, self).__init__()
        self._java_obj = self._new_java_obj("org.apache.spark.ml.feature.OneHotEncoder", self.uid)
        self._setDefault(dropLast=True)
        kwargs = self.__init__._input_kwargs
        self.setParams(**kwargs)

    @keyword_only
    @since("1.4.0")
    def setParams(self, dropLast=True, inputCol=None, outputCol=None):
        """
        setParams(self, dropLast=True, inputCol=None, outputCol=None)
        Sets params for this OneHotEncoder.
        """
        kwargs = self.setParams._input_kwargs
        return self._set(**kwargs)

    @since("1.4.0")
    def setDropLast(self, value):
        """
        Sets the value of :py:attr:`dropLast`.
        """
        return self._set(dropLast=value)

    @since("1.4.0")
    def getDropLast(self):
        """
        Gets the value of dropLast or its default value.
        """
        return self.getOrDefault(self.dropLast)


@inherit_doc
class PolynomialExpansion(JavaTransformer, HasInputCol, HasOutputCol, JavaMLReadable,
                          JavaMLWritable):
    """
    Perform feature expansion in a polynomial space. As said in `wikipedia of Polynomial Expansion
    <http://en.wikipedia.org/wiki/Polynomial_expansion>`_, "In mathematics, an
    expansion of a product of sums expresses it as a sum of products by using the fact that
    multiplication distributes over addition". Take a 2-variable feature vector as an example:
    `(x, y)`, if we want to expand it with degree 2, then we get `(x, x * x, y, x * y, y * y)`.

    >>> from pyspark.ml.linalg import Vectors
    >>> df = spark.createDataFrame([(Vectors.dense([0.5, 2.0]),)], ["dense"])
    >>> px = PolynomialExpansion(degree=2, inputCol="dense", outputCol="expanded")
    >>> px.transform(df).head().expanded
    DenseVector([0.5, 0.25, 2.0, 1.0, 4.0])
    >>> px.setParams(outputCol="test").transform(df).head().test
    DenseVector([0.5, 0.25, 2.0, 1.0, 4.0])
    >>> polyExpansionPath = temp_path + "/poly-expansion"
    >>> px.save(polyExpansionPath)
    >>> loadedPx = PolynomialExpansion.load(polyExpansionPath)
    >>> loadedPx.getDegree() == px.getDegree()
    True

    .. versionadded:: 1.4.0
    """

    degree = Param(Params._dummy(), "degree", "the polynomial degree to expand (>= 1)",
                   typeConverter=TypeConverters.toInt)

    @keyword_only
    def __init__(self, degree=2, inputCol=None, outputCol=None):
        """
        __init__(self, degree=2, inputCol=None, outputCol=None)
        """
        super(PolynomialExpansion, self).__init__()
        self._java_obj = self._new_java_obj(
            "org.apache.spark.ml.feature.PolynomialExpansion", self.uid)
        self._setDefault(degree=2)
        kwargs = self.__init__._input_kwargs
        self.setParams(**kwargs)

    @keyword_only
    @since("1.4.0")
    def setParams(self, degree=2, inputCol=None, outputCol=None):
        """
        setParams(self, degree=2, inputCol=None, outputCol=None)
        Sets params for this PolynomialExpansion.
        """
        kwargs = self.setParams._input_kwargs
        return self._set(**kwargs)

    @since("1.4.0")
    def setDegree(self, value):
        """
        Sets the value of :py:attr:`degree`.
        """
        return self._set(degree=value)

    @since("1.4.0")
    def getDegree(self):
        """
        Gets the value of degree or its default value.
        """
        return self.getOrDefault(self.degree)


@inherit_doc
class QuantileDiscretizer(JavaEstimator, HasInputCol, HasOutputCol, JavaMLReadable, JavaMLWritable):
    """
    .. note:: Experimental

    `QuantileDiscretizer` takes a column with continuous features and outputs a column with binned
    categorical features. The number of bins can be set using the :py:attr:`numBuckets` parameter.
    It is possible that the number of buckets used will be less than this value, for example, if
    there are too few distinct values of the input to create enough distinct quantiles. Note also
    that NaN values are handled specially and placed into their own bucket. For example, if 4
    buckets are used, then non-NaN data will be put into buckets(0-3), but NaNs will be counted in
    a special bucket(4).
    The bin ranges are chosen using an approximate algorithm (see the documentation for
    :py:meth:`~.DataFrameStatFunctions.approxQuantile` for a detailed description).
    The precision of the approximation can be controlled with the
    :py:attr:`relativeError` parameter.
    The lower and upper bin bounds will be `-Infinity` and `+Infinity`, covering all real values.

    >>> df = spark.createDataFrame([(0.1,), (0.4,), (1.2,), (1.5,)], ["values"])
    >>> qds = QuantileDiscretizer(numBuckets=2,
    ...     inputCol="values", outputCol="buckets", relativeError=0.01)
    >>> qds.getRelativeError()
    0.01
    >>> bucketizer = qds.fit(df)
    >>> splits = bucketizer.getSplits()
    >>> splits[0]
    -inf
    >>> print("%2.1f" % round(splits[1], 1))
    0.4
    >>> bucketed = bucketizer.transform(df).head()
    >>> bucketed.buckets
    0.0
    >>> quantileDiscretizerPath = temp_path + "/quantile-discretizer"
    >>> qds.save(quantileDiscretizerPath)
    >>> loadedQds = QuantileDiscretizer.load(quantileDiscretizerPath)
    >>> loadedQds.getNumBuckets() == qds.getNumBuckets()
    True

    .. versionadded:: 2.0.0
    """

    numBuckets = Param(Params._dummy(), "numBuckets",
                       "Maximum number of buckets (quantiles, or " +
                       "categories) into which data points are grouped. Must be >= 2.",
                       typeConverter=TypeConverters.toInt)

    relativeError = Param(Params._dummy(), "relativeError", "The relative target precision for " +
                          "the approximate quantile algorithm used to generate buckets. " +
                          "Must be in the range [0, 1].",
                          typeConverter=TypeConverters.toFloat)

    @keyword_only
    def __init__(self, numBuckets=2, inputCol=None, outputCol=None, relativeError=0.001):
        """
        __init__(self, numBuckets=2, inputCol=None, outputCol=None, relativeError=0.001)
        """
        super(QuantileDiscretizer, self).__init__()
        self._java_obj = self._new_java_obj("org.apache.spark.ml.feature.QuantileDiscretizer",
                                            self.uid)
        self._setDefault(numBuckets=2, relativeError=0.001)
        kwargs = self.__init__._input_kwargs
        self.setParams(**kwargs)

    @keyword_only
    @since("2.0.0")
    def setParams(self, numBuckets=2, inputCol=None, outputCol=None, relativeError=0.001):
        """
        setParams(self, numBuckets=2, inputCol=None, outputCol=None, relativeError=0.001)
        Set the params for the QuantileDiscretizer
        """
        kwargs = self.setParams._input_kwargs
        return self._set(**kwargs)

    @since("2.0.0")
    def setNumBuckets(self, value):
        """
        Sets the value of :py:attr:`numBuckets`.
        """
        return self._set(numBuckets=value)

    @since("2.0.0")
    def getNumBuckets(self):
        """
        Gets the value of numBuckets or its default value.
        """
        return self.getOrDefault(self.numBuckets)

    @since("2.0.0")
    def setRelativeError(self, value):
        """
        Sets the value of :py:attr:`relativeError`.
        """
        return self._set(relativeError=value)

    @since("2.0.0")
    def getRelativeError(self):
        """
        Gets the value of relativeError or its default value.
        """
        return self.getOrDefault(self.relativeError)

    def _create_model(self, java_model):
        """
        Private method to convert the java_model to a Python model.
        """
        return Bucketizer(splits=list(java_model.getSplits()),
                          inputCol=self.getInputCol(),
                          outputCol=self.getOutputCol())


@inherit_doc
@ignore_unicode_prefix
class RegexTokenizer(JavaTransformer, HasInputCol, HasOutputCol, JavaMLReadable, JavaMLWritable):
    """
    A regex based tokenizer that extracts tokens either by using the
    provided regex pattern (in Java dialect) to split the text
    (default) or repeatedly matching the regex (if gaps is false).
    Optional parameters also allow filtering tokens using a minimal
    length.
    It returns an array of strings that can be empty.

    >>> df = spark.createDataFrame([("A B  c",)], ["text"])
    >>> reTokenizer = RegexTokenizer(inputCol="text", outputCol="words")
    >>> reTokenizer.transform(df).head()
    Row(text=u'A B  c', words=[u'a', u'b', u'c'])
    >>> # Change a parameter.
    >>> reTokenizer.setParams(outputCol="tokens").transform(df).head()
    Row(text=u'A B  c', tokens=[u'a', u'b', u'c'])
    >>> # Temporarily modify a parameter.
    >>> reTokenizer.transform(df, {reTokenizer.outputCol: "words"}).head()
    Row(text=u'A B  c', words=[u'a', u'b', u'c'])
    >>> reTokenizer.transform(df).head()
    Row(text=u'A B  c', tokens=[u'a', u'b', u'c'])
    >>> # Must use keyword arguments to specify params.
    >>> reTokenizer.setParams("text")
    Traceback (most recent call last):
        ...
    TypeError: Method setParams forces keyword arguments.
    >>> regexTokenizerPath = temp_path + "/regex-tokenizer"
    >>> reTokenizer.save(regexTokenizerPath)
    >>> loadedReTokenizer = RegexTokenizer.load(regexTokenizerPath)
    >>> loadedReTokenizer.getMinTokenLength() == reTokenizer.getMinTokenLength()
    True
    >>> loadedReTokenizer.getGaps() == reTokenizer.getGaps()
    True

    .. versionadded:: 1.4.0
    """

    minTokenLength = Param(Params._dummy(), "minTokenLength", "minimum token length (>= 0)",
                           typeConverter=TypeConverters.toInt)
    gaps = Param(Params._dummy(), "gaps", "whether regex splits on gaps (True) or matches tokens " +
                 "(False)")
    pattern = Param(Params._dummy(), "pattern", "regex pattern (Java dialect) used for tokenizing",
                    typeConverter=TypeConverters.toString)
    toLowercase = Param(Params._dummy(), "toLowercase", "whether to convert all characters to " +
                        "lowercase before tokenizing", typeConverter=TypeConverters.toBoolean)

    @keyword_only
    def __init__(self, minTokenLength=1, gaps=True, pattern="\\s+", inputCol=None,
                 outputCol=None, toLowercase=True):
        """
        __init__(self, minTokenLength=1, gaps=True, pattern="\\s+", inputCol=None, \
                 outputCol=None, toLowercase=True)
        """
        super(RegexTokenizer, self).__init__()
        self._java_obj = self._new_java_obj("org.apache.spark.ml.feature.RegexTokenizer", self.uid)
        self._setDefault(minTokenLength=1, gaps=True, pattern="\\s+", toLowercase=True)
        kwargs = self.__init__._input_kwargs
        self.setParams(**kwargs)

    @keyword_only
    @since("1.4.0")
    def setParams(self, minTokenLength=1, gaps=True, pattern="\\s+", inputCol=None,
                  outputCol=None, toLowercase=True):
        """
        setParams(self, minTokenLength=1, gaps=True, pattern="\\s+", inputCol=None, \
                  outputCol=None, toLowercase=True)
        Sets params for this RegexTokenizer.
        """
        kwargs = self.setParams._input_kwargs
        return self._set(**kwargs)

    @since("1.4.0")
    def setMinTokenLength(self, value):
        """
        Sets the value of :py:attr:`minTokenLength`.
        """
        return self._set(minTokenLength=value)

    @since("1.4.0")
    def getMinTokenLength(self):
        """
        Gets the value of minTokenLength or its default value.
        """
        return self.getOrDefault(self.minTokenLength)

    @since("1.4.0")
    def setGaps(self, value):
        """
        Sets the value of :py:attr:`gaps`.
        """
        return self._set(gaps=value)

    @since("1.4.0")
    def getGaps(self):
        """
        Gets the value of gaps or its default value.
        """
        return self.getOrDefault(self.gaps)

    @since("1.4.0")
    def setPattern(self, value):
        """
        Sets the value of :py:attr:`pattern`.
        """
        return self._set(pattern=value)

    @since("1.4.0")
    def getPattern(self):
        """
        Gets the value of pattern or its default value.
        """
        return self.getOrDefault(self.pattern)

    @since("2.0.0")
    def setToLowercase(self, value):
        """
        Sets the value of :py:attr:`toLowercase`.
        """
        return self._set(toLowercase=value)

    @since("2.0.0")
    def getToLowercase(self):
        """
        Gets the value of toLowercase or its default value.
        """
        return self.getOrDefault(self.toLowercase)


@inherit_doc
class SQLTransformer(JavaTransformer, JavaMLReadable, JavaMLWritable):
    """
    Implements the transforms which are defined by SQL statement.
    Currently we only support SQL syntax like 'SELECT ... FROM __THIS__'
    where '__THIS__' represents the underlying table of the input dataset.

    >>> df = spark.createDataFrame([(0, 1.0, 3.0), (2, 2.0, 5.0)], ["id", "v1", "v2"])
    >>> sqlTrans = SQLTransformer(
    ...     statement="SELECT *, (v1 + v2) AS v3, (v1 * v2) AS v4 FROM __THIS__")
    >>> sqlTrans.transform(df).head()
    Row(id=0, v1=1.0, v2=3.0, v3=4.0, v4=3.0)
    >>> sqlTransformerPath = temp_path + "/sql-transformer"
    >>> sqlTrans.save(sqlTransformerPath)
    >>> loadedSqlTrans = SQLTransformer.load(sqlTransformerPath)
    >>> loadedSqlTrans.getStatement() == sqlTrans.getStatement()
    True

    .. versionadded:: 1.6.0
    """

    statement = Param(Params._dummy(), "statement", "SQL statement",
                      typeConverter=TypeConverters.toString)

    @keyword_only
    def __init__(self, statement=None):
        """
        __init__(self, statement=None)
        """
        super(SQLTransformer, self).__init__()
        self._java_obj = self._new_java_obj("org.apache.spark.ml.feature.SQLTransformer", self.uid)
        kwargs = self.__init__._input_kwargs
        self.setParams(**kwargs)

    @keyword_only
    @since("1.6.0")
    def setParams(self, statement=None):
        """
        setParams(self, statement=None)
        Sets params for this SQLTransformer.
        """
        kwargs = self.setParams._input_kwargs
        return self._set(**kwargs)

    @since("1.6.0")
    def setStatement(self, value):
        """
        Sets the value of :py:attr:`statement`.
        """
        return self._set(statement=value)

    @since("1.6.0")
    def getStatement(self):
        """
        Gets the value of statement or its default value.
        """
        return self.getOrDefault(self.statement)


@inherit_doc
class StandardScaler(JavaEstimator, HasInputCol, HasOutputCol, JavaMLReadable, JavaMLWritable):
    """
    Standardizes features by removing the mean and scaling to unit variance using column summary
    statistics on the samples in the training set.

    The "unit std" is computed using the `corrected sample standard deviation \
    <https://en.wikipedia.org/wiki/Standard_deviation#Corrected_sample_standard_deviation>`_,
    which is computed as the square root of the unbiased sample variance.

    >>> from pyspark.ml.linalg import Vectors
    >>> df = spark.createDataFrame([(Vectors.dense([0.0]),), (Vectors.dense([2.0]),)], ["a"])
    >>> standardScaler = StandardScaler(inputCol="a", outputCol="scaled")
    >>> model = standardScaler.fit(df)
    >>> model.mean
    DenseVector([1.0])
    >>> model.std
    DenseVector([1.4142])
    >>> model.transform(df).collect()[1].scaled
    DenseVector([1.4142])
    >>> standardScalerPath = temp_path + "/standard-scaler"
    >>> standardScaler.save(standardScalerPath)
    >>> loadedStandardScaler = StandardScaler.load(standardScalerPath)
    >>> loadedStandardScaler.getWithMean() == standardScaler.getWithMean()
    True
    >>> loadedStandardScaler.getWithStd() == standardScaler.getWithStd()
    True
    >>> modelPath = temp_path + "/standard-scaler-model"
    >>> model.save(modelPath)
    >>> loadedModel = StandardScalerModel.load(modelPath)
    >>> loadedModel.std == model.std
    True
    >>> loadedModel.mean == model.mean
    True

    .. versionadded:: 1.4.0
    """

    withMean = Param(Params._dummy(), "withMean", "Center data with mean",
                     typeConverter=TypeConverters.toBoolean)
    withStd = Param(Params._dummy(), "withStd", "Scale to unit standard deviation",
                    typeConverter=TypeConverters.toBoolean)

    @keyword_only
    def __init__(self, withMean=False, withStd=True, inputCol=None, outputCol=None):
        """
        __init__(self, withMean=False, withStd=True, inputCol=None, outputCol=None)
        """
        super(StandardScaler, self).__init__()
        self._java_obj = self._new_java_obj("org.apache.spark.ml.feature.StandardScaler", self.uid)
        self._setDefault(withMean=False, withStd=True)
        kwargs = self.__init__._input_kwargs
        self.setParams(**kwargs)

    @keyword_only
    @since("1.4.0")
    def setParams(self, withMean=False, withStd=True, inputCol=None, outputCol=None):
        """
        setParams(self, withMean=False, withStd=True, inputCol=None, outputCol=None)
        Sets params for this StandardScaler.
        """
        kwargs = self.setParams._input_kwargs
        return self._set(**kwargs)

    @since("1.4.0")
    def setWithMean(self, value):
        """
        Sets the value of :py:attr:`withMean`.
        """
        return self._set(withMean=value)

    @since("1.4.0")
    def getWithMean(self):
        """
        Gets the value of withMean or its default value.
        """
        return self.getOrDefault(self.withMean)

    @since("1.4.0")
    def setWithStd(self, value):
        """
        Sets the value of :py:attr:`withStd`.
        """
        return self._set(withStd=value)

    @since("1.4.0")
    def getWithStd(self):
        """
        Gets the value of withStd or its default value.
        """
        return self.getOrDefault(self.withStd)

    def _create_model(self, java_model):
        return StandardScalerModel(java_model)


class StandardScalerModel(JavaModel, JavaMLReadable, JavaMLWritable):
    """
    Model fitted by :py:class:`StandardScaler`.

    .. versionadded:: 1.4.0
    """

    @property
    @since("2.0.0")
    def std(self):
        """
        Standard deviation of the StandardScalerModel.
        """
        return self._call_java("std")

    @property
    @since("2.0.0")
    def mean(self):
        """
        Mean of the StandardScalerModel.
        """
        return self._call_java("mean")


@inherit_doc
class StringIndexer(JavaEstimator, HasInputCol, HasOutputCol, HasHandleInvalid, JavaMLReadable,
                    JavaMLWritable):
    """
    A label indexer that maps a string column of labels to an ML column of label indices.
    If the input column is numeric, we cast it to string and index the string values.
    The indices are in [0, numLabels), ordered by label frequencies.
    So the most frequent label gets index 0.

    >>> stringIndexer = StringIndexer(inputCol="label", outputCol="indexed", handleInvalid='error')
    >>> model = stringIndexer.fit(stringIndDf)
    >>> td = model.transform(stringIndDf)
    >>> sorted(set([(i[0], i[1]) for i in td.select(td.id, td.indexed).collect()]),
    ...     key=lambda x: x[0])
    [(0, 0.0), (1, 2.0), (2, 1.0), (3, 0.0), (4, 0.0), (5, 1.0)]
    >>> inverter = IndexToString(inputCol="indexed", outputCol="label2", labels=model.labels)
    >>> itd = inverter.transform(td)
    >>> sorted(set([(i[0], str(i[1])) for i in itd.select(itd.id, itd.label2).collect()]),
    ...     key=lambda x: x[0])
    [(0, 'a'), (1, 'b'), (2, 'c'), (3, 'a'), (4, 'a'), (5, 'c')]
    >>> stringIndexerPath = temp_path + "/string-indexer"
    >>> stringIndexer.save(stringIndexerPath)
    >>> loadedIndexer = StringIndexer.load(stringIndexerPath)
    >>> loadedIndexer.getHandleInvalid() == stringIndexer.getHandleInvalid()
    True
    >>> modelPath = temp_path + "/string-indexer-model"
    >>> model.save(modelPath)
    >>> loadedModel = StringIndexerModel.load(modelPath)
    >>> loadedModel.labels == model.labels
    True
    >>> indexToStringPath = temp_path + "/index-to-string"
    >>> inverter.save(indexToStringPath)
    >>> loadedInverter = IndexToString.load(indexToStringPath)
    >>> loadedInverter.getLabels() == inverter.getLabels()
    True

    .. versionadded:: 1.4.0
    """

    @keyword_only
    def __init__(self, inputCol=None, outputCol=None, handleInvalid="error"):
        """
        __init__(self, inputCol=None, outputCol=None, handleInvalid="error")
        """
        super(StringIndexer, self).__init__()
        self._java_obj = self._new_java_obj("org.apache.spark.ml.feature.StringIndexer", self.uid)
        self._setDefault(handleInvalid="error")
        kwargs = self.__init__._input_kwargs
        self.setParams(**kwargs)

    @keyword_only
    @since("1.4.0")
    def setParams(self, inputCol=None, outputCol=None, handleInvalid="error"):
        """
        setParams(self, inputCol=None, outputCol=None, handleInvalid="error")
        Sets params for this StringIndexer.
        """
        kwargs = self.setParams._input_kwargs
        return self._set(**kwargs)

    def _create_model(self, java_model):
        return StringIndexerModel(java_model)


class StringIndexerModel(JavaModel, JavaMLReadable, JavaMLWritable):
    """
    Model fitted by :py:class:`StringIndexer`.

    .. versionadded:: 1.4.0
    """

    @property
    @since("1.5.0")
    def labels(self):
        """
        Ordered list of labels, corresponding to indices to be assigned.
        """
        return self._call_java("labels")


@inherit_doc
class IndexToString(JavaTransformer, HasInputCol, HasOutputCol, JavaMLReadable, JavaMLWritable):
    """
    A :py:class:`Transformer` that maps a column of indices back to a new column of
    corresponding string values.
    The index-string mapping is either from the ML attributes of the input column,
    or from user-supplied labels (which take precedence over ML attributes).
    See L{StringIndexer} for converting strings into indices.

    .. versionadded:: 1.6.0
    """

    labels = Param(Params._dummy(), "labels",
                   "Optional array of labels specifying index-string mapping." +
                   " If not provided or if empty, then metadata from inputCol is used instead.",
                   typeConverter=TypeConverters.toListString)

    @keyword_only
    def __init__(self, inputCol=None, outputCol=None, labels=None):
        """
        __init__(self, inputCol=None, outputCol=None, labels=None)
        """
        super(IndexToString, self).__init__()
        self._java_obj = self._new_java_obj("org.apache.spark.ml.feature.IndexToString",
                                            self.uid)
        kwargs = self.__init__._input_kwargs
        self.setParams(**kwargs)

    @keyword_only
    @since("1.6.0")
    def setParams(self, inputCol=None, outputCol=None, labels=None):
        """
        setParams(self, inputCol=None, outputCol=None, labels=None)
        Sets params for this IndexToString.
        """
        kwargs = self.setParams._input_kwargs
        return self._set(**kwargs)

    @since("1.6.0")
    def setLabels(self, value):
        """
        Sets the value of :py:attr:`labels`.
        """
        return self._set(labels=value)

    @since("1.6.0")
    def getLabels(self):
        """
        Gets the value of :py:attr:`labels` or its default value.
        """
        return self.getOrDefault(self.labels)


class StopWordsRemover(JavaTransformer, HasInputCol, HasOutputCol, JavaMLReadable, JavaMLWritable):
    """
    A feature transformer that filters out stop words from input.
    Note: null values from input array are preserved unless adding null to stopWords explicitly.

    >>> df = spark.createDataFrame([(["a", "b", "c"],)], ["text"])
    >>> remover = StopWordsRemover(inputCol="text", outputCol="words", stopWords=["b"])
    >>> remover.transform(df).head().words == ['a', 'c']
    True
    >>> stopWordsRemoverPath = temp_path + "/stopwords-remover"
    >>> remover.save(stopWordsRemoverPath)
    >>> loadedRemover = StopWordsRemover.load(stopWordsRemoverPath)
    >>> loadedRemover.getStopWords() == remover.getStopWords()
    True
    >>> loadedRemover.getCaseSensitive() == remover.getCaseSensitive()
    True

    .. versionadded:: 1.6.0
    """

    stopWords = Param(Params._dummy(), "stopWords", "The words to be filtered out",
                      typeConverter=TypeConverters.toListString)
    caseSensitive = Param(Params._dummy(), "caseSensitive", "whether to do a case sensitive " +
                          "comparison over the stop words", typeConverter=TypeConverters.toBoolean)

    @keyword_only
    def __init__(self, inputCol=None, outputCol=None, stopWords=None, caseSensitive=False):
        """
        __init__(self, inputCol=None, outputCol=None, stopWords=None, caseSensitive=false)
        """
        super(StopWordsRemover, self).__init__()
        self._java_obj = self._new_java_obj("org.apache.spark.ml.feature.StopWordsRemover",
                                            self.uid)
        self._setDefault(stopWords=StopWordsRemover.loadDefaultStopWords("english"),
                         caseSensitive=False)
        kwargs = self.__init__._input_kwargs
        self.setParams(**kwargs)

    @keyword_only
    @since("1.6.0")
    def setParams(self, inputCol=None, outputCol=None, stopWords=None, caseSensitive=False):
        """
        setParams(self, inputCol=None, outputCol=None, stopWords=None, caseSensitive=false)
        Sets params for this StopWordRemover.
        """
        kwargs = self.setParams._input_kwargs
        return self._set(**kwargs)

    @since("1.6.0")
    def setStopWords(self, value):
        """
        Sets the value of :py:attr:`stopWords`.
        """
        return self._set(stopWords=value)

    @since("1.6.0")
    def getStopWords(self):
        """
        Gets the value of :py:attr:`stopWords` or its default value.
        """
        return self.getOrDefault(self.stopWords)

    @since("1.6.0")
    def setCaseSensitive(self, value):
        """
        Sets the value of :py:attr:`caseSensitive`.
        """
        return self._set(caseSensitive=value)

    @since("1.6.0")
    def getCaseSensitive(self):
        """
        Gets the value of :py:attr:`caseSensitive` or its default value.
        """
        return self.getOrDefault(self.caseSensitive)

    @staticmethod
    @since("2.0.0")
    def loadDefaultStopWords(language):
        """
        Loads the default stop words for the given language.
        Supported languages: danish, dutch, english, finnish, french, german, hungarian,
        italian, norwegian, portuguese, russian, spanish, swedish, turkish
        """
        stopWordsObj = _jvm().org.apache.spark.ml.feature.StopWordsRemover
        return list(stopWordsObj.loadDefaultStopWords(language))


@inherit_doc
@ignore_unicode_prefix
class Tokenizer(JavaTransformer, HasInputCol, HasOutputCol, JavaMLReadable, JavaMLWritable):
    """
    A tokenizer that converts the input string to lowercase and then
    splits it by white spaces.

    >>> df = spark.createDataFrame([("a b c",)], ["text"])
    >>> tokenizer = Tokenizer(inputCol="text", outputCol="words")
    >>> tokenizer.transform(df).head()
    Row(text=u'a b c', words=[u'a', u'b', u'c'])
    >>> # Change a parameter.
    >>> tokenizer.setParams(outputCol="tokens").transform(df).head()
    Row(text=u'a b c', tokens=[u'a', u'b', u'c'])
    >>> # Temporarily modify a parameter.
    >>> tokenizer.transform(df, {tokenizer.outputCol: "words"}).head()
    Row(text=u'a b c', words=[u'a', u'b', u'c'])
    >>> tokenizer.transform(df).head()
    Row(text=u'a b c', tokens=[u'a', u'b', u'c'])
    >>> # Must use keyword arguments to specify params.
    >>> tokenizer.setParams("text")
    Traceback (most recent call last):
        ...
    TypeError: Method setParams forces keyword arguments.
    >>> tokenizerPath = temp_path + "/tokenizer"
    >>> tokenizer.save(tokenizerPath)
    >>> loadedTokenizer = Tokenizer.load(tokenizerPath)
    >>> loadedTokenizer.transform(df).head().tokens == tokenizer.transform(df).head().tokens
    True

    .. versionadded:: 1.3.0
    """

    @keyword_only
    def __init__(self, inputCol=None, outputCol=None):
        """
        __init__(self, inputCol=None, outputCol=None)
        """
        super(Tokenizer, self).__init__()
        self._java_obj = self._new_java_obj("org.apache.spark.ml.feature.Tokenizer", self.uid)
        kwargs = self.__init__._input_kwargs
        self.setParams(**kwargs)

    @keyword_only
    @since("1.3.0")
    def setParams(self, inputCol=None, outputCol=None):
        """
        setParams(self, inputCol=None, outputCol=None)
        Sets params for this Tokenizer.
        """
        kwargs = self.setParams._input_kwargs
        return self._set(**kwargs)


@inherit_doc
class VectorAssembler(JavaTransformer, HasInputCols, HasOutputCol, JavaMLReadable, JavaMLWritable):
    """
    A feature transformer that merges multiple columns into a vector column.

    >>> df = spark.createDataFrame([(1, 0, 3)], ["a", "b", "c"])
    >>> vecAssembler = VectorAssembler(inputCols=["a", "b", "c"], outputCol="features")
    >>> vecAssembler.transform(df).head().features
    DenseVector([1.0, 0.0, 3.0])
    >>> vecAssembler.setParams(outputCol="freqs").transform(df).head().freqs
    DenseVector([1.0, 0.0, 3.0])
    >>> params = {vecAssembler.inputCols: ["b", "a"], vecAssembler.outputCol: "vector"}
    >>> vecAssembler.transform(df, params).head().vector
    DenseVector([0.0, 1.0])
    >>> vectorAssemblerPath = temp_path + "/vector-assembler"
    >>> vecAssembler.save(vectorAssemblerPath)
    >>> loadedAssembler = VectorAssembler.load(vectorAssemblerPath)
    >>> loadedAssembler.transform(df).head().freqs == vecAssembler.transform(df).head().freqs
    True

    .. versionadded:: 1.4.0
    """

    @keyword_only
    def __init__(self, inputCols=None, outputCol=None):
        """
        __init__(self, inputCols=None, outputCol=None)
        """
        super(VectorAssembler, self).__init__()
        self._java_obj = self._new_java_obj("org.apache.spark.ml.feature.VectorAssembler", self.uid)
        kwargs = self.__init__._input_kwargs
        self.setParams(**kwargs)

    @keyword_only
    @since("1.4.0")
    def setParams(self, inputCols=None, outputCol=None):
        """
        setParams(self, inputCols=None, outputCol=None)
        Sets params for this VectorAssembler.
        """
        kwargs = self.setParams._input_kwargs
        return self._set(**kwargs)


@inherit_doc
class VectorIndexer(JavaEstimator, HasInputCol, HasOutputCol, JavaMLReadable, JavaMLWritable):
    """
    Class for indexing categorical feature columns in a dataset of `Vector`.

    This has 2 usage modes:
      - Automatically identify categorical features (default behavior)
         - This helps process a dataset of unknown vectors into a dataset with some continuous
           features and some categorical features. The choice between continuous and categorical
           is based upon a maxCategories parameter.
         - Set maxCategories to the maximum number of categorical any categorical feature should
           have.
         - E.g.: Feature 0 has unique values {-1.0, 0.0}, and feature 1 values {1.0, 3.0, 5.0}.
           If maxCategories = 2, then feature 0 will be declared categorical and use indices {0, 1},
           and feature 1 will be declared continuous.
      - Index all features, if all features are categorical
         - If maxCategories is set to be very large, then this will build an index of unique
           values for all features.
         - Warning: This can cause problems if features are continuous since this will collect ALL
           unique values to the driver.
         - E.g.: Feature 0 has unique values {-1.0, 0.0}, and feature 1 values {1.0, 3.0, 5.0}.
           If maxCategories >= 3, then both features will be declared categorical.

     This returns a model which can transform categorical features to use 0-based indices.

    Index stability:
      - This is not guaranteed to choose the same category index across multiple runs.
      - If a categorical feature includes value 0, then this is guaranteed to map value 0 to
        index 0. This maintains vector sparsity.
      - More stability may be added in the future.

     TODO: Future extensions: The following functionality is planned for the future:
      - Preserve metadata in transform; if a feature's metadata is already present,
        do not recompute.
      - Specify certain features to not index, either via a parameter or via existing metadata.
      - Add warning if a categorical feature has only 1 category.
      - Add option for allowing unknown categories.

    >>> from pyspark.ml.linalg import Vectors
    >>> df = spark.createDataFrame([(Vectors.dense([-1.0, 0.0]),),
    ...     (Vectors.dense([0.0, 1.0]),), (Vectors.dense([0.0, 2.0]),)], ["a"])
    >>> indexer = VectorIndexer(maxCategories=2, inputCol="a", outputCol="indexed")
    >>> model = indexer.fit(df)
    >>> model.transform(df).head().indexed
    DenseVector([1.0, 0.0])
    >>> model.numFeatures
    2
    >>> model.categoryMaps
    {0: {0.0: 0, -1.0: 1}}
    >>> indexer.setParams(outputCol="test").fit(df).transform(df).collect()[1].test
    DenseVector([0.0, 1.0])
    >>> params = {indexer.maxCategories: 3, indexer.outputCol: "vector"}
    >>> model2 = indexer.fit(df, params)
    >>> model2.transform(df).head().vector
    DenseVector([1.0, 0.0])
    >>> vectorIndexerPath = temp_path + "/vector-indexer"
    >>> indexer.save(vectorIndexerPath)
    >>> loadedIndexer = VectorIndexer.load(vectorIndexerPath)
    >>> loadedIndexer.getMaxCategories() == indexer.getMaxCategories()
    True
    >>> modelPath = temp_path + "/vector-indexer-model"
    >>> model.save(modelPath)
    >>> loadedModel = VectorIndexerModel.load(modelPath)
    >>> loadedModel.numFeatures == model.numFeatures
    True
    >>> loadedModel.categoryMaps == model.categoryMaps
    True

    .. versionadded:: 1.4.0
    """

    maxCategories = Param(Params._dummy(), "maxCategories",
                          "Threshold for the number of values a categorical feature can take " +
                          "(>= 2). If a feature is found to have > maxCategories values, then " +
                          "it is declared continuous.", typeConverter=TypeConverters.toInt)

    @keyword_only
    def __init__(self, maxCategories=20, inputCol=None, outputCol=None):
        """
        __init__(self, maxCategories=20, inputCol=None, outputCol=None)
        """
        super(VectorIndexer, self).__init__()
        self._java_obj = self._new_java_obj("org.apache.spark.ml.feature.VectorIndexer", self.uid)
        self._setDefault(maxCategories=20)
        kwargs = self.__init__._input_kwargs
        self.setParams(**kwargs)

    @keyword_only
    @since("1.4.0")
    def setParams(self, maxCategories=20, inputCol=None, outputCol=None):
        """
        setParams(self, maxCategories=20, inputCol=None, outputCol=None)
        Sets params for this VectorIndexer.
        """
        kwargs = self.setParams._input_kwargs
        return self._set(**kwargs)

    @since("1.4.0")
    def setMaxCategories(self, value):
        """
        Sets the value of :py:attr:`maxCategories`.
        """
        return self._set(maxCategories=value)

    @since("1.4.0")
    def getMaxCategories(self):
        """
        Gets the value of maxCategories or its default value.
        """
        return self.getOrDefault(self.maxCategories)

    def _create_model(self, java_model):
        return VectorIndexerModel(java_model)


class VectorIndexerModel(JavaModel, JavaMLReadable, JavaMLWritable):
    """
    Model fitted by :py:class:`VectorIndexer`.

    Transform categorical features to use 0-based indices instead of their original values.
      - Categorical features are mapped to indices.
      - Continuous features (columns) are left unchanged.

    This also appends metadata to the output column, marking features as Numeric (continuous),
    Nominal (categorical), or Binary (either continuous or categorical).
    Non-ML metadata is not carried over from the input to the output column.

    This maintains vector sparsity.

    .. versionadded:: 1.4.0
    """

    @property
    @since("1.4.0")
    def numFeatures(self):
        """
        Number of features, i.e., length of Vectors which this transforms.
        """
        return self._call_java("numFeatures")

    @property
    @since("1.4.0")
    def categoryMaps(self):
        """
        Feature value index.  Keys are categorical feature indices (column indices).
        Values are maps from original features values to 0-based category indices.
        If a feature is not in this map, it is treated as continuous.
        """
        return self._call_java("javaCategoryMaps")


@inherit_doc
class VectorSlicer(JavaTransformer, HasInputCol, HasOutputCol, JavaMLReadable, JavaMLWritable):
    """
    This class takes a feature vector and outputs a new feature vector with a subarray
    of the original features.

    The subset of features can be specified with either indices (`setIndices()`)
    or names (`setNames()`).  At least one feature must be selected. Duplicate features
    are not allowed, so there can be no overlap between selected indices and names.

    The output vector will order features with the selected indices first (in the order given),
    followed by the selected names (in the order given).

    >>> from pyspark.ml.linalg import Vectors
    >>> df = spark.createDataFrame([
    ...     (Vectors.dense([-2.0, 2.3, 0.0, 0.0, 1.0]),),
    ...     (Vectors.dense([0.0, 0.0, 0.0, 0.0, 0.0]),),
    ...     (Vectors.dense([0.6, -1.1, -3.0, 4.5, 3.3]),)], ["features"])
    >>> vs = VectorSlicer(inputCol="features", outputCol="sliced", indices=[1, 4])
    >>> vs.transform(df).head().sliced
    DenseVector([2.3, 1.0])
    >>> vectorSlicerPath = temp_path + "/vector-slicer"
    >>> vs.save(vectorSlicerPath)
    >>> loadedVs = VectorSlicer.load(vectorSlicerPath)
    >>> loadedVs.getIndices() == vs.getIndices()
    True
    >>> loadedVs.getNames() == vs.getNames()
    True

    .. versionadded:: 1.6.0
    """

    indices = Param(Params._dummy(), "indices", "An array of indices to select features from " +
                    "a vector column. There can be no overlap with names.",
                    typeConverter=TypeConverters.toListInt)
    names = Param(Params._dummy(), "names", "An array of feature names to select features from " +
                  "a vector column. These names must be specified by ML " +
                  "org.apache.spark.ml.attribute.Attribute. There can be no overlap with " +
                  "indices.", typeConverter=TypeConverters.toListString)

    @keyword_only
    def __init__(self, inputCol=None, outputCol=None, indices=None, names=None):
        """
        __init__(self, inputCol=None, outputCol=None, indices=None, names=None)
        """
        super(VectorSlicer, self).__init__()
        self._java_obj = self._new_java_obj("org.apache.spark.ml.feature.VectorSlicer", self.uid)
        self._setDefault(indices=[], names=[])
        kwargs = self.__init__._input_kwargs
        self.setParams(**kwargs)

    @keyword_only
    @since("1.6.0")
    def setParams(self, inputCol=None, outputCol=None, indices=None, names=None):
        """
        setParams(self, inputCol=None, outputCol=None, indices=None, names=None):
        Sets params for this VectorSlicer.
        """
        kwargs = self.setParams._input_kwargs
        return self._set(**kwargs)

    @since("1.6.0")
    def setIndices(self, value):
        """
        Sets the value of :py:attr:`indices`.
        """
        return self._set(indices=value)

    @since("1.6.0")
    def getIndices(self):
        """
        Gets the value of indices or its default value.
        """
        return self.getOrDefault(self.indices)

    @since("1.6.0")
    def setNames(self, value):
        """
        Sets the value of :py:attr:`names`.
        """
        return self._set(names=value)

    @since("1.6.0")
    def getNames(self):
        """
        Gets the value of names or its default value.
        """
        return self.getOrDefault(self.names)


@inherit_doc
@ignore_unicode_prefix
class Word2Vec(JavaEstimator, HasStepSize, HasMaxIter, HasSeed, HasInputCol, HasOutputCol,
               JavaMLReadable, JavaMLWritable):
    """
    Word2Vec trains a model of `Map(String, Vector)`, i.e. transforms a word into a code for further
    natural language processing or machine learning process.

    >>> sent = ("a b " * 100 + "a c " * 10).split(" ")
    >>> doc = spark.createDataFrame([(sent,), (sent,)], ["sentence"])
    >>> word2Vec = Word2Vec(vectorSize=5, seed=42, inputCol="sentence", outputCol="model")
    >>> model = word2Vec.fit(doc)
    >>> model.getVectors().show()
    +----+--------------------+
    |word|              vector|
    +----+--------------------+
    |   a|[0.09461779892444...|
    |   b|[1.15474212169647...|
    |   c|[-0.3794820010662...|
    +----+--------------------+
    ...
    >>> from pyspark.sql.functions import format_number as fmt
    >>> model.findSynonyms("a", 2).select("word", fmt("similarity", 5).alias("similarity")).show()
    +----+----------+
    |word|similarity|
    +----+----------+
    |   b|   0.25053|
    |   c|  -0.69805|
    +----+----------+
    ...
    >>> model.transform(doc).head().model
    DenseVector([0.5524, -0.4995, -0.3599, 0.0241, 0.3461])
    >>> word2vecPath = temp_path + "/word2vec"
    >>> word2Vec.save(word2vecPath)
    >>> loadedWord2Vec = Word2Vec.load(word2vecPath)
    >>> loadedWord2Vec.getVectorSize() == word2Vec.getVectorSize()
    True
    >>> loadedWord2Vec.getNumPartitions() == word2Vec.getNumPartitions()
    True
    >>> loadedWord2Vec.getMinCount() == word2Vec.getMinCount()
    True
    >>> modelPath = temp_path + "/word2vec-model"
    >>> model.save(modelPath)
    >>> loadedModel = Word2VecModel.load(modelPath)
    >>> loadedModel.getVectors().first().word == model.getVectors().first().word
    True
    >>> loadedModel.getVectors().first().vector == model.getVectors().first().vector
    True

    .. versionadded:: 1.4.0
    """

    vectorSize = Param(Params._dummy(), "vectorSize",
                       "the dimension of codes after transforming from words",
                       typeConverter=TypeConverters.toInt)
    numPartitions = Param(Params._dummy(), "numPartitions",
                          "number of partitions for sentences of words",
                          typeConverter=TypeConverters.toInt)
    minCount = Param(Params._dummy(), "minCount",
                     "the minimum number of times a token must appear to be included in the " +
                     "word2vec model's vocabulary", typeConverter=TypeConverters.toInt)
    windowSize = Param(Params._dummy(), "windowSize",
                       "the window size (context words from [-window, window]). Default value is 5",
                       typeConverter=TypeConverters.toInt)
    maxSentenceLength = Param(Params._dummy(), "maxSentenceLength",
                              "Maximum length (in words) of each sentence in the input data. " +
                              "Any sentence longer than this threshold will " +
                              "be divided into chunks up to the size.",
                              typeConverter=TypeConverters.toInt)

    @keyword_only
    def __init__(self, vectorSize=100, minCount=5, numPartitions=1, stepSize=0.025, maxIter=1,
                 seed=None, inputCol=None, outputCol=None, windowSize=5, maxSentenceLength=1000):
        """
        __init__(self, vectorSize=100, minCount=5, numPartitions=1, stepSize=0.025, maxIter=1, \
                 seed=None, inputCol=None, outputCol=None, windowSize=5, maxSentenceLength=1000)
        """
        super(Word2Vec, self).__init__()
        self._java_obj = self._new_java_obj("org.apache.spark.ml.feature.Word2Vec", self.uid)
        self._setDefault(vectorSize=100, minCount=5, numPartitions=1, stepSize=0.025, maxIter=1,
                         windowSize=5, maxSentenceLength=1000)
        kwargs = self.__init__._input_kwargs
        self.setParams(**kwargs)

    @keyword_only
    @since("1.4.0")
    def setParams(self, vectorSize=100, minCount=5, numPartitions=1, stepSize=0.025, maxIter=1,
                  seed=None, inputCol=None, outputCol=None, windowSize=5, maxSentenceLength=1000):
        """
        setParams(self, minCount=5, numPartitions=1, stepSize=0.025, maxIter=1, seed=None, \
                 inputCol=None, outputCol=None, windowSize=5, maxSentenceLength=1000)
        Sets params for this Word2Vec.
        """
        kwargs = self.setParams._input_kwargs
        return self._set(**kwargs)

    @since("1.4.0")
    def setVectorSize(self, value):
        """
        Sets the value of :py:attr:`vectorSize`.
        """
        return self._set(vectorSize=value)

    @since("1.4.0")
    def getVectorSize(self):
        """
        Gets the value of vectorSize or its default value.
        """
        return self.getOrDefault(self.vectorSize)

    @since("1.4.0")
    def setNumPartitions(self, value):
        """
        Sets the value of :py:attr:`numPartitions`.
        """
        return self._set(numPartitions=value)

    @since("1.4.0")
    def getNumPartitions(self):
        """
        Gets the value of numPartitions or its default value.
        """
        return self.getOrDefault(self.numPartitions)

    @since("1.4.0")
    def setMinCount(self, value):
        """
        Sets the value of :py:attr:`minCount`.
        """
        return self._set(minCount=value)

    @since("1.4.0")
    def getMinCount(self):
        """
        Gets the value of minCount or its default value.
        """
        return self.getOrDefault(self.minCount)

    @since("2.0.0")
    def setWindowSize(self, value):
        """
        Sets the value of :py:attr:`windowSize`.
        """
        return self._set(windowSize=value)

    @since("2.0.0")
    def getWindowSize(self):
        """
        Gets the value of windowSize or its default value.
        """
        return self.getOrDefault(self.windowSize)

    @since("2.0.0")
    def setMaxSentenceLength(self, value):
        """
        Sets the value of :py:attr:`maxSentenceLength`.
        """
        return self._set(maxSentenceLength=value)

    @since("2.0.0")
    def getMaxSentenceLength(self):
        """
        Gets the value of maxSentenceLength or its default value.
        """
        return self.getOrDefault(self.maxSentenceLength)

    def _create_model(self, java_model):
        return Word2VecModel(java_model)


class Word2VecModel(JavaModel, JavaMLReadable, JavaMLWritable):
    """
    Model fitted by :py:class:`Word2Vec`.

    .. versionadded:: 1.4.0
    """

    @since("1.5.0")
    def getVectors(self):
        """
        Returns the vector representation of the words as a dataframe
        with two fields, word and vector.
        """
        return self._call_java("getVectors")

    @since("1.5.0")
    def findSynonyms(self, word, num):
        """
        Find "num" number of words closest in similarity to "word".
        word can be a string or vector representation.
        Returns a dataframe with two fields word and similarity (which
        gives the cosine similarity).
        """
        if not isinstance(word, basestring):
            word = _convert_to_vector(word)
        return self._call_java("findSynonyms", word, num)


@inherit_doc
class PCA(JavaEstimator, HasInputCol, HasOutputCol, JavaMLReadable, JavaMLWritable):
    """
    PCA trains a model to project vectors to a lower dimensional space of the
    top :py:attr:`k` principal components.

    >>> from pyspark.ml.linalg import Vectors
    >>> data = [(Vectors.sparse(5, [(1, 1.0), (3, 7.0)]),),
    ...     (Vectors.dense([2.0, 0.0, 3.0, 4.0, 5.0]),),
    ...     (Vectors.dense([4.0, 0.0, 0.0, 6.0, 7.0]),)]
    >>> df = spark.createDataFrame(data,["features"])
    >>> pca = PCA(k=2, inputCol="features", outputCol="pca_features")
    >>> model = pca.fit(df)
    >>> model.transform(df).collect()[0].pca_features
    DenseVector([1.648..., -4.013...])
    >>> model.explainedVariance
    DenseVector([0.794..., 0.205...])
    >>> pcaPath = temp_path + "/pca"
    >>> pca.save(pcaPath)
    >>> loadedPca = PCA.load(pcaPath)
    >>> loadedPca.getK() == pca.getK()
    True
    >>> modelPath = temp_path + "/pca-model"
    >>> model.save(modelPath)
    >>> loadedModel = PCAModel.load(modelPath)
    >>> loadedModel.pc == model.pc
    True
    >>> loadedModel.explainedVariance == model.explainedVariance
    True

    .. versionadded:: 1.5.0
    """

    k = Param(Params._dummy(), "k", "the number of principal components",
              typeConverter=TypeConverters.toInt)

    @keyword_only
    def __init__(self, k=None, inputCol=None, outputCol=None):
        """
        __init__(self, k=None, inputCol=None, outputCol=None)
        """
        super(PCA, self).__init__()
        self._java_obj = self._new_java_obj("org.apache.spark.ml.feature.PCA", self.uid)
        kwargs = self.__init__._input_kwargs
        self.setParams(**kwargs)

    @keyword_only
    @since("1.5.0")
    def setParams(self, k=None, inputCol=None, outputCol=None):
        """
        setParams(self, k=None, inputCol=None, outputCol=None)
        Set params for this PCA.
        """
        kwargs = self.setParams._input_kwargs
        return self._set(**kwargs)

    @since("1.5.0")
    def setK(self, value):
        """
        Sets the value of :py:attr:`k`.
        """
        return self._set(k=value)

    @since("1.5.0")
    def getK(self):
        """
        Gets the value of k or its default value.
        """
        return self.getOrDefault(self.k)

    def _create_model(self, java_model):
        return PCAModel(java_model)


class PCAModel(JavaModel, JavaMLReadable, JavaMLWritable):
    """
    Model fitted by :py:class:`PCA`. Transforms vectors to a lower dimensional space.

    .. versionadded:: 1.5.0
    """

    @property
    @since("2.0.0")
    def pc(self):
        """
        Returns a principal components Matrix.
        Each column is one principal component.
        """
        return self._call_java("pc")

    @property
    @since("2.0.0")
    def explainedVariance(self):
        """
        Returns a vector of proportions of variance
        explained by each principal component.
        """
        return self._call_java("explainedVariance")


@inherit_doc
class RFormula(JavaEstimator, HasFeaturesCol, HasLabelCol, JavaMLReadable, JavaMLWritable):
    """
    .. note:: Experimental

    Implements the transforms required for fitting a dataset against an
    R model formula. Currently we support a limited subset of the R
    operators, including '~', '.', ':', '+', and '-'. Also see the `R formula docs
    <http://stat.ethz.ch/R-manual/R-patched/library/stats/html/formula.html>`_.

    >>> df = spark.createDataFrame([
    ...     (1.0, 1.0, "a"),
    ...     (0.0, 2.0, "b"),
    ...     (0.0, 0.0, "a")
    ... ], ["y", "x", "s"])
    >>> rf = RFormula(formula="y ~ x + s")
    >>> model = rf.fit(df)
    >>> model.transform(df).show()
    +---+---+---+---------+-----+
    |  y|  x|  s| features|label|
    +---+---+---+---------+-----+
    |1.0|1.0|  a|[1.0,1.0]|  1.0|
    |0.0|2.0|  b|[2.0,0.0]|  0.0|
    |0.0|0.0|  a|[0.0,1.0]|  0.0|
    +---+---+---+---------+-----+
    ...
    >>> rf.fit(df, {rf.formula: "y ~ . - s"}).transform(df).show()
    +---+---+---+--------+-----+
    |  y|  x|  s|features|label|
    +---+---+---+--------+-----+
    |1.0|1.0|  a|   [1.0]|  1.0|
    |0.0|2.0|  b|   [2.0]|  0.0|
    |0.0|0.0|  a|   [0.0]|  0.0|
    +---+---+---+--------+-----+
    ...
    >>> rFormulaPath = temp_path + "/rFormula"
    >>> rf.save(rFormulaPath)
    >>> loadedRF = RFormula.load(rFormulaPath)
    >>> loadedRF.getFormula() == rf.getFormula()
    True
    >>> loadedRF.getFeaturesCol() == rf.getFeaturesCol()
    True
    >>> loadedRF.getLabelCol() == rf.getLabelCol()
    True
    >>> str(loadedRF)
    'RFormula(y ~ x + s) (uid=...)'
    >>> modelPath = temp_path + "/rFormulaModel"
    >>> model.save(modelPath)
    >>> loadedModel = RFormulaModel.load(modelPath)
    >>> loadedModel.uid == model.uid
    True
    >>> loadedModel.transform(df).show()
    +---+---+---+---------+-----+
    |  y|  x|  s| features|label|
    +---+---+---+---------+-----+
    |1.0|1.0|  a|[1.0,1.0]|  1.0|
    |0.0|2.0|  b|[2.0,0.0]|  0.0|
    |0.0|0.0|  a|[0.0,1.0]|  0.0|
    +---+---+---+---------+-----+
    ...
    >>> str(loadedModel)
    'RFormulaModel(ResolvedRFormula(label=y, terms=[x,s], hasIntercept=true)) (uid=...)'

    .. versionadded:: 1.5.0
    """

    formula = Param(Params._dummy(), "formula", "R model formula",
                    typeConverter=TypeConverters.toString)

    @keyword_only
    def __init__(self, formula=None, featuresCol="features", labelCol="label"):
        """
        __init__(self, formula=None, featuresCol="features", labelCol="label")
        """
        super(RFormula, self).__init__()
        self._java_obj = self._new_java_obj("org.apache.spark.ml.feature.RFormula", self.uid)
        kwargs = self.__init__._input_kwargs
        self.setParams(**kwargs)

    @keyword_only
    @since("1.5.0")
    def setParams(self, formula=None, featuresCol="features", labelCol="label"):
        """
        setParams(self, formula=None, featuresCol="features", labelCol="label")
        Sets params for RFormula.
        """
        kwargs = self.setParams._input_kwargs
        return self._set(**kwargs)

    @since("1.5.0")
    def setFormula(self, value):
        """
        Sets the value of :py:attr:`formula`.
        """
        return self._set(formula=value)

    @since("1.5.0")
    def getFormula(self):
        """
        Gets the value of :py:attr:`formula`.
        """
        return self.getOrDefault(self.formula)

    def _create_model(self, java_model):
        return RFormulaModel(java_model)

    def __str__(self):
        formulaStr = self.getFormula() if self.isDefined(self.formula) else ""
        return "RFormula(%s) (uid=%s)" % (formulaStr, self.uid)


class RFormulaModel(JavaModel, JavaMLReadable, JavaMLWritable):
    """
    .. note:: Experimental

    Model fitted by :py:class:`RFormula`. Fitting is required to determine the
    factor levels of formula terms.

    .. versionadded:: 1.5.0
    """

    def __str__(self):
        resolvedFormula = self._call_java("resolvedFormula")
        return "RFormulaModel(%s) (uid=%s)" % (resolvedFormula, self.uid)


@inherit_doc
class ChiSqSelector(JavaEstimator, HasFeaturesCol, HasOutputCol, HasLabelCol, JavaMLReadable,
                    JavaMLWritable):
    """
    .. note:: Experimental

    Chi-Squared feature selection, which selects categorical features to use for predicting a
    categorical label.

    >>> from pyspark.ml.linalg import Vectors
    >>> df = spark.createDataFrame(
    ...    [(Vectors.dense([0.0, 0.0, 18.0, 1.0]), 1.0),
    ...     (Vectors.dense([0.0, 1.0, 12.0, 0.0]), 0.0),
    ...     (Vectors.dense([1.0, 0.0, 15.0, 0.1]), 0.0)],
    ...    ["features", "label"])
    >>> selector = ChiSqSelector(numTopFeatures=1, outputCol="selectedFeatures")
    >>> model = selector.fit(df)
    >>> model.transform(df).head().selectedFeatures
    DenseVector([1.0])
    >>> model.selectedFeatures
    [3]
    >>> chiSqSelectorPath = temp_path + "/chi-sq-selector"
    >>> selector.save(chiSqSelectorPath)
    >>> loadedSelector = ChiSqSelector.load(chiSqSelectorPath)
    >>> loadedSelector.getNumTopFeatures() == selector.getNumTopFeatures()
    True
    >>> modelPath = temp_path + "/chi-sq-selector-model"
    >>> model.save(modelPath)
    >>> loadedModel = ChiSqSelectorModel.load(modelPath)
    >>> loadedModel.selectedFeatures == model.selectedFeatures
    True

    .. versionadded:: 2.0.0
    """

    selectorType = Param(Params._dummy(), "selectorType",
                         "The selector type of the ChisqSelector. " +
                         "Supported options: kbest (default), percentile, fpr, fdr and fwe.",
                         typeConverter=TypeConverters.toString)

    numTopFeatures = \
        Param(Params._dummy(), "numTopFeatures",
              "Number of features that selector will select, ordered by statistics value " +
              "descending. If the number of features is < numTopFeatures, then this will select " +
              "all features.", typeConverter=TypeConverters.toInt)

    percentile = Param(Params._dummy(), "percentile", "Percentile of features that selector " +
                       "will select, ordered by statistics value descending.",
                       typeConverter=TypeConverters.toFloat)

    alpha = Param(Params._dummy(), "alpha", "alpha means the highest p-value for features " +
                  "to be kept when select type is fpr, alpha means the highest uncorrected " +
                  "p-value for features to to kept when select type is fdr and fwe.",
                  typeConverter=TypeConverters.toFloat)

    @keyword_only
    def __init__(self, numTopFeatures=50, featuresCol="features", outputCol=None,
                 labelCol="label", selectorType="kbest", percentile=0.1, alpha=0.05):
        """
        __init__(self, numTopFeatures=50, featuresCol="features", outputCol=None, \
                 labelCol="label", selectorType="kbest", percentile=0.1, alpha=0.05)
        """
        super(ChiSqSelector, self).__init__()
        self._java_obj = self._new_java_obj("org.apache.spark.ml.feature.ChiSqSelector", self.uid)
        self._setDefault(numTopFeatures=50, selectorType="kbest", percentile=0.1, alpha=0.05)
        kwargs = self.__init__._input_kwargs
        self.setParams(**kwargs)

    @keyword_only
    @since("2.0.0")
    def setParams(self, numTopFeatures=50, featuresCol="features", outputCol=None,
                  labelCol="labels", selectorType="kbest", percentile=0.1, alpha=0.05):
        """
        setParams(self, numTopFeatures=50, featuresCol="features", outputCol=None, \
                  labelCol="labels", selectorType="kbest", percentile=0.1, alpha=0.05)
        Sets params for this ChiSqSelector.
        """
        kwargs = self.setParams._input_kwargs
        return self._set(**kwargs)

    @since("2.1.0")
    def setSelectorType(self, value):
        """
        Sets the value of :py:attr:`selectorType`.
        """
        return self._set(selectorType=value)

    @since("2.1.0")
    def getSelectorType(self):
        """
        Gets the value of selectorType or its default value.
        """
        return self.getOrDefault(self.selectorType)

    @since("2.0.0")
    def setNumTopFeatures(self, value):
        """
        Sets the value of :py:attr:`numTopFeatures`.
        Only applicable when selectorType = "kbest".
        """
        return self._set(numTopFeatures=value)

    @since("2.0.0")
    def getNumTopFeatures(self):
        """
        Gets the value of numTopFeatures or its default value.
        """
        return self.getOrDefault(self.numTopFeatures)

    @since("2.1.0")
    def setPercentile(self, value):
        """
        Sets the value of :py:attr:`percentile`.
        Only applicable when selectorType = "percentile".
        """
        return self._set(percentile=value)

    @since("2.1.0")
    def getPercentile(self):
        """
        Gets the value of percentile or its default value.
        """
        return self.getOrDefault(self.percentile)

    @since("2.1.0")
    def setAlpha(self, value):
        """
        Sets the value of :py:attr:`alpha`.
        """
        return self._set(alpha=value)

    @since("2.1.0")
    def getAlpha(self):
        """
        Gets the value of alpha or its default value.
        """
        return self.getOrDefault(self.alpha)

    def _create_model(self, java_model):
        return ChiSqSelectorModel(java_model)


class ChiSqSelectorModel(JavaModel, JavaMLReadable, JavaMLWritable):
    """
    .. note:: Experimental

    Model fitted by :py:class:`ChiSqSelector`.

    .. versionadded:: 2.0.0
    """

    @property
    @since("2.0.0")
    def selectedFeatures(self):
        """
        List of indices to select (filter).
        """
        return self._call_java("selectedFeatures")


if __name__ == "__main__":
    import doctest
    import tempfile

    import pyspark.ml.feature
    from pyspark.sql import Row, SparkSession

    globs = globals().copy()
    features = pyspark.ml.feature.__dict__.copy()
    globs.update(features)

    # The small batch size here ensures that we see multiple batches,
    # even in these small test examples:
    spark = SparkSession.builder\
        .master("local[2]")\
        .appName("ml.feature tests")\
        .getOrCreate()
    sc = spark.sparkContext
    globs['sc'] = sc
    globs['spark'] = spark
    testData = sc.parallelize([Row(id=0, label="a"), Row(id=1, label="b"),
                               Row(id=2, label="c"), Row(id=3, label="a"),
                               Row(id=4, label="a"), Row(id=5, label="c")], 2)
    globs['stringIndDf'] = spark.createDataFrame(testData)
    temp_path = tempfile.mkdtemp()
    globs['temp_path'] = temp_path
    try:
        (failure_count, test_count) = doctest.testmod(globs=globs, optionflags=doctest.ELLIPSIS)
        spark.stop()
    finally:
        from shutil import rmtree
        try:
            rmtree(temp_path)
        except OSError:
            pass
    if failure_count:
        exit(-1)
