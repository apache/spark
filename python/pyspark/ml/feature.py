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

from pyspark.rdd import ignore_unicode_prefix
from pyspark.ml.param.shared import *
from pyspark.ml.util import keyword_only
from pyspark.ml.wrapper import JavaEstimator, JavaModel, JavaTransformer
from pyspark.mllib.common import inherit_doc
from pyspark.mllib.linalg import _convert_to_vector

__all__ = ['Binarizer', 'Bucketizer', 'ElementwiseProduct', 'HashingTF', 'IDF', 'IDFModel',
           'NGram', 'Normalizer', 'OneHotEncoder', 'PolynomialExpansion', 'RegexTokenizer',
           'StandardScaler', 'StandardScalerModel', 'StringIndexer', 'StringIndexerModel',
           'Tokenizer', 'VectorAssembler', 'VectorIndexer', 'Word2Vec', 'Word2VecModel',
           'PCA', 'PCAModel', 'RFormula', 'RFormulaModel']


@inherit_doc
class Binarizer(JavaTransformer, HasInputCol, HasOutputCol):
    """
    Binarize a column of continuous features given a threshold.

    >>> df = sqlContext.createDataFrame([(0.5,)], ["values"])
    >>> binarizer = Binarizer(threshold=1.0, inputCol="values", outputCol="features")
    >>> binarizer.transform(df).head().features
    0.0
    >>> binarizer.setParams(outputCol="freqs").transform(df).head().freqs
    0.0
    >>> params = {binarizer.threshold: -0.5, binarizer.outputCol: "vector"}
    >>> binarizer.transform(df, params).head().vector
    1.0
    """

    # a placeholder to make it appear in the generated doc
    threshold = Param(Params._dummy(), "threshold",
                      "threshold in binary classification prediction, in range [0, 1]")

    @keyword_only
    def __init__(self, threshold=0.0, inputCol=None, outputCol=None):
        """
        __init__(self, threshold=0.0, inputCol=None, outputCol=None)
        """
        super(Binarizer, self).__init__()
        self._java_obj = self._new_java_obj("org.apache.spark.ml.feature.Binarizer", self.uid)
        self.threshold = Param(self, "threshold",
                               "threshold in binary classification prediction, in range [0, 1]")
        self._setDefault(threshold=0.0)
        kwargs = self.__init__._input_kwargs
        self.setParams(**kwargs)

    @keyword_only
    def setParams(self, threshold=0.0, inputCol=None, outputCol=None):
        """
        setParams(self, threshold=0.0, inputCol=None, outputCol=None)
        Sets params for this Binarizer.
        """
        kwargs = self.setParams._input_kwargs
        return self._set(**kwargs)

    def setThreshold(self, value):
        """
        Sets the value of :py:attr:`threshold`.
        """
        self._paramMap[self.threshold] = value
        return self

    def getThreshold(self):
        """
        Gets the value of threshold or its default value.
        """
        return self.getOrDefault(self.threshold)


@inherit_doc
class Bucketizer(JavaTransformer, HasInputCol, HasOutputCol):
    """
    Maps a column of continuous features to a column of feature buckets.

    >>> df = sqlContext.createDataFrame([(0.1,), (0.4,), (1.2,), (1.5,)], ["values"])
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
    """

    # a placeholder to make it appear in the generated doc
    splits = \
        Param(Params._dummy(), "splits",
              "Split points for mapping continuous features into buckets. With n+1 splits, " +
              "there are n buckets. A bucket defined by splits x,y holds values in the " +
              "range [x,y) except the last bucket, which also includes y. The splits " +
              "should be strictly increasing. Values at -inf, inf must be explicitly " +
              "provided to cover all Double values; otherwise, values outside the splits " +
              "specified will be treated as errors.")

    @keyword_only
    def __init__(self, splits=None, inputCol=None, outputCol=None):
        """
        __init__(self, splits=None, inputCol=None, outputCol=None)
        """
        super(Bucketizer, self).__init__()
        self._java_obj = self._new_java_obj("org.apache.spark.ml.feature.Bucketizer", self.uid)
        #: param for Splitting points for mapping continuous features into buckets. With n+1 splits,
        #  there are n buckets. A bucket defined by splits x,y holds values in the range [x,y)
        #  except the last bucket, which also includes y. The splits should be strictly increasing.
        #  Values at -inf, inf must be explicitly provided to cover all Double values; otherwise,
        #  values outside the splits specified will be treated as errors.
        self.splits = \
            Param(self, "splits",
                  "Split points for mapping continuous features into buckets. With n+1 splits, " +
                  "there are n buckets. A bucket defined by splits x,y holds values in the " +
                  "range [x,y) except the last bucket, which also includes y. The splits " +
                  "should be strictly increasing. Values at -inf, inf must be explicitly " +
                  "provided to cover all Double values; otherwise, values outside the splits " +
                  "specified will be treated as errors.")
        kwargs = self.__init__._input_kwargs
        self.setParams(**kwargs)

    @keyword_only
    def setParams(self, splits=None, inputCol=None, outputCol=None):
        """
        setParams(self, splits=None, inputCol=None, outputCol=None)
        Sets params for this Bucketizer.
        """
        kwargs = self.setParams._input_kwargs
        return self._set(**kwargs)

    def setSplits(self, value):
        """
        Sets the value of :py:attr:`splits`.
        """
        self._paramMap[self.splits] = value
        return self

    def getSplits(self):
        """
        Gets the value of threshold or its default value.
        """
        return self.getOrDefault(self.splits)


@inherit_doc
class ElementwiseProduct(JavaTransformer, HasInputCol, HasOutputCol):
    """
    Outputs the Hadamard product (i.e., the element-wise product) of each input vector
    with a provided "weight" vector. In other words, it scales each column of the dataset
    by a scalar multiplier.

    >>> from pyspark.mllib.linalg import Vectors
    >>> df = sqlContext.createDataFrame([(Vectors.dense([2.0, 1.0, 3.0]),)], ["values"])
    >>> ep = ElementwiseProduct(scalingVec=Vectors.dense([1.0, 2.0, 3.0]),
    ...     inputCol="values", outputCol="eprod")
    >>> ep.transform(df).head().eprod
    DenseVector([2.0, 2.0, 9.0])
    >>> ep.setParams(scalingVec=Vectors.dense([2.0, 3.0, 5.0])).transform(df).head().eprod
    DenseVector([4.0, 3.0, 15.0])
    """

    # a placeholder to make it appear in the generated doc
    scalingVec = Param(Params._dummy(), "scalingVec", "vector for hadamard product, " +
                       "it must be MLlib Vector type.")

    @keyword_only
    def __init__(self, scalingVec=None, inputCol=None, outputCol=None):
        """
        __init__(self, scalingVec=None, inputCol=None, outputCol=None)
        """
        super(ElementwiseProduct, self).__init__()
        self._java_obj = self._new_java_obj("org.apache.spark.ml.feature.ElementwiseProduct",
                                            self.uid)
        self.scalingVec = Param(self, "scalingVec", "vector for hadamard product, " +
                                "it must be MLlib Vector type.")
        kwargs = self.__init__._input_kwargs
        self.setParams(**kwargs)

    @keyword_only
    def setParams(self, scalingVec=None, inputCol=None, outputCol=None):
        """
        setParams(self, scalingVec=None, inputCol=None, outputCol=None)
        Sets params for this ElementwiseProduct.
        """
        kwargs = self.setParams._input_kwargs
        return self._set(**kwargs)

    def setScalingVec(self, value):
        """
        Sets the value of :py:attr:`scalingVec`.
        """
        self._paramMap[self.scalingVec] = value
        return self

    def getScalingVec(self):
        """
        Gets the value of scalingVec or its default value.
        """
        return self.getOrDefault(self.scalingVec)


@inherit_doc
class HashingTF(JavaTransformer, HasInputCol, HasOutputCol, HasNumFeatures):
    """
    Maps a sequence of terms to their term frequencies using the
    hashing trick.

    >>> df = sqlContext.createDataFrame([(["a", "b", "c"],)], ["words"])
    >>> hashingTF = HashingTF(numFeatures=10, inputCol="words", outputCol="features")
    >>> hashingTF.transform(df).head().features
    SparseVector(10, {7: 1.0, 8: 1.0, 9: 1.0})
    >>> hashingTF.setParams(outputCol="freqs").transform(df).head().freqs
    SparseVector(10, {7: 1.0, 8: 1.0, 9: 1.0})
    >>> params = {hashingTF.numFeatures: 5, hashingTF.outputCol: "vector"}
    >>> hashingTF.transform(df, params).head().vector
    SparseVector(5, {2: 1.0, 3: 1.0, 4: 1.0})
    """

    @keyword_only
    def __init__(self, numFeatures=1 << 18, inputCol=None, outputCol=None):
        """
        __init__(self, numFeatures=1 << 18, inputCol=None, outputCol=None)
        """
        super(HashingTF, self).__init__()
        self._java_obj = self._new_java_obj("org.apache.spark.ml.feature.HashingTF", self.uid)
        self._setDefault(numFeatures=1 << 18)
        kwargs = self.__init__._input_kwargs
        self.setParams(**kwargs)

    @keyword_only
    def setParams(self, numFeatures=1 << 18, inputCol=None, outputCol=None):
        """
        setParams(self, numFeatures=1 << 18, inputCol=None, outputCol=None)
        Sets params for this HashingTF.
        """
        kwargs = self.setParams._input_kwargs
        return self._set(**kwargs)


@inherit_doc
class IDF(JavaEstimator, HasInputCol, HasOutputCol):
    """
    Compute the Inverse Document Frequency (IDF) given a collection of documents.

    >>> from pyspark.mllib.linalg import DenseVector
    >>> df = sqlContext.createDataFrame([(DenseVector([1.0, 2.0]),),
    ...     (DenseVector([0.0, 1.0]),), (DenseVector([3.0, 0.2]),)], ["tf"])
    >>> idf = IDF(minDocFreq=3, inputCol="tf", outputCol="idf")
    >>> idf.fit(df).transform(df).head().idf
    DenseVector([0.0, 0.0])
    >>> idf.setParams(outputCol="freqs").fit(df).transform(df).collect()[1].freqs
    DenseVector([0.0, 0.0])
    >>> params = {idf.minDocFreq: 1, idf.outputCol: "vector"}
    >>> idf.fit(df, params).transform(df).head().vector
    DenseVector([0.2877, 0.0])
    """

    # a placeholder to make it appear in the generated doc
    minDocFreq = Param(Params._dummy(), "minDocFreq",
                       "minimum of documents in which a term should appear for filtering")

    @keyword_only
    def __init__(self, minDocFreq=0, inputCol=None, outputCol=None):
        """
        __init__(self, minDocFreq=0, inputCol=None, outputCol=None)
        """
        super(IDF, self).__init__()
        self._java_obj = self._new_java_obj("org.apache.spark.ml.feature.IDF", self.uid)
        self.minDocFreq = Param(self, "minDocFreq",
                                "minimum of documents in which a term should appear for filtering")
        self._setDefault(minDocFreq=0)
        kwargs = self.__init__._input_kwargs
        self.setParams(**kwargs)

    @keyword_only
    def setParams(self, minDocFreq=0, inputCol=None, outputCol=None):
        """
        setParams(self, minDocFreq=0, inputCol=None, outputCol=None)
        Sets params for this IDF.
        """
        kwargs = self.setParams._input_kwargs
        return self._set(**kwargs)

    def setMinDocFreq(self, value):
        """
        Sets the value of :py:attr:`minDocFreq`.
        """
        self._paramMap[self.minDocFreq] = value
        return self

    def getMinDocFreq(self):
        """
        Gets the value of minDocFreq or its default value.
        """
        return self.getOrDefault(self.minDocFreq)

    def _create_model(self, java_model):
        return IDFModel(java_model)


class IDFModel(JavaModel):
    """
    Model fitted by IDF.
    """


@inherit_doc
@ignore_unicode_prefix
class NGram(JavaTransformer, HasInputCol, HasOutputCol):
    """
    A feature transformer that converts the input array of strings into an array of n-grams. Null
    values in the input array are ignored.
    It returns an array of n-grams where each n-gram is represented by a space-separated string of
    words.
    When the input is empty, an empty array is returned.
    When the input array length is less than n (number of elements per n-gram), no n-grams are
    returned.

    >>> df = sqlContext.createDataFrame([Row(inputTokens=["a", "b", "c", "d", "e"])])
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
    """

    # a placeholder to make it appear in the generated doc
    n = Param(Params._dummy(), "n", "number of elements per n-gram (>=1)")

    @keyword_only
    def __init__(self, n=2, inputCol=None, outputCol=None):
        """
        __init__(self, n=2, inputCol=None, outputCol=None)
        """
        super(NGram, self).__init__()
        self._java_obj = self._new_java_obj("org.apache.spark.ml.feature.NGram", self.uid)
        self.n = Param(self, "n", "number of elements per n-gram (>=1)")
        self._setDefault(n=2)
        kwargs = self.__init__._input_kwargs
        self.setParams(**kwargs)

    @keyword_only
    def setParams(self, n=2, inputCol=None, outputCol=None):
        """
        setParams(self, n=2, inputCol=None, outputCol=None)
        Sets params for this NGram.
        """
        kwargs = self.setParams._input_kwargs
        return self._set(**kwargs)

    def setN(self, value):
        """
        Sets the value of :py:attr:`n`.
        """
        self._paramMap[self.n] = value
        return self

    def getN(self):
        """
        Gets the value of n or its default value.
        """
        return self.getOrDefault(self.n)


@inherit_doc
class Normalizer(JavaTransformer, HasInputCol, HasOutputCol):
    """
     Normalize a vector to have unit norm using the given p-norm.

    >>> from pyspark.mllib.linalg import Vectors
    >>> svec = Vectors.sparse(4, {1: 4.0, 3: 3.0})
    >>> df = sqlContext.createDataFrame([(Vectors.dense([3.0, -4.0]), svec)], ["dense", "sparse"])
    >>> normalizer = Normalizer(p=2.0, inputCol="dense", outputCol="features")
    >>> normalizer.transform(df).head().features
    DenseVector([0.6, -0.8])
    >>> normalizer.setParams(inputCol="sparse", outputCol="freqs").transform(df).head().freqs
    SparseVector(4, {1: 0.8, 3: 0.6})
    >>> params = {normalizer.p: 1.0, normalizer.inputCol: "dense", normalizer.outputCol: "vector"}
    >>> normalizer.transform(df, params).head().vector
    DenseVector([0.4286, -0.5714])
    """

    # a placeholder to make it appear in the generated doc
    p = Param(Params._dummy(), "p", "the p norm value.")

    @keyword_only
    def __init__(self, p=2.0, inputCol=None, outputCol=None):
        """
        __init__(self, p=2.0, inputCol=None, outputCol=None)
        """
        super(Normalizer, self).__init__()
        self._java_obj = self._new_java_obj("org.apache.spark.ml.feature.Normalizer", self.uid)
        self.p = Param(self, "p", "the p norm value.")
        self._setDefault(p=2.0)
        kwargs = self.__init__._input_kwargs
        self.setParams(**kwargs)

    @keyword_only
    def setParams(self, p=2.0, inputCol=None, outputCol=None):
        """
        setParams(self, p=2.0, inputCol=None, outputCol=None)
        Sets params for this Normalizer.
        """
        kwargs = self.setParams._input_kwargs
        return self._set(**kwargs)

    def setP(self, value):
        """
        Sets the value of :py:attr:`p`.
        """
        self._paramMap[self.p] = value
        return self

    def getP(self):
        """
        Gets the value of p or its default value.
        """
        return self.getOrDefault(self.p)


@inherit_doc
class OneHotEncoder(JavaTransformer, HasInputCol, HasOutputCol):
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
    """

    # a placeholder to make it appear in the generated doc
    dropLast = Param(Params._dummy(), "dropLast", "whether to drop the last category")

    @keyword_only
    def __init__(self, dropLast=True, inputCol=None, outputCol=None):
        """
        __init__(self, includeFirst=True, inputCol=None, outputCol=None)
        """
        super(OneHotEncoder, self).__init__()
        self._java_obj = self._new_java_obj("org.apache.spark.ml.feature.OneHotEncoder", self.uid)
        self.dropLast = Param(self, "dropLast", "whether to drop the last category")
        self._setDefault(dropLast=True)
        kwargs = self.__init__._input_kwargs
        self.setParams(**kwargs)

    @keyword_only
    def setParams(self, dropLast=True, inputCol=None, outputCol=None):
        """
        setParams(self, dropLast=True, inputCol=None, outputCol=None)
        Sets params for this OneHotEncoder.
        """
        kwargs = self.setParams._input_kwargs
        return self._set(**kwargs)

    def setDropLast(self, value):
        """
        Sets the value of :py:attr:`dropLast`.
        """
        self._paramMap[self.dropLast] = value
        return self

    def getDropLast(self):
        """
        Gets the value of dropLast or its default value.
        """
        return self.getOrDefault(self.dropLast)


@inherit_doc
class PolynomialExpansion(JavaTransformer, HasInputCol, HasOutputCol):
    """
    Perform feature expansion in a polynomial space. As said in wikipedia of Polynomial Expansion,
    which is available at `http://en.wikipedia.org/wiki/Polynomial_expansion`, "In mathematics, an
    expansion of a product of sums expresses it as a sum of products by using the fact that
    multiplication distributes over addition". Take a 2-variable feature vector as an example:
    `(x, y)`, if we want to expand it with degree 2, then we get `(x, x * x, y, x * y, y * y)`.

    >>> from pyspark.mllib.linalg import Vectors
    >>> df = sqlContext.createDataFrame([(Vectors.dense([0.5, 2.0]),)], ["dense"])
    >>> px = PolynomialExpansion(degree=2, inputCol="dense", outputCol="expanded")
    >>> px.transform(df).head().expanded
    DenseVector([0.5, 0.25, 2.0, 1.0, 4.0])
    >>> px.setParams(outputCol="test").transform(df).head().test
    DenseVector([0.5, 0.25, 2.0, 1.0, 4.0])
    """

    # a placeholder to make it appear in the generated doc
    degree = Param(Params._dummy(), "degree", "the polynomial degree to expand (>= 1)")

    @keyword_only
    def __init__(self, degree=2, inputCol=None, outputCol=None):
        """
        __init__(self, degree=2, inputCol=None, outputCol=None)
        """
        super(PolynomialExpansion, self).__init__()
        self._java_obj = self._new_java_obj(
            "org.apache.spark.ml.feature.PolynomialExpansion", self.uid)
        self.degree = Param(self, "degree", "the polynomial degree to expand (>= 1)")
        self._setDefault(degree=2)
        kwargs = self.__init__._input_kwargs
        self.setParams(**kwargs)

    @keyword_only
    def setParams(self, degree=2, inputCol=None, outputCol=None):
        """
        setParams(self, degree=2, inputCol=None, outputCol=None)
        Sets params for this PolynomialExpansion.
        """
        kwargs = self.setParams._input_kwargs
        return self._set(**kwargs)

    def setDegree(self, value):
        """
        Sets the value of :py:attr:`degree`.
        """
        self._paramMap[self.degree] = value
        return self

    def getDegree(self):
        """
        Gets the value of degree or its default value.
        """
        return self.getOrDefault(self.degree)


@inherit_doc
@ignore_unicode_prefix
class RegexTokenizer(JavaTransformer, HasInputCol, HasOutputCol):
    """
    A regex based tokenizer that extracts tokens either by using the
    provided regex pattern (in Java dialect) to split the text
    (default) or repeatedly matching the regex (if gaps is false).
    Optional parameters also allow filtering tokens using a minimal
    length.
    It returns an array of strings that can be empty.

    >>> df = sqlContext.createDataFrame([("a b  c",)], ["text"])
    >>> reTokenizer = RegexTokenizer(inputCol="text", outputCol="words")
    >>> reTokenizer.transform(df).head()
    Row(text=u'a b  c', words=[u'a', u'b', u'c'])
    >>> # Change a parameter.
    >>> reTokenizer.setParams(outputCol="tokens").transform(df).head()
    Row(text=u'a b  c', tokens=[u'a', u'b', u'c'])
    >>> # Temporarily modify a parameter.
    >>> reTokenizer.transform(df, {reTokenizer.outputCol: "words"}).head()
    Row(text=u'a b  c', words=[u'a', u'b', u'c'])
    >>> reTokenizer.transform(df).head()
    Row(text=u'a b  c', tokens=[u'a', u'b', u'c'])
    >>> # Must use keyword arguments to specify params.
    >>> reTokenizer.setParams("text")
    Traceback (most recent call last):
        ...
    TypeError: Method setParams forces keyword arguments.
    """

    # a placeholder to make it appear in the generated doc
    minTokenLength = Param(Params._dummy(), "minTokenLength", "minimum token length (>= 0)")
    gaps = Param(Params._dummy(), "gaps", "whether regex splits on gaps (True) or matches tokens")
    pattern = Param(Params._dummy(), "pattern", "regex pattern (Java dialect) used for tokenizing")

    @keyword_only
    def __init__(self, minTokenLength=1, gaps=True, pattern="\\s+", inputCol=None, outputCol=None):
        """
        __init__(self, minTokenLength=1, gaps=True, pattern="\\s+", inputCol=None, outputCol=None)
        """
        super(RegexTokenizer, self).__init__()
        self._java_obj = self._new_java_obj("org.apache.spark.ml.feature.RegexTokenizer", self.uid)
        self.minTokenLength = Param(self, "minTokenLength", "minimum token length (>= 0)")
        self.gaps = Param(self, "gaps", "whether regex splits on gaps (True) or matches tokens")
        self.pattern = Param(self, "pattern", "regex pattern (Java dialect) used for tokenizing")
        self._setDefault(minTokenLength=1, gaps=True, pattern="\\s+")
        kwargs = self.__init__._input_kwargs
        self.setParams(**kwargs)

    @keyword_only
    def setParams(self, minTokenLength=1, gaps=True, pattern="\\s+", inputCol=None, outputCol=None):
        """
        setParams(self, minTokenLength=1, gaps=True, pattern="\\s+", inputCol=None, outputCol=None)
        Sets params for this RegexTokenizer.
        """
        kwargs = self.setParams._input_kwargs
        return self._set(**kwargs)

    def setMinTokenLength(self, value):
        """
        Sets the value of :py:attr:`minTokenLength`.
        """
        self._paramMap[self.minTokenLength] = value
        return self

    def getMinTokenLength(self):
        """
        Gets the value of minTokenLength or its default value.
        """
        return self.getOrDefault(self.minTokenLength)

    def setGaps(self, value):
        """
        Sets the value of :py:attr:`gaps`.
        """
        self._paramMap[self.gaps] = value
        return self

    def getGaps(self):
        """
        Gets the value of gaps or its default value.
        """
        return self.getOrDefault(self.gaps)

    def setPattern(self, value):
        """
        Sets the value of :py:attr:`pattern`.
        """
        self._paramMap[self.pattern] = value
        return self

    def getPattern(self):
        """
        Gets the value of pattern or its default value.
        """
        return self.getOrDefault(self.pattern)


@inherit_doc
class StandardScaler(JavaEstimator, HasInputCol, HasOutputCol):
    """
    Standardizes features by removing the mean and scaling to unit variance using column summary
    statistics on the samples in the training set.

    >>> from pyspark.mllib.linalg import Vectors
    >>> df = sqlContext.createDataFrame([(Vectors.dense([0.0]),), (Vectors.dense([2.0]),)], ["a"])
    >>> standardScaler = StandardScaler(inputCol="a", outputCol="scaled")
    >>> model = standardScaler.fit(df)
    >>> model.mean
    DenseVector([1.0])
    >>> model.std
    DenseVector([1.4142])
    >>> model.transform(df).collect()[1].scaled
    DenseVector([1.4142])
    """

    # a placeholder to make it appear in the generated doc
    withMean = Param(Params._dummy(), "withMean", "Center data with mean")
    withStd = Param(Params._dummy(), "withStd", "Scale to unit standard deviation")

    @keyword_only
    def __init__(self, withMean=False, withStd=True, inputCol=None, outputCol=None):
        """
        __init__(self, withMean=False, withStd=True, inputCol=None, outputCol=None)
        """
        super(StandardScaler, self).__init__()
        self._java_obj = self._new_java_obj("org.apache.spark.ml.feature.StandardScaler", self.uid)
        self.withMean = Param(self, "withMean", "Center data with mean")
        self.withStd = Param(self, "withStd", "Scale to unit standard deviation")
        self._setDefault(withMean=False, withStd=True)
        kwargs = self.__init__._input_kwargs
        self.setParams(**kwargs)

    @keyword_only
    def setParams(self, withMean=False, withStd=True, inputCol=None, outputCol=None):
        """
        setParams(self, withMean=False, withStd=True, inputCol=None, outputCol=None)
        Sets params for this StandardScaler.
        """
        kwargs = self.setParams._input_kwargs
        return self._set(**kwargs)

    def setWithMean(self, value):
        """
        Sets the value of :py:attr:`withMean`.
        """
        self._paramMap[self.withMean] = value
        return self

    def getWithMean(self):
        """
        Gets the value of withMean or its default value.
        """
        return self.getOrDefault(self.withMean)

    def setWithStd(self, value):
        """
        Sets the value of :py:attr:`withStd`.
        """
        self._paramMap[self.withStd] = value
        return self

    def getWithStd(self):
        """
        Gets the value of withStd or its default value.
        """
        return self.getOrDefault(self.withStd)

    def _create_model(self, java_model):
        return StandardScalerModel(java_model)


class StandardScalerModel(JavaModel):
    """
    Model fitted by StandardScaler.
    """

    @property
    def std(self):
        """
        Standard deviation of the StandardScalerModel.
        """
        return self._call_java("std")

    @property
    def mean(self):
        """
        Mean of the StandardScalerModel.
        """
        return self._call_java("mean")


@inherit_doc
class StringIndexer(JavaEstimator, HasInputCol, HasOutputCol):
    """
    A label indexer that maps a string column of labels to an ML column of label indices.
    If the input column is numeric, we cast it to string and index the string values.
    The indices are in [0, numLabels), ordered by label frequencies.
    So the most frequent label gets index 0.

    >>> stringIndexer = StringIndexer(inputCol="label", outputCol="indexed")
    >>> model = stringIndexer.fit(stringIndDf)
    >>> td = model.transform(stringIndDf)
    >>> sorted(set([(i[0], i[1]) for i in td.select(td.id, td.indexed).collect()]),
    ...     key=lambda x: x[0])
    [(0, 0.0), (1, 2.0), (2, 1.0), (3, 0.0), (4, 0.0), (5, 1.0)]
    """

    @keyword_only
    def __init__(self, inputCol=None, outputCol=None):
        """
        __init__(self, inputCol=None, outputCol=None)
        """
        super(StringIndexer, self).__init__()
        self._java_obj = self._new_java_obj("org.apache.spark.ml.feature.StringIndexer", self.uid)
        kwargs = self.__init__._input_kwargs
        self.setParams(**kwargs)

    @keyword_only
    def setParams(self, inputCol=None, outputCol=None):
        """
        setParams(self, inputCol=None, outputCol=None)
        Sets params for this StringIndexer.
        """
        kwargs = self.setParams._input_kwargs
        return self._set(**kwargs)

    def _create_model(self, java_model):
        return StringIndexerModel(java_model)


class StringIndexerModel(JavaModel):
    """
    Model fitted by StringIndexer.
    """


@inherit_doc
@ignore_unicode_prefix
class Tokenizer(JavaTransformer, HasInputCol, HasOutputCol):
    """
    A tokenizer that converts the input string to lowercase and then
    splits it by white spaces.

    >>> df = sqlContext.createDataFrame([("a b c",)], ["text"])
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
    def setParams(self, inputCol=None, outputCol=None):
        """
        setParams(self, inputCol="input", outputCol="output")
        Sets params for this Tokenizer.
        """
        kwargs = self.setParams._input_kwargs
        return self._set(**kwargs)


@inherit_doc
class VectorAssembler(JavaTransformer, HasInputCols, HasOutputCol):
    """
    A feature transformer that merges multiple columns into a vector column.

    >>> df = sqlContext.createDataFrame([(1, 0, 3)], ["a", "b", "c"])
    >>> vecAssembler = VectorAssembler(inputCols=["a", "b", "c"], outputCol="features")
    >>> vecAssembler.transform(df).head().features
    DenseVector([1.0, 0.0, 3.0])
    >>> vecAssembler.setParams(outputCol="freqs").transform(df).head().freqs
    DenseVector([1.0, 0.0, 3.0])
    >>> params = {vecAssembler.inputCols: ["b", "a"], vecAssembler.outputCol: "vector"}
    >>> vecAssembler.transform(df, params).head().vector
    DenseVector([0.0, 1.0])
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
    def setParams(self, inputCols=None, outputCol=None):
        """
        setParams(self, inputCols=None, outputCol=None)
        Sets params for this VectorAssembler.
        """
        kwargs = self.setParams._input_kwargs
        return self._set(**kwargs)


@inherit_doc
class VectorIndexer(JavaEstimator, HasInputCol, HasOutputCol):
    """
    Class for indexing categorical feature columns in a dataset of [[Vector]].

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

    >>> from pyspark.mllib.linalg import Vectors
    >>> df = sqlContext.createDataFrame([(Vectors.dense([-1.0, 0.0]),),
    ...     (Vectors.dense([0.0, 1.0]),), (Vectors.dense([0.0, 2.0]),)], ["a"])
    >>> indexer = VectorIndexer(maxCategories=2, inputCol="a", outputCol="indexed")
    >>> model = indexer.fit(df)
    >>> model.transform(df).head().indexed
    DenseVector([1.0, 0.0])
    >>> indexer.setParams(outputCol="test").fit(df).transform(df).collect()[1].test
    DenseVector([0.0, 1.0])
    >>> params = {indexer.maxCategories: 3, indexer.outputCol: "vector"}
    >>> model2 = indexer.fit(df, params)
    >>> model2.transform(df).head().vector
    DenseVector([1.0, 0.0])
    """

    # a placeholder to make it appear in the generated doc
    maxCategories = Param(Params._dummy(), "maxCategories",
                          "Threshold for the number of values a categorical feature can take " +
                          "(>= 2). If a feature is found to have > maxCategories values, then " +
                          "it is declared continuous.")

    @keyword_only
    def __init__(self, maxCategories=20, inputCol=None, outputCol=None):
        """
        __init__(self, maxCategories=20, inputCol=None, outputCol=None)
        """
        super(VectorIndexer, self).__init__()
        self._java_obj = self._new_java_obj("org.apache.spark.ml.feature.VectorIndexer", self.uid)
        self.maxCategories = Param(self, "maxCategories",
                                   "Threshold for the number of values a categorical feature " +
                                   "can take (>= 2). If a feature is found to have " +
                                   "> maxCategories values, then it is declared continuous.")
        self._setDefault(maxCategories=20)
        kwargs = self.__init__._input_kwargs
        self.setParams(**kwargs)

    @keyword_only
    def setParams(self, maxCategories=20, inputCol=None, outputCol=None):
        """
        setParams(self, maxCategories=20, inputCol=None, outputCol=None)
        Sets params for this VectorIndexer.
        """
        kwargs = self.setParams._input_kwargs
        return self._set(**kwargs)

    def setMaxCategories(self, value):
        """
        Sets the value of :py:attr:`maxCategories`.
        """
        self._paramMap[self.maxCategories] = value
        return self

    def getMaxCategories(self):
        """
        Gets the value of maxCategories or its default value.
        """
        return self.getOrDefault(self.maxCategories)

    def _create_model(self, java_model):
        return VectorIndexerModel(java_model)


class VectorIndexerModel(JavaModel):
    """
    Model fitted by VectorIndexer.
    """


@inherit_doc
@ignore_unicode_prefix
class Word2Vec(JavaEstimator, HasStepSize, HasMaxIter, HasSeed, HasInputCol, HasOutputCol):
    """
    Word2Vec trains a model of `Map(String, Vector)`, i.e. transforms a word into a code for further
    natural language processing or machine learning process.

    >>> sent = ("a b " * 100 + "a c " * 10).split(" ")
    >>> doc = sqlContext.createDataFrame([(sent,), (sent,)], ["sentence"])
    >>> model = Word2Vec(vectorSize=5, seed=42, inputCol="sentence", outputCol="model").fit(doc)
    >>> model.getVectors().show()
    +----+--------------------+
    |word|              vector|
    +----+--------------------+
    |   a|[-0.3511952459812...|
    |   b|[0.29077222943305...|
    |   c|[0.02315592765808...|
    +----+--------------------+
    ...
    >>> model.findSynonyms("a", 2).show()
    +----+-------------------+
    |word|         similarity|
    +----+-------------------+
    |   b|0.29255685145799626|
    |   c|-0.5414068302988307|
    +----+-------------------+
    ...
    >>> model.transform(doc).head().model
    DenseVector([-0.0422, -0.5138, -0.2546, 0.6885, 0.276])
    """

    # a placeholder to make it appear in the generated doc
    vectorSize = Param(Params._dummy(), "vectorSize",
                       "the dimension of codes after transforming from words")
    numPartitions = Param(Params._dummy(), "numPartitions",
                          "number of partitions for sentences of words")
    minCount = Param(Params._dummy(), "minCount",
                     "the minimum number of times a token must appear to be included in the " +
                     "word2vec model's vocabulary")

    @keyword_only
    def __init__(self, vectorSize=100, minCount=5, numPartitions=1, stepSize=0.025, maxIter=1,
                 seed=None, inputCol=None, outputCol=None):
        """
        __init__(self, vectorSize=100, minCount=5, numPartitions=1, stepSize=0.025, maxIter=1, \
                 seed=None, inputCol=None, outputCol=None)
        """
        super(Word2Vec, self).__init__()
        self._java_obj = self._new_java_obj("org.apache.spark.ml.feature.Word2Vec", self.uid)
        self.vectorSize = Param(self, "vectorSize",
                                "the dimension of codes after transforming from words")
        self.numPartitions = Param(self, "numPartitions",
                                   "number of partitions for sentences of words")
        self.minCount = Param(self, "minCount",
                              "the minimum number of times a token must appear to be included " +
                              "in the word2vec model's vocabulary")
        self._setDefault(vectorSize=100, minCount=5, numPartitions=1, stepSize=0.025, maxIter=1,
                         seed=None)
        kwargs = self.__init__._input_kwargs
        self.setParams(**kwargs)

    @keyword_only
    def setParams(self, vectorSize=100, minCount=5, numPartitions=1, stepSize=0.025, maxIter=1,
                  seed=None, inputCol=None, outputCol=None):
        """
        setParams(self, minCount=5, numPartitions=1, stepSize=0.025, maxIter=1, seed=None, \
                 inputCol=None, outputCol=None)
        Sets params for this Word2Vec.
        """
        kwargs = self.setParams._input_kwargs
        return self._set(**kwargs)

    def setVectorSize(self, value):
        """
        Sets the value of :py:attr:`vectorSize`.
        """
        self._paramMap[self.vectorSize] = value
        return self

    def getVectorSize(self):
        """
        Gets the value of vectorSize or its default value.
        """
        return self.getOrDefault(self.vectorSize)

    def setNumPartitions(self, value):
        """
        Sets the value of :py:attr:`numPartitions`.
        """
        self._paramMap[self.numPartitions] = value
        return self

    def getNumPartitions(self):
        """
        Gets the value of numPartitions or its default value.
        """
        return self.getOrDefault(self.numPartitions)

    def setMinCount(self, value):
        """
        Sets the value of :py:attr:`minCount`.
        """
        self._paramMap[self.minCount] = value
        return self

    def getMinCount(self):
        """
        Gets the value of minCount or its default value.
        """
        return self.getOrDefault(self.minCount)

    def _create_model(self, java_model):
        return Word2VecModel(java_model)


class Word2VecModel(JavaModel):
    """
    Model fitted by Word2Vec.
    """

    def getVectors(self):
        """
        Returns the vector representation of the words as a dataframe
        with two fields, word and vector.
        """
        return self._call_java("getVectors")

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
class PCA(JavaEstimator, HasInputCol, HasOutputCol):
    """
    PCA trains a model to project vectors to a low-dimensional space using PCA.

    >>> from pyspark.mllib.linalg import Vectors
    >>> data = [(Vectors.sparse(5, [(1, 1.0), (3, 7.0)]),),
    ...     (Vectors.dense([2.0, 0.0, 3.0, 4.0, 5.0]),),
    ...     (Vectors.dense([4.0, 0.0, 0.0, 6.0, 7.0]),)]
    >>> df = sqlContext.createDataFrame(data,["features"])
    >>> pca = PCA(k=2, inputCol="features", outputCol="pca_features")
    >>> model = pca.fit(df)
    >>> model.transform(df).collect()[0].pca_features
    DenseVector([1.648..., -4.013...])
    """

    # a placeholder to make it appear in the generated doc
    k = Param(Params._dummy(), "k", "the number of principal components")

    @keyword_only
    def __init__(self, k=None, inputCol=None, outputCol=None):
        """
        __init__(self, k=None, inputCol=None, outputCol=None)
        """
        super(PCA, self).__init__()
        self._java_obj = self._new_java_obj("org.apache.spark.ml.feature.PCA", self.uid)
        self.k = Param(self, "k", "the number of principal components")
        kwargs = self.__init__._input_kwargs
        self.setParams(**kwargs)

    @keyword_only
    def setParams(self, k=None, inputCol=None, outputCol=None):
        """
        setParams(self, k=None, inputCol=None, outputCol=None)
        Set params for this PCA.
        """
        kwargs = self.setParams._input_kwargs
        return self._set(**kwargs)

    def setK(self, value):
        """
        Sets the value of :py:attr:`k`.
        """
        self._paramMap[self.k] = value
        return self

    def getK(self):
        """
        Gets the value of k or its default value.
        """
        return self.getOrDefault(self.k)

    def _create_model(self, java_model):
        return PCAModel(java_model)


class PCAModel(JavaModel):
    """
    Model fitted by PCA.
    """


@inherit_doc
class RFormula(JavaEstimator, HasFeaturesCol, HasLabelCol):
    """
    .. note:: Experimental

    Implements the transforms required for fitting a dataset against an
    R model formula. Currently we support a limited subset of the R
    operators, including '~', '+', '-', and '.'. Also see the R formula
    docs:
    http://stat.ethz.ch/R-manual/R-patched/library/stats/html/formula.html

    >>> df = sqlContext.createDataFrame([
    ...     (1.0, 1.0, "a"),
    ...     (0.0, 2.0, "b"),
    ...     (0.0, 0.0, "a")
    ... ], ["y", "x", "s"])
    >>> rf = RFormula(formula="y ~ x + s")
    >>> rf.fit(df).transform(df).show()
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
    """

    # a placeholder to make it appear in the generated doc
    formula = Param(Params._dummy(), "formula", "R model formula")

    @keyword_only
    def __init__(self, formula=None, featuresCol="features", labelCol="label"):
        """
        __init__(self, formula=None, featuresCol="features", labelCol="label")
        """
        super(RFormula, self).__init__()
        self._java_obj = self._new_java_obj("org.apache.spark.ml.feature.RFormula", self.uid)
        self.formula = Param(self, "formula", "R model formula")
        kwargs = self.__init__._input_kwargs
        self.setParams(**kwargs)

    @keyword_only
    def setParams(self, formula=None, featuresCol="features", labelCol="label"):
        """
        setParams(self, formula=None, featuresCol="features", labelCol="label")
        Sets params for RFormula.
        """
        kwargs = self.setParams._input_kwargs
        return self._set(**kwargs)

    def setFormula(self, value):
        """
        Sets the value of :py:attr:`formula`.
        """
        self._paramMap[self.formula] = value
        return self

    def getFormula(self):
        """
        Gets the value of :py:attr:`formula`.
        """
        return self.getOrDefault(self.formula)

    def _create_model(self, java_model):
        return RFormulaModel(java_model)


class RFormulaModel(JavaModel):
    """
    Model fitted by :py:class:`RFormula`.
    """


if __name__ == "__main__":
    import doctest
    from pyspark.context import SparkContext
    from pyspark.sql import Row, SQLContext
    globs = globals().copy()
    # The small batch size here ensures that we see multiple batches,
    # even in these small test examples:
    sc = SparkContext("local[2]", "ml.feature tests")
    sqlContext = SQLContext(sc)
    globs['sc'] = sc
    globs['sqlContext'] = sqlContext
    testData = sc.parallelize([Row(id=0, label="a"), Row(id=1, label="b"),
                               Row(id=2, label="c"), Row(id=3, label="a"),
                               Row(id=4, label="a"), Row(id=5, label="c")], 2)
    globs['stringIndDf'] = sqlContext.createDataFrame(testData)
    (failure_count, test_count) = doctest.testmod(globs=globs, optionflags=doctest.ELLIPSIS)
    sc.stop()
    if failure_count:
        exit(-1)
