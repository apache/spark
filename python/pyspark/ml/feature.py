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

from pyspark.rdd import ignore_unicode_prefix
from pyspark.ml.param.shared import *
from pyspark.ml.util import keyword_only
from pyspark.ml.wrapper import JavaEstimator, JavaModel, JavaTransformer
from pyspark.mllib.common import inherit_doc

__all__ = ['Binarizer', 'HashingTF', 'IDF', 'IDFModel', 'Normalizer', 'OneHotEncoder',
           'PolynomialExpansion', 'RegexTokenizer', 'StandardScaler', 'StandardScalerModel',
           'StringIndexer', 'StringIndexerModel', 'Tokenizer', 'VectorAssembler', 'VectorIndexer',
           'Word2Vec', 'Word2VecModel']


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

    _java_class = "org.apache.spark.ml.feature.Binarizer"
    # a placeholder to make it appear in the generated doc
    threshold = Param(Params._dummy(), "threshold",
                      "threshold in binary classification prediction, in range [0, 1]")

    @keyword_only
    def __init__(self, threshold=0.0, inputCol=None, outputCol=None):
        """
        __init__(self, threshold=0.0, inputCol=None, outputCol=None)
        """
        super(Binarizer, self).__init__()
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
        self.paramMap[self.threshold] = value
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

    _java_class = "org.apache.spark.ml.feature.Bucketizer"
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
        self.paramMap[self.splits] = value
        return self

    def getSplits(self):
        """
        Gets the value of threshold or its default value.
        """
        return self.getOrDefault(self.splits)


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

    _java_class = "org.apache.spark.ml.feature.HashingTF"

    @keyword_only
    def __init__(self, numFeatures=1 << 18, inputCol=None, outputCol=None):
        """
        __init__(self, numFeatures=1 << 18, inputCol=None, outputCol=None)
        """
        super(HashingTF, self).__init__()
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

    _java_class = "org.apache.spark.ml.feature.IDF"

    # a placeholder to make it appear in the generated doc
    minDocFreq = Param(Params._dummy(), "minDocFreq",
                       "minimum of documents in which a term should appear for filtering")

    @keyword_only
    def __init__(self, minDocFreq=0, inputCol=None, outputCol=None):
        """
        __init__(self, minDocFreq=0, inputCol=None, outputCol=None)
        """
        super(IDF, self).__init__()
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
        self.paramMap[self.minDocFreq] = value
        return self

    def getMinDocFreq(self):
        """
        Gets the value of minDocFreq or its default value.
        """
        return self.getOrDefault(self.minDocFreq)


class IDFModel(JavaModel):
    """
    Model fitted by IDF.
    """


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

    _java_class = "org.apache.spark.ml.feature.Normalizer"

    @keyword_only
    def __init__(self, p=2.0, inputCol=None, outputCol=None):
        """
        __init__(self, p=2.0, inputCol=None, outputCol=None)
        """
        super(Normalizer, self).__init__()
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
        self.paramMap[self.p] = value
        return self

    def getP(self):
        """
        Gets the value of p or its default value.
        """
        return self.getOrDefault(self.p)


@inherit_doc
class OneHotEncoder(JavaTransformer, HasInputCol, HasOutputCol):
    """
    A one-hot encoder that maps a column of label indices to a column of binary vectors, with
    at most a single one-value. By default, the binary vector has an element for each category, so
    with 5 categories, an input value of 2.0 would map to an output vector of
    (0.0, 0.0, 1.0, 0.0, 0.0). If includeFirst is set to false, the first category is omitted, so
    the output vector for the previous example would be (0.0, 1.0, 0.0, 0.0) and an input value
    of 0.0 would map to a vector of all zeros. Including the first category makes the vector columns
    linearly dependent because they sum up to one.

    TODO: This method requires the use of StringIndexer first. Decouple them.

    >>> stringIndexer = StringIndexer(inputCol="label", outputCol="indexed")
    >>> model = stringIndexer.fit(stringIndDf)
    >>> td = model.transform(stringIndDf)
    >>> encoder = OneHotEncoder(includeFirst=False, inputCol="indexed", outputCol="features")
    >>> encoder.transform(td).head().features
    SparseVector(2, {})
    >>> encoder.setParams(outputCol="freqs").transform(td).head().freqs
    SparseVector(2, {})
    >>> params = {encoder.includeFirst: True, encoder.outputCol: "test"}
    >>> encoder.transform(td, params).head().test
    SparseVector(3, {0: 1.0})
    """

    _java_class = "org.apache.spark.ml.feature.OneHotEncoder"

    # a placeholder to make it appear in the generated doc
    includeFirst = Param(Params._dummy(), "includeFirst", "include first category")

    @keyword_only
    def __init__(self, includeFirst=True, inputCol=None, outputCol=None):
        """
        __init__(self, includeFirst=True, inputCol=None, outputCol=None)
        """
        super(OneHotEncoder, self).__init__()
        self.includeFirst = Param(self, "includeFirst", "include first category")
        self._setDefault(includeFirst=True)
        kwargs = self.__init__._input_kwargs
        self.setParams(**kwargs)

    @keyword_only
    def setParams(self, includeFirst=True, inputCol=None, outputCol=None):
        """
        setParams(self, includeFirst=True, inputCol=None, outputCol=None)
        Sets params for this OneHotEncoder.
        """
        kwargs = self.setParams._input_kwargs
        return self._set(**kwargs)

    def setIncludeFirst(self, value):
        """
        Sets the value of :py:attr:`includeFirst`.
        """
        self.paramMap[self.includeFirst] = value
        return self

    def getIncludeFirst(self):
        """
        Gets the value of includeFirst or its default value.
        """
        return self.getOrDefault(self.includeFirst)


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

    _java_class = "org.apache.spark.ml.feature.PolynomialExpansion"

    # a placeholder to make it appear in the generated doc
    degree = Param(Params._dummy(), "degree", "the polynomial degree to expand (>= 1)")

    @keyword_only
    def __init__(self, degree=2, inputCol=None, outputCol=None):
        """
        __init__(self, degree=2, inputCol=None, outputCol=None)
        """
        super(PolynomialExpansion, self).__init__()
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
        self.paramMap[self.degree] = value
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
    A regex based tokenizer that extracts tokens either by repeatedly matching the regex(default)
    or using it to split the text (set matching to false). Optional parameters also allow filtering
    tokens using a minimal length.
    It returns an array of strings that can be empty.

    >>> df = sqlContext.createDataFrame([("a b c",)], ["text"])
    >>> reTokenizer = RegexTokenizer(inputCol="text", outputCol="words")
    >>> reTokenizer.transform(df).head()
    Row(text=u'a b c', words=[u'a', u'b', u'c'])
    >>> # Change a parameter.
    >>> reTokenizer.setParams(outputCol="tokens").transform(df).head()
    Row(text=u'a b c', tokens=[u'a', u'b', u'c'])
    >>> # Temporarily modify a parameter.
    >>> reTokenizer.transform(df, {reTokenizer.outputCol: "words"}).head()
    Row(text=u'a b c', words=[u'a', u'b', u'c'])
    >>> reTokenizer.transform(df).head()
    Row(text=u'a b c', tokens=[u'a', u'b', u'c'])
    >>> # Must use keyword arguments to specify params.
    >>> reTokenizer.setParams("text")
    Traceback (most recent call last):
        ...
    TypeError: Method setParams forces keyword arguments.
    """

    _java_class = "org.apache.spark.ml.feature.RegexTokenizer"
    # a placeholder to make it appear in the generated doc
    minTokenLength = Param(Params._dummy(), "minTokenLength", "minimum token length (>= 0)")
    gaps = Param(Params._dummy(), "gaps", "Set regex to match gaps or tokens")
    pattern = Param(Params._dummy(), "pattern", "regex pattern used for tokenizing")

    @keyword_only
    def __init__(self, minTokenLength=1, gaps=False, pattern="\\p{L}+|[^\\p{L}\\s]+",
                 inputCol=None, outputCol=None):
        """
        __init__(self, minTokenLength=1, gaps=False, pattern="\\p{L}+|[^\\p{L}\\s]+", \
                 inputCol=None, outputCol=None)
        """
        super(RegexTokenizer, self).__init__()
        self.minTokenLength = Param(self, "minLength", "minimum token length (>= 0)")
        self.gaps = Param(self, "gaps", "Set regex to match gaps or tokens")
        self.pattern = Param(self, "pattern", "regex pattern used for tokenizing")
        self._setDefault(minTokenLength=1, gaps=False, pattern="\\p{L}+|[^\\p{L}\\s]+")
        kwargs = self.__init__._input_kwargs
        self.setParams(**kwargs)

    @keyword_only
    def setParams(self, minTokenLength=1, gaps=False, pattern="\\p{L}+|[^\\p{L}\\s]+",
                  inputCol=None, outputCol=None):
        """
        setParams(self, minTokenLength=1, gaps=False, pattern="\\p{L}+|[^\\p{L}\\s]+", \
                  inputCol="input", outputCol="output")
        Sets params for this RegexTokenizer.
        """
        kwargs = self.setParams._input_kwargs
        return self._set(**kwargs)

    def setMinTokenLength(self, value):
        """
        Sets the value of :py:attr:`minTokenLength`.
        """
        self.paramMap[self.minTokenLength] = value
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
        self.paramMap[self.gaps] = value
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
        self.paramMap[self.pattern] = value
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
    >>> model.transform(df).collect()[1].scaled
    DenseVector([1.4142])
    """

    _java_class = "org.apache.spark.ml.feature.StandardScaler"

    # a placeholder to make it appear in the generated doc
    withMean = Param(Params._dummy(), "withMean", "Center data with mean")
    withStd = Param(Params._dummy(), "withStd", "Scale to unit standard deviation")

    @keyword_only
    def __init__(self, withMean=False, withStd=True, inputCol=None, outputCol=None):
        """
        __init__(self, withMean=False, withStd=True, inputCol=None, outputCol=None)
        """
        super(StandardScaler, self).__init__()
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
        self.paramMap[self.withMean] = value
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
        self.paramMap[self.withStd] = value
        return self

    def getWithStd(self):
        """
        Gets the value of withStd or its default value.
        """
        return self.getOrDefault(self.withStd)


class StandardScalerModel(JavaModel):
    """
    Model fitted by StandardScaler.
    """


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

    _java_class = "org.apache.spark.ml.feature.StringIndexer"

    @keyword_only
    def __init__(self, inputCol=None, outputCol=None):
        """
        __init__(self, inputCol=None, outputCol=None)
        """
        super(StringIndexer, self).__init__()
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

    _java_class = "org.apache.spark.ml.feature.Tokenizer"

    @keyword_only
    def __init__(self, inputCol=None, outputCol=None):
        """
        __init__(self, inputCol=None, outputCol=None)
        """
        super(Tokenizer, self).__init__()
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

    _java_class = "org.apache.spark.ml.feature.VectorAssembler"

    @keyword_only
    def __init__(self, inputCols=None, outputCol=None):
        """
        __init__(self, inputCols=None, outputCol=None)
        """
        super(VectorAssembler, self).__init__()
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

    _java_class = "org.apache.spark.ml.feature.VectorIndexer"
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
        self.paramMap[self.maxCategories] = value
        return self

    def getMaxCategories(self):
        """
        Gets the value of maxCategories or its default value.
        """
        return self.getOrDefault(self.maxCategories)


@inherit_doc
@ignore_unicode_prefix
class Word2Vec(JavaEstimator, HasStepSize, HasMaxIter, HasSeed, HasInputCol, HasOutputCol):
    """
    Word2Vec trains a model of `Map(String, Vector)`, i.e. transforms a word into a code for further
    natural language processing or machine learning process.

    >>> sent = ("a b " * 100 + "a c " * 10).split(" ")
    >>> doc = sqlContext.createDataFrame([(sent,), (sent,)], ["sentence"])
    >>> model = Word2Vec(vectorSize=5, seed=42, inputCol="sentence", outputCol="model").fit(doc)
    >>> model.transform(doc).head().model
    DenseVector([-0.0422, -0.5138, -0.2546, 0.6885, 0.276])
    """

    _java_class = "org.apache.spark.ml.feature.Word2Vec"
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
                 seed=42, inputCol=None, outputCol=None):
        """
        __init__(self, vectorSize=100, minCount=5, numPartitions=1, stepSize=0.025, maxIter=1, \
                 seed=42, inputCol=None, outputCol=None)
        """
        super(Word2Vec, self).__init__()
        self.vectorSize = Param(self, "vectorSize",
                                "the dimension of codes after transforming from words")
        self.numPartitions = Param(self, "numPartitions",
                                   "number of partitions for sentences of words")
        self.minCount = Param(self, "minCount",
                              "the minimum number of times a token must appear to be included " +
                              "in the word2vec model's vocabulary")
        self._setDefault(vectorSize=100, minCount=5, numPartitions=1, stepSize=0.025, maxIter=1,
                         seed=42)
        kwargs = self.__init__._input_kwargs
        self.setParams(**kwargs)

    @keyword_only
    def setParams(self, vectorSize=100, minCount=5, numPartitions=1, stepSize=0.025, maxIter=1,
                  seed=42, inputCol=None, outputCol=None):
        """
        setParams(self, minCount=5, numPartitions=1, stepSize=0.025, maxIter=1, seed=42, \
                 inputCol=None, outputCol=None)
        Sets params for this Word2Vec.
        """
        kwargs = self.setParams._input_kwargs
        return self._set(**kwargs)

    def setVectorSize(self, value):
        """
        Sets the value of :py:attr:`vectorSize`.
        """
        self.paramMap[self.vectorSize] = value
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
        self.paramMap[self.numPartitions] = value
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
        self.paramMap[self.minCount] = value
        return self

    def getMinCount(self):
        """
        Gets the value of minCount or its default value.
        """
        return self.getOrDefault(self.minCount)


class Word2VecModel(JavaModel):
    """
    Model fitted by Word2Vec.
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
