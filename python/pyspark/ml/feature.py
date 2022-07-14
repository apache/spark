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
from typing import (
    cast,
    overload,
    Any,
    Dict,
    Generic,
    List,
    Optional,
    Tuple,
    TypeVar,
    Union,
    TYPE_CHECKING,
)

from pyspark import keyword_only, since, SparkContext
from pyspark.ml.linalg import _convert_to_vector, DenseMatrix, DenseVector, Vector
from pyspark.sql.dataframe import DataFrame
from pyspark.ml.param.shared import (
    HasThreshold,
    HasThresholds,
    HasInputCol,
    HasOutputCol,
    HasInputCols,
    HasOutputCols,
    HasHandleInvalid,
    HasRelativeError,
    HasFeaturesCol,
    HasLabelCol,
    HasSeed,
    HasNumFeatures,
    HasStepSize,
    HasMaxIter,
    TypeConverters,
    Param,
    Params,
)
from pyspark.ml.util import JavaMLReadable, JavaMLWritable
from pyspark.ml.wrapper import JavaEstimator, JavaModel, JavaParams, JavaTransformer, _jvm
from pyspark.ml.common import inherit_doc

if TYPE_CHECKING:
    from py4j.java_gateway import JavaObject

JM = TypeVar("JM", bound=JavaTransformer)
P = TypeVar("P", bound=Params)

__all__ = [
    "Binarizer",
    "BucketedRandomProjectionLSH",
    "BucketedRandomProjectionLSHModel",
    "Bucketizer",
    "ChiSqSelector",
    "ChiSqSelectorModel",
    "CountVectorizer",
    "CountVectorizerModel",
    "DCT",
    "ElementwiseProduct",
    "FeatureHasher",
    "HashingTF",
    "IDF",
    "IDFModel",
    "Imputer",
    "ImputerModel",
    "IndexToString",
    "Interaction",
    "MaxAbsScaler",
    "MaxAbsScalerModel",
    "MinHashLSH",
    "MinHashLSHModel",
    "MinMaxScaler",
    "MinMaxScalerModel",
    "NGram",
    "Normalizer",
    "OneHotEncoder",
    "OneHotEncoderModel",
    "PCA",
    "PCAModel",
    "PolynomialExpansion",
    "QuantileDiscretizer",
    "RobustScaler",
    "RobustScalerModel",
    "RegexTokenizer",
    "RFormula",
    "RFormulaModel",
    "SQLTransformer",
    "StandardScaler",
    "StandardScalerModel",
    "StopWordsRemover",
    "StringIndexer",
    "StringIndexerModel",
    "Tokenizer",
    "UnivariateFeatureSelector",
    "UnivariateFeatureSelectorModel",
    "VarianceThresholdSelector",
    "VarianceThresholdSelectorModel",
    "VectorAssembler",
    "VectorIndexer",
    "VectorIndexerModel",
    "VectorSizeHint",
    "VectorSlicer",
    "Word2Vec",
    "Word2VecModel",
]


@inherit_doc
class Binarizer(
    JavaTransformer,
    HasThreshold,
    HasThresholds,
    HasInputCol,
    HasOutputCol,
    HasInputCols,
    HasOutputCols,
    JavaMLReadable["Binarizer"],
    JavaMLWritable,
):
    """
    Binarize a column of continuous features given a threshold. Since 3.0.0,
    :py:class:`Binarize` can map multiple columns at once by setting the :py:attr:`inputCols`
    parameter. Note that when both the :py:attr:`inputCol` and :py:attr:`inputCols` parameters
    are set, an Exception will be thrown. The :py:attr:`threshold` parameter is used for
    single column usage, and :py:attr:`thresholds` is for multiple columns.

    .. versionadded:: 1.4.0

    Examples
    --------
    >>> df = spark.createDataFrame([(0.5,)], ["values"])
    >>> binarizer = Binarizer(threshold=1.0, inputCol="values", outputCol="features")
    >>> binarizer.setThreshold(1.0)
    Binarizer...
    >>> binarizer.setInputCol("values")
    Binarizer...
    >>> binarizer.setOutputCol("features")
    Binarizer...
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
    >>> loadedBinarizer.transform(df).take(1) == binarizer.transform(df).take(1)
    True
    >>> df2 = spark.createDataFrame([(0.5, 0.3)], ["values1", "values2"])
    >>> binarizer2 = Binarizer(thresholds=[0.0, 1.0])
    >>> binarizer2.setInputCols(["values1", "values2"]).setOutputCols(["output1", "output2"])
    Binarizer...
    >>> binarizer2.transform(df2).show()
    +-------+-------+-------+-------+
    |values1|values2|output1|output2|
    +-------+-------+-------+-------+
    |    0.5|    0.3|    1.0|    0.0|
    +-------+-------+-------+-------+
    ...
    """

    _input_kwargs: Dict[str, Any]

    threshold: Param[float] = Param(
        Params._dummy(),
        "threshold",
        "Param for threshold used to binarize continuous features. "
        + "The features greater than the threshold will be binarized to 1.0. "
        + "The features equal to or less than the threshold will be binarized to 0.0",
        typeConverter=TypeConverters.toFloat,
    )
    thresholds: Param[List[float]] = Param(
        Params._dummy(),
        "thresholds",
        "Param for array of threshold used to binarize continuous features. "
        + "This is for multiple columns input. If transforming multiple columns "
        + "and thresholds is not set, but threshold is set, then threshold will "
        + "be applied across all columns.",
        typeConverter=TypeConverters.toListFloat,
    )

    @overload
    def __init__(
        self,
        *,
        threshold: float = ...,
        inputCol: Optional[str] = ...,
        outputCol: Optional[str] = ...,
    ):
        ...

    @overload
    def __init__(
        self,
        *,
        thresholds: Optional[List[float]] = ...,
        inputCols: Optional[List[str]] = ...,
        outputCols: Optional[List[str]] = ...,
    ):
        ...

    @keyword_only
    def __init__(
        self,
        *,
        threshold: float = 0.0,
        inputCol: Optional[str] = None,
        outputCol: Optional[str] = None,
        thresholds: Optional[List[float]] = None,
        inputCols: Optional[List[str]] = None,
        outputCols: Optional[List[str]] = None,
    ):
        """
        __init__(self, \\*, threshold=0.0, inputCol=None, outputCol=None, thresholds=None, \
                 inputCols=None, outputCols=None)
        """
        super(Binarizer, self).__init__()
        self._java_obj = self._new_java_obj("org.apache.spark.ml.feature.Binarizer", self.uid)
        self._setDefault(threshold=0.0)
        kwargs = self._input_kwargs
        self.setParams(**kwargs)

    @overload
    def setParams(
        self,
        *,
        threshold: float = ...,
        inputCol: Optional[str] = ...,
        outputCol: Optional[str] = ...,
    ) -> "Binarizer":
        ...

    @overload
    def setParams(
        self,
        *,
        thresholds: Optional[List[float]] = ...,
        inputCols: Optional[List[str]] = ...,
        outputCols: Optional[List[str]] = ...,
    ) -> "Binarizer":
        ...

    @keyword_only
    @since("1.4.0")
    def setParams(
        self,
        *,
        threshold: float = 0.0,
        inputCol: Optional[str] = None,
        outputCol: Optional[str] = None,
        thresholds: Optional[List[float]] = None,
        inputCols: Optional[List[str]] = None,
        outputCols: Optional[List[str]] = None,
    ) -> "Binarizer":
        """
        setParams(self, \\*, threshold=0.0, inputCol=None, outputCol=None, thresholds=None, \
                  inputCols=None, outputCols=None)
        Sets params for this Binarizer.
        """
        kwargs = self._input_kwargs
        return self._set(**kwargs)

    @since("1.4.0")
    def setThreshold(self, value: float) -> "Binarizer":
        """
        Sets the value of :py:attr:`threshold`.
        """
        return self._set(threshold=value)

    @since("3.0.0")
    def setThresholds(self, value: List[float]) -> "Binarizer":
        """
        Sets the value of :py:attr:`thresholds`.
        """
        return self._set(thresholds=value)

    def setInputCol(self, value: str) -> "Binarizer":
        """
        Sets the value of :py:attr:`inputCol`.
        """
        return self._set(inputCol=value)

    @since("3.0.0")
    def setInputCols(self, value: List[str]) -> "Binarizer":
        """
        Sets the value of :py:attr:`inputCols`.
        """
        return self._set(inputCols=value)

    def setOutputCol(self, value: str) -> "Binarizer":
        """
        Sets the value of :py:attr:`outputCol`.
        """
        return self._set(outputCol=value)

    @since("3.0.0")
    def setOutputCols(self, value: List[str]) -> "Binarizer":
        """
        Sets the value of :py:attr:`outputCols`.
        """
        return self._set(outputCols=value)


class _LSHParams(HasInputCol, HasOutputCol):
    """
    Mixin for Locality Sensitive Hashing (LSH) algorithm parameters.
    """

    numHashTables: Param[int] = Param(
        Params._dummy(),
        "numHashTables",
        "number of hash tables, where "
        + "increasing number of hash tables lowers the false negative rate, "
        + "and decreasing it improves the running performance.",
        typeConverter=TypeConverters.toInt,
    )

    def __init__(self, *args: Any):
        super(_LSHParams, self).__init__(*args)
        self._setDefault(numHashTables=1)

    def getNumHashTables(self) -> int:
        """
        Gets the value of numHashTables or its default value.
        """
        return self.getOrDefault(self.numHashTables)


class _LSH(JavaEstimator[JM], _LSHParams, JavaMLReadable, JavaMLWritable, Generic[JM]):
    """
    Mixin for Locality Sensitive Hashing (LSH).
    """

    def setNumHashTables(self: P, value: int) -> P:
        """
        Sets the value of :py:attr:`numHashTables`.
        """
        return self._set(numHashTables=value)

    def setInputCol(self: P, value: str) -> P:
        """
        Sets the value of :py:attr:`inputCol`.
        """
        return self._set(inputCol=value)

    def setOutputCol(self: P, value: str) -> P:
        """
        Sets the value of :py:attr:`outputCol`.
        """
        return self._set(outputCol=value)


class _LSHModel(JavaModel, _LSHParams):
    """
    Mixin for Locality Sensitive Hashing (LSH) models.
    """

    def setInputCol(self: P, value: str) -> P:
        """
        Sets the value of :py:attr:`inputCol`.
        """
        return self._set(inputCol=value)

    def setOutputCol(self: P, value: str) -> P:
        """
        Sets the value of :py:attr:`outputCol`.
        """
        return self._set(outputCol=value)

    def approxNearestNeighbors(
        self,
        dataset: DataFrame,
        key: Vector,
        numNearestNeighbors: int,
        distCol: str = "distCol",
    ) -> DataFrame:
        """
        Given a large dataset and an item, approximately find at most k items which have the
        closest distance to the item. If the :py:attr:`outputCol` is missing, the method will
        transform the data; if the :py:attr:`outputCol` exists, it will use that. This allows
        caching of the transformed data when necessary.

        Notes
        -----
        This method is experimental and will likely change behavior in the next release.

        Parameters
        ----------
        dataset : :py:class:`pyspark.sql.DataFrame`
            The dataset to search for nearest neighbors of the key.
        key :  :py:class:`pyspark.ml.linalg.Vector`
            Feature vector representing the item to search for.
        numNearestNeighbors : int
            The maximum number of nearest neighbors.
        distCol : str
            Output column for storing the distance between each result row and the key.
            Use "distCol" as default value if it's not specified.

        Returns
        -------
        :py:class:`pyspark.sql.DataFrame`
            A dataset containing at most k items closest to the key. A column "distCol" is
            added to show the distance between each row and the key.
        """
        return self._call_java("approxNearestNeighbors", dataset, key, numNearestNeighbors, distCol)

    def approxSimilarityJoin(
        self,
        datasetA: DataFrame,
        datasetB: DataFrame,
        threshold: float,
        distCol: str = "distCol",
    ) -> DataFrame:
        """
        Join two datasets to approximately find all pairs of rows whose distance are smaller than
        the threshold. If the :py:attr:`outputCol` is missing, the method will transform the data;
        if the :py:attr:`outputCol` exists, it will use that. This allows caching of the
        transformed data when necessary.

        Parameters
        ----------
        datasetA : :py:class:`pyspark.sql.DataFrame`
            One of the datasets to join.
        datasetB : :py:class:`pyspark.sql.DataFrame`
            Another dataset to join.
        threshold : float
            The threshold for the distance of row pairs.
        distCol : str, optional
            Output column for storing the distance between each pair of rows. Use
            "distCol" as default value if it's not specified.

        Returns
        -------
        :py:class:`pyspark.sql.DataFrame`
            A joined dataset containing pairs of rows. The original rows are in columns
            "datasetA" and "datasetB", and a column "distCol" is added to show the distance
            between each pair.
        """
        threshold = TypeConverters.toFloat(threshold)
        return self._call_java("approxSimilarityJoin", datasetA, datasetB, threshold, distCol)


class _BucketedRandomProjectionLSHParams:
    """
    Params for :py:class:`BucketedRandomProjectionLSH` and
    :py:class:`BucketedRandomProjectionLSHModel`.

    .. versionadded:: 3.0.0
    """

    bucketLength: Param[float] = Param(
        Params._dummy(),
        "bucketLength",
        "the length of each hash bucket, " + "a larger bucket lowers the false negative rate.",
        typeConverter=TypeConverters.toFloat,
    )

    @since("2.2.0")
    def getBucketLength(self) -> float:
        """
        Gets the value of bucketLength or its default value.
        """
        return (cast(Params, self)).getOrDefault(self.bucketLength)


@inherit_doc
class BucketedRandomProjectionLSH(
    _LSH["BucketedRandomProjectionLSHModel"],
    _LSHParams,
    _BucketedRandomProjectionLSHParams,
    HasSeed,
    JavaMLReadable["BucketedRandomProjectionLSH"],
    JavaMLWritable,
):
    """
    LSH class for Euclidean distance metrics.
    The input is dense or sparse vectors, each of which represents a point in the Euclidean
    distance space. The output will be vectors of configurable dimension. Hash values in the same
    dimension are calculated by the same hash function.

    .. versionadded:: 2.2.0

    Notes
    -----

    - `Stable Distributions in Wikipedia article on Locality-sensitive hashing \
      <https://en.wikipedia.org/wiki/Locality-sensitive_hashing#Stable_distributions>`_
    - `Hashing for Similarity Search: A Survey <https://arxiv.org/abs/1408.2927>`_

    Examples
    --------
    >>> from pyspark.ml.linalg import Vectors
    >>> from pyspark.sql.functions import col
    >>> data = [(0, Vectors.dense([-1.0, -1.0 ]),),
    ...         (1, Vectors.dense([-1.0, 1.0 ]),),
    ...         (2, Vectors.dense([1.0, -1.0 ]),),
    ...         (3, Vectors.dense([1.0, 1.0]),)]
    >>> df = spark.createDataFrame(data, ["id", "features"])
    >>> brp = BucketedRandomProjectionLSH()
    >>> brp.setInputCol("features")
    BucketedRandomProjectionLSH...
    >>> brp.setOutputCol("hashes")
    BucketedRandomProjectionLSH...
    >>> brp.setSeed(12345)
    BucketedRandomProjectionLSH...
    >>> brp.setBucketLength(1.0)
    BucketedRandomProjectionLSH...
    >>> model = brp.fit(df)
    >>> model.getBucketLength()
    1.0
    >>> model.setOutputCol("hashes")
    BucketedRandomProjectionLSHModel...
    >>> model.transform(df).head()
    Row(id=0, features=DenseVector([-1.0, -1.0]), hashes=[DenseVector([-1.0])])
    >>> data2 = [(4, Vectors.dense([2.0, 2.0 ]),),
    ...          (5, Vectors.dense([2.0, 3.0 ]),),
    ...          (6, Vectors.dense([3.0, 2.0 ]),),
    ...          (7, Vectors.dense([3.0, 3.0]),)]
    >>> df2 = spark.createDataFrame(data2, ["id", "features"])
    >>> model.approxNearestNeighbors(df2, Vectors.dense([1.0, 2.0]), 1).collect()
    [Row(id=4, features=DenseVector([2.0, 2.0]), hashes=[DenseVector([1.0])], distCol=1.0)]
    >>> model.approxSimilarityJoin(df, df2, 3.0, distCol="EuclideanDistance").select(
    ...     col("datasetA.id").alias("idA"),
    ...     col("datasetB.id").alias("idB"),
    ...     col("EuclideanDistance")).show()
    +---+---+-----------------+
    |idA|idB|EuclideanDistance|
    +---+---+-----------------+
    |  3|  6| 2.23606797749979|
    +---+---+-----------------+
    ...
    >>> model.approxSimilarityJoin(df, df2, 3, distCol="EuclideanDistance").select(
    ...     col("datasetA.id").alias("idA"),
    ...     col("datasetB.id").alias("idB"),
    ...     col("EuclideanDistance")).show()
    +---+---+-----------------+
    |idA|idB|EuclideanDistance|
    +---+---+-----------------+
    |  3|  6| 2.23606797749979|
    +---+---+-----------------+
    ...
    >>> brpPath = temp_path + "/brp"
    >>> brp.save(brpPath)
    >>> brp2 = BucketedRandomProjectionLSH.load(brpPath)
    >>> brp2.getBucketLength() == brp.getBucketLength()
    True
    >>> modelPath = temp_path + "/brp-model"
    >>> model.save(modelPath)
    >>> model2 = BucketedRandomProjectionLSHModel.load(modelPath)
    >>> model.transform(df).head().hashes == model2.transform(df).head().hashes
    True
    """

    _input_kwargs: Dict[str, Any]

    @keyword_only
    def __init__(
        self,
        *,
        inputCol: Optional[str] = None,
        outputCol: Optional[str] = None,
        seed: Optional[int] = None,
        numHashTables: int = 1,
        bucketLength: Optional[float] = None,
    ):
        """
        __init__(self, \\*, inputCol=None, outputCol=None, seed=None, numHashTables=1, \
                 bucketLength=None)
        """
        super(BucketedRandomProjectionLSH, self).__init__()
        self._java_obj = self._new_java_obj(
            "org.apache.spark.ml.feature.BucketedRandomProjectionLSH", self.uid
        )
        kwargs = self._input_kwargs
        self.setParams(**kwargs)

    @keyword_only
    @since("2.2.0")
    def setParams(
        self,
        *,
        inputCol: Optional[str] = None,
        outputCol: Optional[str] = None,
        seed: Optional[int] = None,
        numHashTables: int = 1,
        bucketLength: Optional[float] = None,
    ) -> "BucketedRandomProjectionLSH":
        """
        setParams(self, \\*, inputCol=None, outputCol=None, seed=None, numHashTables=1, \
                  bucketLength=None)
        Sets params for this BucketedRandomProjectionLSH.
        """
        kwargs = self._input_kwargs
        return self._set(**kwargs)

    @since("2.2.0")
    def setBucketLength(self, value: float) -> "BucketedRandomProjectionLSH":
        """
        Sets the value of :py:attr:`bucketLength`.
        """
        return self._set(bucketLength=value)

    def setSeed(self, value: int) -> "BucketedRandomProjectionLSH":
        """
        Sets the value of :py:attr:`seed`.
        """
        return self._set(seed=value)

    def _create_model(self, java_model: "JavaObject") -> "BucketedRandomProjectionLSHModel":
        return BucketedRandomProjectionLSHModel(java_model)


class BucketedRandomProjectionLSHModel(
    _LSHModel,
    _BucketedRandomProjectionLSHParams,
    JavaMLReadable["BucketedRandomProjectionLSHModel"],
    JavaMLWritable,
):
    r"""
    Model fitted by :py:class:`BucketedRandomProjectionLSH`, where multiple random vectors are
    stored. The vectors are normalized to be unit vectors and each vector is used in a hash
    function: :math:`h_i(x) = floor(r_i \cdot x / bucketLength)` where :math:`r_i` is the
    i-th random unit vector. The number of buckets will be `(max L2 norm of input vectors) /
    bucketLength`.

    .. versionadded:: 2.2.0
    """


@inherit_doc
class Bucketizer(
    JavaTransformer,
    HasInputCol,
    HasOutputCol,
    HasInputCols,
    HasOutputCols,
    HasHandleInvalid,
    JavaMLReadable["Bucketizer"],
    JavaMLWritable,
):
    """
    Maps a column of continuous features to a column of feature buckets. Since 3.0.0,
    :py:class:`Bucketizer` can map multiple columns at once by setting the :py:attr:`inputCols`
    parameter. Note that when both the :py:attr:`inputCol` and :py:attr:`inputCols` parameters
    are set, an Exception will be thrown. The :py:attr:`splits` parameter is only used for single
    column usage, and :py:attr:`splitsArray` is for multiple columns.

    .. versionadded:: 1.4.0

    Examples
    --------
    >>> values = [(0.1, 0.0), (0.4, 1.0), (1.2, 1.3), (1.5, float("nan")),
    ...     (float("nan"), 1.0), (float("nan"), 0.0)]
    >>> df = spark.createDataFrame(values, ["values1", "values2"])
    >>> bucketizer = Bucketizer()
    >>> bucketizer.setSplits([-float("inf"), 0.5, 1.4, float("inf")])
    Bucketizer...
    >>> bucketizer.setInputCol("values1")
    Bucketizer...
    >>> bucketizer.setOutputCol("buckets")
    Bucketizer...
    >>> bucketed = bucketizer.setHandleInvalid("keep").transform(df).collect()
    >>> bucketed = bucketizer.setHandleInvalid("keep").transform(df.select("values1"))
    >>> bucketed.show(truncate=False)
    +-------+-------+
    |values1|buckets|
    +-------+-------+
    |0.1    |0.0    |
    |0.4    |0.0    |
    |1.2    |1.0    |
    |1.5    |2.0    |
    |NaN    |3.0    |
    |NaN    |3.0    |
    +-------+-------+
    ...
    >>> bucketizer.setParams(outputCol="b").transform(df).head().b
    0.0
    >>> bucketizerPath = temp_path + "/bucketizer"
    >>> bucketizer.save(bucketizerPath)
    >>> loadedBucketizer = Bucketizer.load(bucketizerPath)
    >>> loadedBucketizer.getSplits() == bucketizer.getSplits()
    True
    >>> loadedBucketizer.transform(df).take(1) == bucketizer.transform(df).take(1)
    True
    >>> bucketed = bucketizer.setHandleInvalid("skip").transform(df).collect()
    >>> len(bucketed)
    4
    >>> bucketizer2 = Bucketizer(splitsArray=
    ...     [[-float("inf"), 0.5, 1.4, float("inf")], [-float("inf"), 0.5, float("inf")]],
    ...     inputCols=["values1", "values2"], outputCols=["buckets1", "buckets2"])
    >>> bucketed2 = bucketizer2.setHandleInvalid("keep").transform(df)
    >>> bucketed2.show(truncate=False)
    +-------+-------+--------+--------+
    |values1|values2|buckets1|buckets2|
    +-------+-------+--------+--------+
    |0.1    |0.0    |0.0     |0.0     |
    |0.4    |1.0    |0.0     |1.0     |
    |1.2    |1.3    |1.0     |1.0     |
    |1.5    |NaN    |2.0     |2.0     |
    |NaN    |1.0    |3.0     |1.0     |
    |NaN    |0.0    |3.0     |0.0     |
    +-------+-------+--------+--------+
    ...
    """

    _input_kwargs: Dict[str, Any]

    splits: Param[List[float]] = Param(
        Params._dummy(),
        "splits",
        "Split points for mapping continuous features into buckets. With n+1 splits, "
        + "there are n buckets. A bucket defined by splits x,y holds values in the "
        + "range [x,y) except the last bucket, which also includes y. The splits "
        + "should be of length >= 3 and strictly increasing. Values at -inf, inf must be "
        + "explicitly provided to cover all Double values; otherwise, values outside the "
        + "splits specified will be treated as errors.",
        typeConverter=TypeConverters.toListFloat,
    )

    handleInvalid: Param[str] = Param(
        Params._dummy(),
        "handleInvalid",
        "how to handle invalid entries "
        "containing NaN values. Values outside the splits will always be treated "
        "as errors. Options are 'skip' (filter out rows with invalid values), "
        + "'error' (throw an error), or 'keep' (keep invalid values in a "
        + "special additional bucket). Note that in the multiple column "
        + "case, the invalid handling is applied to all columns. That said "
        + "for 'error' it will throw an error if any invalids are found in "
        + "any column, for 'skip' it will skip rows with any invalids in "
        + "any columns, etc.",
        typeConverter=TypeConverters.toString,
    )

    splitsArray: Param[List[List[float]]] = Param(
        Params._dummy(),
        "splitsArray",
        "The array of split points for mapping "
        + "continuous features into buckets for multiple columns. For each input "
        + "column, with n+1 splits, there are n buckets. A bucket defined by "
        + "splits x,y holds values in the range [x,y) except the last bucket, "
        + "which also includes y. The splits should be of length >= 3 and "
        + "strictly increasing. Values at -inf, inf must be explicitly provided "
        + "to cover all Double values; otherwise, values outside the splits "
        + "specified will be treated as errors.",
        typeConverter=TypeConverters.toListListFloat,
    )

    @overload
    def __init__(
        self,
        *,
        splits: Optional[List[float]] = ...,
        inputCol: Optional[str] = ...,
        outputCol: Optional[str] = ...,
        handleInvalid: str = ...,
    ):
        ...

    @overload
    def __init__(
        self,
        *,
        handleInvalid: str = ...,
        splitsArray: Optional[List[List[float]]] = ...,
        inputCols: Optional[List[str]] = ...,
        outputCols: Optional[List[str]] = ...,
    ):
        ...

    @keyword_only
    def __init__(
        self,
        *,
        splits: Optional[List[float]] = None,
        inputCol: Optional[str] = None,
        outputCol: Optional[str] = None,
        handleInvalid: str = "error",
        splitsArray: Optional[List[List[float]]] = None,
        inputCols: Optional[List[str]] = None,
        outputCols: Optional[List[str]] = None,
    ):
        """
        __init__(self, \\*, splits=None, inputCol=None, outputCol=None, handleInvalid="error", \
                 splitsArray=None, inputCols=None, outputCols=None)
        """
        super(Bucketizer, self).__init__()
        self._java_obj = self._new_java_obj("org.apache.spark.ml.feature.Bucketizer", self.uid)
        self._setDefault(handleInvalid="error")
        kwargs = self._input_kwargs
        self.setParams(**kwargs)

    @overload
    def setParams(
        self,
        *,
        splits: Optional[List[float]] = ...,
        inputCol: Optional[str] = ...,
        outputCol: Optional[str] = ...,
        handleInvalid: str = ...,
    ) -> "Bucketizer":
        ...

    @overload
    def setParams(
        self,
        *,
        handleInvalid: str = ...,
        splitsArray: Optional[List[List[float]]] = ...,
        inputCols: Optional[List[str]] = ...,
        outputCols: Optional[List[str]] = ...,
    ) -> "Bucketizer":
        ...

    @keyword_only
    @since("1.4.0")
    def setParams(
        self,
        *,
        splits: Optional[List[float]] = None,
        inputCol: Optional[str] = None,
        outputCol: Optional[str] = None,
        handleInvalid: str = "error",
        splitsArray: Optional[List[List[float]]] = None,
        inputCols: Optional[List[str]] = None,
        outputCols: Optional[List[str]] = None,
    ) -> "Bucketizer":
        """
        setParams(self, \\*, splits=None, inputCol=None, outputCol=None, handleInvalid="error", \
                  splitsArray=None, inputCols=None, outputCols=None)
        Sets params for this Bucketizer.
        """
        kwargs = self._input_kwargs
        return self._set(**kwargs)

    @since("1.4.0")
    def setSplits(self, value: List[float]) -> "Bucketizer":
        """
        Sets the value of :py:attr:`splits`.
        """
        return self._set(splits=value)

    @since("1.4.0")
    def getSplits(self) -> List[float]:
        """
        Gets the value of threshold or its default value.
        """
        return self.getOrDefault(self.splits)

    @since("3.0.0")
    def setSplitsArray(self, value: List[List[float]]) -> "Bucketizer":
        """
        Sets the value of :py:attr:`splitsArray`.
        """
        return self._set(splitsArray=value)

    @since("3.0.0")
    def getSplitsArray(self) -> List[List[float]]:
        """
        Gets the array of split points or its default value.
        """
        return self.getOrDefault(self.splitsArray)

    def setInputCol(self, value: str) -> "Bucketizer":
        """
        Sets the value of :py:attr:`inputCol`.
        """
        return self._set(inputCol=value)

    @since("3.0.0")
    def setInputCols(self, value: List[str]) -> "Bucketizer":
        """
        Sets the value of :py:attr:`inputCols`.
        """
        return self._set(inputCols=value)

    def setOutputCol(self, value: str) -> "Bucketizer":
        """
        Sets the value of :py:attr:`outputCol`.
        """
        return self._set(outputCol=value)

    @since("3.0.0")
    def setOutputCols(self, value: List[str]) -> "Bucketizer":
        """
        Sets the value of :py:attr:`outputCols`.
        """
        return self._set(outputCols=value)

    def setHandleInvalid(self, value: str) -> "Bucketizer":
        """
        Sets the value of :py:attr:`handleInvalid`.
        """
        return self._set(handleInvalid=value)


class _CountVectorizerParams(JavaParams, HasInputCol, HasOutputCol):
    """
    Params for :py:class:`CountVectorizer` and :py:class:`CountVectorizerModel`.
    """

    minTF: Param[float] = Param(
        Params._dummy(),
        "minTF",
        "Filter to ignore rare words in"
        + " a document. For each document, terms with frequency/count less than the given"
        + " threshold are ignored. If this is an integer >= 1, then this specifies a count (of"
        + " times the term must appear in the document); if this is a double in [0,1), then this "
        + "specifies a fraction (out of the document's token count). Note that the parameter is "
        + "only used in transform of CountVectorizerModel and does not affect fitting. Default 1.0",
        typeConverter=TypeConverters.toFloat,
    )
    minDF: Param[float] = Param(
        Params._dummy(),
        "minDF",
        "Specifies the minimum number of"
        + " different documents a term must appear in to be included in the vocabulary."
        + " If this is an integer >= 1, this specifies the number of documents the term must"
        + " appear in; if this is a double in [0,1), then this specifies the fraction of documents."
        + " Default 1.0",
        typeConverter=TypeConverters.toFloat,
    )
    maxDF: Param[float] = Param(
        Params._dummy(),
        "maxDF",
        "Specifies the maximum number of"
        + " different documents a term could appear in to be included in the vocabulary."
        + " A term that appears more than the threshold will be ignored. If this is an"
        + " integer >= 1, this specifies the maximum number of documents the term could appear in;"
        + " if this is a double in [0,1), then this specifies the maximum"
        + " fraction of documents the term could appear in."
        + " Default (2^63) - 1",
        typeConverter=TypeConverters.toFloat,
    )
    vocabSize: Param[int] = Param(
        Params._dummy(),
        "vocabSize",
        "max size of the vocabulary. Default 1 << 18.",
        typeConverter=TypeConverters.toInt,
    )
    binary: Param[bool] = Param(
        Params._dummy(),
        "binary",
        "Binary toggle to control the output vector values."
        + " If True, all nonzero counts (after minTF filter applied) are set to 1. This is useful"
        + " for discrete probabilistic models that model binary events rather than integer counts."
        + " Default False",
        typeConverter=TypeConverters.toBoolean,
    )

    def __init__(self, *args: Any):
        super(_CountVectorizerParams, self).__init__(*args)
        self._setDefault(minTF=1.0, minDF=1.0, maxDF=2 ** 63 - 1, vocabSize=1 << 18, binary=False)

    @since("1.6.0")
    def getMinTF(self) -> float:
        """
        Gets the value of minTF or its default value.
        """
        return self.getOrDefault(self.minTF)

    @since("1.6.0")
    def getMinDF(self) -> float:
        """
        Gets the value of minDF or its default value.
        """
        return self.getOrDefault(self.minDF)

    @since("2.4.0")
    def getMaxDF(self) -> float:
        """
        Gets the value of maxDF or its default value.
        """
        return self.getOrDefault(self.maxDF)

    @since("1.6.0")
    def getVocabSize(self) -> int:
        """
        Gets the value of vocabSize or its default value.
        """
        return self.getOrDefault(self.vocabSize)

    @since("2.0.0")
    def getBinary(self) -> bool:
        """
        Gets the value of binary or its default value.
        """
        return self.getOrDefault(self.binary)


@inherit_doc
class CountVectorizer(
    JavaEstimator["CountVectorizerModel"],
    _CountVectorizerParams,
    JavaMLReadable["CountVectorizer"],
    JavaMLWritable,
):
    """
    Extracts a vocabulary from document collections and generates a :py:attr:`CountVectorizerModel`.

    .. versionadded:: 1.6.0

    Examples
    --------
    >>> df = spark.createDataFrame(
    ...    [(0, ["a", "b", "c"]), (1, ["a", "b", "b", "c", "a"])],
    ...    ["label", "raw"])
    >>> cv = CountVectorizer()
    >>> cv.setInputCol("raw")
    CountVectorizer...
    >>> cv.setOutputCol("vectors")
    CountVectorizer...
    >>> model = cv.fit(df)
    >>> model.setInputCol("raw")
    CountVectorizerModel...
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
    >>> loadedModel.transform(df).take(1) == model.transform(df).take(1)
    True
    >>> fromVocabModel = CountVectorizerModel.from_vocabulary(["a", "b", "c"],
    ...     inputCol="raw", outputCol="vectors")
    >>> fromVocabModel.transform(df).show(truncate=False)
    +-----+---------------+-------------------------+
    |label|raw            |vectors                  |
    +-----+---------------+-------------------------+
    |0    |[a, b, c]      |(3,[0,1,2],[1.0,1.0,1.0])|
    |1    |[a, b, b, c, a]|(3,[0,1,2],[2.0,2.0,1.0])|
    +-----+---------------+-------------------------+
    ...
    """

    _input_kwargs: Dict[str, Any]

    @keyword_only
    def __init__(
        self,
        *,
        minTF: float = 1.0,
        minDF: float = 1.0,
        maxDF: float = 2 ** 63 - 1,
        vocabSize: int = 1 << 18,
        binary: bool = False,
        inputCol: Optional[str] = None,
        outputCol: Optional[str] = None,
    ):
        """
        __init__(self, \\*, minTF=1.0, minDF=1.0, maxDF=2 ** 63 - 1, vocabSize=1 << 18,\
                 binary=False, inputCol=None,outputCol=None)
        """
        super(CountVectorizer, self).__init__()
        self._java_obj = self._new_java_obj("org.apache.spark.ml.feature.CountVectorizer", self.uid)
        kwargs = self._input_kwargs
        self.setParams(**kwargs)

    @keyword_only
    @since("1.6.0")
    def setParams(
        self,
        *,
        minTF: float = 1.0,
        minDF: float = 1.0,
        maxDF: float = 2 ** 63 - 1,
        vocabSize: int = 1 << 18,
        binary: bool = False,
        inputCol: Optional[str] = None,
        outputCol: Optional[str] = None,
    ) -> "CountVectorizer":
        """
        setParams(self, \\*, minTF=1.0, minDF=1.0, maxDF=2 ** 63 - 1, vocabSize=1 << 18,\
                  binary=False, inputCol=None, outputCol=None)
        Set the params for the CountVectorizer
        """
        kwargs = self._input_kwargs
        return self._set(**kwargs)

    @since("1.6.0")
    def setMinTF(self, value: float) -> "CountVectorizer":
        """
        Sets the value of :py:attr:`minTF`.
        """
        return self._set(minTF=value)

    @since("1.6.0")
    def setMinDF(self, value: float) -> "CountVectorizer":
        """
        Sets the value of :py:attr:`minDF`.
        """
        return self._set(minDF=value)

    @since("2.4.0")
    def setMaxDF(self, value: float) -> "CountVectorizer":
        """
        Sets the value of :py:attr:`maxDF`.
        """
        return self._set(maxDF=value)

    @since("1.6.0")
    def setVocabSize(self, value: int) -> "CountVectorizer":
        """
        Sets the value of :py:attr:`vocabSize`.
        """
        return self._set(vocabSize=value)

    @since("2.0.0")
    def setBinary(self, value: bool) -> "CountVectorizer":
        """
        Sets the value of :py:attr:`binary`.
        """
        return self._set(binary=value)

    def setInputCol(self, value: str) -> "CountVectorizer":
        """
        Sets the value of :py:attr:`inputCol`.
        """
        return self._set(inputCol=value)

    def setOutputCol(self, value: str) -> "CountVectorizer":
        """
        Sets the value of :py:attr:`outputCol`.
        """
        return self._set(outputCol=value)

    def _create_model(self, java_model: "JavaObject") -> "CountVectorizerModel":
        return CountVectorizerModel(java_model)


@inherit_doc
class CountVectorizerModel(
    JavaModel, _CountVectorizerParams, JavaMLReadable["CountVectorizerModel"], JavaMLWritable
):
    """
    Model fitted by :py:class:`CountVectorizer`.

    .. versionadded:: 1.6.0
    """

    @since("3.0.0")
    def setInputCol(self, value: str) -> "CountVectorizerModel":
        """
        Sets the value of :py:attr:`inputCol`.
        """
        return self._set(inputCol=value)

    @since("3.0.0")
    def setOutputCol(self, value: str) -> "CountVectorizerModel":
        """
        Sets the value of :py:attr:`outputCol`.
        """
        return self._set(outputCol=value)

    @classmethod
    @since("2.4.0")
    def from_vocabulary(
        cls,
        vocabulary: List[str],
        inputCol: str,
        outputCol: Optional[str] = None,
        minTF: Optional[float] = None,
        binary: Optional[bool] = None,
    ) -> "CountVectorizerModel":
        """
        Construct the model directly from a vocabulary list of strings,
        requires an active SparkContext.
        """
        sc = SparkContext._active_spark_context
        assert sc is not None and sc._gateway is not None
        java_class = sc._gateway.jvm.java.lang.String
        jvocab = CountVectorizerModel._new_java_array(vocabulary, java_class)
        model = CountVectorizerModel._create_from_java_class(
            "org.apache.spark.ml.feature.CountVectorizerModel", jvocab
        )
        model.setInputCol(inputCol)
        if outputCol is not None:
            model.setOutputCol(outputCol)
        if minTF is not None:
            model.setMinTF(minTF)
        if binary is not None:
            model.setBinary(binary)
        model._set(vocabSize=len(vocabulary))
        return model

    @property  # type: ignore[misc]
    @since("1.6.0")
    def vocabulary(self) -> List[str]:
        """
        An array of terms in the vocabulary.
        """
        return self._call_java("vocabulary")

    @since("2.4.0")
    def setMinTF(self, value: float) -> "CountVectorizerModel":
        """
        Sets the value of :py:attr:`minTF`.
        """
        return self._set(minTF=value)

    @since("2.4.0")
    def setBinary(self, value: bool) -> "CountVectorizerModel":
        """
        Sets the value of :py:attr:`binary`.
        """
        return self._set(binary=value)


@inherit_doc
class DCT(JavaTransformer, HasInputCol, HasOutputCol, JavaMLReadable["DCT"], JavaMLWritable):
    """
    A feature transformer that takes the 1D discrete cosine transform
    of a real vector. No zero padding is performed on the input vector.
    It returns a real vector of the same length representing the DCT.
    The return vector is scaled such that the transform matrix is
    unitary (aka scaled DCT-II).

    .. versionadded:: 1.6.0

    Notes
    -----
    `More information on Wikipedia \
      <https://en.wikipedia.org/wiki/Discrete_cosine_transform#DCT-II Wikipedia>`_.

    Examples
    --------
    >>> from pyspark.ml.linalg import Vectors
    >>> df1 = spark.createDataFrame([(Vectors.dense([5.0, 8.0, 6.0]),)], ["vec"])
    >>> dct = DCT( )
    >>> dct.setInverse(False)
    DCT...
    >>> dct.setInputCol("vec")
    DCT...
    >>> dct.setOutputCol("resultVec")
    DCT...
    >>> df2 = dct.transform(df1)
    >>> df2.head().resultVec
    DenseVector([10.969..., -0.707..., -2.041...])
    >>> df3 = DCT(inverse=True, inputCol="resultVec", outputCol="origVec").transform(df2)
    >>> df3.head().origVec
    DenseVector([5.0, 8.0, 6.0])
    >>> dctPath = temp_path + "/dct"
    >>> dct.save(dctPath)
    >>> loadedDtc = DCT.load(dctPath)
    >>> loadedDtc.transform(df1).take(1) == dct.transform(df1).take(1)
    True
    >>> loadedDtc.getInverse()
    False
    """

    _input_kwargs: Dict[str, Any]

    inverse: Param[bool] = Param(
        Params._dummy(),
        "inverse",
        "Set transformer to perform inverse DCT, " + "default False.",
        typeConverter=TypeConverters.toBoolean,
    )

    @keyword_only
    def __init__(
        self,
        *,
        inverse: bool = False,
        inputCol: Optional[str] = None,
        outputCol: Optional[str] = None,
    ):
        """
        __init__(self, \\*, inverse=False, inputCol=None, outputCol=None)
        """
        super(DCT, self).__init__()
        self._java_obj = self._new_java_obj("org.apache.spark.ml.feature.DCT", self.uid)
        self._setDefault(inverse=False)
        kwargs = self._input_kwargs
        self.setParams(**kwargs)

    @keyword_only
    @since("1.6.0")
    def setParams(
        self,
        *,
        inverse: bool = False,
        inputCol: Optional[str] = None,
        outputCol: Optional[str] = None,
    ) -> "DCT":
        """
        setParams(self, \\*, inverse=False, inputCol=None, outputCol=None)
        Sets params for this DCT.
        """
        kwargs = self._input_kwargs
        return self._set(**kwargs)

    @since("1.6.0")
    def setInverse(self, value: bool) -> "DCT":
        """
        Sets the value of :py:attr:`inverse`.
        """
        return self._set(inverse=value)

    @since("1.6.0")
    def getInverse(self) -> bool:
        """
        Gets the value of inverse or its default value.
        """
        return self.getOrDefault(self.inverse)

    def setInputCol(self, value: str) -> "DCT":
        """
        Sets the value of :py:attr:`inputCol`.
        """
        return self._set(inputCol=value)

    def setOutputCol(self, value: str) -> "DCT":
        """
        Sets the value of :py:attr:`outputCol`.
        """
        return self._set(outputCol=value)


@inherit_doc
class ElementwiseProduct(
    JavaTransformer,
    HasInputCol,
    HasOutputCol,
    JavaMLReadable["ElementwiseProduct"],
    JavaMLWritable,
):
    """
    Outputs the Hadamard product (i.e., the element-wise product) of each input vector
    with a provided "weight" vector. In other words, it scales each column of the dataset
    by a scalar multiplier.

    .. versionadded:: 1.5.0

    Examples
    --------
    >>> from pyspark.ml.linalg import Vectors
    >>> df = spark.createDataFrame([(Vectors.dense([2.0, 1.0, 3.0]),)], ["values"])
    >>> ep = ElementwiseProduct()
    >>> ep.setScalingVec(Vectors.dense([1.0, 2.0, 3.0]))
    ElementwiseProduct...
    >>> ep.setInputCol("values")
    ElementwiseProduct...
    >>> ep.setOutputCol("eprod")
    ElementwiseProduct...
    >>> ep.transform(df).head().eprod
    DenseVector([2.0, 2.0, 9.0])
    >>> ep.setParams(scalingVec=Vectors.dense([2.0, 3.0, 5.0])).transform(df).head().eprod
    DenseVector([4.0, 3.0, 15.0])
    >>> elementwiseProductPath = temp_path + "/elementwise-product"
    >>> ep.save(elementwiseProductPath)
    >>> loadedEp = ElementwiseProduct.load(elementwiseProductPath)
    >>> loadedEp.getScalingVec() == ep.getScalingVec()
    True
    >>> loadedEp.transform(df).take(1) == ep.transform(df).take(1)
    True
    """

    _input_kwargs: Dict[str, Any]

    scalingVec: Param[Vector] = Param(
        Params._dummy(),
        "scalingVec",
        "Vector for hadamard product.",
        typeConverter=TypeConverters.toVector,
    )

    @keyword_only
    def __init__(
        self,
        *,
        scalingVec: Optional[Vector] = None,
        inputCol: Optional[str] = None,
        outputCol: Optional[str] = None,
    ):
        """
        __init__(self, \\*, scalingVec=None, inputCol=None, outputCol=None)
        """
        super(ElementwiseProduct, self).__init__()
        self._java_obj = self._new_java_obj(
            "org.apache.spark.ml.feature.ElementwiseProduct", self.uid
        )
        kwargs = self._input_kwargs
        self.setParams(**kwargs)

    @keyword_only
    @since("1.5.0")
    def setParams(
        self,
        *,
        scalingVec: Optional[Vector] = None,
        inputCol: Optional[str] = None,
        outputCol: Optional[str] = None,
    ) -> "ElementwiseProduct":
        """
        setParams(self, \\*, scalingVec=None, inputCol=None, outputCol=None)
        Sets params for this ElementwiseProduct.
        """
        kwargs = self._input_kwargs
        return self._set(**kwargs)

    @since("2.0.0")
    def setScalingVec(self, value: Vector) -> "ElementwiseProduct":
        """
        Sets the value of :py:attr:`scalingVec`.
        """
        return self._set(scalingVec=value)

    @since("2.0.0")
    def getScalingVec(self) -> Vector:
        """
        Gets the value of scalingVec or its default value.
        """
        return self.getOrDefault(self.scalingVec)

    def setInputCol(self, value: str) -> "ElementwiseProduct":
        """
        Sets the value of :py:attr:`inputCol`.
        """
        return self._set(inputCol=value)

    def setOutputCol(self, value: str) -> "ElementwiseProduct":
        """
        Sets the value of :py:attr:`outputCol`.
        """
        return self._set(outputCol=value)


@inherit_doc
class FeatureHasher(
    JavaTransformer,
    HasInputCols,
    HasOutputCol,
    HasNumFeatures,
    JavaMLReadable["FeatureHasher"],
    JavaMLWritable,
):
    """
    Feature hashing projects a set of categorical or numerical features into a feature vector of
    specified dimension (typically substantially smaller than that of the original feature
    space). This is done using the hashing trick (https://en.wikipedia.org/wiki/Feature_hashing)
    to map features to indices in the feature vector.

    The FeatureHasher transformer operates on multiple columns. Each column may contain either
    numeric or categorical features. Behavior and handling of column data types is as follows:

    * Numeric columns:
        For numeric features, the hash value of the column name is used to map the
        feature value to its index in the feature vector. By default, numeric features
        are not treated as categorical (even when they are integers). To treat them
        as categorical, specify the relevant columns in `categoricalCols`.

    * String columns:
        For categorical features, the hash value of the string "column_name=value"
        is used to map to the vector index, with an indicator value of `1.0`.
        Thus, categorical features are "one-hot" encoded
        (similarly to using :py:class:`OneHotEncoder` with `dropLast=false`).

    * Boolean columns:
        Boolean values are treated in the same way as string columns. That is,
        boolean features are represented as "column_name=true" or "column_name=false",
        with an indicator value of `1.0`.

    Null (missing) values are ignored (implicitly zero in the resulting feature vector).

    Since a simple modulo is used to transform the hash function to a vector index,
    it is advisable to use a power of two as the `numFeatures` parameter;
    otherwise the features will not be mapped evenly to the vector indices.

    .. versionadded:: 2.3.0

    Examples
    --------
    >>> data = [(2.0, True, "1", "foo"), (3.0, False, "2", "bar")]
    >>> cols = ["real", "bool", "stringNum", "string"]
    >>> df = spark.createDataFrame(data, cols)
    >>> hasher = FeatureHasher()
    >>> hasher.setInputCols(cols)
    FeatureHasher...
    >>> hasher.setOutputCol("features")
    FeatureHasher...
    >>> hasher.transform(df).head().features
    SparseVector(262144, {174475: 2.0, 247670: 1.0, 257907: 1.0, 262126: 1.0})
    >>> hasher.setCategoricalCols(["real"]).transform(df).head().features
    SparseVector(262144, {171257: 1.0, 247670: 1.0, 257907: 1.0, 262126: 1.0})
    >>> hasherPath = temp_path + "/hasher"
    >>> hasher.save(hasherPath)
    >>> loadedHasher = FeatureHasher.load(hasherPath)
    >>> loadedHasher.getNumFeatures() == hasher.getNumFeatures()
    True
    >>> loadedHasher.transform(df).head().features == hasher.transform(df).head().features
    True
    """

    _input_kwargs: Dict[str, Any]

    categoricalCols: Param[List[str]] = Param(
        Params._dummy(),
        "categoricalCols",
        "numeric columns to treat as categorical",
        typeConverter=TypeConverters.toListString,
    )

    @keyword_only
    def __init__(
        self,
        *,
        numFeatures: int = 1 << 18,
        inputCols: Optional[List[str]] = None,
        outputCol: Optional[str] = None,
        categoricalCols: Optional[List[str]] = None,
    ):
        """
        __init__(self, \\*, numFeatures=1 << 18, inputCols=None, outputCol=None, \
                 categoricalCols=None)
        """
        super(FeatureHasher, self).__init__()
        self._java_obj = self._new_java_obj("org.apache.spark.ml.feature.FeatureHasher", self.uid)
        self._setDefault(numFeatures=1 << 18)
        kwargs = self._input_kwargs
        self.setParams(**kwargs)

    @keyword_only
    @since("2.3.0")
    def setParams(
        self,
        *,
        numFeatures: int = 1 << 18,
        inputCols: Optional[List[str]] = None,
        outputCol: Optional[str] = None,
        categoricalCols: Optional[List[str]] = None,
    ) -> "FeatureHasher":
        """
        setParams(self, \\*, numFeatures=1 << 18, inputCols=None, outputCol=None, \
                  categoricalCols=None)
        Sets params for this FeatureHasher.
        """
        kwargs = self._input_kwargs
        return self._set(**kwargs)

    @since("2.3.0")
    def setCategoricalCols(self, value: List[str]) -> "FeatureHasher":
        """
        Sets the value of :py:attr:`categoricalCols`.
        """
        return self._set(categoricalCols=value)

    @since("2.3.0")
    def getCategoricalCols(self) -> List[str]:
        """
        Gets the value of binary or its default value.
        """
        return self.getOrDefault(self.categoricalCols)

    def setInputCols(self, value: List[str]) -> "FeatureHasher":
        """
        Sets the value of :py:attr:`inputCols`.
        """
        return self._set(inputCols=value)

    def setOutputCol(self, value: str) -> "FeatureHasher":
        """
        Sets the value of :py:attr:`outputCol`.
        """
        return self._set(outputCol=value)

    def setNumFeatures(self, value: int) -> "FeatureHasher":
        """
        Sets the value of :py:attr:`numFeatures`.
        """
        return self._set(numFeatures=value)


@inherit_doc
class HashingTF(
    JavaTransformer,
    HasInputCol,
    HasOutputCol,
    HasNumFeatures,
    JavaMLReadable["HashingTF"],
    JavaMLWritable,
):
    """
    Maps a sequence of terms to their term frequencies using the hashing trick.
    Currently we use Austin Appleby's MurmurHash 3 algorithm (MurmurHash3_x86_32)
    to calculate the hash code value for the term object.
    Since a simple modulo is used to transform the hash function to a column index,
    it is advisable to use a power of two as the numFeatures parameter;
    otherwise the features will not be mapped evenly to the columns.

    .. versionadded:: 1.3.0

    Examples
    --------
    >>> df = spark.createDataFrame([(["a", "b", "c"],)], ["words"])
    >>> hashingTF = HashingTF(inputCol="words", outputCol="features")
    >>> hashingTF.setNumFeatures(10)
    HashingTF...
    >>> hashingTF.transform(df).head().features
    SparseVector(10, {5: 1.0, 7: 1.0, 8: 1.0})
    >>> hashingTF.setParams(outputCol="freqs").transform(df).head().freqs
    SparseVector(10, {5: 1.0, 7: 1.0, 8: 1.0})
    >>> params = {hashingTF.numFeatures: 5, hashingTF.outputCol: "vector"}
    >>> hashingTF.transform(df, params).head().vector
    SparseVector(5, {0: 1.0, 2: 1.0, 3: 1.0})
    >>> hashingTFPath = temp_path + "/hashing-tf"
    >>> hashingTF.save(hashingTFPath)
    >>> loadedHashingTF = HashingTF.load(hashingTFPath)
    >>> loadedHashingTF.getNumFeatures() == hashingTF.getNumFeatures()
    True
    >>> loadedHashingTF.transform(df).take(1) == hashingTF.transform(df).take(1)
    True
    >>> hashingTF.indexOf("b")
    5
    """

    _input_kwargs: Dict[str, Any]

    binary: Param[bool] = Param(
        Params._dummy(),
        "binary",
        "If True, all non zero counts are set to 1. "
        + "This is useful for discrete probabilistic models that model binary events "
        + "rather than integer counts. Default False.",
        typeConverter=TypeConverters.toBoolean,
    )

    @keyword_only
    def __init__(
        self,
        *,
        numFeatures: int = 1 << 18,
        binary: bool = False,
        inputCol: Optional[str] = None,
        outputCol: Optional[str] = None,
    ):
        """
        __init__(self, \\*, numFeatures=1 << 18, binary=False, inputCol=None, outputCol=None)
        """
        super(HashingTF, self).__init__()
        self._java_obj = self._new_java_obj("org.apache.spark.ml.feature.HashingTF", self.uid)
        self._setDefault(numFeatures=1 << 18, binary=False)
        kwargs = self._input_kwargs
        self.setParams(**kwargs)

    @keyword_only
    @since("1.3.0")
    def setParams(
        self,
        *,
        numFeatures: int = 1 << 18,
        binary: bool = False,
        inputCol: Optional[str] = None,
        outputCol: Optional[str] = None,
    ) -> "HashingTF":
        """
        setParams(self, \\*, numFeatures=1 << 18, binary=False, inputCol=None, outputCol=None)
        Sets params for this HashingTF.
        """
        kwargs = self._input_kwargs
        return self._set(**kwargs)

    @since("2.0.0")
    def setBinary(self, value: bool) -> "HashingTF":
        """
        Sets the value of :py:attr:`binary`.
        """
        return self._set(binary=value)

    @since("2.0.0")
    def getBinary(self) -> bool:
        """
        Gets the value of binary or its default value.
        """
        return self.getOrDefault(self.binary)

    def setInputCol(self, value: str) -> "HashingTF":
        """
        Sets the value of :py:attr:`inputCol`.
        """
        return self._set(inputCol=value)

    def setOutputCol(self, value: str) -> "HashingTF":
        """
        Sets the value of :py:attr:`outputCol`.
        """
        return self._set(outputCol=value)

    def setNumFeatures(self, value: int) -> "HashingTF":
        """
        Sets the value of :py:attr:`numFeatures`.
        """
        return self._set(numFeatures=value)

    @since("3.0.0")
    def indexOf(self, term: Any) -> int:
        """
        Returns the index of the input term.
        """
        self._transfer_params_to_java()
        assert self._java_obj is not None
        return self._java_obj.indexOf(term)


class _IDFParams(HasInputCol, HasOutputCol):
    """
    Params for :py:class:`IDF` and :py:class:`IDFModel`.

    .. versionadded:: 3.0.0
    """

    minDocFreq: Param[int] = Param(
        Params._dummy(),
        "minDocFreq",
        "minimum number of documents in which a term should appear for filtering",
        typeConverter=TypeConverters.toInt,
    )

    @since("1.4.0")
    def getMinDocFreq(self) -> int:
        """
        Gets the value of minDocFreq or its default value.
        """
        return self.getOrDefault(self.minDocFreq)

    def __init__(self, *args: Any):
        super(_IDFParams, self).__init__(*args)
        self._setDefault(minDocFreq=0)


@inherit_doc
class IDF(JavaEstimator["IDFModel"], _IDFParams, JavaMLReadable["IDF"], JavaMLWritable):
    """
    Compute the Inverse Document Frequency (IDF) given a collection of documents.

    .. versionadded:: 1.4.0

    Examples
    --------
    >>> from pyspark.ml.linalg import DenseVector
    >>> df = spark.createDataFrame([(DenseVector([1.0, 2.0]),),
    ...     (DenseVector([0.0, 1.0]),), (DenseVector([3.0, 0.2]),)], ["tf"])
    >>> idf = IDF(minDocFreq=3)
    >>> idf.setInputCol("tf")
    IDF...
    >>> idf.setOutputCol("idf")
    IDF...
    >>> model = idf.fit(df)
    >>> model.setOutputCol("idf")
    IDFModel...
    >>> model.getMinDocFreq()
    3
    >>> model.idf
    DenseVector([0.0, 0.0])
    >>> model.docFreq
    [0, 3]
    >>> model.numDocs == df.count()
    True
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
    """

    _input_kwargs: Dict[str, Any]

    @keyword_only
    def __init__(
        self,
        *,
        minDocFreq: int = 0,
        inputCol: Optional[str] = None,
        outputCol: Optional[str] = None,
    ):
        """
        __init__(self, \\*, minDocFreq=0, inputCol=None, outputCol=None)
        """
        super(IDF, self).__init__()
        self._java_obj = self._new_java_obj("org.apache.spark.ml.feature.IDF", self.uid)
        kwargs = self._input_kwargs
        self.setParams(**kwargs)

    @keyword_only
    @since("1.4.0")
    def setParams(
        self,
        *,
        minDocFreq: int = 0,
        inputCol: Optional[str] = None,
        outputCol: Optional[str] = None,
    ) -> "IDF":
        """
        setParams(self, \\*, minDocFreq=0, inputCol=None, outputCol=None)
        Sets params for this IDF.
        """
        kwargs = self._input_kwargs
        return self._set(**kwargs)

    @since("1.4.0")
    def setMinDocFreq(self, value: int) -> "IDF":
        """
        Sets the value of :py:attr:`minDocFreq`.
        """
        return self._set(minDocFreq=value)

    def setInputCol(self, value: str) -> "IDF":
        """
        Sets the value of :py:attr:`inputCol`.
        """
        return self._set(inputCol=value)

    def setOutputCol(self, value: str) -> "IDF":
        """
        Sets the value of :py:attr:`outputCol`.
        """
        return self._set(outputCol=value)

    def _create_model(self, java_model: "JavaObject") -> "IDFModel":
        return IDFModel(java_model)


class IDFModel(JavaModel, _IDFParams, JavaMLReadable["IDFModel"], JavaMLWritable):
    """
    Model fitted by :py:class:`IDF`.

    .. versionadded:: 1.4.0
    """

    @since("3.0.0")
    def setInputCol(self, value: str) -> "IDFModel":
        """
        Sets the value of :py:attr:`inputCol`.
        """
        return self._set(inputCol=value)

    @since("3.0.0")
    def setOutputCol(self, value: str) -> "IDFModel":
        """
        Sets the value of :py:attr:`outputCol`.
        """
        return self._set(outputCol=value)

    @property  # type: ignore[misc]
    @since("2.0.0")
    def idf(self) -> Vector:
        """
        Returns the IDF vector.
        """
        return self._call_java("idf")

    @property  # type: ignore[misc]
    @since("3.0.0")
    def docFreq(self) -> List[int]:
        """
        Returns the document frequency.
        """
        return self._call_java("docFreq")

    @property  # type: ignore[misc]
    @since("3.0.0")
    def numDocs(self) -> int:
        """
        Returns number of documents evaluated to compute idf
        """
        return self._call_java("numDocs")


class _ImputerParams(HasInputCol, HasInputCols, HasOutputCol, HasOutputCols, HasRelativeError):
    """
    Params for :py:class:`Imputer` and :py:class:`ImputerModel`.

    .. versionadded:: 3.0.0
    """

    strategy: Param[str] = Param(
        Params._dummy(),
        "strategy",
        "strategy for imputation. If mean, then replace missing values using the mean "
        "value of the feature. If median, then replace missing values using the "
        "median value of the feature. If mode, then replace missing using the most "
        "frequent value of the feature.",
        typeConverter=TypeConverters.toString,
    )

    missingValue: Param[float] = Param(
        Params._dummy(),
        "missingValue",
        "The placeholder for the missing values. All occurrences of missingValue "
        "will be imputed.",
        typeConverter=TypeConverters.toFloat,
    )

    def __init__(self, *args: Any):
        super(_ImputerParams, self).__init__(*args)
        self._setDefault(strategy="mean", missingValue=float("nan"), relativeError=0.001)

    @since("2.2.0")
    def getStrategy(self) -> str:
        """
        Gets the value of :py:attr:`strategy` or its default value.
        """
        return self.getOrDefault(self.strategy)

    @since("2.2.0")
    def getMissingValue(self) -> float:
        """
        Gets the value of :py:attr:`missingValue` or its default value.
        """
        return self.getOrDefault(self.missingValue)


@inherit_doc
class Imputer(
    JavaEstimator["ImputerModel"], _ImputerParams, JavaMLReadable["Imputer"], JavaMLWritable
):
    """
    Imputation estimator for completing missing values, using the mean, median or mode
    of the columns in which the missing values are located. The input columns should be of
    numeric type. Currently Imputer does not support categorical features and
    possibly creates incorrect values for a categorical feature.

    Note that the mean/median/mode value is computed after filtering out missing values.
    All Null values in the input columns are treated as missing, and so are also imputed. For
    computing median, :py:meth:`pyspark.sql.DataFrame.approxQuantile` is used with a
    relative error of `0.001`.

    .. versionadded:: 2.2.0

    Examples
    --------
    >>> df = spark.createDataFrame([(1.0, float("nan")), (2.0, float("nan")), (float("nan"), 3.0),
    ...                             (4.0, 4.0), (5.0, 5.0)], ["a", "b"])
    >>> imputer = Imputer()
    >>> imputer.setInputCols(["a", "b"])
    Imputer...
    >>> imputer.setOutputCols(["out_a", "out_b"])
    Imputer...
    >>> imputer.getRelativeError()
    0.001
    >>> model = imputer.fit(df)
    >>> model.setInputCols(["a", "b"])
    ImputerModel...
    >>> model.getStrategy()
    'mean'
    >>> model.surrogateDF.show()
    +---+---+
    |  a|  b|
    +---+---+
    |3.0|4.0|
    +---+---+
    ...
    >>> model.transform(df).show()
    +---+---+-----+-----+
    |  a|  b|out_a|out_b|
    +---+---+-----+-----+
    |1.0|NaN|  1.0|  4.0|
    |2.0|NaN|  2.0|  4.0|
    |NaN|3.0|  3.0|  3.0|
    ...
    >>> imputer.setStrategy("median").setMissingValue(1.0).fit(df).transform(df).show()
    +---+---+-----+-----+
    |  a|  b|out_a|out_b|
    +---+---+-----+-----+
    |1.0|NaN|  4.0|  NaN|
    ...
    >>> df1 = spark.createDataFrame([(1.0,), (2.0,), (float("nan"),), (4.0,), (5.0,)], ["a"])
    >>> imputer1 = Imputer(inputCol="a", outputCol="out_a")
    >>> model1 = imputer1.fit(df1)
    >>> model1.surrogateDF.show()
    +---+
    |  a|
    +---+
    |3.0|
    +---+
    ...
    >>> model1.transform(df1).show()
    +---+-----+
    |  a|out_a|
    +---+-----+
    |1.0|  1.0|
    |2.0|  2.0|
    |NaN|  3.0|
    ...
    >>> imputer1.setStrategy("median").setMissingValue(1.0).fit(df1).transform(df1).show()
    +---+-----+
    |  a|out_a|
    +---+-----+
    |1.0|  4.0|
    ...
    >>> df2 = spark.createDataFrame([(float("nan"),), (float("nan"),), (3.0,), (4.0,), (5.0,)],
    ...                             ["b"])
    >>> imputer2 = Imputer(inputCol="b", outputCol="out_b")
    >>> model2 = imputer2.fit(df2)
    >>> model2.surrogateDF.show()
    +---+
    |  b|
    +---+
    |4.0|
    +---+
    ...
    >>> model2.transform(df2).show()
    +---+-----+
    |  b|out_b|
    +---+-----+
    |NaN|  4.0|
    |NaN|  4.0|
    |3.0|  3.0|
    ...
    >>> imputer2.setStrategy("median").setMissingValue(1.0).fit(df2).transform(df2).show()
    +---+-----+
    |  b|out_b|
    +---+-----+
    |NaN|  NaN|
    ...
    >>> imputerPath = temp_path + "/imputer"
    >>> imputer.save(imputerPath)
    >>> loadedImputer = Imputer.load(imputerPath)
    >>> loadedImputer.getStrategy() == imputer.getStrategy()
    True
    >>> loadedImputer.getMissingValue()
    1.0
    >>> modelPath = temp_path + "/imputer-model"
    >>> model.save(modelPath)
    >>> loadedModel = ImputerModel.load(modelPath)
    >>> loadedModel.transform(df).head().out_a == model.transform(df).head().out_a
    True
    """

    _input_kwargs: Dict[str, Any]

    @overload
    def __init__(
        self,
        *,
        strategy: str = ...,
        missingValue: float = ...,
        inputCols: Optional[List[str]] = ...,
        outputCols: Optional[List[str]] = ...,
        relativeError: float = ...,
    ):
        ...

    @overload
    def __init__(
        self,
        *,
        strategy: str = ...,
        missingValue: float = ...,
        inputCol: Optional[str] = ...,
        outputCol: Optional[str] = ...,
        relativeError: float = ...,
    ):
        ...

    @keyword_only
    def __init__(
        self,
        *,
        strategy: str = "mean",
        missingValue: float = float("nan"),
        inputCols: Optional[List[str]] = None,
        outputCols: Optional[List[str]] = None,
        inputCol: Optional[str] = None,
        outputCol: Optional[str] = None,
        relativeError: float = 0.001,
    ):
        """
        __init__(self, \\*, strategy="mean", missingValue=float("nan"), inputCols=None, \
                 outputCols=None, inputCol=None, outputCol=None, relativeError=0.001):
        """
        super(Imputer, self).__init__()
        self._java_obj = self._new_java_obj("org.apache.spark.ml.feature.Imputer", self.uid)
        kwargs = self._input_kwargs
        self.setParams(**kwargs)

    @overload
    def setParams(
        self,
        *,
        strategy: str = ...,
        missingValue: float = ...,
        inputCols: Optional[List[str]] = ...,
        outputCols: Optional[List[str]] = ...,
        relativeError: float = ...,
    ) -> "Imputer":
        ...

    @overload
    def setParams(
        self,
        *,
        strategy: str = ...,
        missingValue: float = ...,
        inputCol: Optional[str] = ...,
        outputCol: Optional[str] = ...,
        relativeError: float = ...,
    ) -> "Imputer":
        ...

    @keyword_only
    @since("2.2.0")
    def setParams(
        self,
        *,
        strategy: str = "mean",
        missingValue: float = float("nan"),
        inputCols: Optional[List[str]] = None,
        outputCols: Optional[List[str]] = None,
        inputCol: Optional[str] = None,
        outputCol: Optional[str] = None,
        relativeError: float = 0.001,
    ) -> "Imputer":
        """
        setParams(self, \\*, strategy="mean", missingValue=float("nan"), inputCols=None, \
                  outputCols=None, inputCol=None, outputCol=None, relativeError=0.001)
        Sets params for this Imputer.
        """
        kwargs = self._input_kwargs
        return self._set(**kwargs)

    @since("2.2.0")
    def setStrategy(self, value: str) -> "Imputer":
        """
        Sets the value of :py:attr:`strategy`.
        """
        return self._set(strategy=value)

    @since("2.2.0")
    def setMissingValue(self, value: float) -> "Imputer":
        """
        Sets the value of :py:attr:`missingValue`.
        """
        return self._set(missingValue=value)

    @since("2.2.0")
    def setInputCols(self, value: List[str]) -> "Imputer":
        """
        Sets the value of :py:attr:`inputCols`.
        """
        return self._set(inputCols=value)

    @since("2.2.0")
    def setOutputCols(self, value: List[str]) -> "Imputer":
        """
        Sets the value of :py:attr:`outputCols`.
        """
        return self._set(outputCols=value)

    @since("3.0.0")
    def setInputCol(self, value: str) -> "Imputer":
        """
        Sets the value of :py:attr:`inputCol`.
        """
        return self._set(inputCol=value)

    @since("3.0.0")
    def setOutputCol(self, value: str) -> "Imputer":
        """
        Sets the value of :py:attr:`outputCol`.
        """
        return self._set(outputCol=value)

    @since("3.0.0")
    def setRelativeError(self, value: float) -> "Imputer":
        """
        Sets the value of :py:attr:`relativeError`.
        """
        return self._set(relativeError=value)

    def _create_model(self, java_model: "JavaObject") -> "ImputerModel":
        return ImputerModel(java_model)


class ImputerModel(JavaModel, _ImputerParams, JavaMLReadable["ImputerModel"], JavaMLWritable):
    """
    Model fitted by :py:class:`Imputer`.

    .. versionadded:: 2.2.0
    """

    @since("3.0.0")
    def setInputCols(self, value: List[str]) -> "ImputerModel":
        """
        Sets the value of :py:attr:`inputCols`.
        """
        return self._set(inputCols=value)

    @since("3.0.0")
    def setOutputCols(self, value: List[str]) -> "ImputerModel":
        """
        Sets the value of :py:attr:`outputCols`.
        """
        return self._set(outputCols=value)

    @since("3.0.0")
    def setInputCol(self, value: str) -> "ImputerModel":
        """
        Sets the value of :py:attr:`inputCol`.
        """
        return self._set(inputCol=value)

    @since("3.0.0")
    def setOutputCol(self, value: str) -> "ImputerModel":
        """
        Sets the value of :py:attr:`outputCol`.
        """
        return self._set(outputCol=value)

    @property  # type: ignore[misc]
    @since("2.2.0")
    def surrogateDF(self) -> DataFrame:
        """
        Returns a DataFrame containing inputCols and their corresponding surrogates,
        which are used to replace the missing values in the input DataFrame.
        """
        return self._call_java("surrogateDF")


@inherit_doc
class Interaction(
    JavaTransformer,
    HasInputCols,
    HasOutputCol,
    JavaMLReadable["Interaction"],
    JavaMLWritable,
):
    """
    Implements the feature interaction transform. This transformer takes in Double and Vector type
    columns and outputs a flattened vector of their feature interactions. To handle interaction,
    we first one-hot encode any nominal features. Then, a vector of the feature cross-products is
    produced.

    For example, given the input feature values `Double(2)` and `Vector(3, 4)`, the output would be
    `Vector(6, 8)` if all input features were numeric. If the first feature was instead nominal
    with four categories, the output would then be `Vector(0, 0, 0, 0, 3, 4, 0, 0)`.

    .. versionadded:: 3.0.0

    Examples
    --------
    >>> df = spark.createDataFrame([(0.0, 1.0), (2.0, 3.0)], ["a", "b"])
    >>> interaction = Interaction()
    >>> interaction.setInputCols(["a", "b"])
    Interaction...
    >>> interaction.setOutputCol("ab")
    Interaction...
    >>> interaction.transform(df).show()
    +---+---+-----+
    |  a|  b|   ab|
    +---+---+-----+
    |0.0|1.0|[0.0]|
    |2.0|3.0|[6.0]|
    +---+---+-----+
    ...
    >>> interactionPath = temp_path + "/interaction"
    >>> interaction.save(interactionPath)
    >>> loadedInteraction = Interaction.load(interactionPath)
    >>> loadedInteraction.transform(df).head().ab == interaction.transform(df).head().ab
    True
    """

    _input_kwargs: Dict[str, Any]

    @keyword_only
    def __init__(self, *, inputCols: Optional[List[str]] = None, outputCol: Optional[str] = None):
        """
        __init__(self, \\*, inputCols=None, outputCol=None):
        """
        super(Interaction, self).__init__()
        self._java_obj = self._new_java_obj("org.apache.spark.ml.feature.Interaction", self.uid)
        self._setDefault()
        kwargs = self._input_kwargs
        self.setParams(**kwargs)

    @keyword_only
    @since("3.0.0")
    def setParams(
        self, *, inputCols: Optional[List[str]] = None, outputCol: Optional[str] = None
    ) -> "Interaction":
        """
        setParams(self, \\*, inputCols=None, outputCol=None)
        Sets params for this Interaction.
        """
        kwargs = self._input_kwargs
        return self._set(**kwargs)

    @since("3.0.0")
    def setInputCols(self, value: List[str]) -> "Interaction":
        """
        Sets the value of :py:attr:`inputCols`.
        """
        return self._set(inputCols=value)

    @since("3.0.0")
    def setOutputCol(self, value: str) -> "Interaction":
        """
        Sets the value of :py:attr:`outputCol`.
        """
        return self._set(outputCol=value)


class _MaxAbsScalerParams(HasInputCol, HasOutputCol):
    """
    Params for :py:class:`MaxAbsScaler` and :py:class:`MaxAbsScalerModel`.

    .. versionadded:: 3.0.0
    """

    pass


@inherit_doc
class MaxAbsScaler(
    JavaEstimator["MaxAbsScalerModel"],
    _MaxAbsScalerParams,
    JavaMLReadable["MaxAbsScaler"],
    JavaMLWritable,
):
    """
    Rescale each feature individually to range [-1, 1] by dividing through the largest maximum
    absolute value in each feature. It does not shift/center the data, and thus does not destroy
    any sparsity.

    .. versionadded:: 2.0.0

    Examples
    --------
    >>> from pyspark.ml.linalg import Vectors
    >>> df = spark.createDataFrame([(Vectors.dense([1.0]),), (Vectors.dense([2.0]),)], ["a"])
    >>> maScaler = MaxAbsScaler(outputCol="scaled")
    >>> maScaler.setInputCol("a")
    MaxAbsScaler...
    >>> model = maScaler.fit(df)
    >>> model.setOutputCol("scaledOutput")
    MaxAbsScalerModel...
    >>> model.transform(df).show()
    +-----+------------+
    |    a|scaledOutput|
    +-----+------------+
    |[1.0]|       [0.5]|
    |[2.0]|       [1.0]|
    +-----+------------+
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
    >>> loadedModel.transform(df).take(1) == model.transform(df).take(1)
    True
    """

    _input_kwargs: Dict[str, Any]

    @keyword_only
    def __init__(self, *, inputCol: Optional[str] = None, outputCol: Optional[str] = None):
        """
        __init__(self, \\*, inputCol=None, outputCol=None)
        """
        super(MaxAbsScaler, self).__init__()
        self._java_obj = self._new_java_obj("org.apache.spark.ml.feature.MaxAbsScaler", self.uid)
        self._setDefault()
        kwargs = self._input_kwargs
        self.setParams(**kwargs)

    @keyword_only
    @since("2.0.0")
    def setParams(
        self, *, inputCol: Optional[str] = None, outputCol: Optional[str] = None
    ) -> "MaxAbsScaler":
        """
        setParams(self, \\*, inputCol=None, outputCol=None)
        Sets params for this MaxAbsScaler.
        """
        kwargs = self._input_kwargs
        return self._set(**kwargs)

    def setInputCol(self, value: str) -> "MaxAbsScaler":
        """
        Sets the value of :py:attr:`inputCol`.
        """
        return self._set(inputCol=value)

    def setOutputCol(self, value: str) -> "MaxAbsScaler":
        """
        Sets the value of :py:attr:`outputCol`.
        """
        return self._set(outputCol=value)

    def _create_model(self, java_model: "JavaObject") -> "MaxAbsScalerModel":
        return MaxAbsScalerModel(java_model)


class MaxAbsScalerModel(
    JavaModel, _MaxAbsScalerParams, JavaMLReadable["MaxAbsScalerModel"], JavaMLWritable
):
    """
    Model fitted by :py:class:`MaxAbsScaler`.

    .. versionadded:: 2.0.0
    """

    @since("3.0.0")
    def setInputCol(self, value: str) -> "MaxAbsScalerModel":
        """
        Sets the value of :py:attr:`inputCol`.
        """
        return self._set(inputCol=value)

    @since("3.0.0")
    def setOutputCol(self, value: str) -> "MaxAbsScalerModel":
        """
        Sets the value of :py:attr:`outputCol`.
        """
        return self._set(outputCol=value)

    @property  # type: ignore[misc]
    @since("2.0.0")
    def maxAbs(self) -> Vector:
        """
        Max Abs vector.
        """
        return self._call_java("maxAbs")


@inherit_doc
class MinHashLSH(
    _LSH["MinHashLSHModel"],
    HasInputCol,
    HasOutputCol,
    HasSeed,
    JavaMLReadable["MinHashLSH"],
    JavaMLWritable,
):

    """
    LSH class for Jaccard distance.
    The input can be dense or sparse vectors, but it is more efficient if it is sparse.
    For example, `Vectors.sparse(10, [(2, 1.0), (3, 1.0), (5, 1.0)])` means there are 10 elements
    in the space. This set contains elements 2, 3, and 5. Also, any input vector must have at
    least 1 non-zero index, and all non-zero values are treated as binary "1" values.

    .. versionadded:: 2.2.0

    Notes
    -----
    See `Wikipedia on MinHash <https://en.wikipedia.org/wiki/MinHash>`_

    Examples
    --------
    >>> from pyspark.ml.linalg import Vectors
    >>> from pyspark.sql.functions import col
    >>> data = [(0, Vectors.sparse(6, [0, 1, 2], [1.0, 1.0, 1.0]),),
    ...         (1, Vectors.sparse(6, [2, 3, 4], [1.0, 1.0, 1.0]),),
    ...         (2, Vectors.sparse(6, [0, 2, 4], [1.0, 1.0, 1.0]),)]
    >>> df = spark.createDataFrame(data, ["id", "features"])
    >>> mh = MinHashLSH()
    >>> mh.setInputCol("features")
    MinHashLSH...
    >>> mh.setOutputCol("hashes")
    MinHashLSH...
    >>> mh.setSeed(12345)
    MinHashLSH...
    >>> model = mh.fit(df)
    >>> model.setInputCol("features")
    MinHashLSHModel...
    >>> model.transform(df).head()
    Row(id=0, features=SparseVector(6, {0: 1.0, 1: 1.0, 2: 1.0}), hashes=[DenseVector([6179668...
    >>> data2 = [(3, Vectors.sparse(6, [1, 3, 5], [1.0, 1.0, 1.0]),),
    ...          (4, Vectors.sparse(6, [2, 3, 5], [1.0, 1.0, 1.0]),),
    ...          (5, Vectors.sparse(6, [1, 2, 4], [1.0, 1.0, 1.0]),)]
    >>> df2 = spark.createDataFrame(data2, ["id", "features"])
    >>> key = Vectors.sparse(6, [1, 2], [1.0, 1.0])
    >>> model.approxNearestNeighbors(df2, key, 1).collect()
    [Row(id=5, features=SparseVector(6, {1: 1.0, 2: 1.0, 4: 1.0}), hashes=[DenseVector([6179668...
    >>> model.approxSimilarityJoin(df, df2, 0.6, distCol="JaccardDistance").select(
    ...     col("datasetA.id").alias("idA"),
    ...     col("datasetB.id").alias("idB"),
    ...     col("JaccardDistance")).show()
    +---+---+---------------+
    |idA|idB|JaccardDistance|
    +---+---+---------------+
    |  0|  5|            0.5|
    |  1|  4|            0.5|
    +---+---+---------------+
    ...
    >>> mhPath = temp_path + "/mh"
    >>> mh.save(mhPath)
    >>> mh2 = MinHashLSH.load(mhPath)
    >>> mh2.getOutputCol() == mh.getOutputCol()
    True
    >>> modelPath = temp_path + "/mh-model"
    >>> model.save(modelPath)
    >>> model2 = MinHashLSHModel.load(modelPath)
    >>> model.transform(df).head().hashes == model2.transform(df).head().hashes
    True
    """

    _input_kwargs: Dict[str, Any]

    @keyword_only
    def __init__(
        self,
        *,
        inputCol: Optional[str] = None,
        outputCol: Optional[str] = None,
        seed: Optional[int] = None,
        numHashTables: int = 1,
    ):
        """
        __init__(self, \\*, inputCol=None, outputCol=None, seed=None, numHashTables=1)
        """
        super(MinHashLSH, self).__init__()
        self._java_obj = self._new_java_obj("org.apache.spark.ml.feature.MinHashLSH", self.uid)
        kwargs = self._input_kwargs
        self.setParams(**kwargs)

    @keyword_only
    @since("2.2.0")
    def setParams(
        self,
        *,
        inputCol: Optional[str] = None,
        outputCol: Optional[str] = None,
        seed: Optional[int] = None,
        numHashTables: int = 1,
    ) -> "MinHashLSH":
        """
        setParams(self, \\*, inputCol=None, outputCol=None, seed=None, numHashTables=1)
        Sets params for this MinHashLSH.
        """
        kwargs = self._input_kwargs
        return self._set(**kwargs)

    def setSeed(self, value: int) -> "MinHashLSH":
        """
        Sets the value of :py:attr:`seed`.
        """
        return self._set(seed=value)

    def _create_model(self, java_model: "JavaObject") -> "MinHashLSHModel":
        return MinHashLSHModel(java_model)


class MinHashLSHModel(_LSHModel, JavaMLReadable, JavaMLWritable):
    r"""
    Model produced by :py:class:`MinHashLSH`, where where multiple hash functions are stored. Each
    hash function is picked from the following family of hash functions, where :math:`a_i` and
    :math:`b_i` are randomly chosen integers less than prime:
    :math:`h_i(x) = ((x \cdot a_i + b_i) \mod prime)` This hash family is approximately min-wise
    independent according to the reference.

    .. versionadded:: 2.2.0

    Notes
    -----
    See Tom Bohman, Colin Cooper, and Alan Frieze. "Min-wise independent linear permutations."
    Electronic Journal of Combinatorics 7 (2000): R26.
    """


class _MinMaxScalerParams(HasInputCol, HasOutputCol):
    """
    Params for :py:class:`MinMaxScaler` and :py:class:`MinMaxScalerModel`.

    .. versionadded:: 3.0.0
    """

    min: Param[float] = Param(
        Params._dummy(),
        "min",
        "Lower bound of the output feature range",
        typeConverter=TypeConverters.toFloat,
    )
    max: Param[float] = Param(
        Params._dummy(),
        "max",
        "Upper bound of the output feature range",
        typeConverter=TypeConverters.toFloat,
    )

    def __init__(self, *args: Any):
        super(_MinMaxScalerParams, self).__init__(*args)
        self._setDefault(min=0.0, max=1.0)

    @since("1.6.0")
    def getMin(self) -> float:
        """
        Gets the value of min or its default value.
        """
        return self.getOrDefault(self.min)

    @since("1.6.0")
    def getMax(self) -> float:
        """
        Gets the value of max or its default value.
        """
        return self.getOrDefault(self.max)


@inherit_doc
class MinMaxScaler(
    JavaEstimator["MinMaxScalerModel"],
    _MinMaxScalerParams,
    JavaMLReadable["MinMaxScaler"],
    JavaMLWritable,
):
    """
    Rescale each feature individually to a common range [min, max] linearly using column summary
    statistics, which is also known as min-max normalization or Rescaling. The rescaled value for
    feature E is calculated as,

    Rescaled(e_i) = (e_i - E_min) / (E_max - E_min) * (max - min) + min

    For the case E_max == E_min, Rescaled(e_i) = 0.5 * (max + min)

    .. versionadded:: 1.6.0

    Notes
    -----
    Since zero values will probably be transformed to non-zero values, output of the
    transformer will be DenseVector even for sparse input.

    Examples
    --------
    >>> from pyspark.ml.linalg import Vectors
    >>> df = spark.createDataFrame([(Vectors.dense([0.0]),), (Vectors.dense([2.0]),)], ["a"])
    >>> mmScaler = MinMaxScaler(outputCol="scaled")
    >>> mmScaler.setInputCol("a")
    MinMaxScaler...
    >>> model = mmScaler.fit(df)
    >>> model.setOutputCol("scaledOutput")
    MinMaxScalerModel...
    >>> model.originalMin
    DenseVector([0.0])
    >>> model.originalMax
    DenseVector([2.0])
    >>> model.transform(df).show()
    +-----+------------+
    |    a|scaledOutput|
    +-----+------------+
    |[0.0]|       [0.0]|
    |[2.0]|       [1.0]|
    +-----+------------+
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
    >>> loadedModel.transform(df).take(1) == model.transform(df).take(1)
    True
    """

    _input_kwargs: Dict[str, Any]

    @keyword_only
    def __init__(
        self,
        *,
        min: float = 0.0,
        max: float = 1.0,
        inputCol: Optional[str] = None,
        outputCol: Optional[str] = None,
    ):
        """
        __init__(self, \\*, min=0.0, max=1.0, inputCol=None, outputCol=None)
        """
        super(MinMaxScaler, self).__init__()
        self._java_obj = self._new_java_obj("org.apache.spark.ml.feature.MinMaxScaler", self.uid)
        kwargs = self._input_kwargs
        self.setParams(**kwargs)

    @keyword_only
    @since("1.6.0")
    def setParams(
        self,
        *,
        min: float = 0.0,
        max: float = 1.0,
        inputCol: Optional[str] = None,
        outputCol: Optional[str] = None,
    ) -> "MinMaxScaler":
        """
        setParams(self, \\*, min=0.0, max=1.0, inputCol=None, outputCol=None)
        Sets params for this MinMaxScaler.
        """
        kwargs = self._input_kwargs
        return self._set(**kwargs)

    @since("1.6.0")
    def setMin(self, value: float) -> "MinMaxScaler":
        """
        Sets the value of :py:attr:`min`.
        """
        return self._set(min=value)

    @since("1.6.0")
    def setMax(self, value: float) -> "MinMaxScaler":
        """
        Sets the value of :py:attr:`max`.
        """
        return self._set(max=value)

    def setInputCol(self, value: str) -> "MinMaxScaler":
        """
        Sets the value of :py:attr:`inputCol`.
        """
        return self._set(inputCol=value)

    def setOutputCol(self, value: str) -> "MinMaxScaler":
        """
        Sets the value of :py:attr:`outputCol`.
        """
        return self._set(outputCol=value)

    def _create_model(self, java_model: "JavaObject") -> "MinMaxScalerModel":
        return MinMaxScalerModel(java_model)


class MinMaxScalerModel(
    JavaModel, _MinMaxScalerParams, JavaMLReadable["MinMaxScalerModel"], JavaMLWritable
):
    """
    Model fitted by :py:class:`MinMaxScaler`.

    .. versionadded:: 1.6.0
    """

    @since("3.0.0")
    def setInputCol(self, value: str) -> "MinMaxScalerModel":
        """
        Sets the value of :py:attr:`inputCol`.
        """
        return self._set(inputCol=value)

    @since("3.0.0")
    def setOutputCol(self, value: str) -> "MinMaxScalerModel":
        """
        Sets the value of :py:attr:`outputCol`.
        """
        return self._set(outputCol=value)

    @since("3.0.0")
    def setMin(self, value: float) -> "MinMaxScalerModel":
        """
        Sets the value of :py:attr:`min`.
        """
        return self._set(min=value)

    @since("3.0.0")
    def setMax(self, value: float) -> "MinMaxScalerModel":
        """
        Sets the value of :py:attr:`max`.
        """
        return self._set(max=value)

    @property  # type: ignore[misc]
    @since("2.0.0")
    def originalMin(self) -> Vector:
        """
        Min value for each original column during fitting.
        """
        return self._call_java("originalMin")

    @property  # type: ignore[misc]
    @since("2.0.0")
    def originalMax(self) -> Vector:
        """
        Max value for each original column during fitting.
        """
        return self._call_java("originalMax")


@inherit_doc
class NGram(JavaTransformer, HasInputCol, HasOutputCol, JavaMLReadable["NGram"], JavaMLWritable):
    """
    A feature transformer that converts the input array of strings into an array of n-grams. Null
    values in the input array are ignored.
    It returns an array of n-grams where each n-gram is represented by a space-separated string of
    words.
    When the input is empty, an empty array is returned.
    When the input array length is less than n (number of elements per n-gram), no n-grams are
    returned.

    .. versionadded:: 1.5.0

    Examples
    --------
    >>> df = spark.createDataFrame([Row(inputTokens=["a", "b", "c", "d", "e"])])
    >>> ngram = NGram(n=2)
    >>> ngram.setInputCol("inputTokens")
    NGram...
    >>> ngram.setOutputCol("nGrams")
    NGram...
    >>> ngram.transform(df).head()
    Row(inputTokens=['a', 'b', 'c', 'd', 'e'], nGrams=['a b', 'b c', 'c d', 'd e'])
    >>> # Change n-gram length
    >>> ngram.setParams(n=4).transform(df).head()
    Row(inputTokens=['a', 'b', 'c', 'd', 'e'], nGrams=['a b c d', 'b c d e'])
    >>> # Temporarily modify output column.
    >>> ngram.transform(df, {ngram.outputCol: "output"}).head()
    Row(inputTokens=['a', 'b', 'c', 'd', 'e'], output=['a b c d', 'b c d e'])
    >>> ngram.transform(df).head()
    Row(inputTokens=['a', 'b', 'c', 'd', 'e'], nGrams=['a b c d', 'b c d e'])
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
    >>> loadedNGram.transform(df).take(1) == ngram.transform(df).take(1)
    True
    """

    _input_kwargs: Dict[str, Any]

    n: Param[int] = Param(
        Params._dummy(),
        "n",
        "number of elements per n-gram (>=1)",
        typeConverter=TypeConverters.toInt,
    )

    @keyword_only
    def __init__(
        self, *, n: int = 2, inputCol: Optional[str] = None, outputCol: Optional[str] = None
    ):
        """
        __init__(self, \\*, n=2, inputCol=None, outputCol=None)
        """
        super(NGram, self).__init__()
        self._java_obj = self._new_java_obj("org.apache.spark.ml.feature.NGram", self.uid)
        self._setDefault(n=2)
        kwargs = self._input_kwargs
        self.setParams(**kwargs)

    @keyword_only
    @since("1.5.0")
    def setParams(
        self, *, n: int = 2, inputCol: Optional[str] = None, outputCol: Optional[str] = None
    ) -> "NGram":
        """
        setParams(self, \\*, n=2, inputCol=None, outputCol=None)
        Sets params for this NGram.
        """
        kwargs = self._input_kwargs
        return self._set(**kwargs)

    @since("1.5.0")
    def setN(self, value: int) -> "NGram":
        """
        Sets the value of :py:attr:`n`.
        """
        return self._set(n=value)

    @since("1.5.0")
    def getN(self) -> int:
        """
        Gets the value of n or its default value.
        """
        return self.getOrDefault(self.n)

    def setInputCol(self, value: str) -> "NGram":
        """
        Sets the value of :py:attr:`inputCol`.
        """
        return self._set(inputCol=value)

    def setOutputCol(self, value: str) -> "NGram":
        """
        Sets the value of :py:attr:`outputCol`.
        """
        return self._set(outputCol=value)


@inherit_doc
class Normalizer(
    JavaTransformer,
    HasInputCol,
    HasOutputCol,
    JavaMLReadable["Normalizer"],
    JavaMLWritable,
):
    """
     Normalize a vector to have unit norm using the given p-norm.

    .. versionadded:: 1.4.0

    Examples
    --------
    >>> from pyspark.ml.linalg import Vectors
    >>> svec = Vectors.sparse(4, {1: 4.0, 3: 3.0})
    >>> df = spark.createDataFrame([(Vectors.dense([3.0, -4.0]), svec)], ["dense", "sparse"])
    >>> normalizer = Normalizer(p=2.0)
    >>> normalizer.setInputCol("dense")
    Normalizer...
    >>> normalizer.setOutputCol("features")
    Normalizer...
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
    >>> loadedNormalizer.transform(df).take(1) == normalizer.transform(df).take(1)
    True
    """

    _input_kwargs: Dict[str, Any]

    p = Param(Params._dummy(), "p", "the p norm value.", typeConverter=TypeConverters.toFloat)

    @keyword_only
    def __init__(
        self, *, p: float = 2.0, inputCol: Optional[str] = None, outputCol: Optional[str] = None
    ):
        """
        __init__(self, \\*, p=2.0, inputCol=None, outputCol=None)
        """
        super(Normalizer, self).__init__()
        self._java_obj = self._new_java_obj("org.apache.spark.ml.feature.Normalizer", self.uid)
        self._setDefault(p=2.0)
        kwargs = self._input_kwargs
        self.setParams(**kwargs)

    @keyword_only
    @since("1.4.0")
    def setParams(
        self, *, p: float = 2.0, inputCol: Optional[str] = None, outputCol: Optional[str] = None
    ) -> "Normalizer":
        """
        setParams(self, \\*, p=2.0, inputCol=None, outputCol=None)
        Sets params for this Normalizer.
        """
        kwargs = self._input_kwargs
        return self._set(**kwargs)

    @since("1.4.0")
    def setP(self, value: float) -> "Normalizer":
        """
        Sets the value of :py:attr:`p`.
        """
        return self._set(p=value)

    @since("1.4.0")
    def getP(self) -> float:
        """
        Gets the value of p or its default value.
        """
        return self.getOrDefault(self.p)

    def setInputCol(self, value: str) -> "Normalizer":
        """
        Sets the value of :py:attr:`inputCol`.
        """
        return self._set(inputCol=value)

    def setOutputCol(self, value: str) -> "Normalizer":
        """
        Sets the value of :py:attr:`outputCol`.
        """
        return self._set(outputCol=value)


class _OneHotEncoderParams(
    HasInputCol, HasInputCols, HasOutputCol, HasOutputCols, HasHandleInvalid
):
    """
    Params for :py:class:`OneHotEncoder` and :py:class:`OneHotEncoderModel`.

    .. versionadded:: 3.0.0
    """

    handleInvalid: Param[str] = Param(
        Params._dummy(),
        "handleInvalid",
        "How to handle invalid data during "
        + "transform(). Options are 'keep' (invalid data presented as an extra "
        + "categorical feature) or error (throw an error). Note that this Param "
        + "is only used during transform; during fitting, invalid data will "
        + "result in an error.",
        typeConverter=TypeConverters.toString,
    )

    dropLast: Param[bool] = Param(
        Params._dummy(),
        "dropLast",
        "whether to drop the last category",
        typeConverter=TypeConverters.toBoolean,
    )

    def __init__(self, *args: Any):
        super(_OneHotEncoderParams, self).__init__(*args)
        self._setDefault(handleInvalid="error", dropLast=True)

    @since("2.3.0")
    def getDropLast(self) -> bool:
        """
        Gets the value of dropLast or its default value.
        """
        return self.getOrDefault(self.dropLast)


@inherit_doc
class OneHotEncoder(
    JavaEstimator["OneHotEncoderModel"],
    _OneHotEncoderParams,
    JavaMLReadable["OneHotEncoder"],
    JavaMLWritable,
):
    """
    A one-hot encoder that maps a column of category indices to a column of binary vectors, with
    at most a single one-value per row that indicates the input category index.
    For example with 5 categories, an input value of 2.0 would map to an output vector of
    `[0.0, 0.0, 1.0, 0.0]`.
    The last category is not included by default (configurable via :py:attr:`dropLast`),
    because it makes the vector entries sum up to one, and hence linearly dependent.
    So an input value of 4.0 maps to `[0.0, 0.0, 0.0, 0.0]`.

    When :py:attr:`handleInvalid` is configured to 'keep', an extra "category" indicating invalid
    values is added as last category. So when :py:attr:`dropLast` is true, invalid values are
    encoded as all-zeros vector.

    .. versionadded:: 2.3.0

    Notes
    -----
    This is different from scikit-learn's OneHotEncoder, which keeps all categories.
    The output vectors are sparse.

    When encoding multi-column by using :py:attr:`inputCols` and
    :py:attr:`outputCols` params, input/output cols come in pairs, specified by the order in
    the arrays, and each pair is treated independently.

    See Also
    --------
    StringIndexer : for converting categorical values into category indices

    Examples
    --------
    >>> from pyspark.ml.linalg import Vectors
    >>> df = spark.createDataFrame([(0.0,), (1.0,), (2.0,)], ["input"])
    >>> ohe = OneHotEncoder()
    >>> ohe.setInputCols(["input"])
    OneHotEncoder...
    >>> ohe.setOutputCols(["output"])
    OneHotEncoder...
    >>> model = ohe.fit(df)
    >>> model.setOutputCols(["output"])
    OneHotEncoderModel...
    >>> model.getHandleInvalid()
    'error'
    >>> model.transform(df).head().output
    SparseVector(2, {0: 1.0})
    >>> single_col_ohe = OneHotEncoder(inputCol="input", outputCol="output")
    >>> single_col_model = single_col_ohe.fit(df)
    >>> single_col_model.transform(df).head().output
    SparseVector(2, {0: 1.0})
    >>> ohePath = temp_path + "/ohe"
    >>> ohe.save(ohePath)
    >>> loadedOHE = OneHotEncoder.load(ohePath)
    >>> loadedOHE.getInputCols() == ohe.getInputCols()
    True
    >>> modelPath = temp_path + "/ohe-model"
    >>> model.save(modelPath)
    >>> loadedModel = OneHotEncoderModel.load(modelPath)
    >>> loadedModel.categorySizes == model.categorySizes
    True
    >>> loadedModel.transform(df).take(1) == model.transform(df).take(1)
    True
    """

    _input_kwargs: Dict[str, Any]

    @overload
    def __init__(
        self,
        *,
        inputCols: Optional[List[str]] = ...,
        outputCols: Optional[List[str]] = ...,
        handleInvalid: str = ...,
        dropLast: bool = ...,
    ):
        ...

    @overload
    def __init__(
        self,
        *,
        handleInvalid: str = ...,
        dropLast: bool = ...,
        inputCol: Optional[str] = ...,
        outputCol: Optional[str] = ...,
    ):
        ...

    @keyword_only
    def __init__(
        self,
        *,
        inputCols: Optional[List[str]] = None,
        outputCols: Optional[List[str]] = None,
        handleInvalid: str = "error",
        dropLast: bool = True,
        inputCol: Optional[str] = None,
        outputCol: Optional[str] = None,
    ):
        """
        __init__(self, \\*, inputCols=None, outputCols=None, handleInvalid="error", dropLast=True, \
                 inputCol=None, outputCol=None)
        """
        super(OneHotEncoder, self).__init__()
        self._java_obj = self._new_java_obj("org.apache.spark.ml.feature.OneHotEncoder", self.uid)
        kwargs = self._input_kwargs
        self.setParams(**kwargs)

    @overload
    def setParams(
        self,
        *,
        inputCols: Optional[List[str]] = ...,
        outputCols: Optional[List[str]] = ...,
        handleInvalid: str = ...,
        dropLast: bool = ...,
    ) -> "OneHotEncoder":
        ...

    @overload
    def setParams(
        self,
        *,
        handleInvalid: str = ...,
        dropLast: bool = ...,
        inputCol: Optional[str] = ...,
        outputCol: Optional[str] = ...,
    ) -> "OneHotEncoder":
        ...

    @keyword_only
    @since("2.3.0")
    def setParams(
        self,
        *,
        inputCols: Optional[List[str]] = None,
        outputCols: Optional[List[str]] = None,
        handleInvalid: str = "error",
        dropLast: bool = True,
        inputCol: Optional[str] = None,
        outputCol: Optional[str] = None,
    ) -> "OneHotEncoder":
        """
        setParams(self, \\*, inputCols=None, outputCols=None, handleInvalid="error", \
                  dropLast=True, inputCol=None, outputCol=None)
        Sets params for this OneHotEncoder.
        """
        kwargs = self._input_kwargs
        return self._set(**kwargs)

    @since("2.3.0")
    def setDropLast(self, value: bool) -> "OneHotEncoder":
        """
        Sets the value of :py:attr:`dropLast`.
        """
        return self._set(dropLast=value)

    @since("3.0.0")
    def setInputCols(self, value: List[str]) -> "OneHotEncoder":
        """
        Sets the value of :py:attr:`inputCols`.
        """
        return self._set(inputCols=value)

    @since("3.0.0")
    def setOutputCols(self, value: List[str]) -> "OneHotEncoder":
        """
        Sets the value of :py:attr:`outputCols`.
        """
        return self._set(outputCols=value)

    @since("3.0.0")
    def setHandleInvalid(self, value: str) -> "OneHotEncoder":
        """
        Sets the value of :py:attr:`handleInvalid`.
        """
        return self._set(handleInvalid=value)

    @since("3.0.0")
    def setInputCol(self, value: str) -> "OneHotEncoder":
        """
        Sets the value of :py:attr:`inputCol`.
        """
        return self._set(inputCol=value)

    @since("3.0.0")
    def setOutputCol(self, value: str) -> "OneHotEncoder":
        """
        Sets the value of :py:attr:`outputCol`.
        """
        return self._set(outputCol=value)

    def _create_model(self, java_model: "JavaObject") -> "OneHotEncoderModel":
        return OneHotEncoderModel(java_model)


class OneHotEncoderModel(
    JavaModel, _OneHotEncoderParams, JavaMLReadable["OneHotEncoderModel"], JavaMLWritable
):
    """
    Model fitted by :py:class:`OneHotEncoder`.

    .. versionadded:: 2.3.0
    """

    @since("3.0.0")
    def setDropLast(self, value: bool) -> "OneHotEncoderModel":
        """
        Sets the value of :py:attr:`dropLast`.
        """
        return self._set(dropLast=value)

    @since("3.0.0")
    def setInputCols(self, value: List[str]) -> "OneHotEncoderModel":
        """
        Sets the value of :py:attr:`inputCols`.
        """
        return self._set(inputCols=value)

    @since("3.0.0")
    def setOutputCols(self, value: List[str]) -> "OneHotEncoderModel":
        """
        Sets the value of :py:attr:`outputCols`.
        """
        return self._set(outputCols=value)

    @since("3.0.0")
    def setInputCol(self, value: str) -> "OneHotEncoderModel":
        """
        Sets the value of :py:attr:`inputCol`.
        """
        return self._set(inputCol=value)

    @since("3.0.0")
    def setOutputCol(self, value: str) -> "OneHotEncoderModel":
        """
        Sets the value of :py:attr:`outputCol`.
        """
        return self._set(outputCol=value)

    @since("3.0.0")
    def setHandleInvalid(self, value: str) -> "OneHotEncoderModel":
        """
        Sets the value of :py:attr:`handleInvalid`.
        """
        return self._set(handleInvalid=value)

    @property  # type: ignore[misc]
    @since("2.3.0")
    def categorySizes(self) -> List[int]:
        """
        Original number of categories for each feature being encoded.
        The array contains one value for each input column, in order.
        """
        return self._call_java("categorySizes")


@inherit_doc
class PolynomialExpansion(
    JavaTransformer,
    HasInputCol,
    HasOutputCol,
    JavaMLReadable["PolynomialExpansion"],
    JavaMLWritable,
):
    """
    Perform feature expansion in a polynomial space. As said in `wikipedia of Polynomial Expansion
    <http://en.wikipedia.org/wiki/Polynomial_expansion>`_, "In mathematics, an
    expansion of a product of sums expresses it as a sum of products by using the fact that
    multiplication distributes over addition". Take a 2-variable feature vector as an example:
    `(x, y)`, if we want to expand it with degree 2, then we get `(x, x * x, y, x * y, y * y)`.

    .. versionadded:: 1.4.0

    Examples
    --------
    >>> from pyspark.ml.linalg import Vectors
    >>> df = spark.createDataFrame([(Vectors.dense([0.5, 2.0]),)], ["dense"])
    >>> px = PolynomialExpansion(degree=2)
    >>> px.setInputCol("dense")
    PolynomialExpansion...
    >>> px.setOutputCol("expanded")
    PolynomialExpansion...
    >>> px.transform(df).head().expanded
    DenseVector([0.5, 0.25, 2.0, 1.0, 4.0])
    >>> px.setParams(outputCol="test").transform(df).head().test
    DenseVector([0.5, 0.25, 2.0, 1.0, 4.0])
    >>> polyExpansionPath = temp_path + "/poly-expansion"
    >>> px.save(polyExpansionPath)
    >>> loadedPx = PolynomialExpansion.load(polyExpansionPath)
    >>> loadedPx.getDegree() == px.getDegree()
    True
    >>> loadedPx.transform(df).take(1) == px.transform(df).take(1)
    True
    """

    _input_kwargs: Dict[str, Any]

    degree: Param[int] = Param(
        Params._dummy(),
        "degree",
        "the polynomial degree to expand (>= 1)",
        typeConverter=TypeConverters.toInt,
    )

    @keyword_only
    def __init__(
        self, *, degree: int = 2, inputCol: Optional[str] = None, outputCol: Optional[str] = None
    ):
        """
        __init__(self, \\*, degree=2, inputCol=None, outputCol=None)
        """
        super(PolynomialExpansion, self).__init__()
        self._java_obj = self._new_java_obj(
            "org.apache.spark.ml.feature.PolynomialExpansion", self.uid
        )
        self._setDefault(degree=2)
        kwargs = self._input_kwargs
        self.setParams(**kwargs)

    @keyword_only
    @since("1.4.0")
    def setParams(
        self, *, degree: int = 2, inputCol: Optional[str] = None, outputCol: Optional[str] = None
    ) -> "PolynomialExpansion":
        """
        setParams(self, \\*, degree=2, inputCol=None, outputCol=None)
        Sets params for this PolynomialExpansion.
        """
        kwargs = self._input_kwargs
        return self._set(**kwargs)

    @since("1.4.0")
    def setDegree(self, value: int) -> "PolynomialExpansion":
        """
        Sets the value of :py:attr:`degree`.
        """
        return self._set(degree=value)

    @since("1.4.0")
    def getDegree(self) -> int:
        """
        Gets the value of degree or its default value.
        """
        return self.getOrDefault(self.degree)

    def setInputCol(self, value: str) -> "PolynomialExpansion":
        """
        Sets the value of :py:attr:`inputCol`.
        """
        return self._set(inputCol=value)

    def setOutputCol(self, value: str) -> "PolynomialExpansion":
        """
        Sets the value of :py:attr:`outputCol`.
        """
        return self._set(outputCol=value)


@inherit_doc
class QuantileDiscretizer(
    JavaEstimator,
    HasInputCol,
    HasOutputCol,
    HasInputCols,
    HasOutputCols,
    HasHandleInvalid,
    HasRelativeError,
    JavaMLReadable["QuantileDiscretizer"],
    JavaMLWritable,
):
    """
    :py:class:`QuantileDiscretizer` takes a column with continuous features and outputs a column
    with binned categorical features. The number of bins can be set using the :py:attr:`numBuckets`
    parameter. It is possible that the number of buckets used will be less than this value, for
    example, if there are too few distinct values of the input to create enough distinct quantiles.
    Since 3.0.0, :py:class:`QuantileDiscretizer` can map multiple columns at once by setting the
    :py:attr:`inputCols` parameter. If both of the :py:attr:`inputCol` and :py:attr:`inputCols`
    parameters are set, an Exception will be thrown. To specify the number of buckets for each
    column, the :py:attr:`numBucketsArray` parameter can be set, or if the number of buckets
    should be the same across columns, :py:attr:`numBuckets` can be set as a convenience.

    .. versionadded:: 2.0.0

    Notes
    -----
    NaN handling: Note also that
    :py:class:`QuantileDiscretizer` will raise an error when it finds NaN values in the dataset,
    but the user can also choose to either keep or remove NaN values within the dataset by setting
    :py:attr:`handleInvalid` parameter. If the user chooses to keep NaN values, they will be
    handled specially and placed into their own bucket, for example, if 4 buckets are used, then
    non-NaN data will be put into buckets[0-3], but NaNs will be counted in a special bucket[4].

    Algorithm: The bin ranges are chosen using an approximate algorithm (see the documentation for
    :py:meth:`~.DataFrameStatFunctions.approxQuantile` for a detailed description).
    The precision of the approximation can be controlled with the
    :py:attr:`relativeError` parameter.
    The lower and upper bin bounds will be `-Infinity` and `+Infinity`, covering all real values.

    Examples
    --------
    >>> values = [(0.1,), (0.4,), (1.2,), (1.5,), (float("nan"),), (float("nan"),)]
    >>> df1 = spark.createDataFrame(values, ["values"])
    >>> qds1 = QuantileDiscretizer(inputCol="values", outputCol="buckets")
    >>> qds1.setNumBuckets(2)
    QuantileDiscretizer...
    >>> qds1.setRelativeError(0.01)
    QuantileDiscretizer...
    >>> qds1.setHandleInvalid("error")
    QuantileDiscretizer...
    >>> qds1.getRelativeError()
    0.01
    >>> bucketizer = qds1.fit(df1)
    >>> qds1.setHandleInvalid("keep").fit(df1).transform(df1).count()
    6
    >>> qds1.setHandleInvalid("skip").fit(df1).transform(df1).count()
    4
    >>> splits = bucketizer.getSplits()
    >>> splits[0]
    -inf
    >>> print("%2.1f" % round(splits[1], 1))
    0.4
    >>> bucketed = bucketizer.transform(df1).head()
    >>> bucketed.buckets
    0.0
    >>> quantileDiscretizerPath = temp_path + "/quantile-discretizer"
    >>> qds1.save(quantileDiscretizerPath)
    >>> loadedQds = QuantileDiscretizer.load(quantileDiscretizerPath)
    >>> loadedQds.getNumBuckets() == qds1.getNumBuckets()
    True
    >>> inputs = [(0.1, 0.0), (0.4, 1.0), (1.2, 1.3), (1.5, 1.5),
    ...     (float("nan"), float("nan")), (float("nan"), float("nan"))]
    >>> df2 = spark.createDataFrame(inputs, ["input1", "input2"])
    >>> qds2 = QuantileDiscretizer(relativeError=0.01, handleInvalid="error", numBuckets=2,
    ...     inputCols=["input1", "input2"], outputCols=["output1", "output2"])
    >>> qds2.getRelativeError()
    0.01
    >>> qds2.setHandleInvalid("keep").fit(df2).transform(df2).show()
    +------+------+-------+-------+
    |input1|input2|output1|output2|
    +------+------+-------+-------+
    |   0.1|   0.0|    0.0|    0.0|
    |   0.4|   1.0|    1.0|    1.0|
    |   1.2|   1.3|    1.0|    1.0|
    |   1.5|   1.5|    1.0|    1.0|
    |   NaN|   NaN|    2.0|    2.0|
    |   NaN|   NaN|    2.0|    2.0|
    +------+------+-------+-------+
    ...
    >>> qds3 = QuantileDiscretizer(relativeError=0.01, handleInvalid="error",
    ...      numBucketsArray=[5, 10], inputCols=["input1", "input2"],
    ...      outputCols=["output1", "output2"])
    >>> qds3.setHandleInvalid("skip").fit(df2).transform(df2).show()
    +------+------+-------+-------+
    |input1|input2|output1|output2|
    +------+------+-------+-------+
    |   0.1|   0.0|    1.0|    1.0|
    |   0.4|   1.0|    2.0|    2.0|
    |   1.2|   1.3|    3.0|    3.0|
    |   1.5|   1.5|    4.0|    4.0|
    +------+------+-------+-------+
    ...
    """

    _input_kwargs: Dict[str, Any]

    numBuckets: Param[int] = Param(
        Params._dummy(),
        "numBuckets",
        "Maximum number of buckets (quantiles, or "
        + "categories) into which data points are grouped. Must be >= 2.",
        typeConverter=TypeConverters.toInt,
    )

    handleInvalid: Param[str] = Param(
        Params._dummy(),
        "handleInvalid",
        "how to handle invalid entries. "
        + "Options are skip (filter out rows with invalid values), "
        + "error (throw an error), or keep (keep invalid values in a special "
        + "additional bucket). Note that in the multiple columns "
        + "case, the invalid handling is applied to all columns. That said "
        + "for 'error' it will throw an error if any invalids are found in "
        + "any columns, for 'skip' it will skip rows with any invalids in "
        + "any columns, etc.",
        typeConverter=TypeConverters.toString,
    )

    numBucketsArray: Param[List[int]] = Param(
        Params._dummy(),
        "numBucketsArray",
        "Array of number of buckets "
        + "(quantiles, or categories) into which data points are grouped. "
        + "This is for multiple columns input. If transforming multiple "
        + "columns and numBucketsArray is not set, but numBuckets is set, "
        + "then numBuckets will be applied across all columns.",
        typeConverter=TypeConverters.toListInt,
    )

    @overload
    def __init__(
        self,
        *,
        numBuckets: int = ...,
        inputCol: Optional[str] = ...,
        outputCol: Optional[str] = ...,
        relativeError: float = ...,
        handleInvalid: str = ...,
    ):
        ...

    @overload
    def __init__(
        self,
        *,
        relativeError: float = ...,
        handleInvalid: str = ...,
        numBucketsArray: Optional[List[int]] = ...,
        inputCols: Optional[List[str]] = ...,
        outputCols: Optional[List[str]] = ...,
    ):
        ...

    @keyword_only
    def __init__(
        self,
        *,
        numBuckets: int = 2,
        inputCol: Optional[str] = None,
        outputCol: Optional[str] = None,
        relativeError: float = 0.001,
        handleInvalid: str = "error",
        numBucketsArray: Optional[List[int]] = None,
        inputCols: Optional[List[str]] = None,
        outputCols: Optional[List[str]] = None,
    ):
        """
        __init__(self, \\*, numBuckets=2, inputCol=None, outputCol=None, relativeError=0.001, \
                 handleInvalid="error", numBucketsArray=None, inputCols=None, outputCols=None)
        """
        super(QuantileDiscretizer, self).__init__()
        self._java_obj = self._new_java_obj(
            "org.apache.spark.ml.feature.QuantileDiscretizer", self.uid
        )
        self._setDefault(numBuckets=2, relativeError=0.001, handleInvalid="error")
        kwargs = self._input_kwargs
        self.setParams(**kwargs)

    @overload
    def setParams(
        self,
        *,
        numBuckets: int = ...,
        inputCol: Optional[str] = ...,
        outputCol: Optional[str] = ...,
        relativeError: float = ...,
        handleInvalid: str = ...,
    ) -> "QuantileDiscretizer":
        ...

    @overload
    def setParams(
        self,
        *,
        relativeError: float = ...,
        handleInvalid: str = ...,
        numBucketsArray: Optional[List[int]] = ...,
        inputCols: Optional[List[str]] = ...,
        outputCols: Optional[List[str]] = ...,
    ) -> "QuantileDiscretizer":
        ...

    @keyword_only
    @since("2.0.0")
    def setParams(
        self,
        *,
        numBuckets: int = 2,
        inputCol: Optional[str] = None,
        outputCol: Optional[str] = None,
        relativeError: float = 0.001,
        handleInvalid: str = "error",
        numBucketsArray: Optional[List[int]] = None,
        inputCols: Optional[List[str]] = None,
        outputCols: Optional[List[str]] = None,
    ) -> "QuantileDiscretizer":
        """
        setParams(self, \\*, numBuckets=2, inputCol=None, outputCol=None, relativeError=0.001, \
                  handleInvalid="error", numBucketsArray=None, inputCols=None, outputCols=None)
        Set the params for the QuantileDiscretizer
        """
        kwargs = self._input_kwargs
        return self._set(**kwargs)

    @since("2.0.0")
    def setNumBuckets(self, value: int) -> "QuantileDiscretizer":
        """
        Sets the value of :py:attr:`numBuckets`.
        """
        return self._set(numBuckets=value)

    @since("2.0.0")
    def getNumBuckets(self) -> int:
        """
        Gets the value of numBuckets or its default value.
        """
        return self.getOrDefault(self.numBuckets)

    @since("3.0.0")
    def setNumBucketsArray(self, value: List[int]) -> "QuantileDiscretizer":
        """
        Sets the value of :py:attr:`numBucketsArray`.
        """
        return self._set(numBucketsArray=value)

    @since("3.0.0")
    def getNumBucketsArray(self) -> List[int]:
        """
        Gets the value of numBucketsArray or its default value.
        """
        return self.getOrDefault(self.numBucketsArray)

    @since("2.0.0")
    def setRelativeError(self, value: float) -> "QuantileDiscretizer":
        """
        Sets the value of :py:attr:`relativeError`.
        """
        return self._set(relativeError=value)

    def setInputCol(self, value: str) -> "QuantileDiscretizer":
        """
        Sets the value of :py:attr:`inputCol`.
        """
        return self._set(inputCol=value)

    @since("3.0.0")
    def setInputCols(self, value: List[str]) -> "QuantileDiscretizer":
        """
        Sets the value of :py:attr:`inputCols`.
        """
        return self._set(inputCols=value)

    def setOutputCol(self, value: str) -> "QuantileDiscretizer":
        """
        Sets the value of :py:attr:`outputCol`.
        """
        return self._set(outputCol=value)

    @since("3.0.0")
    def setOutputCols(self, value: List[str]) -> "QuantileDiscretizer":
        """
        Sets the value of :py:attr:`outputCols`.
        """
        return self._set(outputCols=value)

    def setHandleInvalid(self, value: str) -> "QuantileDiscretizer":
        """
        Sets the value of :py:attr:`handleInvalid`.
        """
        return self._set(handleInvalid=value)

    def _create_model(self, java_model: "JavaObject") -> Bucketizer:
        """
        Private method to convert the java_model to a Python model.
        """
        if self.isSet(self.inputCol):
            return Bucketizer(
                splits=list(java_model.getSplits()),
                inputCol=self.getInputCol(),
                outputCol=self.getOutputCol(),
                handleInvalid=self.getHandleInvalid(),
            )
        else:
            splitsArrayList = [list(x) for x in list(java_model.getSplitsArray())]
            return Bucketizer(
                splitsArray=splitsArrayList,
                inputCols=self.getInputCols(),
                outputCols=self.getOutputCols(),
                handleInvalid=self.getHandleInvalid(),
            )


class _RobustScalerParams(HasInputCol, HasOutputCol, HasRelativeError):
    """
    Params for :py:class:`RobustScaler` and :py:class:`RobustScalerModel`.

    .. versionadded:: 3.0.0
    """

    lower: Param[float] = Param(
        Params._dummy(),
        "lower",
        "Lower quantile to calculate quantile range",
        typeConverter=TypeConverters.toFloat,
    )
    upper: Param[float] = Param(
        Params._dummy(),
        "upper",
        "Upper quantile to calculate quantile range",
        typeConverter=TypeConverters.toFloat,
    )
    withCentering: Param[bool] = Param(
        Params._dummy(),
        "withCentering",
        "Whether to center data with median",
        typeConverter=TypeConverters.toBoolean,
    )
    withScaling: Param[bool] = Param(
        Params._dummy(),
        "withScaling",
        "Whether to scale the data to " "quantile range",
        typeConverter=TypeConverters.toBoolean,
    )

    def __init__(self, *args: Any):
        super(_RobustScalerParams, self).__init__(*args)
        self._setDefault(
            lower=0.25, upper=0.75, withCentering=False, withScaling=True, relativeError=0.001
        )

    @since("3.0.0")
    def getLower(self) -> float:
        """
        Gets the value of lower or its default value.
        """
        return self.getOrDefault(self.lower)

    @since("3.0.0")
    def getUpper(self) -> float:
        """
        Gets the value of upper or its default value.
        """
        return self.getOrDefault(self.upper)

    @since("3.0.0")
    def getWithCentering(self) -> bool:
        """
        Gets the value of withCentering or its default value.
        """
        return self.getOrDefault(self.withCentering)

    @since("3.0.0")
    def getWithScaling(self) -> bool:
        """
        Gets the value of withScaling or its default value.
        """
        return self.getOrDefault(self.withScaling)


@inherit_doc
class RobustScaler(
    JavaEstimator, _RobustScalerParams, JavaMLReadable["RobustScaler"], JavaMLWritable
):
    """
    RobustScaler removes the median and scales the data according to the quantile range.
    The quantile range is by default IQR (Interquartile Range, quantile range between the
    1st quartile = 25th quantile and the 3rd quartile = 75th quantile) but can be configured.
    Centering and scaling happen independently on each feature by computing the relevant
    statistics on the samples in the training set. Median and quantile range are then
    stored to be used on later data using the transform method.
    Note that NaN values are ignored in the computation of medians and ranges.

    .. versionadded:: 3.0.0

    Examples
    --------
    >>> from pyspark.ml.linalg import Vectors
    >>> data = [(0, Vectors.dense([0.0, 0.0]),),
    ...         (1, Vectors.dense([1.0, -1.0]),),
    ...         (2, Vectors.dense([2.0, -2.0]),),
    ...         (3, Vectors.dense([3.0, -3.0]),),
    ...         (4, Vectors.dense([4.0, -4.0]),),]
    >>> df = spark.createDataFrame(data, ["id", "features"])
    >>> scaler = RobustScaler()
    >>> scaler.setInputCol("features")
    RobustScaler...
    >>> scaler.setOutputCol("scaled")
    RobustScaler...
    >>> model = scaler.fit(df)
    >>> model.setOutputCol("output")
    RobustScalerModel...
    >>> model.median
    DenseVector([2.0, -2.0])
    >>> model.range
    DenseVector([2.0, 2.0])
    >>> model.transform(df).collect()[1].output
    DenseVector([0.5, -0.5])
    >>> scalerPath = temp_path + "/robust-scaler"
    >>> scaler.save(scalerPath)
    >>> loadedScaler = RobustScaler.load(scalerPath)
    >>> loadedScaler.getWithCentering() == scaler.getWithCentering()
    True
    >>> loadedScaler.getWithScaling() == scaler.getWithScaling()
    True
    >>> modelPath = temp_path + "/robust-scaler-model"
    >>> model.save(modelPath)
    >>> loadedModel = RobustScalerModel.load(modelPath)
    >>> loadedModel.median == model.median
    True
    >>> loadedModel.range == model.range
    True
    >>> loadedModel.transform(df).take(1) == model.transform(df).take(1)
    True
    """

    _input_kwargs: Dict[str, Any]

    @keyword_only
    def __init__(
        self,
        *,
        lower: float = 0.25,
        upper: float = 0.75,
        withCentering: bool = False,
        withScaling: bool = True,
        inputCol: Optional[str] = None,
        outputCol: Optional[str] = None,
        relativeError: float = 0.001,
    ):
        """
        __init__(self, \\*, lower=0.25, upper=0.75, withCentering=False, withScaling=True, \
                 inputCol=None, outputCol=None, relativeError=0.001)
        """
        super(RobustScaler, self).__init__()
        self._java_obj = self._new_java_obj("org.apache.spark.ml.feature.RobustScaler", self.uid)
        kwargs = self._input_kwargs
        self.setParams(**kwargs)

    @keyword_only
    @since("3.0.0")
    def setParams(
        self,
        *,
        lower: float = 0.25,
        upper: float = 0.75,
        withCentering: bool = False,
        withScaling: bool = True,
        inputCol: Optional[str] = None,
        outputCol: Optional[str] = None,
        relativeError: float = 0.001,
    ) -> "RobustScaler":
        """
        setParams(self, \\*, lower=0.25, upper=0.75, withCentering=False, withScaling=True, \
                  inputCol=None, outputCol=None, relativeError=0.001)
        Sets params for this RobustScaler.
        """
        kwargs = self._input_kwargs
        return self._set(**kwargs)

    @since("3.0.0")
    def setLower(self, value: float) -> "RobustScaler":
        """
        Sets the value of :py:attr:`lower`.
        """
        return self._set(lower=value)

    @since("3.0.0")
    def setUpper(self, value: float) -> "RobustScaler":
        """
        Sets the value of :py:attr:`upper`.
        """
        return self._set(upper=value)

    @since("3.0.0")
    def setWithCentering(self, value: bool) -> "RobustScaler":
        """
        Sets the value of :py:attr:`withCentering`.
        """
        return self._set(withCentering=value)

    @since("3.0.0")
    def setWithScaling(self, value: bool) -> "RobustScaler":
        """
        Sets the value of :py:attr:`withScaling`.
        """
        return self._set(withScaling=value)

    @since("3.0.0")
    def setInputCol(self, value: str) -> "RobustScaler":
        """
        Sets the value of :py:attr:`inputCol`.
        """
        return self._set(inputCol=value)

    @since("3.0.0")
    def setOutputCol(self, value: str) -> "RobustScaler":
        """
        Sets the value of :py:attr:`outputCol`.
        """
        return self._set(outputCol=value)

    @since("3.0.0")
    def setRelativeError(self, value: float) -> "RobustScaler":
        """
        Sets the value of :py:attr:`relativeError`.
        """
        return self._set(relativeError=value)

    def _create_model(self, java_model: "JavaObject") -> "RobustScalerModel":
        return RobustScalerModel(java_model)


class RobustScalerModel(
    JavaModel, _RobustScalerParams, JavaMLReadable["RobustScalerModel"], JavaMLWritable
):
    """
    Model fitted by :py:class:`RobustScaler`.

    .. versionadded:: 3.0.0
    """

    @since("3.0.0")
    def setInputCol(self, value: str) -> "RobustScalerModel":
        """
        Sets the value of :py:attr:`inputCol`.
        """
        return self._set(inputCol=value)

    @since("3.0.0")
    def setOutputCol(self, value: str) -> "RobustScalerModel":
        """
        Sets the value of :py:attr:`outputCol`.
        """
        return self._set(outputCol=value)

    @property  # type: ignore[misc]
    @since("3.0.0")
    def median(self) -> Vector:
        """
        Median of the RobustScalerModel.
        """
        return self._call_java("median")

    @property  # type: ignore[misc]
    @since("3.0.0")
    def range(self) -> Vector:
        """
        Quantile range of the RobustScalerModel.
        """
        return self._call_java("range")


@inherit_doc
class RegexTokenizer(
    JavaTransformer,
    HasInputCol,
    HasOutputCol,
    JavaMLReadable["RegexTokenizer"],
    JavaMLWritable,
):
    """
    A regex based tokenizer that extracts tokens either by using the
    provided regex pattern (in Java dialect) to split the text
    (default) or repeatedly matching the regex (if gaps is false).
    Optional parameters also allow filtering tokens using a minimal
    length.
    It returns an array of strings that can be empty.

    .. versionadded:: 1.4.0

    Examples
    --------
    >>> df = spark.createDataFrame([("A B  c",)], ["text"])
    >>> reTokenizer = RegexTokenizer()
    >>> reTokenizer.setInputCol("text")
    RegexTokenizer...
    >>> reTokenizer.setOutputCol("words")
    RegexTokenizer...
    >>> reTokenizer.transform(df).head()
    Row(text='A B  c', words=['a', 'b', 'c'])
    >>> # Change a parameter.
    >>> reTokenizer.setParams(outputCol="tokens").transform(df).head()
    Row(text='A B  c', tokens=['a', 'b', 'c'])
    >>> # Temporarily modify a parameter.
    >>> reTokenizer.transform(df, {reTokenizer.outputCol: "words"}).head()
    Row(text='A B  c', words=['a', 'b', 'c'])
    >>> reTokenizer.transform(df).head()
    Row(text='A B  c', tokens=['a', 'b', 'c'])
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
    >>> loadedReTokenizer.transform(df).take(1) == reTokenizer.transform(df).take(1)
    True
    """

    _input_kwargs: Dict[str, Any]

    minTokenLength: Param[int] = Param(
        Params._dummy(),
        "minTokenLength",
        "minimum token length (>= 0)",
        typeConverter=TypeConverters.toInt,
    )
    gaps: Param[bool] = Param(
        Params._dummy(),
        "gaps",
        "whether regex splits on gaps (True) or matches tokens " + "(False)",
    )
    pattern: Param[str] = Param(
        Params._dummy(),
        "pattern",
        "regex pattern (Java dialect) used for tokenizing",
        typeConverter=TypeConverters.toString,
    )
    toLowercase: Param[bool] = Param(
        Params._dummy(),
        "toLowercase",
        "whether to convert all characters to " + "lowercase before tokenizing",
        typeConverter=TypeConverters.toBoolean,
    )

    @keyword_only
    def __init__(
        self,
        *,
        minTokenLength: int = 1,
        gaps: bool = True,
        pattern: str = "\\s+",
        inputCol: Optional[str] = None,
        outputCol: Optional[str] = None,
        toLowercase: bool = True,
    ):
        """
        __init__(self, \\*, minTokenLength=1, gaps=True, pattern="\\s+", inputCol=None, \
                 outputCol=None, toLowercase=True)
        """
        super(RegexTokenizer, self).__init__()
        self._java_obj = self._new_java_obj("org.apache.spark.ml.feature.RegexTokenizer", self.uid)
        self._setDefault(minTokenLength=1, gaps=True, pattern="\\s+", toLowercase=True)
        kwargs = self._input_kwargs
        self.setParams(**kwargs)

    @keyword_only
    @since("1.4.0")
    def setParams(
        self,
        *,
        minTokenLength: int = 1,
        gaps: bool = True,
        pattern: str = "\\s+",
        inputCol: Optional[str] = None,
        outputCol: Optional[str] = None,
        toLowercase: bool = True,
    ) -> "RegexTokenizer":
        """
        setParams(self, \\*, minTokenLength=1, gaps=True, pattern="\\s+", inputCol=None, \
                  outputCol=None, toLowercase=True)
        Sets params for this RegexTokenizer.
        """
        kwargs = self._input_kwargs
        return self._set(**kwargs)

    @since("1.4.0")
    def setMinTokenLength(self, value: int) -> "RegexTokenizer":
        """
        Sets the value of :py:attr:`minTokenLength`.
        """
        return self._set(minTokenLength=value)

    @since("1.4.0")
    def getMinTokenLength(self) -> int:
        """
        Gets the value of minTokenLength or its default value.
        """
        return self.getOrDefault(self.minTokenLength)

    @since("1.4.0")
    def setGaps(self, value: bool) -> "RegexTokenizer":
        """
        Sets the value of :py:attr:`gaps`.
        """
        return self._set(gaps=value)

    @since("1.4.0")
    def getGaps(self) -> bool:
        """
        Gets the value of gaps or its default value.
        """
        return self.getOrDefault(self.gaps)

    @since("1.4.0")
    def setPattern(self, value: str) -> "RegexTokenizer":
        """
        Sets the value of :py:attr:`pattern`.
        """
        return self._set(pattern=value)

    @since("1.4.0")
    def getPattern(self) -> str:
        """
        Gets the value of pattern or its default value.
        """
        return self.getOrDefault(self.pattern)

    @since("2.0.0")
    def setToLowercase(self, value: bool) -> "RegexTokenizer":
        """
        Sets the value of :py:attr:`toLowercase`.
        """
        return self._set(toLowercase=value)

    @since("2.0.0")
    def getToLowercase(self) -> bool:
        """
        Gets the value of toLowercase or its default value.
        """
        return self.getOrDefault(self.toLowercase)

    def setInputCol(self, value: str) -> "RegexTokenizer":
        """
        Sets the value of :py:attr:`inputCol`.
        """
        return self._set(inputCol=value)

    def setOutputCol(self, value: str) -> "RegexTokenizer":
        """
        Sets the value of :py:attr:`outputCol`.
        """
        return self._set(outputCol=value)


@inherit_doc
class SQLTransformer(JavaTransformer, JavaMLReadable["SQLTransformer"], JavaMLWritable):
    """
    Implements the transforms which are defined by SQL statement.
    Currently we only support SQL syntax like `SELECT ... FROM __THIS__`
    where `__THIS__` represents the underlying table of the input dataset.

    .. versionadded:: 1.6.0

    Examples
    --------
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
    >>> loadedSqlTrans.transform(df).take(1) == sqlTrans.transform(df).take(1)
    True
    """

    _input_kwargs: Dict[str, Any]

    statement = Param(
        Params._dummy(), "statement", "SQL statement", typeConverter=TypeConverters.toString
    )

    @keyword_only
    def __init__(self, *, statement: Optional[str] = None):
        """
        __init__(self, \\*, statement=None)
        """
        super(SQLTransformer, self).__init__()
        self._java_obj = self._new_java_obj("org.apache.spark.ml.feature.SQLTransformer", self.uid)
        kwargs = self._input_kwargs
        self.setParams(**kwargs)

    @keyword_only
    @since("1.6.0")
    def setParams(self, *, statement: Optional[str] = None) -> "SQLTransformer":
        """
        setParams(self, \\*, statement=None)
        Sets params for this SQLTransformer.
        """
        kwargs = self._input_kwargs
        return self._set(**kwargs)

    @since("1.6.0")
    def setStatement(self, value: str) -> "SQLTransformer":
        """
        Sets the value of :py:attr:`statement`.
        """
        return self._set(statement=value)

    @since("1.6.0")
    def getStatement(self) -> str:
        """
        Gets the value of statement or its default value.
        """
        return self.getOrDefault(self.statement)


class _StandardScalerParams(HasInputCol, HasOutputCol):
    """
    Params for :py:class:`StandardScaler` and :py:class:`StandardScalerModel`.

    .. versionadded:: 3.0.0
    """

    withMean: Param[bool] = Param(
        Params._dummy(), "withMean", "Center data with mean", typeConverter=TypeConverters.toBoolean
    )
    withStd: Param[bool] = Param(
        Params._dummy(),
        "withStd",
        "Scale to unit standard deviation",
        typeConverter=TypeConverters.toBoolean,
    )

    def __init__(self, *args: Any):
        super(_StandardScalerParams, self).__init__(*args)
        self._setDefault(withMean=False, withStd=True)

    @since("1.4.0")
    def getWithMean(self) -> bool:
        """
        Gets the value of withMean or its default value.
        """
        return self.getOrDefault(self.withMean)

    @since("1.4.0")
    def getWithStd(self) -> bool:
        """
        Gets the value of withStd or its default value.
        """
        return self.getOrDefault(self.withStd)


@inherit_doc
class StandardScaler(
    JavaEstimator["StandardScalerModel"],
    _StandardScalerParams,
    JavaMLReadable["StandardScaler"],
    JavaMLWritable,
):
    """
    Standardizes features by removing the mean and scaling to unit variance using column summary
    statistics on the samples in the training set.

    The "unit std" is computed using the `corrected sample standard deviation \
    <https://en.wikipedia.org/wiki/Standard_deviation#Corrected_sample_standard_deviation>`_,
    which is computed as the square root of the unbiased sample variance.

    .. versionadded:: 1.4.0

    Examples
    --------
    >>> from pyspark.ml.linalg import Vectors
    >>> df = spark.createDataFrame([(Vectors.dense([0.0]),), (Vectors.dense([2.0]),)], ["a"])
    >>> standardScaler = StandardScaler()
    >>> standardScaler.setInputCol("a")
    StandardScaler...
    >>> standardScaler.setOutputCol("scaled")
    StandardScaler...
    >>> model = standardScaler.fit(df)
    >>> model.getInputCol()
    'a'
    >>> model.setOutputCol("output")
    StandardScalerModel...
    >>> model.mean
    DenseVector([1.0])
    >>> model.std
    DenseVector([1.4142])
    >>> model.transform(df).collect()[1].output
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
    >>> loadedModel.transform(df).take(1) == model.transform(df).take(1)
    True
    """

    _input_kwargs: Dict[str, Any]

    @keyword_only
    def __init__(
        self,
        *,
        withMean: bool = False,
        withStd: bool = True,
        inputCol: Optional[str] = None,
        outputCol: Optional[str] = None,
    ):
        """
        __init__(self, \\*, withMean=False, withStd=True, inputCol=None, outputCol=None)
        """
        super(StandardScaler, self).__init__()
        self._java_obj = self._new_java_obj("org.apache.spark.ml.feature.StandardScaler", self.uid)
        kwargs = self._input_kwargs
        self.setParams(**kwargs)

    @keyword_only
    @since("1.4.0")
    def setParams(
        self,
        *,
        withMean: bool = False,
        withStd: bool = True,
        inputCol: Optional[str] = None,
        outputCol: Optional[str] = None,
    ) -> "StandardScaler":
        """
        setParams(self, \\*, withMean=False, withStd=True, inputCol=None, outputCol=None)
        Sets params for this StandardScaler.
        """
        kwargs = self._input_kwargs
        return self._set(**kwargs)

    @since("1.4.0")
    def setWithMean(self, value: bool) -> "StandardScaler":
        """
        Sets the value of :py:attr:`withMean`.
        """
        return self._set(withMean=value)

    @since("1.4.0")
    def setWithStd(self, value: bool) -> "StandardScaler":
        """
        Sets the value of :py:attr:`withStd`.
        """
        return self._set(withStd=value)

    def setInputCol(self, value: str) -> "StandardScaler":
        """
        Sets the value of :py:attr:`inputCol`.
        """
        return self._set(inputCol=value)

    def setOutputCol(self, value: str) -> "StandardScaler":
        """
        Sets the value of :py:attr:`outputCol`.
        """
        return self._set(outputCol=value)

    def _create_model(self, java_model: "JavaObject") -> "StandardScalerModel":
        return StandardScalerModel(java_model)


class StandardScalerModel(
    JavaModel,
    _StandardScalerParams,
    JavaMLReadable["StandardScalerModel"],
    JavaMLWritable,
):
    """
    Model fitted by :py:class:`StandardScaler`.

    .. versionadded:: 1.4.0
    """

    def setInputCol(self, value: str) -> "StandardScalerModel":
        """
        Sets the value of :py:attr:`inputCol`.
        """
        return self._set(inputCol=value)

    def setOutputCol(self, value: str) -> "StandardScalerModel":
        """
        Sets the value of :py:attr:`outputCol`.
        """
        return self._set(outputCol=value)

    @property  # type: ignore[misc]
    @since("2.0.0")
    def std(self) -> Vector:
        """
        Standard deviation of the StandardScalerModel.
        """
        return self._call_java("std")

    @property  # type: ignore[misc]
    @since("2.0.0")
    def mean(self) -> Vector:
        """
        Mean of the StandardScalerModel.
        """
        return self._call_java("mean")


class _StringIndexerParams(
    JavaParams, HasHandleInvalid, HasInputCol, HasOutputCol, HasInputCols, HasOutputCols
):
    """
    Params for :py:class:`StringIndexer` and :py:class:`StringIndexerModel`.
    """

    stringOrderType: Param[str] = Param(
        Params._dummy(),
        "stringOrderType",
        "How to order labels of string column. The first label after "
        + "ordering is assigned an index of 0. Supported options: "
        + "frequencyDesc, frequencyAsc, alphabetDesc, alphabetAsc. "
        + "Default is frequencyDesc. In case of equal frequency when "
        + "under frequencyDesc/Asc, the strings are further sorted "
        + "alphabetically",
        typeConverter=TypeConverters.toString,
    )

    handleInvalid: Param[str] = Param(
        Params._dummy(),
        "handleInvalid",
        "how to handle invalid data (unseen "
        + "or NULL values) in features and label column of string type. "
        + "Options are 'skip' (filter out rows with invalid data), "
        + "error (throw an error), or 'keep' (put invalid data "
        + "in a special additional bucket, at index numLabels).",
        typeConverter=TypeConverters.toString,
    )

    def __init__(self, *args: Any):
        super(_StringIndexerParams, self).__init__(*args)
        self._setDefault(handleInvalid="error", stringOrderType="frequencyDesc")

    @since("2.3.0")
    def getStringOrderType(self) -> str:
        """
        Gets the value of :py:attr:`stringOrderType` or its default value 'frequencyDesc'.
        """
        return self.getOrDefault(self.stringOrderType)


@inherit_doc
class StringIndexer(
    JavaEstimator["StringIndexerModel"],
    _StringIndexerParams,
    JavaMLReadable["StringIndexer"],
    JavaMLWritable,
):
    """
    A label indexer that maps a string column of labels to an ML column of label indices.
    If the input column is numeric, we cast it to string and index the string values.
    The indices are in [0, numLabels). By default, this is ordered by label frequencies
    so the most frequent label gets index 0. The ordering behavior is controlled by
    setting :py:attr:`stringOrderType`. Its default value is 'frequencyDesc'.

    .. versionadded:: 1.4.0

    Examples
    --------
    >>> stringIndexer = StringIndexer(inputCol="label", outputCol="indexed",
    ...     stringOrderType="frequencyDesc")
    >>> stringIndexer.setHandleInvalid("error")
    StringIndexer...
    >>> model = stringIndexer.fit(stringIndDf)
    >>> model.setHandleInvalid("error")
    StringIndexerModel...
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
    >>> loadedModel.transform(stringIndDf).take(1) == model.transform(stringIndDf).take(1)
    True
    >>> stringIndexer.getStringOrderType()
    'frequencyDesc'
    >>> stringIndexer = StringIndexer(inputCol="label", outputCol="indexed", handleInvalid="error",
    ...     stringOrderType="alphabetDesc")
    >>> model = stringIndexer.fit(stringIndDf)
    >>> td = model.transform(stringIndDf)
    >>> sorted(set([(i[0], i[1]) for i in td.select(td.id, td.indexed).collect()]),
    ...     key=lambda x: x[0])
    [(0, 2.0), (1, 1.0), (2, 0.0), (3, 2.0), (4, 2.0), (5, 0.0)]
    >>> fromlabelsModel = StringIndexerModel.from_labels(["a", "b", "c"],
    ...     inputCol="label", outputCol="indexed", handleInvalid="error")
    >>> result = fromlabelsModel.transform(stringIndDf)
    >>> sorted(set([(i[0], i[1]) for i in result.select(result.id, result.indexed).collect()]),
    ...     key=lambda x: x[0])
    [(0, 0.0), (1, 1.0), (2, 2.0), (3, 0.0), (4, 0.0), (5, 2.0)]
    >>> testData = sc.parallelize([Row(id=0, label1="a", label2="e"),
    ...                            Row(id=1, label1="b", label2="f"),
    ...                            Row(id=2, label1="c", label2="e"),
    ...                            Row(id=3, label1="a", label2="f"),
    ...                            Row(id=4, label1="a", label2="f"),
    ...                            Row(id=5, label1="c", label2="f")], 3)
    >>> multiRowDf = spark.createDataFrame(testData)
    >>> inputs = ["label1", "label2"]
    >>> outputs = ["index1", "index2"]
    >>> stringIndexer = StringIndexer(inputCols=inputs, outputCols=outputs)
    >>> model = stringIndexer.fit(multiRowDf)
    >>> result = model.transform(multiRowDf)
    >>> sorted(set([(i[0], i[1], i[2]) for i in result.select(result.id, result.index1,
    ...     result.index2).collect()]), key=lambda x: x[0])
    [(0, 0.0, 1.0), (1, 2.0, 0.0), (2, 1.0, 1.0), (3, 0.0, 0.0), (4, 0.0, 0.0), (5, 1.0, 0.0)]
    >>> fromlabelsModel = StringIndexerModel.from_arrays_of_labels([["a", "b", "c"], ["e", "f"]],
    ...     inputCols=inputs, outputCols=outputs)
    >>> result = fromlabelsModel.transform(multiRowDf)
    >>> sorted(set([(i[0], i[1], i[2]) for i in result.select(result.id, result.index1,
    ...     result.index2).collect()]), key=lambda x: x[0])
    [(0, 0.0, 0.0), (1, 1.0, 1.0), (2, 2.0, 0.0), (3, 0.0, 1.0), (4, 0.0, 1.0), (5, 2.0, 1.0)]
    """

    _input_kwargs: Dict[str, Any]

    @overload
    def __init__(
        self,
        *,
        inputCol: Optional[str] = ...,
        outputCol: Optional[str] = ...,
        handleInvalid: str = ...,
        stringOrderType: str = ...,
    ):
        ...

    @overload
    def __init__(
        self,
        *,
        inputCols: Optional[List[str]] = ...,
        outputCols: Optional[List[str]] = ...,
        handleInvalid: str = ...,
        stringOrderType: str = ...,
    ):
        ...

    @keyword_only
    def __init__(
        self,
        *,
        inputCol: Optional[str] = None,
        outputCol: Optional[str] = None,
        inputCols: Optional[List[str]] = None,
        outputCols: Optional[List[str]] = None,
        handleInvalid: str = "error",
        stringOrderType: str = "frequencyDesc",
    ):
        """
        __init__(self, \\*, inputCol=None, outputCol=None, inputCols=None, outputCols=None, \
                 handleInvalid="error", stringOrderType="frequencyDesc")
        """
        super(StringIndexer, self).__init__()
        self._java_obj = self._new_java_obj("org.apache.spark.ml.feature.StringIndexer", self.uid)
        kwargs = self._input_kwargs
        self.setParams(**kwargs)

    @overload
    def setParams(
        self,
        *,
        inputCol: Optional[str] = ...,
        outputCol: Optional[str] = ...,
        handleInvalid: str = ...,
        stringOrderType: str = ...,
    ) -> "StringIndexer":
        ...

    @overload
    def setParams(
        self,
        *,
        inputCols: Optional[List[str]] = ...,
        outputCols: Optional[List[str]] = ...,
        handleInvalid: str = ...,
        stringOrderType: str = ...,
    ) -> "StringIndexer":
        ...

    @keyword_only
    @since("1.4.0")
    def setParams(
        self,
        *,
        inputCol: Optional[str] = None,
        outputCol: Optional[str] = None,
        inputCols: Optional[List[str]] = None,
        outputCols: Optional[List[str]] = None,
        handleInvalid: str = "error",
        stringOrderType: str = "frequencyDesc",
    ) -> "StringIndexer":
        """
        setParams(self, \\*, inputCol=None, outputCol=None, inputCols=None, outputCols=None, \
                  handleInvalid="error", stringOrderType="frequencyDesc")
        Sets params for this StringIndexer.
        """
        kwargs = self._input_kwargs
        return self._set(**kwargs)

    def _create_model(self, java_model: "JavaObject") -> "StringIndexerModel":
        return StringIndexerModel(java_model)

    @since("2.3.0")
    def setStringOrderType(self, value: str) -> "StringIndexer":
        """
        Sets the value of :py:attr:`stringOrderType`.
        """
        return self._set(stringOrderType=value)

    def setInputCol(self, value: str) -> "StringIndexer":
        """
        Sets the value of :py:attr:`inputCol`.
        """
        return self._set(inputCol=value)

    @since("3.0.0")
    def setInputCols(self, value: List[str]) -> "StringIndexer":
        """
        Sets the value of :py:attr:`inputCols`.
        """
        return self._set(inputCols=value)

    def setOutputCol(self, value: str) -> "StringIndexer":
        """
        Sets the value of :py:attr:`outputCol`.
        """
        return self._set(outputCol=value)

    @since("3.0.0")
    def setOutputCols(self, value: List[str]) -> "StringIndexer":
        """
        Sets the value of :py:attr:`outputCols`.
        """
        return self._set(outputCols=value)

    def setHandleInvalid(self, value: str) -> "StringIndexer":
        """
        Sets the value of :py:attr:`handleInvalid`.
        """
        return self._set(handleInvalid=value)


class StringIndexerModel(
    JavaModel, _StringIndexerParams, JavaMLReadable["StringIndexerModel"], JavaMLWritable
):
    """
    Model fitted by :py:class:`StringIndexer`.

    .. versionadded:: 1.4.0
    """

    def setInputCol(self, value: str) -> "StringIndexerModel":
        """
        Sets the value of :py:attr:`inputCol`.
        """
        return self._set(inputCol=value)

    @since("3.0.0")
    def setInputCols(self, value: List[str]) -> "StringIndexerModel":
        """
        Sets the value of :py:attr:`inputCols`.
        """
        return self._set(inputCols=value)

    def setOutputCol(self, value: str) -> "StringIndexerModel":
        """
        Sets the value of :py:attr:`outputCol`.
        """
        return self._set(outputCol=value)

    @since("3.0.0")
    def setOutputCols(self, value: List[str]) -> "StringIndexerModel":
        """
        Sets the value of :py:attr:`outputCols`.
        """
        return self._set(outputCols=value)

    @since("2.4.0")
    def setHandleInvalid(self, value: str) -> "StringIndexerModel":
        """
        Sets the value of :py:attr:`handleInvalid`.
        """
        return self._set(handleInvalid=value)

    @classmethod
    @since("2.4.0")
    def from_labels(
        cls,
        labels: List[str],
        inputCol: str,
        outputCol: Optional[str] = None,
        handleInvalid: Optional[str] = None,
    ) -> "StringIndexerModel":
        """
        Construct the model directly from an array of label strings,
        requires an active SparkContext.
        """
        sc = SparkContext._active_spark_context
        assert sc is not None and sc._gateway is not None
        java_class = sc._gateway.jvm.java.lang.String
        jlabels = StringIndexerModel._new_java_array(labels, java_class)
        model = StringIndexerModel._create_from_java_class(
            "org.apache.spark.ml.feature.StringIndexerModel", jlabels
        )
        model.setInputCol(inputCol)
        if outputCol is not None:
            model.setOutputCol(outputCol)
        if handleInvalid is not None:
            model.setHandleInvalid(handleInvalid)
        return model

    @classmethod
    @since("3.0.0")
    def from_arrays_of_labels(
        cls,
        arrayOfLabels: List[List[str]],
        inputCols: List[str],
        outputCols: Optional[List[str]] = None,
        handleInvalid: Optional[str] = None,
    ) -> "StringIndexerModel":
        """
        Construct the model directly from an array of array of label strings,
        requires an active SparkContext.
        """
        sc = SparkContext._active_spark_context
        assert sc is not None and sc._gateway is not None
        java_class = sc._gateway.jvm.java.lang.String
        jlabels = StringIndexerModel._new_java_array(arrayOfLabels, java_class)
        model = StringIndexerModel._create_from_java_class(
            "org.apache.spark.ml.feature.StringIndexerModel", jlabels
        )
        model.setInputCols(inputCols)
        if outputCols is not None:
            model.setOutputCols(outputCols)
        if handleInvalid is not None:
            model.setHandleInvalid(handleInvalid)
        return model

    @property  # type: ignore[misc]
    @since("1.5.0")
    def labels(self) -> List[str]:
        """
        Ordered list of labels, corresponding to indices to be assigned.

        .. deprecated:: 3.1.0
            It will be removed in future versions. Use `labelsArray` method instead.
        """
        return self._call_java("labels")

    @property  # type: ignore[misc]
    @since("3.0.2")
    def labelsArray(self) -> List[str]:
        """
        Array of ordered list of labels, corresponding to indices to be assigned
        for each input column.
        """
        return self._call_java("labelsArray")


@inherit_doc
class IndexToString(
    JavaTransformer,
    HasInputCol,
    HasOutputCol,
    JavaMLReadable["IndexToString"],
    JavaMLWritable,
):
    """
    A :py:class:`pyspark.ml.base.Transformer` that maps a column of indices back to a new column of
    corresponding string values.
    The index-string mapping is either from the ML attributes of the input column,
    or from user-supplied labels (which take precedence over ML attributes).

    .. versionadded:: 1.6.0

    See Also
    --------
    StringIndexer : for converting categorical values into category indices
    """

    _input_kwargs: Dict[str, Any]

    labels: Param[List[str]] = Param(
        Params._dummy(),
        "labels",
        "Optional array of labels specifying index-string mapping."
        + " If not provided or if empty, then metadata from inputCol is used instead.",
        typeConverter=TypeConverters.toListString,
    )

    @keyword_only
    def __init__(
        self,
        *,
        inputCol: Optional[str] = None,
        outputCol: Optional[str] = None,
        labels: Optional[List[str]] = None,
    ):
        """
        __init__(self, \\*, inputCol=None, outputCol=None, labels=None)
        """
        super(IndexToString, self).__init__()
        self._java_obj = self._new_java_obj("org.apache.spark.ml.feature.IndexToString", self.uid)
        kwargs = self._input_kwargs
        self.setParams(**kwargs)

    @keyword_only
    @since("1.6.0")
    def setParams(
        self,
        *,
        inputCol: Optional[str] = None,
        outputCol: Optional[str] = None,
        labels: Optional[List[str]] = None,
    ) -> "IndexToString":
        """
        setParams(self, \\*, inputCol=None, outputCol=None, labels=None)
        Sets params for this IndexToString.
        """
        kwargs = self._input_kwargs
        return self._set(**kwargs)

    @since("1.6.0")
    def setLabels(self, value: List[str]) -> "IndexToString":
        """
        Sets the value of :py:attr:`labels`.
        """
        return self._set(labels=value)

    @since("1.6.0")
    def getLabels(self) -> List[str]:
        """
        Gets the value of :py:attr:`labels` or its default value.
        """
        return self.getOrDefault(self.labels)

    def setInputCol(self, value: str) -> "IndexToString":
        """
        Sets the value of :py:attr:`inputCol`.
        """
        return self._set(inputCol=value)

    def setOutputCol(self, value: str) -> "IndexToString":
        """
        Sets the value of :py:attr:`outputCol`.
        """
        return self._set(outputCol=value)


class StopWordsRemover(
    JavaTransformer,
    HasInputCol,
    HasOutputCol,
    HasInputCols,
    HasOutputCols,
    JavaMLReadable["StopWordsRemover"],
    JavaMLWritable,
):
    """
    A feature transformer that filters out stop words from input.
    Since 3.0.0, :py:class:`StopWordsRemover` can filter out multiple columns at once by setting
    the :py:attr:`inputCols` parameter. Note that when both the :py:attr:`inputCol` and
    :py:attr:`inputCols` parameters are set, an Exception will be thrown.

    .. versionadded:: 1.6.0

    Notes
    -----
    null values from input array are preserved unless adding null to stopWords explicitly.

    Examples
    --------
    >>> df = spark.createDataFrame([(["a", "b", "c"],)], ["text"])
    >>> remover = StopWordsRemover(stopWords=["b"])
    >>> remover.setInputCol("text")
    StopWordsRemover...
    >>> remover.setOutputCol("words")
    StopWordsRemover...
    >>> remover.transform(df).head().words == ['a', 'c']
    True
    >>> stopWordsRemoverPath = temp_path + "/stopwords-remover"
    >>> remover.save(stopWordsRemoverPath)
    >>> loadedRemover = StopWordsRemover.load(stopWordsRemoverPath)
    >>> loadedRemover.getStopWords() == remover.getStopWords()
    True
    >>> loadedRemover.getCaseSensitive() == remover.getCaseSensitive()
    True
    >>> loadedRemover.transform(df).take(1) == remover.transform(df).take(1)
    True
    >>> df2 = spark.createDataFrame([(["a", "b", "c"], ["a", "b"])], ["text1", "text2"])
    >>> remover2 = StopWordsRemover(stopWords=["b"])
    >>> remover2.setInputCols(["text1", "text2"]).setOutputCols(["words1", "words2"])
    StopWordsRemover...
    >>> remover2.transform(df2).show()
    +---------+------+------+------+
    |    text1| text2|words1|words2|
    +---------+------+------+------+
    |[a, b, c]|[a, b]|[a, c]|   [a]|
    +---------+------+------+------+
    ...
    """

    _input_kwargs: Dict[str, Any]

    stopWords: Param[List[str]] = Param(
        Params._dummy(),
        "stopWords",
        "The words to be filtered out",
        typeConverter=TypeConverters.toListString,
    )
    caseSensitive: Param[bool] = Param(
        Params._dummy(),
        "caseSensitive",
        "whether to do a case sensitive " + "comparison over the stop words",
        typeConverter=TypeConverters.toBoolean,
    )
    locale: Param[str] = Param(
        Params._dummy(),
        "locale",
        "locale of the input. ignored when case sensitive " + "is true",
        typeConverter=TypeConverters.toString,
    )

    @overload
    def __init__(
        self,
        *,
        inputCol: Optional[str] = ...,
        outputCol: Optional[str] = ...,
        stopWords: Optional[List[str]] = ...,
        caseSensitive: bool = ...,
        locale: Optional[str] = ...,
    ):
        ...

    @overload
    def __init__(
        self,
        *,
        stopWords: Optional[List[str]] = ...,
        caseSensitive: bool = ...,
        locale: Optional[str] = ...,
        inputCols: Optional[List[str]] = ...,
        outputCols: Optional[List[str]] = ...,
    ):
        ...

    @keyword_only
    def __init__(
        self,
        *,
        inputCol: Optional[str] = None,
        outputCol: Optional[str] = None,
        stopWords: Optional[List[str]] = None,
        caseSensitive: bool = False,
        locale: Optional[str] = None,
        inputCols: Optional[List[str]] = None,
        outputCols: Optional[List[str]] = None,
    ):
        """
        __init__(self, \\*, inputCol=None, outputCol=None, stopWords=None, caseSensitive=false, \
                 locale=None, inputCols=None, outputCols=None)
        """
        super(StopWordsRemover, self).__init__()
        self._java_obj = self._new_java_obj(
            "org.apache.spark.ml.feature.StopWordsRemover", self.uid
        )
        self._setDefault(
            stopWords=StopWordsRemover.loadDefaultStopWords("english"),
            caseSensitive=False,
            locale=self._java_obj.getLocale(),
        )
        kwargs = self._input_kwargs
        self.setParams(**kwargs)

    @overload
    def setParams(
        self,
        *,
        inputCol: Optional[str] = ...,
        outputCol: Optional[str] = ...,
        stopWords: Optional[List[str]] = ...,
        caseSensitive: bool = ...,
        locale: Optional[str] = ...,
    ) -> "StopWordsRemover":
        ...

    @overload
    def setParams(
        self,
        *,
        stopWords: Optional[List[str]] = ...,
        caseSensitive: bool = ...,
        locale: Optional[str] = ...,
        inputCols: Optional[List[str]] = ...,
        outputCols: Optional[List[str]] = ...,
    ) -> "StopWordsRemover":
        ...

    @keyword_only
    @since("1.6.0")
    def setParams(
        self,
        *,
        inputCol: Optional[str] = None,
        outputCol: Optional[str] = None,
        stopWords: Optional[List[str]] = None,
        caseSensitive: bool = False,
        locale: Optional[str] = None,
        inputCols: Optional[List[str]] = None,
        outputCols: Optional[List[str]] = None,
    ) -> "StopWordsRemover":
        """
        setParams(self, \\*, inputCol=None, outputCol=None, stopWords=None, caseSensitive=false, \
                  locale=None, inputCols=None, outputCols=None)
        Sets params for this StopWordRemover.
        """
        kwargs = self._input_kwargs
        return self._set(**kwargs)

    @since("1.6.0")
    def setStopWords(self, value: List[str]) -> "StopWordsRemover":
        """
        Sets the value of :py:attr:`stopWords`.
        """
        return self._set(stopWords=value)

    @since("1.6.0")
    def getStopWords(self) -> List[str]:
        """
        Gets the value of :py:attr:`stopWords` or its default value.
        """
        return self.getOrDefault(self.stopWords)

    @since("1.6.0")
    def setCaseSensitive(self, value: bool) -> "StopWordsRemover":
        """
        Sets the value of :py:attr:`caseSensitive`.
        """
        return self._set(caseSensitive=value)

    @since("1.6.0")
    def getCaseSensitive(self) -> bool:
        """
        Gets the value of :py:attr:`caseSensitive` or its default value.
        """
        return self.getOrDefault(self.caseSensitive)

    @since("2.4.0")
    def setLocale(self, value: str) -> "StopWordsRemover":
        """
        Sets the value of :py:attr:`locale`.
        """
        return self._set(locale=value)

    @since("2.4.0")
    def getLocale(self) -> str:
        """
        Gets the value of :py:attr:`locale`.
        """
        return self.getOrDefault(self.locale)

    def setInputCol(self, value: str) -> "StopWordsRemover":
        """
        Sets the value of :py:attr:`inputCol`.
        """
        return self._set(inputCol=value)

    def setOutputCol(self, value: str) -> "StopWordsRemover":
        """
        Sets the value of :py:attr:`outputCol`.
        """
        return self._set(outputCol=value)

    @since("3.0.0")
    def setInputCols(self, value: List[str]) -> "StopWordsRemover":
        """
        Sets the value of :py:attr:`inputCols`.
        """
        return self._set(inputCols=value)

    @since("3.0.0")
    def setOutputCols(self, value: List[str]) -> "StopWordsRemover":
        """
        Sets the value of :py:attr:`outputCols`.
        """
        return self._set(outputCols=value)

    @staticmethod
    @since("2.0.0")
    def loadDefaultStopWords(language: str) -> List[str]:
        """
        Loads the default stop words for the given language.
        Supported languages: danish, dutch, english, finnish, french, german, hungarian,
        italian, norwegian, portuguese, russian, spanish, swedish, turkish
        """
        stopWordsObj = _jvm().org.apache.spark.ml.feature.StopWordsRemover
        return list(stopWordsObj.loadDefaultStopWords(language))


@inherit_doc
class Tokenizer(
    JavaTransformer,
    HasInputCol,
    HasOutputCol,
    JavaMLReadable["Tokenizer"],
    JavaMLWritable,
):
    """
    A tokenizer that converts the input string to lowercase and then
    splits it by white spaces.

    .. versionadded:: 1.3.0

    Examples
    --------
    >>> df = spark.createDataFrame([("a b c",)], ["text"])
    >>> tokenizer = Tokenizer(outputCol="words")
    >>> tokenizer.setInputCol("text")
    Tokenizer...
    >>> tokenizer.transform(df).head()
    Row(text='a b c', words=['a', 'b', 'c'])
    >>> # Change a parameter.
    >>> tokenizer.setParams(outputCol="tokens").transform(df).head()
    Row(text='a b c', tokens=['a', 'b', 'c'])
    >>> # Temporarily modify a parameter.
    >>> tokenizer.transform(df, {tokenizer.outputCol: "words"}).head()
    Row(text='a b c', words=['a', 'b', 'c'])
    >>> tokenizer.transform(df).head()
    Row(text='a b c', tokens=['a', 'b', 'c'])
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
    """

    _input_kwargs: Dict[str, Any]

    @keyword_only
    def __init__(self, *, inputCol: Optional[str] = None, outputCol: Optional[str] = None):
        """
        __init__(self, \\*, inputCol=None, outputCol=None)
        """
        super(Tokenizer, self).__init__()
        self._java_obj = self._new_java_obj("org.apache.spark.ml.feature.Tokenizer", self.uid)
        kwargs = self._input_kwargs
        self.setParams(**kwargs)

    @keyword_only
    @since("1.3.0")
    def setParams(
        self, *, inputCol: Optional[str] = None, outputCol: Optional[str] = None
    ) -> "Tokenizer":
        """
        setParams(self, \\*, inputCol=None, outputCol=None)
        Sets params for this Tokenizer.
        """
        kwargs = self._input_kwargs
        return self._set(**kwargs)

    def setInputCol(self, value: str) -> "Tokenizer":
        """
        Sets the value of :py:attr:`inputCol`.
        """
        return self._set(inputCol=value)

    def setOutputCol(self, value: str) -> "Tokenizer":
        """
        Sets the value of :py:attr:`outputCol`.
        """
        return self._set(outputCol=value)


@inherit_doc
class VectorAssembler(
    JavaTransformer,
    HasInputCols,
    HasOutputCol,
    HasHandleInvalid,
    JavaMLReadable["VectorAssembler"],
    JavaMLWritable,
):
    """
    A feature transformer that merges multiple columns into a vector column.

    .. versionadded:: 1.4.0

    Examples
    --------
    >>> df = spark.createDataFrame([(1, 0, 3)], ["a", "b", "c"])
    >>> vecAssembler = VectorAssembler(outputCol="features")
    >>> vecAssembler.setInputCols(["a", "b", "c"])
    VectorAssembler...
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
    >>> dfWithNullsAndNaNs = spark.createDataFrame(
    ...    [(1.0, 2.0, None), (3.0, float("nan"), 4.0), (5.0, 6.0, 7.0)], ["a", "b", "c"])
    >>> vecAssembler2 = VectorAssembler(inputCols=["a", "b", "c"], outputCol="features",
    ...    handleInvalid="keep")
    >>> vecAssembler2.transform(dfWithNullsAndNaNs).show()
    +---+---+----+-------------+
    |  a|  b|   c|     features|
    +---+---+----+-------------+
    |1.0|2.0|null|[1.0,2.0,NaN]|
    |3.0|NaN| 4.0|[3.0,NaN,4.0]|
    |5.0|6.0| 7.0|[5.0,6.0,7.0]|
    +---+---+----+-------------+
    ...
    >>> vecAssembler2.setParams(handleInvalid="skip").transform(dfWithNullsAndNaNs).show()
    +---+---+---+-------------+
    |  a|  b|  c|     features|
    +---+---+---+-------------+
    |5.0|6.0|7.0|[5.0,6.0,7.0]|
    +---+---+---+-------------+
    ...
    """

    _input_kwargs: Dict[str, Any]

    handleInvalid: Param[str] = Param(
        Params._dummy(),
        "handleInvalid",
        "How to handle invalid data (NULL "
        + "and NaN values). Options are 'skip' (filter out rows with invalid "
        + "data), 'error' (throw an error), or 'keep' (return relevant number "
        + "of NaN in the output). Column lengths are taken from the size of ML "
        + "Attribute Group, which can be set using `VectorSizeHint` in a "
        + "pipeline before `VectorAssembler`. Column lengths can also be "
        + "inferred from first rows of the data since it is safe to do so but "
        + "only in case of 'error' or 'skip').",
        typeConverter=TypeConverters.toString,
    )

    @keyword_only
    def __init__(
        self,
        *,
        inputCols: Optional[List[str]] = None,
        outputCol: Optional[str] = None,
        handleInvalid: str = "error",
    ):
        """
        __init__(self, \\*, inputCols=None, outputCol=None, handleInvalid="error")
        """
        super(VectorAssembler, self).__init__()
        self._java_obj = self._new_java_obj("org.apache.spark.ml.feature.VectorAssembler", self.uid)
        self._setDefault(handleInvalid="error")
        kwargs = self._input_kwargs
        self.setParams(**kwargs)

    @keyword_only
    @since("1.4.0")
    def setParams(
        self,
        *,
        inputCols: Optional[List[str]] = None,
        outputCol: Optional[str] = None,
        handleInvalid: str = "error",
    ) -> "VectorAssembler":
        """
        setParams(self, \\*, inputCols=None, outputCol=None, handleInvalid="error")
        Sets params for this VectorAssembler.
        """
        kwargs = self._input_kwargs
        return self._set(**kwargs)

    def setInputCols(self, value: List[str]) -> "VectorAssembler":
        """
        Sets the value of :py:attr:`inputCols`.
        """
        return self._set(inputCols=value)

    def setOutputCol(self, value: str) -> "VectorAssembler":
        """
        Sets the value of :py:attr:`outputCol`.
        """
        return self._set(outputCol=value)

    def setHandleInvalid(self, value: str) -> "VectorAssembler":
        """
        Sets the value of :py:attr:`handleInvalid`.
        """
        return self._set(handleInvalid=value)


class _VectorIndexerParams(HasInputCol, HasOutputCol, HasHandleInvalid):
    """
    Params for :py:class:`VectorIndexer` and :py:class:`VectorIndexerModel`.

    .. versionadded:: 3.0.0
    """

    maxCategories: Param[int] = Param(
        Params._dummy(),
        "maxCategories",
        "Threshold for the number of values a categorical feature can take "
        + "(>= 2). If a feature is found to have > maxCategories values, then "
        + "it is declared continuous.",
        typeConverter=TypeConverters.toInt,
    )

    handleInvalid: Param[str] = Param(
        Params._dummy(),
        "handleInvalid",
        "How to handle invalid data "
        + "(unseen labels or NULL values). Options are 'skip' (filter out "
        + "rows with invalid data), 'error' (throw an error), or 'keep' (put "
        + "invalid data in a special additional bucket, at index of the number "
        + "of categories of the feature).",
        typeConverter=TypeConverters.toString,
    )

    def __init__(self, *args: Any):
        super(_VectorIndexerParams, self).__init__(*args)
        self._setDefault(maxCategories=20, handleInvalid="error")

    @since("1.4.0")
    def getMaxCategories(self) -> int:
        """
        Gets the value of maxCategories or its default value.
        """
        return self.getOrDefault(self.maxCategories)


@inherit_doc
class VectorIndexer(
    JavaEstimator["VectorIndexerModel"],
    _VectorIndexerParams,
    HasHandleInvalid,
    JavaMLReadable["VectorIndexer"],
    JavaMLWritable,
):
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

    .. versionadded:: 1.4.0

    Examples
    --------
    >>> from pyspark.ml.linalg import Vectors
    >>> df = spark.createDataFrame([(Vectors.dense([-1.0, 0.0]),),
    ...     (Vectors.dense([0.0, 1.0]),), (Vectors.dense([0.0, 2.0]),)], ["a"])
    >>> indexer = VectorIndexer(maxCategories=2, inputCol="a")
    >>> indexer.setOutputCol("indexed")
    VectorIndexer...
    >>> model = indexer.fit(df)
    >>> indexer.getHandleInvalid()
    'error'
    >>> model.setOutputCol("output")
    VectorIndexerModel...
    >>> model.transform(df).head().output
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
    >>> loadedModel.transform(df).take(1) == model.transform(df).take(1)
    True
    >>> dfWithInvalid = spark.createDataFrame([(Vectors.dense([3.0, 1.0]),)], ["a"])
    >>> indexer.getHandleInvalid()
    'error'
    >>> model3 = indexer.setHandleInvalid("skip").fit(df)
    >>> model3.transform(dfWithInvalid).count()
    0
    >>> model4 = indexer.setParams(handleInvalid="keep", outputCol="indexed").fit(df)
    >>> model4.transform(dfWithInvalid).head().indexed
    DenseVector([2.0, 1.0])
    """

    _input_kwargs: Dict[str, Any]

    @keyword_only
    def __init__(
        self,
        *,
        maxCategories: int = 20,
        inputCol: Optional[str] = None,
        outputCol: Optional[str] = None,
        handleInvalid: str = "error",
    ):
        """
        __init__(self, \\*, maxCategories=20, inputCol=None, outputCol=None, handleInvalid="error")
        """
        super(VectorIndexer, self).__init__()
        self._java_obj = self._new_java_obj("org.apache.spark.ml.feature.VectorIndexer", self.uid)
        kwargs = self._input_kwargs
        self.setParams(**kwargs)

    @keyword_only
    @since("1.4.0")
    def setParams(
        self,
        *,
        maxCategories: int = 20,
        inputCol: Optional[str] = None,
        outputCol: Optional[str] = None,
        handleInvalid: str = "error",
    ) -> "VectorIndexer":
        """
        setParams(self, \\*, maxCategories=20, inputCol=None, outputCol=None, handleInvalid="error")
        Sets params for this VectorIndexer.
        """
        kwargs = self._input_kwargs
        return self._set(**kwargs)

    @since("1.4.0")
    def setMaxCategories(self, value: int) -> "VectorIndexer":
        """
        Sets the value of :py:attr:`maxCategories`.
        """
        return self._set(maxCategories=value)

    def setInputCol(self, value: str) -> "VectorIndexer":
        """
        Sets the value of :py:attr:`inputCol`.
        """
        return self._set(inputCol=value)

    def setOutputCol(self, value: str) -> "VectorIndexer":
        """
        Sets the value of :py:attr:`outputCol`.
        """
        return self._set(outputCol=value)

    def setHandleInvalid(self, value: str) -> "VectorIndexer":
        """
        Sets the value of :py:attr:`handleInvalid`.
        """
        return self._set(handleInvalid=value)

    def _create_model(self, java_model: "JavaObject") -> "VectorIndexerModel":
        return VectorIndexerModel(java_model)


class VectorIndexerModel(
    JavaModel, _VectorIndexerParams, JavaMLReadable["VectorIndexerModel"], JavaMLWritable
):
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

    @since("3.0.0")
    def setInputCol(self, value: str) -> "VectorIndexerModel":
        """
        Sets the value of :py:attr:`inputCol`.
        """
        return self._set(inputCol=value)

    @since("3.0.0")
    def setOutputCol(self, value: str) -> "VectorIndexerModel":
        """
        Sets the value of :py:attr:`outputCol`.
        """
        return self._set(outputCol=value)

    @property  # type: ignore[misc]
    @since("1.4.0")
    def numFeatures(self) -> int:
        """
        Number of features, i.e., length of Vectors which this transforms.
        """
        return self._call_java("numFeatures")

    @property  # type: ignore[misc]
    @since("1.4.0")
    def categoryMaps(self) -> Dict[int, Tuple[float, int]]:
        """
        Feature value index.  Keys are categorical feature indices (column indices).
        Values are maps from original features values to 0-based category indices.
        If a feature is not in this map, it is treated as continuous.
        """
        return self._call_java("javaCategoryMaps")


@inherit_doc
class VectorSlicer(
    JavaTransformer,
    HasInputCol,
    HasOutputCol,
    JavaMLReadable["VectorSlicer"],
    JavaMLWritable,
):
    """
    This class takes a feature vector and outputs a new feature vector with a subarray
    of the original features.

    The subset of features can be specified with either indices (`setIndices()`)
    or names (`setNames()`).  At least one feature must be selected. Duplicate features
    are not allowed, so there can be no overlap between selected indices and names.

    The output vector will order features with the selected indices first (in the order given),
    followed by the selected names (in the order given).

    .. versionadded:: 1.6.0

    Examples
    --------
    >>> from pyspark.ml.linalg import Vectors
    >>> df = spark.createDataFrame([
    ...     (Vectors.dense([-2.0, 2.3, 0.0, 0.0, 1.0]),),
    ...     (Vectors.dense([0.0, 0.0, 0.0, 0.0, 0.0]),),
    ...     (Vectors.dense([0.6, -1.1, -3.0, 4.5, 3.3]),)], ["features"])
    >>> vs = VectorSlicer(outputCol="sliced", indices=[1, 4])
    >>> vs.setInputCol("features")
    VectorSlicer...
    >>> vs.transform(df).head().sliced
    DenseVector([2.3, 1.0])
    >>> vectorSlicerPath = temp_path + "/vector-slicer"
    >>> vs.save(vectorSlicerPath)
    >>> loadedVs = VectorSlicer.load(vectorSlicerPath)
    >>> loadedVs.getIndices() == vs.getIndices()
    True
    >>> loadedVs.getNames() == vs.getNames()
    True
    >>> loadedVs.transform(df).take(1) == vs.transform(df).take(1)
    True
    """

    _input_kwargs: Dict[str, Any]

    indices: Param[List[int]] = Param(
        Params._dummy(),
        "indices",
        "An array of indices to select features from "
        + "a vector column. There can be no overlap with names.",
        typeConverter=TypeConverters.toListInt,
    )
    names: Param[List[str]] = Param(
        Params._dummy(),
        "names",
        "An array of feature names to select features from "
        + "a vector column. These names must be specified by ML "
        + "org.apache.spark.ml.attribute.Attribute. There can be no overlap with "
        + "indices.",
        typeConverter=TypeConverters.toListString,
    )

    @keyword_only
    def __init__(
        self,
        *,
        inputCol: Optional[str] = None,
        outputCol: Optional[str] = None,
        indices: Optional[List[int]] = None,
        names: Optional[List[str]] = None,
    ):
        """
        __init__(self, \\*, inputCol=None, outputCol=None, indices=None, names=None)
        """
        super(VectorSlicer, self).__init__()
        self._java_obj = self._new_java_obj("org.apache.spark.ml.feature.VectorSlicer", self.uid)
        self._setDefault(indices=[], names=[])
        kwargs = self._input_kwargs
        self.setParams(**kwargs)

    @keyword_only
    @since("1.6.0")
    def setParams(
        self,
        *,
        inputCol: Optional[str] = None,
        outputCol: Optional[str] = None,
        indices: Optional[List[int]] = None,
        names: Optional[List[str]] = None,
    ) -> "VectorSlicer":
        """
        setParams(self, \\*, inputCol=None, outputCol=None, indices=None, names=None):
        Sets params for this VectorSlicer.
        """
        kwargs = self._input_kwargs
        return self._set(**kwargs)

    @since("1.6.0")
    def setIndices(self, value: List[int]) -> "VectorSlicer":
        """
        Sets the value of :py:attr:`indices`.
        """
        return self._set(indices=value)

    @since("1.6.0")
    def getIndices(self) -> List[int]:
        """
        Gets the value of indices or its default value.
        """
        return self.getOrDefault(self.indices)

    @since("1.6.0")
    def setNames(self, value: List[str]) -> "VectorSlicer":
        """
        Sets the value of :py:attr:`names`.
        """
        return self._set(names=value)

    @since("1.6.0")
    def getNames(self) -> List[str]:
        """
        Gets the value of names or its default value.
        """
        return self.getOrDefault(self.names)

    def setInputCol(self, value: str) -> "VectorSlicer":
        """
        Sets the value of :py:attr:`inputCol`.
        """
        return self._set(inputCol=value)

    def setOutputCol(self, value: str) -> "VectorSlicer":
        """
        Sets the value of :py:attr:`outputCol`.
        """
        return self._set(outputCol=value)


class _Word2VecParams(HasStepSize, HasMaxIter, HasSeed, HasInputCol, HasOutputCol):
    """
    Params for :py:class:`Word2Vec` and :py:class:`Word2VecModel`.

    .. versionadded:: 3.0.0
    """

    vectorSize: Param[int] = Param(
        Params._dummy(),
        "vectorSize",
        "the dimension of codes after transforming from words",
        typeConverter=TypeConverters.toInt,
    )
    numPartitions: Param[int] = Param(
        Params._dummy(),
        "numPartitions",
        "number of partitions for sentences of words",
        typeConverter=TypeConverters.toInt,
    )
    minCount: Param[int] = Param(
        Params._dummy(),
        "minCount",
        "the minimum number of times a token must appear to be included in the "
        + "word2vec model's vocabulary",
        typeConverter=TypeConverters.toInt,
    )
    windowSize: Param[int] = Param(
        Params._dummy(),
        "windowSize",
        "the window size (context words from [-window, window]). Default value is 5",
        typeConverter=TypeConverters.toInt,
    )
    maxSentenceLength: Param[int] = Param(
        Params._dummy(),
        "maxSentenceLength",
        "Maximum length (in words) of each sentence in the input data. "
        + "Any sentence longer than this threshold will "
        + "be divided into chunks up to the size.",
        typeConverter=TypeConverters.toInt,
    )

    def __init__(self, *args: Any):
        super(_Word2VecParams, self).__init__(*args)
        self._setDefault(
            vectorSize=100,
            minCount=5,
            numPartitions=1,
            stepSize=0.025,
            maxIter=1,
            windowSize=5,
            maxSentenceLength=1000,
        )

    @since("1.4.0")
    def getVectorSize(self) -> int:
        """
        Gets the value of vectorSize or its default value.
        """
        return self.getOrDefault(self.vectorSize)

    @since("1.4.0")
    def getNumPartitions(self) -> int:
        """
        Gets the value of numPartitions or its default value.
        """
        return self.getOrDefault(self.numPartitions)

    @since("1.4.0")
    def getMinCount(self) -> int:
        """
        Gets the value of minCount or its default value.
        """
        return self.getOrDefault(self.minCount)

    @since("2.0.0")
    def getWindowSize(self) -> int:
        """
        Gets the value of windowSize or its default value.
        """
        return self.getOrDefault(self.windowSize)

    @since("2.0.0")
    def getMaxSentenceLength(self) -> int:
        """
        Gets the value of maxSentenceLength or its default value.
        """
        return self.getOrDefault(self.maxSentenceLength)


@inherit_doc
class Word2Vec(
    JavaEstimator["Word2VecModel"],
    _Word2VecParams,
    JavaMLReadable["Word2Vec"],
    JavaMLWritable,
):
    """
    Word2Vec trains a model of `Map(String, Vector)`, i.e. transforms a word into a code for further
    natural language processing or machine learning process.

    .. versionadded:: 1.4.0

    Examples
    --------
    >>> sent = ("a b " * 100 + "a c " * 10).split(" ")
    >>> doc = spark.createDataFrame([(sent,), (sent,)], ["sentence"])
    >>> word2Vec = Word2Vec(vectorSize=5, seed=42, inputCol="sentence", outputCol="model")
    >>> word2Vec.setMaxIter(10)
    Word2Vec...
    >>> word2Vec.getMaxIter()
    10
    >>> word2Vec.clear(word2Vec.maxIter)
    >>> model = word2Vec.fit(doc)
    >>> model.getMinCount()
    5
    >>> model.setInputCol("sentence")
    Word2VecModel...
    >>> model.getVectors().show()
    +----+--------------------+
    |word|              vector|
    +----+--------------------+
    |   a|[0.0951...
    |   b|[-1.202...
    |   c|[0.3015...
    +----+--------------------+
    ...
    >>> model.findSynonymsArray("a", 2)
    [('b', 0.015859...), ('c', -0.568079...)]
    >>> from pyspark.sql.functions import format_number as fmt
    >>> model.findSynonyms("a", 2).select("word", fmt("similarity", 5).alias("similarity")).show()
    +----+----------+
    |word|similarity|
    +----+----------+
    |   b|   0.01586|
    |   c|  -0.56808|
    +----+----------+
    ...
    >>> model.transform(doc).head().model
    DenseVector([-0.4833, 0.1855, -0.273, -0.0509, -0.4769])
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
    >>> loadedModel.transform(doc).take(1) == model.transform(doc).take(1)
    True
    """

    _input_kwargs: Dict[str, Any]

    @keyword_only
    def __init__(
        self,
        *,
        vectorSize: int = 100,
        minCount: int = 5,
        numPartitions: int = 1,
        stepSize: float = 0.025,
        maxIter: int = 1,
        seed: Optional[int] = None,
        inputCol: Optional[str] = None,
        outputCol: Optional[str] = None,
        windowSize: int = 5,
        maxSentenceLength: int = 1000,
    ):
        """
        __init__(self, \\*, vectorSize=100, minCount=5, numPartitions=1, stepSize=0.025, \
                 maxIter=1, seed=None, inputCol=None, outputCol=None, windowSize=5, \
                 maxSentenceLength=1000)
        """
        super(Word2Vec, self).__init__()
        self._java_obj = self._new_java_obj("org.apache.spark.ml.feature.Word2Vec", self.uid)
        kwargs = self._input_kwargs
        self.setParams(**kwargs)

    @keyword_only
    @since("1.4.0")
    def setParams(
        self,
        *,
        vectorSize: int = 100,
        minCount: int = 5,
        numPartitions: int = 1,
        stepSize: float = 0.025,
        maxIter: int = 1,
        seed: Optional[int] = None,
        inputCol: Optional[str] = None,
        outputCol: Optional[str] = None,
        windowSize: int = 5,
        maxSentenceLength: int = 1000,
    ) -> "Word2Vec":
        """
        setParams(self, \\*, minCount=5, numPartitions=1, stepSize=0.025, maxIter=1, \
                  seed=None, inputCol=None, outputCol=None, windowSize=5, \
                  maxSentenceLength=1000)
        Sets params for this Word2Vec.
        """
        kwargs = self._input_kwargs
        return self._set(**kwargs)

    @since("1.4.0")
    def setVectorSize(self, value: int) -> "Word2Vec":
        """
        Sets the value of :py:attr:`vectorSize`.
        """
        return self._set(vectorSize=value)

    @since("1.4.0")
    def setNumPartitions(self, value: int) -> "Word2Vec":
        """
        Sets the value of :py:attr:`numPartitions`.
        """
        return self._set(numPartitions=value)

    @since("1.4.0")
    def setMinCount(self, value: int) -> "Word2Vec":
        """
        Sets the value of :py:attr:`minCount`.
        """
        return self._set(minCount=value)

    @since("2.0.0")
    def setWindowSize(self, value: int) -> "Word2Vec":
        """
        Sets the value of :py:attr:`windowSize`.
        """
        return self._set(windowSize=value)

    @since("2.0.0")
    def setMaxSentenceLength(self, value: int) -> "Word2Vec":
        """
        Sets the value of :py:attr:`maxSentenceLength`.
        """
        return self._set(maxSentenceLength=value)

    def setMaxIter(self, value: int) -> "Word2Vec":
        """
        Sets the value of :py:attr:`maxIter`.
        """
        return self._set(maxIter=value)

    def setInputCol(self, value: str) -> "Word2Vec":
        """
        Sets the value of :py:attr:`inputCol`.
        """
        return self._set(inputCol=value)

    def setOutputCol(self, value: str) -> "Word2Vec":
        """
        Sets the value of :py:attr:`outputCol`.
        """
        return self._set(outputCol=value)

    def setSeed(self, value: int) -> "Word2Vec":
        """
        Sets the value of :py:attr:`seed`.
        """
        return self._set(seed=value)

    @since("1.4.0")
    def setStepSize(self, value: float) -> "Word2Vec":
        """
        Sets the value of :py:attr:`stepSize`.
        """
        return self._set(stepSize=value)

    def _create_model(self, java_model: "JavaObject") -> "Word2VecModel":
        return Word2VecModel(java_model)


class Word2VecModel(JavaModel, _Word2VecParams, JavaMLReadable["Word2VecModel"], JavaMLWritable):
    """
    Model fitted by :py:class:`Word2Vec`.

    .. versionadded:: 1.4.0
    """

    @since("1.5.0")
    def getVectors(self) -> DataFrame:
        """
        Returns the vector representation of the words as a dataframe
        with two fields, word and vector.
        """
        return self._call_java("getVectors")

    def setInputCol(self, value: str) -> "Word2VecModel":
        """
        Sets the value of :py:attr:`inputCol`.
        """
        return self._set(inputCol=value)

    def setOutputCol(self, value: str) -> "Word2VecModel":
        """
        Sets the value of :py:attr:`outputCol`.
        """
        return self._set(outputCol=value)

    @since("1.5.0")
    def findSynonyms(self, word: Union[str, Vector], num: int) -> DataFrame:
        """
        Find "num" number of words closest in similarity to "word".
        word can be a string or vector representation.
        Returns a dataframe with two fields word and similarity (which
        gives the cosine similarity).
        """
        if not isinstance(word, str):
            word = _convert_to_vector(word)
        return self._call_java("findSynonyms", word, num)

    @since("2.3.0")
    def findSynonymsArray(self, word: Union[Vector, str], num: int) -> List[Tuple[str, float]]:
        """
        Find "num" number of words closest in similarity to "word".
        word can be a string or vector representation.
        Returns an array with two fields word and similarity (which
        gives the cosine similarity).
        """
        if not isinstance(word, str):
            word = _convert_to_vector(word)
        assert self._java_obj is not None
        tuples = self._java_obj.findSynonymsArray(word, num)
        return list(map(lambda st: (st._1(), st._2()), list(tuples)))


class _PCAParams(HasInputCol, HasOutputCol):
    """
    Params for :py:class:`PCA` and :py:class:`PCAModel`.

    .. versionadded:: 3.0.0
    """

    k: Param[int] = Param(
        Params._dummy(),
        "k",
        "the number of principal components",
        typeConverter=TypeConverters.toInt,
    )

    @since("1.5.0")
    def getK(self) -> int:
        """
        Gets the value of k or its default value.
        """
        return self.getOrDefault(self.k)


@inherit_doc
class PCA(JavaEstimator["PCAModel"], _PCAParams, JavaMLReadable["PCA"], JavaMLWritable):
    """
    PCA trains a model to project vectors to a lower dimensional space of the
    top :py:attr:`k` principal components.

    .. versionadded:: 1.5.0

    Examples
    --------
    >>> from pyspark.ml.linalg import Vectors
    >>> data = [(Vectors.sparse(5, [(1, 1.0), (3, 7.0)]),),
    ...     (Vectors.dense([2.0, 0.0, 3.0, 4.0, 5.0]),),
    ...     (Vectors.dense([4.0, 0.0, 0.0, 6.0, 7.0]),)]
    >>> df = spark.createDataFrame(data,["features"])
    >>> pca = PCA(k=2, inputCol="features")
    >>> pca.setOutputCol("pca_features")
    PCA...
    >>> model = pca.fit(df)
    >>> model.getK()
    2
    >>> model.setOutputCol("output")
    PCAModel...
    >>> model.transform(df).collect()[0].output
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
    >>> loadedModel.transform(df).take(1) == model.transform(df).take(1)
    True
    """

    _input_kwargs: Dict[str, Any]

    @keyword_only
    def __init__(
        self,
        *,
        k: Optional[int] = None,
        inputCol: Optional[str] = None,
        outputCol: Optional[str] = None,
    ):
        """
        __init__(self, \\*, k=None, inputCol=None, outputCol=None)
        """
        super(PCA, self).__init__()
        self._java_obj = self._new_java_obj("org.apache.spark.ml.feature.PCA", self.uid)
        kwargs = self._input_kwargs
        self.setParams(**kwargs)

    @keyword_only
    @since("1.5.0")
    def setParams(
        self,
        *,
        k: Optional[int] = None,
        inputCol: Optional[str] = None,
        outputCol: Optional[str] = None,
    ) -> "PCA":
        """
        setParams(self, \\*, k=None, inputCol=None, outputCol=None)
        Set params for this PCA.
        """
        kwargs = self._input_kwargs
        return self._set(**kwargs)

    @since("1.5.0")
    def setK(self, value: int) -> "PCA":
        """
        Sets the value of :py:attr:`k`.
        """
        return self._set(k=value)

    def setInputCol(self, value: str) -> "PCA":
        """
        Sets the value of :py:attr:`inputCol`.
        """
        return self._set(inputCol=value)

    def setOutputCol(self, value: str) -> "PCA":
        """
        Sets the value of :py:attr:`outputCol`.
        """
        return self._set(outputCol=value)

    def _create_model(self, java_model: "JavaObject") -> "PCAModel":
        return PCAModel(java_model)


class PCAModel(JavaModel, _PCAParams, JavaMLReadable["PCAModel"], JavaMLWritable):
    """
    Model fitted by :py:class:`PCA`. Transforms vectors to a lower dimensional space.

    .. versionadded:: 1.5.0
    """

    @since("3.0.0")
    def setInputCol(self, value: str) -> "PCAModel":
        """
        Sets the value of :py:attr:`inputCol`.
        """
        return self._set(inputCol=value)

    @since("3.0.0")
    def setOutputCol(self, value: str) -> "PCAModel":
        """
        Sets the value of :py:attr:`outputCol`.
        """
        return self._set(outputCol=value)

    @property  # type: ignore[misc]
    @since("2.0.0")
    def pc(self) -> DenseMatrix:
        """
        Returns a principal components Matrix.
        Each column is one principal component.
        """
        return self._call_java("pc")

    @property  # type: ignore[misc]
    @since("2.0.0")
    def explainedVariance(self) -> DenseVector:
        """
        Returns a vector of proportions of variance
        explained by each principal component.
        """
        return self._call_java("explainedVariance")


class _RFormulaParams(HasFeaturesCol, HasLabelCol, HasHandleInvalid):
    """
    Params for :py:class:`RFormula` and :py:class:`RFormula`.

    .. versionadded:: 3.0.0
    """

    formula: Param[str] = Param(
        Params._dummy(), "formula", "R model formula", typeConverter=TypeConverters.toString
    )

    forceIndexLabel: Param[bool] = Param(
        Params._dummy(),
        "forceIndexLabel",
        "Force to index label whether it is numeric or string",
        typeConverter=TypeConverters.toBoolean,
    )

    stringIndexerOrderType: Param[str] = Param(
        Params._dummy(),
        "stringIndexerOrderType",
        "How to order categories of a string feature column used by "
        + "StringIndexer. The last category after ordering is dropped "
        + "when encoding strings. Supported options: frequencyDesc, "
        + "frequencyAsc, alphabetDesc, alphabetAsc. The default value "
        + "is frequencyDesc. When the ordering is set to alphabetDesc, "
        + "RFormula drops the same category as R when encoding strings.",
        typeConverter=TypeConverters.toString,
    )

    handleInvalid: Param[str] = Param(
        Params._dummy(),
        "handleInvalid",
        "how to handle invalid entries. "
        + "Options are 'skip' (filter out rows with invalid values), "
        + "'error' (throw an error), or 'keep' (put invalid data in a special "
        + "additional bucket, at index numLabels).",
        typeConverter=TypeConverters.toString,
    )

    def __init__(self, *args: Any):
        super(_RFormulaParams, self).__init__(*args)
        self._setDefault(
            forceIndexLabel=False, stringIndexerOrderType="frequencyDesc", handleInvalid="error"
        )

    @since("1.5.0")
    def getFormula(self) -> str:
        """
        Gets the value of :py:attr:`formula`.
        """
        return self.getOrDefault(self.formula)

    @since("2.1.0")
    def getForceIndexLabel(self) -> bool:
        """
        Gets the value of :py:attr:`forceIndexLabel`.
        """
        return self.getOrDefault(self.forceIndexLabel)

    @since("2.3.0")
    def getStringIndexerOrderType(self) -> str:
        """
        Gets the value of :py:attr:`stringIndexerOrderType` or its default value 'frequencyDesc'.
        """
        return self.getOrDefault(self.stringIndexerOrderType)


@inherit_doc
class RFormula(
    JavaEstimator["RFormulaModel"],
    _RFormulaParams,
    JavaMLReadable["RFormula"],
    JavaMLWritable,
):
    """
    Implements the transforms required for fitting a dataset against an
    R model formula. Currently we support a limited subset of the R
    operators, including '~', '.', ':', '+', '-', '*', and '^'.

    .. versionadded:: 1.5.0

    Notes
    -----
    Also see the `R formula docs
    <http://stat.ethz.ch/R-manual/R-patched/library/stats/html/formula.html>`_.

    Examples
    --------
    >>> df = spark.createDataFrame([
    ...     (1.0, 1.0, "a"),
    ...     (0.0, 2.0, "b"),
    ...     (0.0, 0.0, "a")
    ... ], ["y", "x", "s"])
    >>> rf = RFormula(formula="y ~ x + s")
    >>> model = rf.fit(df)
    >>> model.getLabelCol()
    'label'
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
    >>> loadedRF.getHandleInvalid() == rf.getHandleInvalid()
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
    """

    _input_kwargs: Dict[str, Any]

    @keyword_only
    def __init__(
        self,
        *,
        formula: Optional[str] = None,
        featuresCol: str = "features",
        labelCol: str = "label",
        forceIndexLabel: bool = False,
        stringIndexerOrderType: str = "frequencyDesc",
        handleInvalid: str = "error",
    ):
        """
        __init__(self, \\*, formula=None, featuresCol="features", labelCol="label", \
                 forceIndexLabel=False, stringIndexerOrderType="frequencyDesc", \
                 handleInvalid="error")
        """
        super(RFormula, self).__init__()
        self._java_obj = self._new_java_obj("org.apache.spark.ml.feature.RFormula", self.uid)
        kwargs = self._input_kwargs
        self.setParams(**kwargs)

    @keyword_only
    @since("1.5.0")
    def setParams(
        self,
        *,
        formula: Optional[str] = None,
        featuresCol: str = "features",
        labelCol: str = "label",
        forceIndexLabel: bool = False,
        stringIndexerOrderType: str = "frequencyDesc",
        handleInvalid: str = "error",
    ) -> "RFormula":
        """
        setParams(self, \\*, formula=None, featuresCol="features", labelCol="label", \
                  forceIndexLabel=False, stringIndexerOrderType="frequencyDesc", \
                  handleInvalid="error")
        Sets params for RFormula.
        """
        kwargs = self._input_kwargs
        return self._set(**kwargs)

    @since("1.5.0")
    def setFormula(self, value: str) -> "RFormula":
        """
        Sets the value of :py:attr:`formula`.
        """
        return self._set(formula=value)

    @since("2.1.0")
    def setForceIndexLabel(self, value: bool) -> "RFormula":
        """
        Sets the value of :py:attr:`forceIndexLabel`.
        """
        return self._set(forceIndexLabel=value)

    @since("2.3.0")
    def setStringIndexerOrderType(self, value: str) -> "RFormula":
        """
        Sets the value of :py:attr:`stringIndexerOrderType`.
        """
        return self._set(stringIndexerOrderType=value)

    def setFeaturesCol(self, value: str) -> "RFormula":
        """
        Sets the value of :py:attr:`featuresCol`.
        """
        return self._set(featuresCol=value)

    def setLabelCol(self, value: str) -> "RFormula":
        """
        Sets the value of :py:attr:`labelCol`.
        """
        return self._set(labelCol=value)

    def setHandleInvalid(self, value: str) -> "RFormula":
        """
        Sets the value of :py:attr:`handleInvalid`.
        """
        return self._set(handleInvalid=value)

    def _create_model(self, java_model: "JavaObject") -> "RFormulaModel":
        return RFormulaModel(java_model)

    def __str__(self) -> str:
        formulaStr = self.getFormula() if self.isDefined(self.formula) else ""
        return "RFormula(%s) (uid=%s)" % (formulaStr, self.uid)


class RFormulaModel(JavaModel, _RFormulaParams, JavaMLReadable["RFormulaModel"], JavaMLWritable):
    """
    Model fitted by :py:class:`RFormula`. Fitting is required to determine the
    factor levels of formula terms.

    .. versionadded:: 1.5.0
    """

    def __str__(self) -> str:
        resolvedFormula = self._call_java("resolvedFormula")
        return "RFormulaModel(%s) (uid=%s)" % (resolvedFormula, self.uid)


class _SelectorParams(HasFeaturesCol, HasOutputCol, HasLabelCol):
    """
    Params for :py:class:`Selector` and :py:class:`SelectorModel`.

    .. versionadded:: 3.1.0
    """

    selectorType: Param[str] = Param(
        Params._dummy(),
        "selectorType",
        "The selector type. "
        + "Supported options: numTopFeatures (default), percentile, fpr, fdr, fwe.",
        typeConverter=TypeConverters.toString,
    )

    numTopFeatures: Param[int] = Param(
        Params._dummy(),
        "numTopFeatures",
        "Number of features that selector will select, ordered by ascending p-value. "
        + "If the number of features is < numTopFeatures, then this will select "
        + "all features.",
        typeConverter=TypeConverters.toInt,
    )

    percentile: Param[float] = Param(
        Params._dummy(),
        "percentile",
        "Percentile of features that selector " + "will select, ordered by ascending p-value.",
        typeConverter=TypeConverters.toFloat,
    )

    fpr: Param[float] = Param(
        Params._dummy(),
        "fpr",
        "The highest p-value for features to be kept.",
        typeConverter=TypeConverters.toFloat,
    )

    fdr: Param[float] = Param(
        Params._dummy(),
        "fdr",
        "The upper bound of the expected false discovery rate.",
        typeConverter=TypeConverters.toFloat,
    )

    fwe: Param[float] = Param(
        Params._dummy(),
        "fwe",
        "The upper bound of the expected family-wise error rate.",
        typeConverter=TypeConverters.toFloat,
    )

    def __init__(self, *args: Any):
        super(_SelectorParams, self).__init__(*args)
        self._setDefault(
            numTopFeatures=50,
            selectorType="numTopFeatures",
            percentile=0.1,
            fpr=0.05,
            fdr=0.05,
            fwe=0.05,
        )

    @since("2.1.0")
    def getSelectorType(self) -> str:
        """
        Gets the value of selectorType or its default value.
        """
        return self.getOrDefault(self.selectorType)

    @since("2.0.0")
    def getNumTopFeatures(self) -> int:
        """
        Gets the value of numTopFeatures or its default value.
        """
        return self.getOrDefault(self.numTopFeatures)

    @since("2.1.0")
    def getPercentile(self) -> float:
        """
        Gets the value of percentile or its default value.
        """
        return self.getOrDefault(self.percentile)

    @since("2.1.0")
    def getFpr(self) -> float:
        """
        Gets the value of fpr or its default value.
        """
        return self.getOrDefault(self.fpr)

    @since("2.2.0")
    def getFdr(self) -> float:
        """
        Gets the value of fdr or its default value.
        """
        return self.getOrDefault(self.fdr)

    @since("2.2.0")
    def getFwe(self) -> float:
        """
        Gets the value of fwe or its default value.
        """
        return self.getOrDefault(self.fwe)


class _Selector(JavaEstimator[JM], _SelectorParams, JavaMLReadable, JavaMLWritable, Generic[JM]):
    """
    Mixin for Selectors.
    """

    @since("2.1.0")
    def setSelectorType(self: P, value: str) -> P:
        """
        Sets the value of :py:attr:`selectorType`.
        """
        return self._set(selectorType=value)

    @since("2.0.0")
    def setNumTopFeatures(self: P, value: int) -> P:
        """
        Sets the value of :py:attr:`numTopFeatures`.
        Only applicable when selectorType = "numTopFeatures".
        """
        return self._set(numTopFeatures=value)

    @since("2.1.0")
    def setPercentile(self: P, value: float) -> P:
        """
        Sets the value of :py:attr:`percentile`.
        Only applicable when selectorType = "percentile".
        """
        return self._set(percentile=value)

    @since("2.1.0")
    def setFpr(self: P, value: float) -> P:
        """
        Sets the value of :py:attr:`fpr`.
        Only applicable when selectorType = "fpr".
        """
        return self._set(fpr=value)

    @since("2.2.0")
    def setFdr(self: P, value: float) -> P:
        """
        Sets the value of :py:attr:`fdr`.
        Only applicable when selectorType = "fdr".
        """
        return self._set(fdr=value)

    @since("2.2.0")
    def setFwe(self: P, value: float) -> P:
        """
        Sets the value of :py:attr:`fwe`.
        Only applicable when selectorType = "fwe".
        """
        return self._set(fwe=value)

    def setFeaturesCol(self: P, value: str) -> P:
        """
        Sets the value of :py:attr:`featuresCol`.
        """
        return self._set(featuresCol=value)

    def setOutputCol(self: P, value: str) -> P:
        """
        Sets the value of :py:attr:`outputCol`.
        """
        return self._set(outputCol=value)

    def setLabelCol(self: P, value: str) -> P:
        """
        Sets the value of :py:attr:`labelCol`.
        """
        return self._set(labelCol=value)


class _SelectorModel(JavaModel, _SelectorParams):
    """
    Mixin for Selector models.
    """

    @since("3.0.0")
    def setFeaturesCol(self: P, value: str) -> P:
        """
        Sets the value of :py:attr:`featuresCol`.
        """
        return self._set(featuresCol=value)

    @since("3.0.0")
    def setOutputCol(self: P, value: str) -> P:
        """
        Sets the value of :py:attr:`outputCol`.
        """
        return self._set(outputCol=value)

    @property  # type: ignore[misc]
    @since("2.0.0")
    def selectedFeatures(self) -> List[int]:
        """
        List of indices to select (filter).
        """
        return self._call_java("selectedFeatures")


@inherit_doc
class ChiSqSelector(
    _Selector["ChiSqSelectorModel"],
    JavaMLReadable["ChiSqSelector"],
    JavaMLWritable,
):
    """
    Chi-Squared feature selection, which selects categorical features to use for predicting a
    categorical label.
    The selector supports different selection methods: `numTopFeatures`, `percentile`, `fpr`,
    `fdr`, `fwe`.

     * `numTopFeatures` chooses a fixed number of top features according to a chi-squared test.

     * `percentile` is similar but chooses a fraction of all features
       instead of a fixed number.

     * `fpr` chooses all features whose p-values are below a threshold,
       thus controlling the false positive rate of selection.

     * `fdr` uses the `Benjamini-Hochberg procedure <https://en.wikipedia.org/wiki/
       False_discovery_rate#Benjamini.E2.80.93Hochberg_procedure>`_
       to choose all features whose false discovery rate is below a threshold.

     * `fwe` chooses all features whose p-values are below a threshold. The threshold is scaled by
       1/numFeatures, thus controlling the family-wise error rate of selection.

    By default, the selection method is `numTopFeatures`, with the default number of top features
    set to 50.

    .. deprecated:: 3.1.0
        Use UnivariateFeatureSelector

    .. versionadded:: 2.0.0

    Examples
    --------
    >>> from pyspark.ml.linalg import Vectors
    >>> df = spark.createDataFrame(
    ...    [(Vectors.dense([0.0, 0.0, 18.0, 1.0]), 1.0),
    ...     (Vectors.dense([0.0, 1.0, 12.0, 0.0]), 0.0),
    ...     (Vectors.dense([1.0, 0.0, 15.0, 0.1]), 0.0)],
    ...    ["features", "label"])
    >>> selector = ChiSqSelector(numTopFeatures=1, outputCol="selectedFeatures")
    >>> model = selector.fit(df)
    >>> model.getFeaturesCol()
    'features'
    >>> model.setFeaturesCol("features")
    ChiSqSelectorModel...
    >>> model.transform(df).head().selectedFeatures
    DenseVector([18.0])
    >>> model.selectedFeatures
    [2]
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
    >>> loadedModel.transform(df).take(1) == model.transform(df).take(1)
    True
    """

    _input_kwargs: Dict[str, Any]

    @keyword_only
    def __init__(
        self,
        *,
        numTopFeatures: int = 50,
        featuresCol: str = "features",
        outputCol: Optional[str] = None,
        labelCol: str = "label",
        selectorType: str = "numTopFeatures",
        percentile: float = 0.1,
        fpr: float = 0.05,
        fdr: float = 0.05,
        fwe: float = 0.05,
    ):
        """
        __init__(self, \\*, numTopFeatures=50, featuresCol="features", outputCol=None, \
                 labelCol="label", selectorType="numTopFeatures", percentile=0.1, fpr=0.05, \
                 fdr=0.05, fwe=0.05)
        """
        super(ChiSqSelector, self).__init__()
        self._java_obj = self._new_java_obj("org.apache.spark.ml.feature.ChiSqSelector", self.uid)
        kwargs = self._input_kwargs
        self.setParams(**kwargs)

    @keyword_only
    @since("2.0.0")
    def setParams(
        self,
        *,
        numTopFeatures: int = 50,
        featuresCol: str = "features",
        outputCol: Optional[str] = None,
        labelCol: str = "label",
        selectorType: str = "numTopFeatures",
        percentile: float = 0.1,
        fpr: float = 0.05,
        fdr: float = 0.05,
        fwe: float = 0.05,
    ) -> "ChiSqSelector":
        """
        setParams(self, \\*, numTopFeatures=50, featuresCol="features", outputCol=None, \
                  labelCol="label", selectorType="numTopFeatures", percentile=0.1, fpr=0.05, \
                  fdr=0.05, fwe=0.05)
        Sets params for this ChiSqSelector.
        """
        kwargs = self._input_kwargs
        return self._set(**kwargs)

    def _create_model(self, java_model: "JavaObject") -> "ChiSqSelectorModel":
        return ChiSqSelectorModel(java_model)


class ChiSqSelectorModel(_SelectorModel, JavaMLReadable["ChiSqSelectorModel"], JavaMLWritable):
    """
    Model fitted by :py:class:`ChiSqSelector`.

    .. versionadded:: 2.0.0
    """


@inherit_doc
class VectorSizeHint(
    JavaTransformer,
    HasInputCol,
    HasHandleInvalid,
    JavaMLReadable["VectorSizeHint"],
    JavaMLWritable,
):
    """
    A feature transformer that adds size information to the metadata of a vector column.
    VectorAssembler needs size information for its input columns and cannot be used on streaming
    dataframes without this metadata.

    .. versionadded:: 2.3.0

    Notes
    -----
    VectorSizeHint modifies `inputCol` to include size metadata and does not have an outputCol.

    Examples
    --------
    >>> from pyspark.ml.linalg import Vectors
    >>> from pyspark.ml import Pipeline, PipelineModel
    >>> data = [(Vectors.dense([1., 2., 3.]), 4.)]
    >>> df = spark.createDataFrame(data, ["vector", "float"])
    >>>
    >>> sizeHint = VectorSizeHint(inputCol="vector", size=3, handleInvalid="skip")
    >>> vecAssembler = VectorAssembler(inputCols=["vector", "float"], outputCol="assembled")
    >>> pipeline = Pipeline(stages=[sizeHint, vecAssembler])
    >>>
    >>> pipelineModel = pipeline.fit(df)
    >>> pipelineModel.transform(df).head().assembled
    DenseVector([1.0, 2.0, 3.0, 4.0])
    >>> vectorSizeHintPath = temp_path + "/vector-size-hint-pipeline"
    >>> pipelineModel.save(vectorSizeHintPath)
    >>> loadedPipeline = PipelineModel.load(vectorSizeHintPath)
    >>> loaded = loadedPipeline.transform(df).head().assembled
    >>> expected = pipelineModel.transform(df).head().assembled
    >>> loaded == expected
    True
    """

    _input_kwargs: Dict[str, Any]

    size: Param[int] = Param(
        Params._dummy(), "size", "Size of vectors in column.", typeConverter=TypeConverters.toInt
    )

    handleInvalid: Param[str] = Param(
        Params._dummy(),
        "handleInvalid",
        "How to handle invalid vectors in inputCol. Invalid vectors include "
        "nulls and vectors with the wrong size. The options are `skip` (filter "
        "out rows with invalid vectors), `error` (throw an error) and "
        "`optimistic` (do not check the vector size, and keep all rows). "
        "`error` by default.",
        TypeConverters.toString,
    )

    @keyword_only
    def __init__(
        self,
        *,
        inputCol: Optional[str] = None,
        size: Optional[int] = None,
        handleInvalid: str = "error",
    ):
        """
        __init__(self, \\*, inputCol=None, size=None, handleInvalid="error")
        """
        super(VectorSizeHint, self).__init__()
        self._java_obj = self._new_java_obj("org.apache.spark.ml.feature.VectorSizeHint", self.uid)
        self._setDefault(handleInvalid="error")
        self.setParams(**self._input_kwargs)

    @keyword_only
    @since("2.3.0")
    def setParams(
        self,
        *,
        inputCol: Optional[str] = None,
        size: Optional[str] = None,
        handleInvalid: str = "error",
    ) -> "VectorSizeHint":
        """
        setParams(self, \\*, inputCol=None, size=None, handleInvalid="error")
        Sets params for this VectorSizeHint.
        """
        kwargs = self._input_kwargs
        return self._set(**kwargs)

    @since("2.3.0")
    def getSize(self) -> int:
        """Gets size param, the size of vectors in `inputCol`."""
        return self.getOrDefault(self.size)

    @since("2.3.0")
    def setSize(self, value: int) -> "VectorSizeHint":
        """Sets size param, the size of vectors in `inputCol`."""
        return self._set(size=value)

    def setInputCol(self, value: str) -> "VectorSizeHint":
        """
        Sets the value of :py:attr:`inputCol`.
        """
        return self._set(inputCol=value)

    def setHandleInvalid(self, value: str) -> "VectorSizeHint":
        """
        Sets the value of :py:attr:`handleInvalid`.
        """
        return self._set(handleInvalid=value)


class _VarianceThresholdSelectorParams(HasFeaturesCol, HasOutputCol):
    """
    Params for :py:class:`VarianceThresholdSelector` and
    :py:class:`VarianceThresholdSelectorModel`.

    .. versionadded:: 3.1.0
    """

    varianceThreshold: Param[float] = Param(
        Params._dummy(),
        "varianceThreshold",
        "Param for variance threshold. Features with a variance not "
        + "greater than this threshold will be removed. The default value "
        + "is 0.0.",
        typeConverter=TypeConverters.toFloat,
    )

    @since("3.1.0")
    def getVarianceThreshold(self) -> float:
        """
        Gets the value of varianceThreshold or its default value.
        """
        return self.getOrDefault(self.varianceThreshold)


@inherit_doc
class VarianceThresholdSelector(
    JavaEstimator["VarianceThresholdSelectorModel"],
    _VarianceThresholdSelectorParams,
    JavaMLReadable["VarianceThresholdSelector"],
    JavaMLWritable,
):
    """
    Feature selector that removes all low-variance features. Features with a
    variance not greater than the threshold will be removed. The default is to keep
    all features with non-zero variance, i.e. remove the features that have the
    same value in all samples.

    .. versionadded:: 3.1.0

    Examples
    --------
    >>> from pyspark.ml.linalg import Vectors
    >>> df = spark.createDataFrame(
    ...    [(Vectors.dense([6.0, 7.0, 0.0, 7.0, 6.0, 0.0]),),
    ...     (Vectors.dense([0.0, 9.0, 6.0, 0.0, 5.0, 9.0]),),
    ...     (Vectors.dense([0.0, 9.0, 3.0, 0.0, 5.0, 5.0]),),
    ...     (Vectors.dense([0.0, 9.0, 8.0, 5.0, 6.0, 4.0]),),
    ...     (Vectors.dense([8.0, 9.0, 6.0, 5.0, 4.0, 4.0]),),
    ...     (Vectors.dense([8.0, 9.0, 6.0, 0.0, 0.0, 0.0]),)],
    ...    ["features"])
    >>> selector = VarianceThresholdSelector(varianceThreshold=8.2, outputCol="selectedFeatures")
    >>> model = selector.fit(df)
    >>> model.getFeaturesCol()
    'features'
    >>> model.setFeaturesCol("features")
    VarianceThresholdSelectorModel...
    >>> model.transform(df).head().selectedFeatures
    DenseVector([6.0, 7.0, 0.0])
    >>> model.selectedFeatures
    [0, 3, 5]
    >>> varianceThresholdSelectorPath = temp_path + "/variance-threshold-selector"
    >>> selector.save(varianceThresholdSelectorPath)
    >>> loadedSelector = VarianceThresholdSelector.load(varianceThresholdSelectorPath)
    >>> loadedSelector.getVarianceThreshold() == selector.getVarianceThreshold()
    True
    >>> modelPath = temp_path + "/variance-threshold-selector-model"
    >>> model.save(modelPath)
    >>> loadedModel = VarianceThresholdSelectorModel.load(modelPath)
    >>> loadedModel.selectedFeatures == model.selectedFeatures
    True
    >>> loadedModel.transform(df).take(1) == model.transform(df).take(1)
    True
    """

    _input_kwargs: Dict[str, Any]

    @keyword_only
    def __init__(
        self,
        *,
        featuresCol: str = "features",
        outputCol: Optional[str] = None,
        varianceThreshold: float = 0.0,
    ):
        """
        __init__(self, \\*, featuresCol="features", outputCol=None, varianceThreshold=0.0)
        """
        super(VarianceThresholdSelector, self).__init__()
        self._java_obj = self._new_java_obj(
            "org.apache.spark.ml.feature.VarianceThresholdSelector", self.uid
        )
        self._setDefault(varianceThreshold=0.0)
        kwargs = self._input_kwargs
        self.setParams(**kwargs)

    @keyword_only
    @since("3.1.0")
    def setParams(
        self,
        *,
        featuresCol: str = "features",
        outputCol: Optional[str] = None,
        varianceThreshold: float = 0.0,
    ) -> "VarianceThresholdSelector":
        """
        setParams(self, \\*, featuresCol="features", outputCol=None, varianceThreshold=0.0)
        Sets params for this VarianceThresholdSelector.
        """
        kwargs = self._input_kwargs
        return self._set(**kwargs)

    @since("3.1.0")
    def setVarianceThreshold(self, value: float) -> "VarianceThresholdSelector":
        """
        Sets the value of :py:attr:`varianceThreshold`.
        """
        return self._set(varianceThreshold=value)

    @since("3.1.0")
    def setFeaturesCol(self, value: str) -> "VarianceThresholdSelector":
        """
        Sets the value of :py:attr:`featuresCol`.
        """
        return self._set(featuresCol=value)

    @since("3.1.0")
    def setOutputCol(self, value: str) -> "VarianceThresholdSelector":
        """
        Sets the value of :py:attr:`outputCol`.
        """
        return self._set(outputCol=value)

    def _create_model(self, java_model: "JavaObject") -> "VarianceThresholdSelectorModel":
        return VarianceThresholdSelectorModel(java_model)


class VarianceThresholdSelectorModel(
    JavaModel,
    _VarianceThresholdSelectorParams,
    JavaMLReadable["VarianceThresholdSelectorModel"],
    JavaMLWritable,
):
    """
    Model fitted by :py:class:`VarianceThresholdSelector`.

    .. versionadded:: 3.1.0
    """

    @since("3.1.0")
    def setFeaturesCol(self, value: str) -> "VarianceThresholdSelectorModel":
        """
        Sets the value of :py:attr:`featuresCol`.
        """
        return self._set(featuresCol=value)

    @since("3.1.0")
    def setOutputCol(self, value: str) -> "VarianceThresholdSelectorModel":
        """
        Sets the value of :py:attr:`outputCol`.
        """
        return self._set(outputCol=value)

    @property  # type: ignore[misc]
    @since("3.1.0")
    def selectedFeatures(self) -> List[int]:
        """
        List of indices to select (filter).
        """
        return self._call_java("selectedFeatures")


class _UnivariateFeatureSelectorParams(HasFeaturesCol, HasOutputCol, HasLabelCol):
    """
    Params for :py:class:`UnivariateFeatureSelector` and
    :py:class:`UnivariateFeatureSelectorModel`.

    .. versionadded:: 3.1.0
    """

    featureType: Param[str] = Param(
        Params._dummy(),
        "featureType",
        "The feature type. " + "Supported options: categorical, continuous.",
        typeConverter=TypeConverters.toString,
    )

    labelType: Param[str] = Param(
        Params._dummy(),
        "labelType",
        "The label type. " + "Supported options: categorical, continuous.",
        typeConverter=TypeConverters.toString,
    )

    selectionMode: Param[str] = Param(
        Params._dummy(),
        "selectionMode",
        "The selection mode. "
        + "Supported options: numTopFeatures (default), percentile, fpr, "
        + "fdr, fwe.",
        typeConverter=TypeConverters.toString,
    )

    selectionThreshold: Param[float] = Param(
        Params._dummy(),
        "selectionThreshold",
        "The upper bound of the " + "features that selector will select.",
        typeConverter=TypeConverters.toFloat,
    )

    def __init__(self, *args: Any):
        super(_UnivariateFeatureSelectorParams, self).__init__(*args)
        self._setDefault(selectionMode="numTopFeatures")

    @since("3.1.1")
    def getFeatureType(self) -> str:
        """
        Gets the value of featureType or its default value.
        """
        return self.getOrDefault(self.featureType)

    @since("3.1.1")
    def getLabelType(self) -> str:
        """
        Gets the value of labelType or its default value.
        """
        return self.getOrDefault(self.labelType)

    @since("3.1.1")
    def getSelectionMode(self) -> str:
        """
        Gets the value of selectionMode or its default value.
        """
        return self.getOrDefault(self.selectionMode)

    @since("3.1.1")
    def getSelectionThreshold(self) -> float:
        """
        Gets the value of selectionThreshold or its default value.
        """
        return self.getOrDefault(self.selectionThreshold)


@inherit_doc
class UnivariateFeatureSelector(
    JavaEstimator["UnivariateFeatureSelectorModel"],
    _UnivariateFeatureSelectorParams,
    JavaMLReadable["UnivariateFeatureSelector"],
    JavaMLWritable,
):
    """
    UnivariateFeatureSelector
    Feature selector based on univariate statistical tests against labels. Currently, Spark
    supports three Univariate Feature Selectors: chi-squared, ANOVA F-test and F-value.
    User can choose Univariate Feature Selector by setting `featureType` and `labelType`,
    and Spark will pick the score function based on the specified `featureType` and `labelType`.

    The following combination of `featureType` and `labelType` are supported:

    - `featureType` `categorical` and `labelType` `categorical`, Spark uses chi-squared,
      i.e. chi2 in sklearn.
    - `featureType` `continuous` and `labelType` `categorical`, Spark uses ANOVA F-test,
      i.e. f_classif in sklearn.
    - `featureType` `continuous` and `labelType` `continuous`, Spark uses F-value,
      i.e. f_regression in sklearn.

    The `UnivariateFeatureSelector` supports different selection modes: `numTopFeatures`,
    `percentile`, `fpr`, `fdr`, `fwe`.

    - `numTopFeatures` chooses a fixed number of top features according to a according to a
      hypothesis.
    - `percentile` is similar but chooses a fraction of all features
      instead of a fixed number.
    - `fpr` chooses all features whose p-values are below a threshold,
      thus controlling the false positive rate of selection.
    - `fdr` uses the `Benjamini-Hochberg procedure \
      <https://en.wikipedia.org/wiki/False_discovery_rate#Benjamini.E2.80.93Hochberg_procedure>`_
      to choose all features whose false discovery rate is below a threshold.
    - `fwe` chooses all features whose p-values are below a threshold. The threshold is scaled by
      1 / `numFeatures`, thus controlling the family-wise error rate of selection.

    By default, the selection mode is `numTopFeatures`.

    .. versionadded:: 3.1.1

    Examples
    --------
    >>> from pyspark.ml.linalg import Vectors
    >>> df = spark.createDataFrame(
    ...    [(Vectors.dense([1.7, 4.4, 7.6, 5.8, 9.6, 2.3]), 3.0),
    ...     (Vectors.dense([8.8, 7.3, 5.7, 7.3, 2.2, 4.1]), 2.0),
    ...     (Vectors.dense([1.2, 9.5, 2.5, 3.1, 8.7, 2.5]), 1.0),
    ...     (Vectors.dense([3.7, 9.2, 6.1, 4.1, 7.5, 3.8]), 2.0),
    ...     (Vectors.dense([8.9, 5.2, 7.8, 8.3, 5.2, 3.0]), 4.0),
    ...     (Vectors.dense([7.9, 8.5, 9.2, 4.0, 9.4, 2.1]), 4.0)],
    ...    ["features", "label"])
    >>> selector = UnivariateFeatureSelector(outputCol="selectedFeatures")
    >>> selector.setFeatureType("continuous").setLabelType("categorical").setSelectionThreshold(1)
    UnivariateFeatureSelector...
    >>> model = selector.fit(df)
    >>> model.getFeaturesCol()
    'features'
    >>> model.setFeaturesCol("features")
    UnivariateFeatureSelectorModel...
    >>> model.transform(df).head().selectedFeatures
    DenseVector([7.6])
    >>> model.selectedFeatures
    [2]
    >>> selectorPath = temp_path + "/selector"
    >>> selector.save(selectorPath)
    >>> loadedSelector = UnivariateFeatureSelector.load(selectorPath)
    >>> loadedSelector.getSelectionThreshold() == selector.getSelectionThreshold()
    True
    >>> modelPath = temp_path + "/selector-model"
    >>> model.save(modelPath)
    >>> loadedModel = UnivariateFeatureSelectorModel.load(modelPath)
    >>> loadedModel.selectedFeatures == model.selectedFeatures
    True
    >>> loadedModel.transform(df).take(1) == model.transform(df).take(1)
    True
    """

    _input_kwargs: Dict[str, Any]

    @keyword_only
    def __init__(
        self,
        *,
        featuresCol: str = "features",
        outputCol: Optional[str] = None,
        labelCol: str = "label",
        selectionMode: str = "numTopFeatures",
    ):
        """
        __init__(self, \\*, featuresCol="features", outputCol=None, \
                 labelCol="label", selectionMode="numTopFeatures")
        """
        super(UnivariateFeatureSelector, self).__init__()
        self._java_obj = self._new_java_obj(
            "org.apache.spark.ml.feature.UnivariateFeatureSelector", self.uid
        )
        kwargs = self._input_kwargs
        self.setParams(**kwargs)

    @keyword_only
    @since("3.1.1")
    def setParams(
        self,
        *,
        featuresCol: str = "features",
        outputCol: Optional[str] = None,
        labelCol: str = "label",
        selectionMode: str = "numTopFeatures",
    ) -> "UnivariateFeatureSelector":
        """
        setParams(self, \\*, featuresCol="features", outputCol=None, \
                  labelCol="label", selectionMode="numTopFeatures")
        Sets params for this UnivariateFeatureSelector.
        """
        kwargs = self._input_kwargs
        return self._set(**kwargs)

    @since("3.1.1")
    def setFeatureType(self, value: str) -> "UnivariateFeatureSelector":
        """
        Sets the value of :py:attr:`featureType`.
        """
        return self._set(featureType=value)

    @since("3.1.1")
    def setLabelType(self, value: str) -> "UnivariateFeatureSelector":
        """
        Sets the value of :py:attr:`labelType`.
        """
        return self._set(labelType=value)

    @since("3.1.1")
    def setSelectionMode(self, value: str) -> "UnivariateFeatureSelector":
        """
        Sets the value of :py:attr:`selectionMode`.
        """
        return self._set(selectionMode=value)

    @since("3.1.1")
    def setSelectionThreshold(self, value: float) -> "UnivariateFeatureSelector":
        """
        Sets the value of :py:attr:`selectionThreshold`.
        """
        return self._set(selectionThreshold=value)

    def setFeaturesCol(self, value: str) -> "UnivariateFeatureSelector":
        """
        Sets the value of :py:attr:`featuresCol`.
        """
        return self._set(featuresCol=value)

    def setOutputCol(self, value: str) -> "UnivariateFeatureSelector":
        """
        Sets the value of :py:attr:`outputCol`.
        """
        return self._set(outputCol=value)

    def setLabelCol(self, value: str) -> "UnivariateFeatureSelector":
        """
        Sets the value of :py:attr:`labelCol`.
        """
        return self._set(labelCol=value)

    def _create_model(self, java_model: "JavaObject") -> "UnivariateFeatureSelectorModel":
        return UnivariateFeatureSelectorModel(java_model)


class UnivariateFeatureSelectorModel(
    JavaModel,
    _UnivariateFeatureSelectorParams,
    JavaMLReadable["UnivariateFeatureSelectorModel"],
    JavaMLWritable,
):
    """
    Model fitted by :py:class:`UnivariateFeatureSelector`.

    .. versionadded:: 3.1.1
    """

    @since("3.1.1")
    def setFeaturesCol(self, value: str) -> "UnivariateFeatureSelectorModel":
        """
        Sets the value of :py:attr:`featuresCol`.
        """
        return self._set(featuresCol=value)

    @since("3.1.1")
    def setOutputCol(self, value: str) -> "UnivariateFeatureSelectorModel":
        """
        Sets the value of :py:attr:`outputCol`.
        """
        return self._set(outputCol=value)

    @property  # type: ignore[misc]
    @since("3.1.1")
    def selectedFeatures(self) -> List[int]:
        """
        List of indices to select (filter).
        """
        return self._call_java("selectedFeatures")


if __name__ == "__main__":
    import doctest
    import sys
    import tempfile

    import pyspark.ml.feature
    from pyspark.sql import Row, SparkSession

    globs = globals().copy()
    features = pyspark.ml.feature.__dict__.copy()
    globs.update(features)

    # The small batch size here ensures that we see multiple batches,
    # even in these small test examples:
    spark = SparkSession.builder.master("local[2]").appName("ml.feature tests").getOrCreate()
    sc = spark.sparkContext
    globs["sc"] = sc
    globs["spark"] = spark
    testData = sc.parallelize(
        [
            Row(id=0, label="a"),
            Row(id=1, label="b"),
            Row(id=2, label="c"),
            Row(id=3, label="a"),
            Row(id=4, label="a"),
            Row(id=5, label="c"),
        ],
        2,
    )
    globs["stringIndDf"] = spark.createDataFrame(testData)
    temp_path = tempfile.mkdtemp()
    globs["temp_path"] = temp_path
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
        sys.exit(-1)
