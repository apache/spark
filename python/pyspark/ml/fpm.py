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
from typing import Any, Dict, Optional, TYPE_CHECKING

from pyspark import keyword_only, since
from pyspark.sql import DataFrame
from pyspark.ml.util import JavaMLWritable, JavaMLReadable, try_remote_attribute_relation
from pyspark.ml.wrapper import JavaEstimator, JavaModel, JavaParams
from pyspark.ml.param.shared import HasPredictionCol, Param, TypeConverters, Params

if TYPE_CHECKING:
    from py4j.java_gateway import JavaObject

__all__ = ["FPGrowth", "FPGrowthModel", "PrefixSpan"]


class _FPGrowthParams(HasPredictionCol):
    """
    Params for :py:class:`FPGrowth` and :py:class:`FPGrowthModel`.

    .. versionadded:: 3.0.0
    """

    itemsCol: Param[str] = Param(
        Params._dummy(), "itemsCol", "items column name", typeConverter=TypeConverters.toString
    )
    minSupport: Param[float] = Param(
        Params._dummy(),
        "minSupport",
        "Minimal support level of the frequent pattern. [0.0, 1.0]. "
        + "Any pattern that appears more than (minSupport * size-of-the-dataset) "
        + "times will be output in the frequent itemsets.",
        typeConverter=TypeConverters.toFloat,
    )
    numPartitions: Param[int] = Param(
        Params._dummy(),
        "numPartitions",
        "Number of partitions (at least 1) used by parallel FP-growth. "
        + "By default the param is not set, "
        + "and partition number of the input dataset is used.",
        typeConverter=TypeConverters.toInt,
    )
    minConfidence: Param[float] = Param(
        Params._dummy(),
        "minConfidence",
        "Minimal confidence for generating Association Rule. [0.0, 1.0]. "
        + "minConfidence will not affect the mining for frequent itemsets, "
        + "but will affect the association rules generation.",
        typeConverter=TypeConverters.toFloat,
    )

    def __init__(self, *args: Any):
        super(_FPGrowthParams, self).__init__(*args)
        self._setDefault(
            minSupport=0.3, minConfidence=0.8, itemsCol="items", predictionCol="prediction"
        )

    def getItemsCol(self) -> str:
        """
        Gets the value of itemsCol or its default value.
        """
        return self.getOrDefault(self.itemsCol)

    def getMinSupport(self) -> float:
        """
        Gets the value of minSupport or its default value.
        """
        return self.getOrDefault(self.minSupport)

    def getNumPartitions(self) -> int:
        """
        Gets the value of :py:attr:`numPartitions` or its default value.
        """
        return self.getOrDefault(self.numPartitions)

    def getMinConfidence(self) -> float:
        """
        Gets the value of minConfidence or its default value.
        """
        return self.getOrDefault(self.minConfidence)


class FPGrowthModel(JavaModel, _FPGrowthParams, JavaMLWritable, JavaMLReadable["FPGrowthModel"]):
    """
    Model fitted by FPGrowth.

    .. versionadded:: 2.2.0
    """

    @since("3.0.0")
    def setItemsCol(self, value: str) -> "FPGrowthModel":
        """
        Sets the value of :py:attr:`itemsCol`.
        """
        return self._set(itemsCol=value)

    @since("3.0.0")
    def setMinConfidence(self, value: float) -> "FPGrowthModel":
        """
        Sets the value of :py:attr:`minConfidence`.
        """
        return self._set(minConfidence=value)

    @since("3.0.0")
    def setPredictionCol(self, value: str) -> "FPGrowthModel":
        """
        Sets the value of :py:attr:`predictionCol`.
        """
        return self._set(predictionCol=value)

    @property
    @since("2.2.0")
    @try_remote_attribute_relation
    def freqItemsets(self) -> DataFrame:
        """
        DataFrame with two columns:
        * `items` - Itemset of the same type as the input column.
        * `freq`  - Frequency of the itemset (`LongType`).
        """
        return self._call_java("freqItemsets")

    @property
    @since("2.2.0")
    @try_remote_attribute_relation
    def associationRules(self) -> DataFrame:
        """
        DataFrame with four columns:
        * `antecedent`  - Array of the same type as the input column.
        * `consequent`  - Array of the same type as the input column.
        * `confidence`  - Confidence for the rule (`DoubleType`).
        * `lift`        - Lift for the rule (`DoubleType`).
        """
        return self._call_java("associationRules")


class FPGrowth(
    JavaEstimator[FPGrowthModel], _FPGrowthParams, JavaMLWritable, JavaMLReadable["FPGrowth"]
):
    r"""
    A parallel FP-growth algorithm to mine frequent itemsets.

    .. versionadded:: 2.2.0

    Notes
    -----

    The algorithm is described in
    Li et al., PFP: Parallel FP-Growth for Query Recommendation [1]_.
    PFP distributes computation in such a way that each worker executes an
    independent group of mining tasks. The FP-Growth algorithm is described in
    Han et al., Mining frequent patterns without candidate generation [2]_

    NULL values in the feature column are ignored during `fit()`.

    Internally `transform` `collects` and `broadcasts` association rules.


    .. [1] Haoyuan Li, Yi Wang, Dong Zhang, Ming Zhang, and Edward Y. Chang. 2008.
        Pfp: parallel fp-growth for query recommendation.
        In Proceedings of the 2008 ACM conference on Recommender systems (RecSys '08).
        Association for Computing Machinery, New York, NY, USA, 107-114.
        DOI: https://doi.org/10.1145/1454008.1454027
    .. [2] Jiawei Han, Jian Pei, and Yiwen Yin. 2000.
        Mining frequent patterns without candidate generation.
        SIGMOD Rec. 29, 2 (June 2000), 1-12.
        DOI: https://doi.org/10.1145/335191.335372


    Examples
    --------
    >>> from pyspark.sql.functions import split
    >>> data = (spark.read
    ...     .text("data/mllib/sample_fpgrowth.txt")
    ...     .select(split("value", "\s+").alias("items")))
    >>> data.show(truncate=False)
    +------------------------+
    |items                   |
    +------------------------+
    |[r, z, h, k, p]         |
    |[z, y, x, w, v, u, t, s]|
    |[s, x, o, n, r]         |
    |[x, z, y, m, t, s, q, e]|
    |[z]                     |
    |[x, z, y, r, q, t, p]   |
    +------------------------+
    ...
    >>> fp = FPGrowth(minSupport=0.2, minConfidence=0.7)
    >>> fpm = fp.fit(data)
    >>> fpm.setPredictionCol("newPrediction")
    FPGrowthModel...
    >>> fpm.freqItemsets.sort("items").show(5)
    +---------+----+
    |    items|freq|
    +---------+----+
    |      [p]|   2|
    |   [p, r]|   2|
    |[p, r, z]|   2|
    |   [p, z]|   2|
    |      [q]|   2|
    +---------+----+
    only showing top 5 rows
    >>> fpm.associationRules.sort("antecedent", "consequent").show(5)
    +----------+----------+----------+----+------------------+
    |antecedent|consequent|confidence|lift|           support|
    +----------+----------+----------+----+------------------+
    |       [p]|       [r]|       1.0| 2.0|0.3333333333333333|
    |       [p]|       [z]|       1.0| 1.2|0.3333333333333333|
    |    [p, r]|       [z]|       1.0| 1.2|0.3333333333333333|
    |    [p, z]|       [r]|       1.0| 2.0|0.3333333333333333|
    |       [q]|       [t]|       1.0| 2.0|0.3333333333333333|
    +----------+----------+----------+----+------------------+
    only showing top 5 rows
    >>> new_data = spark.createDataFrame([(["t", "s"], )], ["items"])
    >>> sorted(fpm.transform(new_data).first().newPrediction)
    ['x', 'y', 'z']
    >>> model_path = temp_path + "/fpm_model"
    >>> fpm.save(model_path)
    >>> model2 = FPGrowthModel.load(model_path)
    >>> fpm.transform(data).take(1) == model2.transform(data).take(1)
    True
    """
    _input_kwargs: Dict[str, Any]

    @keyword_only
    def __init__(
        self,
        *,
        minSupport: float = 0.3,
        minConfidence: float = 0.8,
        itemsCol: str = "items",
        predictionCol: str = "prediction",
        numPartitions: Optional[int] = None,
    ):
        """
        __init__(self, \\*, minSupport=0.3, minConfidence=0.8, itemsCol="items", \
                 predictionCol="prediction", numPartitions=None)
        """
        super(FPGrowth, self).__init__()
        self._java_obj = self._new_java_obj("org.apache.spark.ml.fpm.FPGrowth", self.uid)
        kwargs = self._input_kwargs
        self.setParams(**kwargs)

    @keyword_only
    @since("2.2.0")
    def setParams(
        self,
        *,
        minSupport: float = 0.3,
        minConfidence: float = 0.8,
        itemsCol: str = "items",
        predictionCol: str = "prediction",
        numPartitions: Optional[int] = None,
    ) -> "FPGrowth":
        """
        setParams(self, \\*, minSupport=0.3, minConfidence=0.8, itemsCol="items", \
                  predictionCol="prediction", numPartitions=None)
        """
        kwargs = self._input_kwargs
        return self._set(**kwargs)

    def setItemsCol(self, value: str) -> "FPGrowth":
        """
        Sets the value of :py:attr:`itemsCol`.
        """
        return self._set(itemsCol=value)

    def setMinSupport(self, value: float) -> "FPGrowth":
        """
        Sets the value of :py:attr:`minSupport`.
        """
        return self._set(minSupport=value)

    def setNumPartitions(self, value: int) -> "FPGrowth":
        """
        Sets the value of :py:attr:`numPartitions`.
        """
        return self._set(numPartitions=value)

    def setMinConfidence(self, value: float) -> "FPGrowth":
        """
        Sets the value of :py:attr:`minConfidence`.
        """
        return self._set(minConfidence=value)

    def setPredictionCol(self, value: str) -> "FPGrowth":
        """
        Sets the value of :py:attr:`predictionCol`.
        """
        return self._set(predictionCol=value)

    def _create_model(self, java_model: "JavaObject") -> FPGrowthModel:
        return FPGrowthModel(java_model)


class PrefixSpan(JavaParams):
    """
    A parallel PrefixSpan algorithm to mine frequent sequential patterns.
    The PrefixSpan algorithm is described in J. Pei, et al., PrefixSpan: Mining Sequential Patterns
    Efficiently by Prefix-Projected Pattern Growth
    (see `here <https://doi.org/10.1109/ICDE.2001.914830>`_).
    This class is not yet an Estimator/Transformer, use :py:func:`findFrequentSequentialPatterns`
    method to run the PrefixSpan algorithm.

    .. versionadded:: 2.4.0

    Notes
    -----
    See `Sequential Pattern Mining (Wikipedia) \
      <https://en.wikipedia.org/wiki/Sequential_Pattern_Mining>`_

    Examples
    --------
    >>> from pyspark.ml.fpm import PrefixSpan
    >>> from pyspark.sql import Row
    >>> df = sc.parallelize([Row(sequence=[[1, 2], [3]]),
    ...                      Row(sequence=[[1], [3, 2], [1, 2]]),
    ...                      Row(sequence=[[1, 2], [5]]),
    ...                      Row(sequence=[[6]])]).toDF()
    >>> prefixSpan = PrefixSpan()
    >>> prefixSpan.getMaxLocalProjDBSize()
    32000000
    >>> prefixSpan.getSequenceCol()
    'sequence'
    >>> prefixSpan.setMinSupport(0.5)
    PrefixSpan...
    >>> prefixSpan.setMaxPatternLength(5)
    PrefixSpan...
    >>> prefixSpan.findFrequentSequentialPatterns(df).sort("sequence").show(truncate=False)
    +----------+----+
    |sequence  |freq|
    +----------+----+
    |[[1]]     |3   |
    |[[1], [3]]|2   |
    |[[2]]     |3   |
    |[[2, 1]]  |3   |
    |[[3]]     |2   |
    +----------+----+
    ...
    """

    _input_kwargs: Dict[str, Any]

    minSupport: Param[float] = Param(
        Params._dummy(),
        "minSupport",
        "The minimal support level of the "
        + "sequential pattern. Sequential pattern that appears more than "
        + "(minSupport * size-of-the-dataset) times will be output. Must be >= 0.",
        typeConverter=TypeConverters.toFloat,
    )

    maxPatternLength: Param[int] = Param(
        Params._dummy(),
        "maxPatternLength",
        "The maximal length of the sequential pattern. Must be > 0.",
        typeConverter=TypeConverters.toInt,
    )

    maxLocalProjDBSize: Param[int] = Param(
        Params._dummy(),
        "maxLocalProjDBSize",
        "The maximum number of items (including delimiters used in the "
        + "internal storage format) allowed in a projected database before "
        + "local processing. If a projected database exceeds this size, "
        + "another iteration of distributed prefix growth is run. "
        + "Must be > 0.",
        typeConverter=TypeConverters.toInt,
    )

    sequenceCol: Param[str] = Param(
        Params._dummy(),
        "sequenceCol",
        "The name of the sequence column in "
        + "dataset, rows with nulls in this column are ignored.",
        typeConverter=TypeConverters.toString,
    )

    @keyword_only
    def __init__(
        self,
        *,
        minSupport: float = 0.1,
        maxPatternLength: int = 10,
        maxLocalProjDBSize: int = 32000000,
        sequenceCol: str = "sequence",
    ):
        """
        __init__(self, \\*, minSupport=0.1, maxPatternLength=10, maxLocalProjDBSize=32000000, \
                 sequenceCol="sequence")
        """
        super(PrefixSpan, self).__init__()
        self._java_obj = self._new_java_obj("org.apache.spark.ml.fpm.PrefixSpan", self.uid)
        self._setDefault(
            minSupport=0.1, maxPatternLength=10, maxLocalProjDBSize=32000000, sequenceCol="sequence"
        )
        kwargs = self._input_kwargs
        self.setParams(**kwargs)

    @keyword_only
    @since("2.4.0")
    def setParams(
        self,
        *,
        minSupport: float = 0.1,
        maxPatternLength: int = 10,
        maxLocalProjDBSize: int = 32000000,
        sequenceCol: str = "sequence",
    ) -> "PrefixSpan":
        """
        setParams(self, \\*, minSupport=0.1, maxPatternLength=10, maxLocalProjDBSize=32000000, \
                  sequenceCol="sequence")
        """
        kwargs = self._input_kwargs
        return self._set(**kwargs)

    @since("3.0.0")
    def setMinSupport(self, value: float) -> "PrefixSpan":
        """
        Sets the value of :py:attr:`minSupport`.
        """
        return self._set(minSupport=value)

    @since("3.0.0")
    def getMinSupport(self) -> float:
        """
        Gets the value of minSupport or its default value.
        """
        return self.getOrDefault(self.minSupport)

    @since("3.0.0")
    def setMaxPatternLength(self, value: int) -> "PrefixSpan":
        """
        Sets the value of :py:attr:`maxPatternLength`.
        """
        return self._set(maxPatternLength=value)

    @since("3.0.0")
    def getMaxPatternLength(self) -> int:
        """
        Gets the value of maxPatternLength or its default value.
        """
        return self.getOrDefault(self.maxPatternLength)

    @since("3.0.0")
    def setMaxLocalProjDBSize(self, value: int) -> "PrefixSpan":
        """
        Sets the value of :py:attr:`maxLocalProjDBSize`.
        """
        return self._set(maxLocalProjDBSize=value)

    @since("3.0.0")
    def getMaxLocalProjDBSize(self) -> int:
        """
        Gets the value of maxLocalProjDBSize or its default value.
        """
        return self.getOrDefault(self.maxLocalProjDBSize)

    @since("3.0.0")
    def setSequenceCol(self, value: str) -> "PrefixSpan":
        """
        Sets the value of :py:attr:`sequenceCol`.
        """
        return self._set(sequenceCol=value)

    @since("3.0.0")
    def getSequenceCol(self) -> str:
        """
        Gets the value of sequenceCol or its default value.
        """
        return self.getOrDefault(self.sequenceCol)

    def findFrequentSequentialPatterns(self, dataset: DataFrame) -> DataFrame:
        """
        Finds the complete set of frequent sequential patterns in the input sequences of itemsets.

        .. versionadded:: 2.4.0

        Parameters
        ----------
        dataset : :py:class:`pyspark.sql.DataFrame`
            A dataframe containing a sequence column which is
            `ArrayType(ArrayType(T))` type, T is the item type for the input dataset.

        Returns
        -------
        :py:class:`pyspark.sql.DataFrame`
            A `DataFrame` that contains columns of sequence and corresponding frequency.
            The schema of it will be:

            - `sequence: ArrayType(ArrayType(T))` (T is the item type)
            - `freq: Long`
        """

        self._transfer_params_to_java()
        assert self._java_obj is not None
        jdf = self._java_obj.findFrequentSequentialPatterns(dataset._jdf)
        return DataFrame(jdf, dataset.sparkSession)


if __name__ == "__main__":
    import doctest
    import pyspark.ml.fpm
    from pyspark.sql import SparkSession

    globs = pyspark.ml.fpm.__dict__.copy()
    # The small batch size here ensures that we see multiple batches,
    # even in these small test examples:
    spark = SparkSession.builder.master("local[2]").appName("ml.fpm tests").getOrCreate()
    sc = spark.sparkContext
    globs["sc"] = sc
    globs["spark"] = spark
    import tempfile

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
