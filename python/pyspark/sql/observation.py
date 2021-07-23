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
from uuid import uuid4

from pyspark.sql import column, Column, DataFrame, Row

basestring = unicode = str

__all__ = ["Observation"]


class Observation:
    """Class to observe (named) metrics on a :class:`DataFrame`.

    Metrics are aggregation expressions, which are applied to the DataFrame while is is being
    processed by an action.

    .. versionadded:: 3.3.0

    The metrics have the following guarantees:

    - It will compute the defined aggregates (metrics) on all the data that is flowing through
      the Dataset during the action.
    - It will report the value of the defined aggregate columns as soon as we reach the end of
      the action.

    The metrics columns must either contain a literal (e.g. lit(42)), or should contain one or
    more aggregate functions (e.g. sum(a) or sum(a + b) + avg(c) - lit(1)). Expressions that
    contain references to the input Dataset's columns must always be wrapped in an aggregate
    function.

    An Observation instance collects the metrics while the first action is executed. Subsequent
    actions do not modify the metrics returned by `Observation.get`. Retrieval of the metric via
    `Observation.get` blocks until the first action has finished and metrics become available.

    This class does not support streaming datasets.

    Examples
    --------
    >>> from pyspark.sql.functions import col, count, lit, max
    >>> from pyspark.sql.observation import Observation
    >>> observation = Observation("my_metrics")
    >>> observed_df = df.observe(observation, count(lit(1)).alias("count"), max(col("age")))
    >>> observed_df.count()
    2
    >>> observation.get
    Row(count=2, max(age)=5)
    """
    def __init__(self, name=None):
        """Constructs a named or unnamed Observation instance.

        Parameters
        ----------
        name : str, optional
            default is a random UUID string. This is the name of the Observation and the metric.
        """
        assert isinstance(name, basestring), "name should be a string"
        self._name = name or str(uuid4())
        self._jvm = None
        self._jo = None

    def _on(self, df, *exprs):
        """Attaches this observation to the given :class:`DataFrame` to observe aggregations.

        Parameters
        ----------
        df : :class:`DataFrame`
            the :class:`DataFrame` to be observed
        exprs : list of :class:`Column`
            column expressions (:class:`Column`).

        Returns
        -------
        :class:`DataFrame`
            the observed :class:`DataFrame`.
        """
        assert exprs, "exprs should not be empty"
        assert all(isinstance(c, Column) for c in exprs), "all exprs should be Column"
        assert self._jo is None, "an Observation can be used with a DataFrame only once"

        self._jvm = df._sc._jvm
        self._jo = self._jvm.org.apache.spark.sql.Observation(self._name)
        observed_df = self._jo.on(df._jdf,
                                  exprs[0]._jc,
                                  column._to_seq(df._sc, [c._jc for c in exprs[1:]]))
        return DataFrame(observed_df, df.sql_ctx)

    @property
    def get(self):
        """Get the observed metrics.

        Waits until the observed dataset finishes its first action. Only the result of the
        first action is available. Subsequent actions do not modify the result.

        Returns
        -------
        :class:`Row`
            the observed metrics
        """
        assert self._jo is not None, 'call DataFrame.observe'
        jrow = self._jo.get()
        return self._to_row(jrow)

    def _to_row(self, jrow):
        field_names = jrow.schema().fieldNames()
        values_scala_map = jrow.getValuesMap(self._jvm.PythonUtils.toSeq(list(field_names)))
        values_java_map = self._jvm.scala.collection.JavaConversions.mapAsJavaMap(values_scala_map)
        return Row(**values_java_map)


def _test():
    import doctest
    import sys
    from pyspark.context import SparkContext
    from pyspark.sql import SparkSession
    from pyspark.sql.types import StructType, StructField, IntegerType, StringType
    import pyspark.sql.observation
    globs = pyspark.sql.observation.__dict__.copy()
    sc = SparkContext('local[4]', 'PythonTest')
    globs['sc'] = sc
    globs['spark'] = SparkSession(sc)
    globs['df'] = sc.parallelize([(2, 'Alice'), (5, 'Bob')]) \
        .toDF(StructType([StructField('age', IntegerType()),
                          StructField('name', StringType())]))

    (failure_count, test_count) = doctest.testmod(pyspark.sql.observation, globs=globs)
    globs['sc'].stop()
    if failure_count:
        sys.exit(-1)


if __name__ == "__main__":
    _test()
