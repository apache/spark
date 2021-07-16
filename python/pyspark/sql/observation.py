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
from typing import Optional
from uuid import uuid4

from pyspark.sql import column, Column, DataFrame, Row

basestring = unicode = str

__all__ = ["Observation"]


class Observation:
    """
    Class to observe (named) metrics on a :class:`DataFrame`. Metrics are aggregation expressions,
    which are applied to the DataFrame while is is being processed by an action.

    .. versionadded:: 3.2.0

    The metrics have the following guarantees:

    - It will compute the defined aggregates (metrics) on all the data that is flowing through
      the Dataset during the action.
    - It will report the value of the defined aggregate columns as soon as we reach the end of
      the action.

    The metrics columns must either contain a literal (e.g. lit(42)), or should contain one or
    more aggregate functions (e.g. sum(a) or sum(a + b) + avg(c) - lit(1)). Expressions that
    contain references to the input Dataset's columns must always be wrapped in an aggregate
    function.

    Example:
        >>> observation = Observation("my_metrics")
        >>> observed_df = df.observe(observation, count(lit(1)).as("rows"), max($"id").as("maxid"))
        >>> observed_df.write.parquet("ds.parquet")
        >>> metrics = observation.get

    An Observation instance collects the metrics while the first action is executed. Subsequent
    actions do not modify the metrics returned by `Observation.get`. Retrieval of the metric via
    `Observation.get` blocks until the first action has finished and metrics become available.
    You can add a timeout to that blocking via `Observation.waitCompleted`:

        if observation.waitCompleted(millis=100):
          return observation.get

    This class does not support streaming datasets.
    """
    def __init__(self, name: Optional[str] = None):
        assert isinstance(name, basestring), "name should be a string"
        self._name = name or str(uuid4())
        self._jvm = None
        self._jo = None

    def _on(self, df: DataFrame, *exprs: Column) -> DataFrame:
        """
        Attaches this observation to the given :class:`Dataset` to observe aggregation expressions.

        :param df: the `DataFrame` to be observe
        :param exprs: aggregation expressions
        :return: the observed `DataFrame`
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
    def get(self) -> Row:
        """
        Get the observed metrics. This waits until the observed dataset finishes its first action.
        If you want to wait for the result and provide a timeout, use `waitCompleted`. Only the
        result of the first action is available. Subsequent actions do not modify the result.

        :return: the metrics :class:`Row`
        """
        assert self._jo is not None, 'call DataFrame.observe / Observation.on first'
        jrow = self._jo.get()
        return self._to_row(jrow)

    def _to_row(self, jrow):
        field_names = jrow.schema().fieldNames()
        valuesScalaMap = jrow.getValuesMap(self._jvm.PythonUtils.toSeq(list(field_names)))
        valuesJavaMap = self._jvm.scala.collection.JavaConversions.mapAsJavaMap(valuesScalaMap)
        return Row(**valuesJavaMap)
