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
import os
from typing import Any, Dict, Optional, TYPE_CHECKING

from pyspark.errors import PySparkTypeError, PySparkValueError, PySparkAssertionError
from pyspark.sql.column import Column
from pyspark.sql.dataframe import DataFrame
from pyspark.sql.utils import is_remote

if TYPE_CHECKING:
    from py4j.java_gateway import JavaObject, JVMView


__all__ = ["Observation"]


class Observation:
    """Class to observe (named) metrics on a :class:`DataFrame`.

    Metrics are aggregation expressions, which are applied to the DataFrame while it is being
    processed by an action.

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

    .. versionadded:: 3.3.0

    Notes
    -----
    This class does not support streaming datasets.

    Examples
    --------
    >>> from pyspark.sql.functions import col, count, lit, max
    >>> from pyspark.sql import Observation
    >>> df = spark.createDataFrame([["Alice", 2], ["Bob", 5]], ["name", "age"])
    >>> observation = Observation("my metrics")
    >>> observed_df = df.observe(observation, count(lit(1)).alias("count"), max(col("age")))
    >>> observed_df.count()
    2
    >>> observation.get
    {'count': 2, 'max(age)': 5}
    """

    def __new__(cls, *args: Any, **kwargs: Any) -> Any:
        if is_remote() and "PYSPARK_NO_NAMESPACE_SHARE" not in os.environ:
            from pyspark.sql.connect.observation import Observation as ConnectObservation

            return ConnectObservation(*args, **kwargs)
        return super().__new__(cls)

    def __init__(self, name: Optional[str] = None) -> None:
        """Constructs a named or unnamed Observation instance.

        Parameters
        ----------
        name : str, optional
            default is a random UUID string. This is the name of the Observation and the metric.
        """
        if name is not None:
            if not isinstance(name, str):
                raise PySparkTypeError(
                    errorClass="NOT_STR",
                    messageParameters={"arg_name": "name", "arg_type": type(name).__name__},
                )
            if name == "":
                raise PySparkValueError(
                    errorClass="VALUE_NOT_NON_EMPTY_STR",
                    messageParameters={"arg_name": "name", "arg_value": name},
                )
        self._name = name
        self._jvm: Optional[JVMView] = None
        self._jo: Optional["JavaObject"] = None

    def _on(self, df: DataFrame, *exprs: Column) -> DataFrame:
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
        from pyspark.sql.classic.column import _to_seq

        if self._jo is not None:
            raise PySparkAssertionError(errorClass="REUSE_OBSERVATION", messageParameters={})

        self._jvm = df._sc._jvm
        assert self._jvm is not None
        cls = self._jvm.org.apache.spark.sql.Observation
        self._jo = cls(self._name) if self._name is not None else cls()
        observed_df = df._jdf.observe(
            self._jo, exprs[0]._jc, _to_seq(df._sc, [c._jc for c in exprs[1:]])
        )
        return DataFrame(observed_df, df.sparkSession)

    @property
    def get(self) -> Dict[str, Any]:
        """Get the observed metrics.

        Waits until the observed dataset finishes its first action. Only the result of the
        first action is available. Subsequent actions do not modify the result.

        Returns
        -------
        dict
            the observed metrics
        """
        if self._jo is None:
            raise PySparkAssertionError(errorClass="NO_OBSERVE_BEFORE_GET", messageParameters={})

        jmap = self._jo.getAsJava()
        # return a pure Python dict, not jmap which is a py4j JavaMap
        return {k: v for k, v in jmap.items()}


def _test() -> None:
    import doctest
    import sys
    from pyspark.core.context import SparkContext
    from pyspark.sql import SparkSession
    import pyspark.sql.observation

    globs = pyspark.sql.observation.__dict__.copy()
    sc = SparkContext("local[4]", "PythonTest")
    globs["spark"] = SparkSession(sc)

    (failure_count, test_count) = doctest.testmod(pyspark.sql.observation, globs=globs)
    sc.stop()
    if failure_count:
        sys.exit(-1)


if __name__ == "__main__":
    _test()
