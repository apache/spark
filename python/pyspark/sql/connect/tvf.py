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
from typing import Optional, TYPE_CHECKING

from pyspark.errors import PySparkValueError
from pyspark.sql.tvf import TableValuedFunction as PySparkTableValuedFunction

if TYPE_CHECKING:
    from pyspark.sql.connect.column import Column
    from pyspark.sql.connect.dataframe import DataFrame
    from pyspark.sql.connect.session import SparkSession


class TableValuedFunction:
    __doc__ = PySparkTableValuedFunction.__doc__

    def __init__(self, sparkSession: "SparkSession"):
        self._sparkSession = sparkSession

    def range(
        self,
        start: int,
        end: Optional[int] = None,
        step: int = 1,
        numPartitions: Optional[int] = None,
    ) -> "DataFrame":
        return self._sparkSession.range(  # type: ignore[return-value]
            start, end, step, numPartitions
        )

    range.__doc__ = PySparkTableValuedFunction.range.__doc__

    def explode(self, collection: "Column") -> "DataFrame":
        return self._fn("explode", collection)

    explode.__doc__ = PySparkTableValuedFunction.explode.__doc__

    def explode_outer(self, collection: "Column") -> "DataFrame":
        return self._fn("explode_outer", collection)

    explode_outer.__doc__ = PySparkTableValuedFunction.explode_outer.__doc__

    def inline(self, input: "Column") -> "DataFrame":
        return self._fn("inline", input)

    inline.__doc__ = PySparkTableValuedFunction.inline.__doc__

    def inline_outer(self, input: "Column") -> "DataFrame":
        return self._fn("inline_outer", input)

    inline_outer.__doc__ = PySparkTableValuedFunction.inline_outer.__doc__

    def json_tuple(self, input: "Column", *fields: "Column") -> "DataFrame":
        if len(fields) == 0:
            raise PySparkValueError(
                errorClass="CANNOT_BE_EMPTY",
                messageParameters={"item": "field"},
            )
        return self._fn("json_tuple", input, *fields)

    json_tuple.__doc__ = PySparkTableValuedFunction.json_tuple.__doc__

    def posexplode(self, collection: "Column") -> "DataFrame":
        return self._fn("posexplode", collection)

    posexplode.__doc__ = PySparkTableValuedFunction.posexplode.__doc__

    def posexplode_outer(self, collection: "Column") -> "DataFrame":
        return self._fn("posexplode_outer", collection)

    posexplode_outer.__doc__ = PySparkTableValuedFunction.posexplode_outer.__doc__

    def stack(self, n: "Column", *fields: "Column") -> "DataFrame":
        return self._fn("stack", n, *fields)

    stack.__doc__ = PySparkTableValuedFunction.stack.__doc__

    def collations(self) -> "DataFrame":
        return self._fn("collations")

    collations.__doc__ = PySparkTableValuedFunction.collations.__doc__

    def sql_keywords(self) -> "DataFrame":
        return self._fn("sql_keywords")

    sql_keywords.__doc__ = PySparkTableValuedFunction.sql_keywords.__doc__

    def variant_explode(self, input: "Column") -> "DataFrame":
        return self._fn("variant_explode", input)

    variant_explode.__doc__ = PySparkTableValuedFunction.variant_explode.__doc__

    def variant_explode_outer(self, input: "Column") -> "DataFrame":
        return self._fn("variant_explode_outer", input)

    variant_explode_outer.__doc__ = PySparkTableValuedFunction.variant_explode_outer.__doc__

    def _fn(self, name: str, *args: "Column") -> "DataFrame":
        from pyspark.sql.connect.dataframe import DataFrame
        from pyspark.sql.connect.plan import UnresolvedTableValuedFunction
        from pyspark.sql.connect.functions.builtin import _to_col

        return DataFrame(
            UnresolvedTableValuedFunction(name, [_to_col(arg) for arg in args]), self._sparkSession
        )


def _test() -> None:
    import os
    import sys

    if os.environ.get("PYTHON_GIL", "?") == "0":
        print("Not supported in no-GIL mode", file=sys.stderr)
        sys.exit(0)

    from pyspark.testing import should_test_connect

    if not should_test_connect:
        print("Skipping pyspark.sql.connect.tvf doctests", file=sys.stderr)
        sys.exit(0)

    import doctest
    from pyspark.sql import SparkSession as PySparkSession
    import pyspark.sql.connect.tvf

    globs = pyspark.sql.connect.tvf.__dict__.copy()

    globs["spark"] = (
        PySparkSession.builder.appName("sql.connect.tvf tests")
        .remote(os.environ.get("SPARK_CONNECT_TESTING_REMOTE", "local[4]"))
        .getOrCreate()
    )

    (failure_count, test_count) = doctest.testmod(
        pyspark.sql.connect.tvf,
        globs=globs,
        optionflags=doctest.ELLIPSIS
        | doctest.NORMALIZE_WHITESPACE
        | doctest.IGNORE_EXCEPTION_DETAIL,
    )

    globs["spark"].stop()

    if failure_count:
        sys.exit(-1)


if __name__ == "__main__":
    _test()
