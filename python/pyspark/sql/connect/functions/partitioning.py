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
from pyspark.sql.connect.utils import check_dependencies

check_dependencies(__name__)

from typing import Union, TYPE_CHECKING

from pyspark.errors import PySparkTypeError
from pyspark.sql import functions as pysparkfuncs
from pyspark.sql.column import Column
from pyspark.sql.connect.functions.builtin import _to_col, _invoke_function_over_columns
from pyspark.sql.connect.functions.builtin import lit, _invoke_function


if TYPE_CHECKING:
    from pyspark.sql.connect._typing import ColumnOrName


def bucket(numBuckets: Union[Column, int], col: "ColumnOrName") -> Column:
    if isinstance(numBuckets, int):
        _numBuckets = lit(numBuckets)
    elif isinstance(numBuckets, Column):
        _numBuckets = numBuckets
    else:
        raise PySparkTypeError(
            error_class="NOT_COLUMN_OR_INT",
            message_parameters={
                "arg_name": "numBuckets",
                "arg_type": type(numBuckets).__name__,
            },
        )

    return _invoke_function("bucket", _numBuckets, _to_col(col))


bucket.__doc__ = pysparkfuncs.partitioning.bucket.__doc__


def years(col: "ColumnOrName") -> Column:
    return _invoke_function_over_columns("years", col)


years.__doc__ = pysparkfuncs.partitioning.years.__doc__


def months(col: "ColumnOrName") -> Column:
    return _invoke_function_over_columns("months", col)


months.__doc__ = pysparkfuncs.partitioning.months.__doc__


def days(col: "ColumnOrName") -> Column:
    return _invoke_function_over_columns("days", col)


days.__doc__ = pysparkfuncs.partitioning.days.__doc__


def hours(col: "ColumnOrName") -> Column:
    return _invoke_function_over_columns("hours", col)


hours.__doc__ = pysparkfuncs.partitioning.hours.__doc__


def _test() -> None:
    import sys
    import os
    import doctest
    from pyspark.sql import SparkSession as PySparkSession
    import pyspark.sql.connect.functions.partitioning

    globs = pyspark.sql.connect.functions.partitioning.__dict__.copy()

    globs["spark"] = (
        PySparkSession.builder.appName("sql.connect.functions tests")
        .remote(os.environ.get("SPARK_CONNECT_TESTING_REMOTE", "local[4]"))
        .getOrCreate()
    )

    (failure_count, test_count) = doctest.testmod(
        pyspark.sql.connect.functions.partitioning,
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
