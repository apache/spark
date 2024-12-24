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

from typing import Sequence, TYPE_CHECKING

from pyspark.errors import PySparkValueError
from pyspark.sql import functions as pysparkfuncs
from pyspark.sql.column import Column


if TYPE_CHECKING:
    from pyspark.sql.connect._typing import ColumnOrName


__all__: Sequence[str] = []


def crc32(col: "ColumnOrName") -> Column:
    from pyspark.sql.connect.functions.builtin import _invoke_function_over_columns

    return _invoke_function_over_columns("crc32", col)


crc32.__doc__ = pysparkfuncs.crc32.__doc__


def hash(*cols: "ColumnOrName") -> Column:
    from pyspark.sql.connect.functions.builtin import _invoke_function_over_columns

    return _invoke_function_over_columns("hash", *cols)


hash.__doc__ = pysparkfuncs.hash.__doc__


def md5(col: "ColumnOrName") -> Column:
    from pyspark.sql.connect.functions.builtin import _invoke_function_over_columns

    return _invoke_function_over_columns("md5", col)


md5.__doc__ = pysparkfuncs.md5.__doc__


def sha(col: "ColumnOrName") -> Column:
    from pyspark.sql.connect.functions.builtin import _invoke_function_over_columns

    return _invoke_function_over_columns("sha", col)


sha.__doc__ = pysparkfuncs.sha.__doc__


def sha1(col: "ColumnOrName") -> Column:
    from pyspark.sql.connect.functions.builtin import _invoke_function_over_columns

    return _invoke_function_over_columns("sha1", col)


sha1.__doc__ = pysparkfuncs.sha1.__doc__


def sha2(col: "ColumnOrName", numBits: int) -> Column:
    from pyspark.sql.connect.functions.builtin import _invoke_function_over_columns, lit

    if numBits not in [0, 224, 256, 384, 512]:
        raise PySparkValueError(
            errorClass="VALUE_NOT_ALLOWED",
            messageParameters={
                "arg_name": "numBits",
                "allowed_values": "[0, 224, 256, 384, 512]",
            },
        )
    return _invoke_function_over_columns("sha2", col, lit(numBits))


sha2.__doc__ = pysparkfuncs.sha2.__doc__


def xxhash64(*cols: "ColumnOrName") -> Column:
    from pyspark.sql.connect.functions.builtin import _invoke_function_over_columns

    return _invoke_function_over_columns("xxhash64", *cols)


xxhash64.__doc__ = pysparkfuncs.xxhash64.__doc__


def _test() -> None:
    import sys
    import os
    import doctest
    from pyspark.sql import SparkSession as PySparkSession
    import pyspark.sql.connect.functions._hash_funcs

    globs = pyspark.sql.connect.functions._hash_funcs.__dict__.copy()

    globs["spark"] = (
        PySparkSession.builder.appName("sql.connect.functions._hash_funcs tests")
        .remote(os.environ.get("SPARK_CONNECT_TESTING_REMOTE", "local[4]"))
        .getOrCreate()
    )

    (failure_count, test_count) = doctest.testmod(
        pyspark.sql.connect.functions._hash_funcs,
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
