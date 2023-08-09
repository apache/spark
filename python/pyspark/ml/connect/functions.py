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

from pyspark.ml import functions as PyMLFunctions

from pyspark.sql.connect.column import Column
from pyspark.sql.connect.functions import _invoke_function, _to_col, lit


def vector_to_array(col: Column, dtype: str = "float64") -> Column:
    return _invoke_function("vector_to_array", _to_col(col), lit(dtype))


vector_to_array.__doc__ = PyMLFunctions.vector_to_array.__doc__


def array_to_vector(col: Column) -> Column:
    return _invoke_function("array_to_vector", _to_col(col))


array_to_vector.__doc__ = PyMLFunctions.array_to_vector.__doc__


def _test() -> None:
    import sys
    import doctest
    from pyspark.sql import SparkSession as PySparkSession
    import pyspark.ml.connect.functions

    globs = pyspark.ml.connect.functions.__dict__.copy()

    # TODO: split vector_to_array doctest since it includes .mllib vectors
    del pyspark.ml.connect.functions.vector_to_array.__doc__

    # TODO: spark.createDataFrame should support UDT
    del pyspark.ml.connect.functions.array_to_vector.__doc__

    globs["spark"] = (
        PySparkSession.builder.appName("ml.connect.functions tests")
        .remote("local[4]")
        .getOrCreate()
    )

    (failure_count, test_count) = doctest.testmod(
        pyspark.ml.connect.functions,
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
