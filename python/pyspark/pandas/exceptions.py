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

"""
Exceptions/Errors used in pandas-on-Spark.
"""
from typing import Optional


class DataError(Exception):
    pass


class SparkPandasIndexingError(Exception):
    pass


def code_change_hint(pandas_function: str, spark_target_function: str) -> str:
    return "You are trying to use pandas function {}, use spark function {}".format(
        pandas_function, spark_target_function
    )


class SparkPandasNotImplementedError(NotImplementedError):
    def __init__(
        self,
        pandas_function: str,
        spark_target_function: str,
        description: str,
    ):
        self.pandas_source = pandas_function
        self.spark_target = spark_target_function
        hint = code_change_hint(pandas_function, spark_target_function)
        if len(description) > 0:
            description += " " + hint
        else:
            description = hint
        super().__init__(description)


class PandasNotImplementedError(NotImplementedError):
    def __init__(
        self,
        class_name: str,
        method_name: Optional[str] = None,
        arg_name: Optional[str] = None,
        property_name: Optional[str] = None,
        scalar_name: Optional[str] = None,
        deprecated: bool = False,
        reason: str = "",
    ):
        assert [method_name is not None, property_name is not None, scalar_name is not None].count(
            True
        ) == 1
        self.class_name = class_name
        self.method_name = method_name
        self.arg_name = arg_name
        if method_name is not None:
            if arg_name is not None:
                msg = "The method `{0}.{1}()` does not support `{2}` parameter. {3}".format(
                    class_name, method_name, arg_name, reason
                )
            else:
                if deprecated:
                    msg = (
                        "The method `{0}.{1}()` is deprecated in pandas and will therefore "
                        + "not be supported in pandas-on-Spark. {2}"
                    ).format(class_name, method_name, reason)
                else:
                    if reason == "":
                        reason = " yet."
                    else:
                        reason = ". " + reason
                    msg = "The method `{0}.{1}()` is not implemented{2}".format(
                        class_name, method_name, reason
                    )
        elif scalar_name is not None:
            msg = (
                "The scalar `{0}.{1}` is not reimplemented in pyspark.pandas;"
                " use `pd.{1}`.".format(class_name, scalar_name)
            )
        else:
            if deprecated:
                msg = (
                    "The property `{0}.{1}()` is deprecated in pandas and will therefore "
                    + "not be supported in pandas-on-Spark. {2}"
                ).format(class_name, property_name, reason)
            else:
                if reason == "":
                    reason = " yet."
                else:
                    reason = ". " + reason
                msg = "The property `{0}.{1}()` is not implemented{2}".format(
                    class_name, property_name, reason
                )
        super().__init__(msg)


def _test() -> None:
    import os
    import doctest
    import sys
    from pyspark.sql import SparkSession
    import pyspark.pandas.exceptions

    os.chdir(os.environ["SPARK_HOME"])

    globs = pyspark.pandas.exceptions.__dict__.copy()
    globs["ps"] = pyspark.pandas
    spark = (
        SparkSession.builder.master("local[4]")
        .appName("pyspark.pandas.exceptions tests")
        .getOrCreate()
    )
    (failure_count, test_count) = doctest.testmod(
        pyspark.pandas.exceptions,
        globs=globs,
        optionflags=doctest.ELLIPSIS | doctest.NORMALIZE_WHITESPACE,
    )
    spark.stop()
    if failure_count:
        sys.exit(-1)


if __name__ == "__main__":
    _test()
