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
A collections of analysis functions
"""
import sys
import functools
import warnings

if sys.version < "3":
    from itertools import imap as map

if sys.version >= '3':
    basestring = str

from pyspark import since, SparkContext
from pyspark.sql.functions import array, col, explode, lit, struct


def melt(df, id_vars, value_vars, var_name="variable", value_name="value"):
    """Convert :class:`DataFrame` from wide to long format.
    Based on [this](https://stackoverflow.com/a/41673644/12474509) implementation
    :param id_vars: id columns to melt over.
    :param value_vars: value columns to melt.
    :param var_name: Column name for output id.
    :param value_name: Column name for output values.
    >>> import pandas as pd
    >>> pdf = pd.DataFrame({'A': {0: 'a', 1: 'b', 2: 'c'},
               'B': {0: 1, 1: 3, 2: 5},
               'C': {0: 2, 1: 4, 2: 6}})
    >>> pd.melt(pdf, id_vars=['A'], value_vars=['B', 'C'])
           A variable  value
        0  a        B      1
        1  b        B      3
        2  c        B      5
        3  a        C      2
        4  b        C      4
        5  c        C      6
    """
    _vars_and_vals = array(*(
        struct(lit(c).alias(var_name), col(c).alias(value_name))
        for c in value_vars))

    _tmp = df.withColumn("_vars_and_vals", explode(_vars_and_vals))

    cols = id_vars + [
        col("_vars_and_vals")[x].alias(x) for x in [var_name, value_name]]
    return _tmp.select(*cols)


blacklist = []
__all__ = [k for k, v in globals().items()
           if not k.startswith('_') and k[0].islower() and callable(v) and k not in blacklist]
__all__.sort()


def _test():
    import doctest
    from pyspark.sql import Row, SparkSession
    import pyspark.sql.functions
    globs = pyspark.sql.functions.__dict__.copy()
    spark = SparkSession.builder\
        .master("local[4]")\
        .appName("sql.functions tests")\
        .getOrCreate()
    sc = spark.sparkContext
    globs['sc'] = sc
    globs['spark'] = spark
    globs['df'] = spark.createDataFrame([Row(name='Alice', age=2), Row(name='Bob', age=5)])

    spark.conf.set("spark.sql.legacy.utcTimestampFunc.enabled", "true")
    (failure_count, test_count) = doctest.testmod(
        pyspark.sql.functions, globs=globs,
        optionflags=doctest.ELLIPSIS | doctest.NORMALIZE_WHITESPACE)
    spark.conf.unset("spark.sql.legacy.utcTimestampFunc.enabled")

    spark.stop()
    if failure_count:
        sys.exit(-1)


if __name__ == "__main__":
    _test()
