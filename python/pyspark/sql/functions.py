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
A collections of builtin functions
"""

from itertools import imap

from py4j.java_collections import ListConverter

from pyspark import SparkContext
from pyspark.rdd import _prepare_for_python_RDD
from pyspark.serializers import PickleSerializer, AutoBatchedSerializer
from pyspark.sql.types import StringType
from pyspark.sql.dataframe import Column, _to_java_column


__all__ = ['countDistinct', 'approxCountDistinct', 'udf']


def _create_function(name, doc=""):
    """ Create a function for aggregator by name"""
    def _(col):
        sc = SparkContext._active_spark_context
        jc = getattr(sc._jvm.functions, name)(col._jc if isinstance(col, Column) else col)
        return Column(jc)
    _.__name__ = name
    _.__doc__ = doc
    return _


_functions = {
    'lit': 'Creates a :class:`Column` of literal value.',
    'col': 'Returns a :class:`Column` based on the given column name.',
    'column': 'Returns a :class:`Column` based on the given column name.',
    'asc': 'Returns a sort expression based on the ascending order of the given column name.',
    'desc': 'Returns a sort expression based on the descending order of the given column name.',

    'upper': 'Converts a string expression to upper case.',
    'lower': 'Converts a string expression to upper case.',
    'sqrt': 'Computes the square root of the specified float value.',
    'abs': 'Computes the absolutle value.',

    'max': 'Aggregate function: returns the maximum value of the expression in a group.',
    'min': 'Aggregate function: returns the minimum value of the expression in a group.',
    'first': 'Aggregate function: returns the first value in a group.',
    'last': 'Aggregate function: returns the last value in a group.',
    'count': 'Aggregate function: returns the number of items in a group.',
    'sum': 'Aggregate function: returns the sum of all values in the expression.',
    'avg': 'Aggregate function: returns the average of the values in a group.',
    'mean': 'Aggregate function: returns the average of the values in a group.',
    'sumDistinct': 'Aggregate function: returns the sum of distinct values in the expression.',
}


for _name, _doc in _functions.items():
    globals()[_name] = _create_function(_name, _doc)
del _name, _doc
__all__ += _functions.keys()
__all__.sort()


def countDistinct(col, *cols):
    """Returns a new :class:`Column` for distinct count of ``col`` or ``cols``.

    >>> df.agg(countDistinct(df.age, df.name).alias('c')).collect()
    [Row(c=2)]

    >>> df.agg(countDistinct("age", "name").alias('c')).collect()
    [Row(c=2)]
    """
    sc = SparkContext._active_spark_context
    jcols = ListConverter().convert([_to_java_column(c) for c in cols], sc._gateway._gateway_client)
    jc = sc._jvm.functions.countDistinct(_to_java_column(col), sc._jvm.PythonUtils.toSeq(jcols))
    return Column(jc)


def approxCountDistinct(col, rsd=None):
    """Returns a new :class:`Column` for approximate distinct count of ``col``.

    >>> df.agg(approxCountDistinct(df.age).alias('c')).collect()
    [Row(c=2)]
    """
    sc = SparkContext._active_spark_context
    if rsd is None:
        jc = sc._jvm.functions.approxCountDistinct(_to_java_column(col))
    else:
        jc = sc._jvm.functions.approxCountDistinct(_to_java_column(col), rsd)
    return Column(jc)


class UserDefinedFunction(object):
    """
    User defined function in Python
    """
    def __init__(self, func, returnType):
        self.func = func
        self.returnType = returnType
        self._broadcast = None
        self._judf = self._create_judf()

    def _create_judf(self):
        f = self.func  # put it in closure `func`
        func = lambda _, it: imap(lambda x: f(*x), it)
        ser = AutoBatchedSerializer(PickleSerializer())
        command = (func, None, ser, ser)
        sc = SparkContext._active_spark_context
        pickled_command, broadcast_vars, env, includes = _prepare_for_python_RDD(sc, command, self)
        ssql_ctx = sc._jvm.SQLContext(sc._jsc.sc())
        jdt = ssql_ctx.parseDataType(self.returnType.json())
        fname = f.__name__ if hasattr(f, '__name__') else f.__class__.__name__
        judf = sc._jvm.UserDefinedPythonFunction(fname, bytearray(pickled_command), env,
                                                 includes, sc.pythonExec, broadcast_vars,
                                                 sc._javaAccumulator, jdt)
        return judf

    def __del__(self):
        if self._broadcast is not None:
            self._broadcast.unpersist()
            self._broadcast = None

    def __call__(self, *cols):
        sc = SparkContext._active_spark_context
        jcols = ListConverter().convert([_to_java_column(c) for c in cols],
                                        sc._gateway._gateway_client)
        jc = self._judf.apply(sc._jvm.PythonUtils.toSeq(jcols))
        return Column(jc)


def udf(f, returnType=StringType()):
    """Creates a :class:`Column` expression representing a user defined function (UDF).

    >>> from pyspark.sql.types import IntegerType
    >>> slen = udf(lambda s: len(s), IntegerType())
    >>> df.select(slen(df.name).alias('slen')).collect()
    [Row(slen=5), Row(slen=3)]
    """
    return UserDefinedFunction(f, returnType)


def _test():
    import doctest
    from pyspark.context import SparkContext
    from pyspark.sql import Row, SQLContext
    import pyspark.sql.functions
    globs = pyspark.sql.functions.__dict__.copy()
    sc = SparkContext('local[4]', 'PythonTest')
    globs['sc'] = sc
    globs['sqlCtx'] = SQLContext(sc)
    globs['df'] = sc.parallelize([Row(name='Alice', age=2), Row(name='Bob', age=5)]).toDF()
    (failure_count, test_count) = doctest.testmod(
        pyspark.sql.functions, globs=globs,
        optionflags=doctest.ELLIPSIS | doctest.NORMALIZE_WHITESPACE)
    globs['sc'].stop()
    if failure_count:
        exit(-1)


if __name__ == "__main__":
    _test()
