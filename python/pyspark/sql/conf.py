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

import sys

from pyspark import since, _NoValue
from pyspark.rdd import ignore_unicode_prefix


class RuntimeConfig(object):
    """User-facing configuration API, accessible through `SparkSession.conf`.

    Options set here are automatically propagated to the Hadoop configuration during I/O.
    """

    def __init__(self, jconf):
        """Create a new RuntimeConfig that wraps the underlying JVM object."""
        self._jconf = jconf

    @ignore_unicode_prefix
    @since(2.0)
    def set(self, key, value):
        """Sets the given Spark runtime configuration property."""
        self._jconf.set(key, value)

    @ignore_unicode_prefix
    @since(2.0)
    def get(self, key, default=_NoValue):
        """Returns the value of Spark runtime configuration property for the given key,
        assuming it is set.
        """
        self._checkType(key, "key")
        if default is _NoValue:
            return self._jconf.get(key)
        else:
            if default is not None:
                self._checkType(default, "default")
            return self._jconf.get(key, default)

    @ignore_unicode_prefix
    @since(2.0)
    def unset(self, key):
        """Resets the configuration property for the given key."""
        self._jconf.unset(key)

    def _checkType(self, obj, identifier):
        """Assert that an object is of type str."""
        if not isinstance(obj, str) and not isinstance(obj, unicode):
            raise TypeError("expected %s '%s' to be a string (was '%s')" %
                            (identifier, obj, type(obj).__name__))


class ConfigEntry(object):
    """An entry contains all meta information for a configuration"""

    def __init__(self, confKey):
        """Create a new ConfigEntry with config key"""
        self.confKey = confKey
        self.converter = None
        self.default = _NoValue

    def boolConf(self):
        """Designate current config entry is boolean config"""
        self.converter = lambda x: str(x).lower() == "true"
        return self

    def intConf(self):
        """Designate current config entry is integer config"""
        self.converter = lambda x: int(x)
        return self

    def stringConf(self):
        """Designate current config entry is string config"""
        self.converter = lambda x: str(x)
        return self

    def withDefault(self, default):
        """Give a default value for current config entry, the default value will be set
        to _NoValue when its absent"""
        self.default = default
        return self

    def read(self, ctx):
        """Read value from this config entry through sql context"""
        return self.converter(ctx.getConf(self.confKey, self.default))


class SQLConf(object):
    """A class that enables the getting of SQL config parameters in pyspark"""

    REPL_EAGER_EVAL_ENABLED = ConfigEntry("spark.sql.repl.eagerEval.enabled")\
        .boolConf()\
        .withDefault("false")

    REPL_EAGER_EVAL_MAX_NUM_ROWS = ConfigEntry("spark.sql.repl.eagerEval.maxNumRows")\
        .intConf()\
        .withDefault("20")

    REPL_EAGER_EVAL_TRUNCATE = ConfigEntry("spark.sql.repl.eagerEval.truncate")\
        .intConf()\
        .withDefault("20")

    PANDAS_RESPECT_SESSION_LOCAL_TIMEZONE = \
        ConfigEntry("spark.sql.execution.pandas.respectSessionTimeZone")\
        .boolConf()

    SESSION_LOCAL_TIMEZONE = ConfigEntry("spark.sql.session.timeZone")\
        .stringConf()

    ARROW_EXECUTION_ENABLED = ConfigEntry("spark.sql.execution.arrow.enabled")\
        .boolConf()\
        .withDefault("false")

    ARROW_FALLBACK_ENABLED = ConfigEntry("spark.sql.execution.arrow.fallback.enabled")\
        .boolConf()\
        .withDefault("true")

    def __init__(self, sql_ctx):
        """Create a SQLConf with sql context"""
        self._sql_ctx = sql_ctx

    def isReplEagerEvalEnabled(self):
        return self.REPL_EAGER_EVAL_ENABLED.read(self._sql_ctx)

    def replEagerEvalMaxNumRows(self):
        return self.REPL_EAGER_EVAL_MAX_NUM_ROWS.read(self._sql_ctx)

    def replEagerEvalTruncate(self):
        return self.REPL_EAGER_EVAL_TRUNCATE.read(self._sql_ctx)

    def pandasRespectSessionTimeZone(self):
        return self.PANDAS_RESPECT_SESSION_LOCAL_TIMEZONE.read(self._sql_ctx)

    def sessionLocalTimeZone(self):
        return self.SESSION_LOCAL_TIMEZONE.read(self._sql_ctx)

    def arrowEnabled(self):
        return self.ARROW_EXECUTION_ENABLED.read(self._sql_ctx)

    def arrowFallbackEnabled(self):
        return self.ARROW_FALLBACK_ENABLED.read(self._sql_ctx)


def _test():
    import os
    import doctest
    from pyspark.sql.session import SparkSession
    import pyspark.sql.conf

    os.chdir(os.environ["SPARK_HOME"])

    globs = pyspark.sql.conf.__dict__.copy()
    spark = SparkSession.builder\
        .master("local[4]")\
        .appName("sql.conf tests")\
        .getOrCreate()
    globs['sc'] = spark.sparkContext
    globs['spark'] = spark
    (failure_count, test_count) = doctest.testmod(pyspark.sql.conf, globs=globs)
    spark.stop()
    if failure_count:
        sys.exit(-1)

if __name__ == "__main__":
    _test()
