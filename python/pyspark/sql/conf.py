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

from pyspark import since
from pyspark.rdd import ignore_unicode_prefix


class RuntimeConfig(object):
    """User-facing configuration API, accessible through `SparkSession.conf`.

    Options set here are automatically propagated to the Hadoop configuration during I/O.
    This a thin wrapper around its Scala implementation org.apache.spark.sql.RuntimeConfig.
    """

    def __init__(self, jconf):
        """Create a new RuntimeConfig that wraps the underlying JVM object."""
        self._jconf = jconf

    @ignore_unicode_prefix
    @since(2.0)
    def set(self, key, value):
        """Sets the given Spark runtime configuration property.

        >>> spark.conf.set("garble", "marble")
        >>> spark.getConf("garble")
        u'marble'
        """
        self._jconf.set(key, value)

    @ignore_unicode_prefix
    @since(2.0)
    def get(self, key):
        """Returns the value of Spark runtime configuration property for the given key,
        assuming it is set.

        >>> spark.setConf("bogo", "sipeo")
        >>> spark.conf.get("bogo")
        u'sipeo'
        >>> spark.conf.get("definitely.not.set") # doctest: +IGNORE_EXCEPTION_DETAIL
        Traceback (most recent call last):
            ...
        Py4JJavaError: ...
        """
        return self._jconf.get(key)

    @ignore_unicode_prefix
    @since(2.0)
    def getOption(self, key):
        """Returns the value of Spark runtime configuration property for the given key,
        or None if it is not set.

        >>> spark.setConf("bogo", "sipeo")
        >>> spark.conf.getOption("bogo")
        u'sipeo'
        >>> spark.conf.getOption("definitely.not.set") is None
        True
        """
        iter = self._jconf.getOption(key).iterator()
        if iter.hasNext():
            return iter.next()
        else:
            return None

    @ignore_unicode_prefix
    @since(2.0)
    def unset(self, key):
        """Resets the configuration property for the given key.

        >>> spark.setConf("armado", "larmado")
        >>> spark.getConf("armado")
        u'larmado'
        >>> spark.conf.unset("armado")
        >>> spark.getConf("armado") # doctest: +IGNORE_EXCEPTION_DETAIL
        Traceback (most recent call last):
            ...
        Py4JJavaError: ...
        """
        self._jconf.unset(key)


def _test():
    import os
    import doctest
    from pyspark.context import SparkContext
    from pyspark.sql.session import SparkSession
    import pyspark.sql.conf

    os.chdir(os.environ["SPARK_HOME"])

    globs = pyspark.sql.conf.__dict__.copy()
    sc = SparkContext('local[4]', 'PythonTest')
    globs['sc'] = sc
    globs['spark'] = SparkSession(sc)
    (failure_count, test_count) = doctest.testmod(pyspark.sql.conf, globs=globs)
    globs['sc'].stop()
    if failure_count:
        exit(-1)

if __name__ == "__main__":
    _test()
