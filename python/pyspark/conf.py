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
>>> from pyspark.conf import SparkConf
>>> from pyspark.context import SparkContext
>>> conf = SparkConf()
>>> conf.setMaster("local").setAppName("My app")
<pyspark.conf.SparkConf object at ...>
>>> conf.get("spark.master")
u'local'
>>> conf.get("spark.app.name")
u'My app'
>>> sc = SparkContext(conf=conf)
>>> sc.master
u'local'
>>> sc.appName
u'My app'
>>> sc.sparkHome == None
True

>>> conf = SparkConf()
>>> conf.setSparkHome("/path")
<pyspark.conf.SparkConf object at ...>
>>> conf.get("spark.home")
u'/path'
>>> conf.setExecutorEnv("VAR1", "value1")
<pyspark.conf.SparkConf object at ...>
>>> conf.setExecutorEnv(pairs = [("VAR3", "value3"), ("VAR4", "value4")])
<pyspark.conf.SparkConf object at ...>
>>> conf.get("spark.executorEnv.VAR1")
u'value1'
>>> print conf.toDebugString()
spark.executorEnv.VAR1=value1
spark.executorEnv.VAR3=value3
spark.executorEnv.VAR4=value4
spark.home=/path
>>> sorted(conf.getAll(), key=lambda p: p[0])
[(u'spark.executorEnv.VAR1', u'value1'), (u'spark.executorEnv.VAR3', u'value3'), (u'spark.executorEnv.VAR4', u'value4'), (u'spark.home', u'/path')]
"""


class SparkConf(object):
    """
    Configuration for a Spark application. Used to set various Spark
    parameters as key-value pairs.

    Most of the time, you would create a SparkConf object with
    C{SparkConf()}, which will load values from C{spark.*} Java system
    properties as well. In this case, any parameters you set directly on
    the C{SparkConf} object take priority over system properties.

    For unit tests, you can also call C{SparkConf(false)} to skip
    loading external settings and get the same configuration no matter
    what the system properties are.

    All setter methods in this class support chaining. For example,
    you can write C{conf.setMaster("local").setAppName("My app")}.

    Note that once a SparkConf object is passed to Spark, it is cloned
    and can no longer be modified by the user.
    """

    def __init__(self, loadDefaults=True, _jvm=None):
        """
        Create a new Spark configuration.

        @param loadDefaults: whether to load values from Java system
               properties (True by default)
        @param _jvm: internal parameter used to pass a handle to the
               Java VM; does not need to be set by users
        """
        from pyspark.context import SparkContext
        SparkContext._ensure_initialized()
        _jvm = _jvm or SparkContext._jvm
        self._jconf = _jvm.SparkConf(loadDefaults)

    def set(self, key, value):
        """Set a configuration property."""
        self._jconf.set(key, unicode(value))
        return self

    def setMaster(self, value):
        """Set master URL to connect to."""
        self._jconf.setMaster(value)
        return self

    def setAppName(self, value):
        """Set application name."""
        self._jconf.setAppName(value)
        return self

    def setSparkHome(self, value):
        """Set path where Spark is installed on worker nodes."""
        self._jconf.setSparkHome(value)
        return self

    def setExecutorEnv(self, key=None, value=None, pairs=None):
        """Set an environment variable to be passed to executors."""
        if (key != None and pairs != None) or (key == None and pairs == None):
            raise Exception("Either pass one key-value pair or a list of pairs")
        elif key != None:
            self._jconf.setExecutorEnv(key, value)
        elif pairs != None:
            for (k, v) in pairs:
                self._jconf.setExecutorEnv(k, v)
        return self

    def setAll(self, pairs):
        """
        Set multiple parameters, passed as a list of key-value pairs.

        @param pairs: list of key-value pairs to set
        """
        for (k, v) in pairs:
            self._jconf.set(k, v)
        return self

    def get(self, key, defaultValue=None):
        """Get the configured value for some key, or return a default otherwise."""
        if defaultValue == None:   # Py4J doesn't call the right get() if we pass None
            if not self._jconf.contains(key):
                return None
            return self._jconf.get(key)
        else:
            return self._jconf.get(key, defaultValue)

    def getAll(self):
        """Get all values as a list of key-value pairs."""
        pairs = []
        for elem in self._jconf.getAll():
            pairs.append((elem._1(), elem._2()))
        return pairs

    def contains(self, key):
        """Does this configuration contain a given key?"""
        return self._jconf.contains(key)

    def toDebugString(self):
        """
        Returns a printable version of the configuration, as a list of
        key=value pairs, one per line.
        """
        return self._jconf.toDebugString()


def _test():
    import doctest
    (failure_count, test_count) = doctest.testmod(optionflags=doctest.ELLIPSIS)
    if failure_count:
        exit(-1)


if __name__ == "__main__":
    _test()
