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
'local'
>>> conf.get("spark.app.name")
'My app'
>>> sc = SparkContext(conf=conf)
>>> sc.master
u'local'
>>> sc.appName
u'My app'
>>> sc.sparkHome is None
True

>>> conf = SparkConf(loadDefaults=False)
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
>>> print(conf.toDebugString())
spark.executorEnv.VAR1=value1
spark.executorEnv.VAR3=value3
spark.executorEnv.VAR4=value4
spark.home=/path
>>> sorted(conf.getAll(), key=lambda p: p[0])
[(u'spark.executorEnv.VAR1', u'value1'), (u'spark.executorEnv.VAR3', u'value3'), \
(u'spark.executorEnv.VAR4', u'value4'), (u'spark.home', u'/path')]
"""

__all__ = ['SparkConf']

import sys
import re

if sys.version > '3':
    unicode = str
    __doc__ = re.sub(r"(\W|^)[uU](['])", r'\1\2', __doc__)


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

    def __init__(self, loadDefaults=True, _jvm=None, _jconf=None):
        """
        Create a new Spark configuration.

        :param loadDefaults: whether to load values from Java system
               properties (True by default)
        :param _jvm: internal parameter used to pass a handle to the
               Java VM; does not need to be set by users
        :param _jconf: Optionally pass in an existing SparkConf handle
               to use its parameters
        """
        if _jconf:
            self._jconf = _jconf
        else:
            from pyspark.context import SparkContext
            _jvm = _jvm or SparkContext._jvm

            if _jvm:
                # JVM is created, so create self._jconf directly through JVM
                self._jconf = _jvm.SparkConf(loadDefaults)
            else:
                # JVM is not created, so store data in self._conf first
                self._jconf = None
                self._conf = {}
                self.loadDefaults = loadDefaults

    def _set_jvm(self, _jvm):
        # JVM is created so we switch from self._conf to self._jconf
        # Because self._conf has already been pass to JVM so self._jconf will be created properly.
        self._jconf = _jvm.SparkConf(self.loadDefaults)
        self._conf.clear()

    def set(self, key, value):
        """Set a configuration property."""
        # Try to set self._jconf first if JVM is created, set self._conf is JVM is not created yet.
        if self._jconf:
            self._jconf.set(key, unicode(value))
        else:
            # Don't use unicode for self._conf, otherwise we will get exception when launching jvm.
            self._conf[key] = value
        return self

    def setIfMissing(self, key, value):
        """Set a configuration property, if not already set."""
        if self.get(key) is None:
            self.set(key, value)
        return self

    def setMaster(self, value):
        """Set master URL to connect to."""
        self.set("spark.master", value)
        return self

    def setAppName(self, value):
        """Set application name."""
        self.set("spark.app.name", value)
        return self

    def setSparkHome(self, value):
        """Set path where Spark is installed on worker nodes."""
        self.set("spark.home", value)
        return self

    def setExecutorEnv(self, key=None, value=None, pairs=None):
        """Set an environment variable to be passed to executors."""
        if (key is not None and pairs is not None) or (key is None and pairs is None):
            raise Exception("Either pass one key-value pair or a list of pairs")
        elif key is not None:
            self.set("spark.executorEnv." + key, value)
        elif pairs is not None:
            for (k, v) in pairs:
                self.set("spark.executorEnv." + k, v)
        return self

    def setAll(self, pairs):
        """
        Set multiple parameters, passed as a list of key-value pairs.

        :param pairs: list of key-value pairs to set
        """
        for (k, v) in pairs:
            self.set(k, v)
        return self

    def get(self, key, defaultValue=None):
        """Get the configured value for some key, or return a default otherwise."""
        if defaultValue is None:   # Py4J doesn't call the right get() if we pass None
            if self._jconf:
                if not self._jconf.contains(key):
                    return None
                return self._jconf.get(key)
            else:
                if key not in self._conf:
                    return None
                return self._conf[key]
        else:
            if self._jconf:
                return self._jconf.get(key, defaultValue)
            else:
                return self._conf.get(key, defaultValue)

    def getAll(self):
        """Get all values as a list of key-value pairs."""
        pairs = []
        if self._jconf:
            for elem in self._jconf.getAll():
                pairs.append((elem._1(), elem._2()))
        else:
            for k, v in self._conf.items():
                pairs.append((k, v))
        return pairs

    def contains(self, key):
        """Does this configuration contain a given key?"""
        if self._jconf:
            return self._jconf.contains(key)
        else:
            return key in self._conf

    def toDebugString(self):
        """
        Returns a printable version of the configuration, as a list of
        key=value pairs, one per line.
        """
        if self._jconf:
            return self._jconf.toDebugString()
        else:
            return '\n'.join('%s=%s' % (k, v) for k, v in self._conf.items())


def _test():
    import doctest
    (failure_count, test_count) = doctest.testmod(optionflags=doctest.ELLIPSIS)
    if failure_count:
        exit(-1)


if __name__ == "__main__":
    _test()
