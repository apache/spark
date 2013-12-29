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
>>> conf.get("spark.appName")
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
>>> sorted(conf.getAll(), key=lambda p: p[0])
[(u'spark.executorEnv.VAR1', u'value1'), (u'spark.executorEnv.VAR3', u'value3'), (u'spark.executorEnv.VAR4', u'value4'), (u'spark.home', u'/path')]
"""


class SparkConf(object):
    def __init__(self, loadDefaults=True, _jvm=None):
        from pyspark.context import SparkContext
        SparkContext._ensure_initialized()
        _jvm = _jvm or SparkContext._jvm
        self._jconf = _jvm.SparkConf(loadDefaults)

    def set(self, key, value):
        self._jconf.set(key, value)
        return self

    def setMaster(self, value):
        self._jconf.setMaster(value)
        return self

    def setAppName(self, value):
        self._jconf.setAppName(value)
        return self

    def setSparkHome(self, value):
        self._jconf.setSparkHome(value)
        return self

    def setExecutorEnv(self, key=None, value=None, pairs=None):
        if (key != None and pairs != None) or (key == None and pairs == None):
            raise Exception("Either pass one key-value pair or a list of pairs")
        elif key != None:
            self._jconf.setExecutorEnv(key, value)
        elif pairs != None:
            for (k, v) in pairs:
                self._jconf.setExecutorEnv(k, v)
        return self

    def setAll(self, pairs):
        for (k, v) in pairs:
            self._jconf.set(k, v)
        return self

    def get(self, key):
        return self._jconf.get(key)

    def getOrElse(self, key, defaultValue):
        return self._jconf.getOrElse(key, defaultValue)

    def getAll(self):
        pairs = []
        for elem in self._jconf.getAll():
            pairs.append((elem._1(), elem._2()))
        return pairs

    def contains(self, key):
        return self._jconf.contains(key)


def _test():
    import doctest
    (failure_count, test_count) = doctest.testmod(optionflags=doctest.ELLIPSIS)
    if failure_count:
        exit(-1)


if __name__ == "__main__":
    _test()
