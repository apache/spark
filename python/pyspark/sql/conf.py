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


class RuntimeConfig(object):
    """User-facing configuration API, accessible through `SparkSession.conf`.

    Options set here are automatically propagated to the Hadoop configuration during I/O.
    This is modeled after its Scala implementation org.apache.spark.sql.RuntimeConfig.

    :param jconf: The JVM RuntimeConfig object.
    """

    def __init__(self, jconf):
        self._jconf = jconf

    @since(2.0)
    def set(self, key, value):
        """Sets the given Spark runtime configuration property."""
        self._jconf.set(key, value)

    @since(2.0)
    def get(self, key):
        """Returns the value of Spark runtime configuration property for the given key,
        assuming it is set.
        """
        return self._jconf.get(key)

    @since(2.0)
    def getOption(self, key):
        """Returns the value of Spark runtime configuration property for the given key,
        or None if it is not set.
        """
        iter = self._jconf.getOption("x").iterator()
        if iter.hasNext():
            return iter.next()
        else:
            return None

    @since(2.0)
    def unset(self, key):
        """Resets the configuration property for the given key."""
        self._jconf.unset(key)
