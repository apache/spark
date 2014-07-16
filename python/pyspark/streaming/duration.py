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

from pyspark.streaming import utils

class Duration(object):
    """
    Duration for Spark Streaming application. Used to set duration

    Most of the time, you would create a Duration object with
    C{Duration()}, which will load values from C{spark.streaming.*} Java system
    properties as well. In this case, any parameters you set directly on
    the C{Duration} object take priority over system properties.

    """
    def __init__(self, millis, _jvm=None):
        """
        Create new Duration.

        @param millis: milisecond

        """
        self._millis = millis

        from pyspark.context import SparkContext
        SparkContext._ensure_initialized()
        _jvm = _jvm or SparkContext._jvm
        self._jduration = _jvm.Duration(millis)

    def toString(self):
        """ Return duration as string """
        return str(self._millis) + " ms"

    def isZero(self):
        """ Check if millis is zero """
        return self._millis == 0

    def prettyPrint(self):
        """
        Return a human-readable string representing a duration
        """
        return utils.msDurationToString(self._millis)

    def milliseconds(self):
        """ Return millisecond """
        return self._millis

    def toFormattedString(self):
        """ Return millisecond """
        return str(self._millis)

    def max(self, other):
        """ Return higher Duration """
        Duration._is_duration(other)
        if self > other:
            return self
        else:
            return other

    def min(self, other):
        """ Return lower Durattion """
        Duration._is_duration(other)
        if self < other:
            return self
        else:
            return other

    def __str__(self):
        return self.toString()

    def __add__(self, other):
        """ Add Duration and Duration """
        Duration._is_duration(other)
        return Duration(self._millis + other._millis)

    def __sub__(self, other):
        """ Subtract Duration by Duration  """
        Duration._is_duration(other)
        return Duration(self._millis - other._millis)

    def __mul__(self, other):
        """ Multiple Duration by Duration """
        Duration._is_duration(other)
        return Duration(self._millis * other._millis)

    def __div__(self, other):
        """
        Divide Duration by Duration
        for Python 2.X
        """
        Duration._is_duration(other)
        return Duration(self._millis / other._millis)

    def __truediv__(self, other):
        """
        Divide Duration by Duration
        for Python 3.0
        """
        Duration._is_duration(other)
        return Duration(self._millis / other._millis)

    def __floordiv__(self, other):
        """ Divide Duration by Duration """
        Duration._is_duration(other)
        return Duration(self._millis // other._millis)

    def __len__(self):
        """ Length of miilisecond in Duration """
        return len(self._millis)

    def __lt__(self, other):
        """ Duration < Duration """
        Duration._is_duration(other)
        return self._millis < other._millis

    def __le__(self, other):
        """ Duration <= Duration """
        Duration._is_duration(other)
        return self.millis <= other._millis

    def __eq__(self, other):
        """ Duration ==  Duration """
        Duration._is_duration(other)
        return self._millis == other._millis

    def __ne__(self, other):
        """ Duration != Duration """
        Duration._is_duration(other)
        return self._millis != other._millis

    def __gt__(self, other):
        """ Duration > Duration """
        Duration._is_duration(other)
        return self._millis > other._millis

    def __ge__(self, other):
        """ Duration >= Duration """
        Duration._is_duration(other)
        return self._millis >= other._millis

    @classmethod
    def _is_duration(self, instance):
        """ is instance Duration """
        if not isinstance(instance, Duration):
            raise TypeError("This should be Duration")

def Milliseconds(milliseconds):
    """
    Helper function that creates instance of [[pysparkstreaming.duration]] representing
    a given number of milliseconds.
    """
    return Duration(milliseconds)

def Seconds(seconds):
    """
    Helper function that creates instance of [[pysparkstreaming.duration]] representing
    a given number of seconds.
    """
    return Duration(seconds * 1000)

def Minites(minites):
    """
    Helper function that creates instance of [[pysparkstreaming.duration]] representing
    a given number of minutes.
    """
    return Duration(minutes * 60000)

if __name__ == "__main__":
    d = Duration(1)
    print d
    print d.milliseconds()

