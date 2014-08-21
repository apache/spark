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

from pyspark.streaming import util


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
        """
        Return duration as string

        >>> d_10 = Duration(10)
        >>> d_10.toString()
        '10 ms'
        """
        return str(self._millis) + " ms"

    def isZero(self):
        """
        Check if millis is zero

        >>> d_10 = Duration(10)
        >>> d_10.isZero()
        False
        >>> d_0 = Duration(0)
        >>> d_0.isZero()
        True
        """
        return self._millis == 0

    def prettyPrint(self):
        """
        Return a human-readable string representing a duration

        >>> d_10 = Duration(10)
        >>> d_10.prettyPrint()
        '10 ms'
        >>> d_1sec = Duration(1000)
        >>> d_1sec.prettyPrint()
        '1.0 s'
        >>> d_1min = Duration(60 * 1000)
        >>> d_1min.prettyPrint()
        '1.0 m'
        >>> d_1hour = Duration(60 * 60 * 1000)
        >>> d_1hour.prettyPrint()
        '1.00 h'
        """
        return util.msDurationToString(self._millis)

    def milliseconds(self):
        """
        Return millisecond

        >>> d_10 = Duration(10)
        >>> d_10.milliseconds()
        10

        """
        return self._millis

    def toFormattedString(self):
        """
        Return millisecond

        >>> d_10 = Duration(10)
        >>> d_10.toFormattedString()
        '10'

        """
        return str(self._millis)

    def max(self, other):
        """
        Return higher Duration

        >>> d_10 = Duration(10)
        >>> d_100 = Duration(100)
        >>> d_max = d_10.max(d_100)
        >>> print d_max
        100 ms

        """
        Duration._is_duration(other)
        if self > other:
            return self
        else:
            return other

    def min(self, other):
        """
        Return lower Durattion

        >>> d_10 = Duration(10)
        >>> d_100 = Duration(100)
        >>> d_min = d_10.min(d_100)
        >>> print d_min
        10 ms

        """
        Duration._is_duration(other)
        if self < other:
            return self
        else:
            return other

    def __str__(self):
        """
        >>> d_10 = Duration(10)
        >>> str(d_10)
        '10 ms'

        """
        return self.toString()

    def __add__(self, other):
        """
        Add Duration and Duration

        >>> d_10 = Duration(10)
        >>> d_100 = Duration(100)
        >>> d_110 = d_10 + d_100
        >>> print d_110
        110 ms
        """
        Duration._is_duration(other)
        return Duration(self._millis + other._millis)

    def __sub__(self, other):
        """
        Subtract Duration by Duration

        >>> d_10 = Duration(10)
        >>> d_100 = Duration(100)
        >>> d_90 =  d_100 - d_10
        >>> print d_90
        90 ms

        """
        Duration._is_duration(other)
        return Duration(self._millis - other._millis)

    def __mul__(self, other):
        """
        Multiple Duration by Duration

        >>> d_10 = Duration(10)
        >>> d_100 = Duration(100)
        >>> d_1000 = d_10 * d_100
        >>> print d_1000
        1000 ms

        """
        Duration._is_duration(other)
        return Duration(self._millis * other._millis)

    def __div__(self, other):
        """
        Divide Duration by Duration
        for Python 2.X

        >>> d_10 = Duration(10)
        >>> d_20 = Duration(20)
        >>> d_2 = d_20 / d_10
        >>> print d_2
        2 ms

        """
        Duration._is_duration(other)
        return Duration(self._millis / other._millis)

    def __truediv__(self, other):
        """
        Divide Duration by Duration
        for Python 3.0

        >>> d_10 = Duration(10)
        >>> d_20 = Duration(20)
        >>> d_2 = d_20 / d_10
        >>> print d_2
        2 ms

        """
        Duration._is_duration(other)
        return Duration(self._millis / other._millis)

    def __floordiv__(self, other):
        """
        Divide Duration by Duration

        >>> d_10 = Duration(10)
        >>> d_3 = Duration(3)
        >>> d_3 = d_10 // d_3
        >>> print d_3
        3 ms

        """
        Duration._is_duration(other)
        return Duration(self._millis // other._millis)

    def __lt__(self, other):
        """
        Duration < Duration

        >>> d_10 = Duration(10)
        >>> d_20 = Duration(20)
        >>> d_10 < d_20
        True
        >>> d_20 < d_10
        False

        """
        Duration._is_duration(other)
        return self._millis < other._millis

    def __le__(self, other):
        """
        Duration <= Duration

        >>> d_10 = Duration(10)
        >>> d_20 = Duration(20)
        >>> d_10 <= d_20
        True
        >>> d_20 <= d_10
        False

        """
        Duration._is_duration(other)
        return self._millis <= other._millis

    def __eq__(self, other):
        """
        Duration ==  Duration

        >>> d_10 = Duration(10)
        >>> d_20 = Duration(20)
        >>> d_10 == d_20
        False
        >>> other_d_10 = Duration(10)
        >>> d_10 == other_d_10
        True

        """
        Duration._is_duration(other)
        return self._millis == other._millis

    def __ne__(self, other):
        """
        Duration != Duration

        >>> d_10 = Duration(10)
        >>> d_20 = Duration(20)
        >>> d_10 != d_20
        True
        >>> other_d_10 = Duration(10)
        >>> d_10 != other_d_10
        False

        """
        Duration._is_duration(other)
        return self._millis != other._millis

    def __gt__(self, other):
        """
        Duration > Duration

        >>> d_10 = Duration(10)
        >>> d_20 = Duration(20)
        >>> d_10 > d_20
        False
        >>> d_20 > d_10
        True

        """
        Duration._is_duration(other)
        return self._millis > other._millis

    def __ge__(self, other):
        """
        Duration >= Duration

        >>> d_10 = Duration(10)
        >>> d_20 = Duration(20)
        >>> d_10 < d_20
        True
        >>> d_20 < d_10
        False


        """
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

    >>> milliseconds = Milliseconds(1)
    >>> d_1 = Duration(1)
    >>> milliseconds == d_1
    True

    """
    return Duration(milliseconds)


def Seconds(seconds):
    """
    Helper function that creates instance of [[pysparkstreaming.duration]] representing
    a given number of seconds.

    >>> seconds = Seconds(1)
    >>> d_1sec = Duration(1000)
    >>> seconds == d_1sec
    True

    """
    return Duration(seconds * 1000)


def Minutes(minutes):
    """
    Helper function that creates instance of [[pysparkstreaming.duration]] representing
    a given number of minutes.

    >>> minutes = Minutes(1)
    >>> d_1min = Duration(60 * 1000)
    >>> minutes == d_1min
    True

    """
    return Duration(minutes * 60 * 1000)
