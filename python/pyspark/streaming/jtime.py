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

from pyspark.streaming.duration import Duration

"""
The name of this file, time is not a good naming for python
because if we do import time when we want to use native python time package, it does
not import python time package.
"""
# TODO: add doctest


class Time(object):
    """
    Time for Spark Streaming application. Used to set Time

    Most of the time, you would create a Duration object with
    C{Time()}, which will load values from C{spark.streaming.*} Java system
    properties as well. In this case, any parameters you set directly on
    the C{Time} object take priority over system properties.

    """
    def __init__(self, millis, _jvm=None):
        """
        Create new Time.

        @param millis: milisecond

        @param _jvm: internal parameter used to pass a handle to the
               Java VM; does not need to be set by users

        """
        self._millis = millis

        from pyspark.context import StreamingContext
        StreamingContext._ensure_initialized()
        _jvm = _jvm or StreamingContext._jvm
        self._jtime = _jvm.Time(millis)

    def toString(self):
        """ Return time as string """
        return str(self._millis) + " ms"

    def milliseconds(self):
        """ Return millisecond """
        return self._millis

    def max(self, other):
        """ Return higher Time """
        Time._is_time(other)
        if self > other:
            return self
        else:
            return other

    def min(self, other):
        """ Return lower Time """
        Time._is_time(other)
        if self < other:
            return self
        else:
            return other

    def __add__(self, other):
        """ Add Time and Time """
        Duration._is_duration(other)
        return Time(self._millis + other._millis)

    def __sub__(self, other):
        """ Subtract Time by Duration or Time """
        if isinstance(other, Duration):
            return Time(self._millis - other._millis)
        elif isinstance(other, Time):
            return Duration(self._millis, other._millis)
        else:
            raise TypeError

    def __lt__(self, other):
        """ Time < Time """
        Time._is_time(other)
        return self._millis < other._millis

    def __le__(self, other):
        """ Time <= Time """
        Time._is_time(other)
        return self._millis <= other._millis

    def __eq__(self, other):
        """ Time ==  Time """
        Time._is_time(other)
        return self._millis == other._millis

    def __ne__(self, other):
        """ Time != Time """
        Time._is_time(other)
        return self._millis != other._millis

    def __gt__(self, other):
        """ Time > Time """
        Time._is_time(other)
        return self._millis > other._millis

    def __ge__(self, other):
        """ Time >= Time """
        Time._is_time(other)
        return self._millis >= other._millis

    def isMultipbleOf(self, duration):
        """ is multiple by Duration """
        Duration._is_duration(duration)
        return self._millis % duration._millis == 0

    @classmethod
    def _is_time(self, instance):
        """ is instance Time """
        if not isinstance(instance, Time):
            raise TypeError

# TODO: implement until
# TODO: implement to

