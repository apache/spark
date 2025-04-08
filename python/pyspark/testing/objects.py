#
# Licensed to the Apache Software Foundation (ASF) under one or more
# contributor license agreements.  See the NOTICE file distributed with
# this work for additional information regarding copyright ownership.
# The ASF licenses this file to You under the Apache License, Version 2.0
# (the "License"); you may not use this file except in compliance with
# the License.  You may obtain a copy of the License at
#
# http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#
import datetime

from pyspark.sql.types import ArrayType, DoubleType, UserDefinedType


class UTCOffsetTimezone(datetime.tzinfo):
    """
    Specifies timezone in UTC offset
    """

    def __init__(self, offset=0):
        self.ZERO = datetime.timedelta(hours=offset)

    def utcoffset(self, dt):
        return self.ZERO

    def dst(self, dt):
        return self.ZERO


class ExamplePointUDT(UserDefinedType):
    """
    User-defined type (UDT) for ExamplePoint.
    """

    @classmethod
    def sqlType(cls):
        return ArrayType(DoubleType(), False)

    @classmethod
    def module(cls):
        return "pyspark.sql.tests"

    @classmethod
    def scalaUDT(cls):
        return "org.apache.spark.sql.test.ExamplePointUDT"

    def serialize(self, obj):
        return [obj.x, obj.y]

    def deserialize(self, datum):
        return ExamplePoint(datum[0], datum[1])


class ExamplePoint:
    """
    An example class to demonstrate UDT in Scala, Java, and Python.
    """

    __UDT__ = ExamplePointUDT()

    def __init__(self, x, y):
        self.x = x
        self.y = y

    def __repr__(self):
        return "ExamplePoint(%s,%s)" % (self.x, self.y)

    def __str__(self):
        return "(%s,%s)" % (self.x, self.y)

    def __eq__(self, other):
        return isinstance(other, self.__class__) and other.x == self.x and other.y == self.y


class PythonOnlyUDT(UserDefinedType):
    """
    User-defined type (UDT) for ExamplePoint.
    """

    @classmethod
    def sqlType(cls):
        return ArrayType(DoubleType(), False)

    @classmethod
    def module(cls):
        return "__main__"

    def serialize(self, obj):
        return [obj.x, obj.y]

    def deserialize(self, datum):
        return PythonOnlyPoint(datum[0], datum[1])

    @staticmethod
    def foo():
        pass

    @property
    def props(self):
        return {}


class PythonOnlyPoint(ExamplePoint):
    """
    An example class to demonstrate UDT in only Python
    """

    __UDT__ = PythonOnlyUDT()  # type: ignore


class MyObject:
    def __init__(self, key, value):
        self.key = key
        self.value = value
