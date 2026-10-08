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


from typing import Any

from pyspark.sql import Column
from pyspark.sql import functions as F


class _ClassProperty:
    """Custom read-only class property descriptor.

    The underlying method should take the class as the sole argument.
    """

    def __init__(self, f: Any) -> None:
        self.f = f
        self.__doc__ = f.__doc__

    def __get__(self, instance: Any, owner: type) -> Any:
        return self.f(owner)


class AggregateMessages:
    """Collection of utilities usable with :meth:`graphframes.GraphFrame.aggregateMessages()`."""

    @_ClassProperty
    def src(cls) -> Column:
        """Reference for source column, used for specifying messages."""
        return F.col("src")

    @_ClassProperty
    def dst(cls) -> Column:
        """Reference for destination column, used for specifying messages."""
        return F.col("dst")

    @_ClassProperty
    def edge(cls) -> Column:
        """Reference for edge column, used for specifying messages."""
        return F.col("edge")

    @_ClassProperty
    def msg(cls) -> Column:
        """Reference for message column, used for specifying aggregation function."""
        return F.col("MSG")
