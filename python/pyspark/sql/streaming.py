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

from abc import ABCMeta, abstractmethod

from pyspark import since

__all__ = ["ContinuousQuery"]


class ContinuousQuery(object):
    """
    A handle to a query that is executing continuously in the background as new data arrives.
    All these methods are thread-safe.

    .. note:: Experimental

    .. versionadded:: 2.0
    """

    def __init__(self, jcq, sqlContext):
        self._jcq = jcq
        self._sqlContext = sqlContext

    @property
    @since(2.0)
    def name(self):
        """The name of the continuous query.
        """
        return self._jcq.name()

    @property
    @since(2.0)
    def isActive(self):
        """Whether this continuous query is currently active or not.
        """
        return self._jcq.isActive()

    @since(2.0)
    def awaitTermination(self, timeoutMs=None):
        """Waits for the termination of `this` query, either by :func:`query.stop()` or by an
        exception. If the query has terminated with an exception, then the exception will be thrown.
        If `timeoutMs` is set, it returns whether the query has terminated or not within the
        `timeoutMs` milliseconds.

        If the query has terminated, then all subsequent calls to this method will either return
        immediately (if the query was terminated by :func:`stop()`), or throw the exception
        immediately (if the query has terminated with exception).

        throws ContinuousQueryException, if `this` query has terminated with an exception
        """
        if timeoutMs is not None:
            if type(timeoutMs) != int or timeoutMs < 0:
                raise ValueError("timeoutMs must be a positive integer. Got %s" % timeoutMs)
            return self._jcq.awaitTermination(timeoutMs)
        else:
            return self._jcq.awaitTermination()

    @since(2.0)
    def processAllAvailable(self):
        """Blocks until all available data in the source has been processed an committed to the
        sink. This method is intended for testing. Note that in the case of continually arriving
        data, this method may block forever. Additionally, this method is only guaranteed to block
        until data that has been synchronously appended data to a stream source prior to invocation.
        (i.e. `getOffset` must immediately reflect the addition).
        """
        return self._jcq.processAllAvailable()

    @since(2.0)
    def stop(self):
        """Stop this continuous query.
        """
        self._jcq.stop()


class Trigger(object):
    """Used to indicate how often results should be produced by a :class:`ContinuousQuery`.

    .. note:: Experimental

    .. versionadded:: 2.0
    """

    __metaclass__ = ABCMeta

    @abstractmethod
    def _to_java_trigger(self, sqlContext):
        """Internal method to construct the trigger on the jvm.
        """
        pass


class ProcessingTime(Trigger):
    """A trigger that runs a query periodically based on the processing time. If `interval` is 0,
    the query will run as fast as possible.

    The interval should be given as a string, e.g. '2 seconds', '5 minutes', ...

    .. note:: Experimental

    .. versionadded:: 2.0
    """

    def __init__(self, interval):
        if interval is None or type(interval) != str or len(interval.strip()) == 0:
            raise ValueError("interval should be a non empty interval string, e.g. '2 seconds'.")
        self.interval = interval

    def _to_java_trigger(self, sqlContext):
        return sqlContext._sc._jvm.org.apache.spark.sql.ProcessingTime.create(self.interval)
