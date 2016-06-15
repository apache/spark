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

import sys
if sys.version >= '3':
    intlike = int
else:
    intlike = (int, long)

from abc import ABCMeta, abstractmethod

from pyspark import since
from pyspark.rdd import ignore_unicode_prefix

__all__ = ["StreamingQuery"]


class StreamingQuery(object):
    """
    A handle to a query that is executing continuously in the background as new data arrives.
    All these methods are thread-safe.

    .. note:: Experimental

    .. versionadded:: 2.0
    """

    def __init__(self, jsq):
        self._jsq = jsq

    @property
    @since(2.0)
    def id(self):
        """The id of the streaming query. This id is unique across all queries that have been
        started in the current process.
        """
        return self._jsq.id()

    @property
    @since(2.0)
    def name(self):
        """The name of the streaming query. This name is unique across all active queries.
        """
        return self._jsq.name()

    @property
    @since(2.0)
    def isActive(self):
        """Whether this streaming query is currently active or not.
        """
        return self._jsq.isActive()

    @since(2.0)
    def awaitTermination(self, timeout=None):
        """Waits for the termination of `this` query, either by :func:`query.stop()` or by an
        exception. If the query has terminated with an exception, then the exception will be thrown.
        If `timeout` is set, it returns whether the query has terminated or not within the
        `timeout` seconds.

        If the query has terminated, then all subsequent calls to this method will either return
        immediately (if the query was terminated by :func:`stop()`), or throw the exception
        immediately (if the query has terminated with exception).

        throws :class:`StreamingQueryException`, if `this` query has terminated with an exception
        """
        if timeout is not None:
            if not isinstance(timeout, (int, float)) or timeout < 0:
                raise ValueError("timeout must be a positive integer or float. Got %s" % timeout)
            return self._jsq.awaitTermination(int(timeout * 1000))
        else:
            return self._jsq.awaitTermination()

    @since(2.0)
    def processAllAvailable(self):
        """Blocks until all available data in the source has been processed and committed to the
        sink. This method is intended for testing. Note that in the case of continually arriving
        data, this method may block forever. Additionally, this method is only guaranteed to block
        until data that has been synchronously appended data to a stream source prior to invocation.
        (i.e. `getOffset` must immediately reflect the addition).
        """
        return self._jsq.processAllAvailable()

    @since(2.0)
    def stop(self):
        """Stop this streaming query.
        """
        self._jsq.stop()


class StreamingQueryManager(object):
    """A class to manage all the :class:`StreamingQuery` StreamingQueries active.

    .. note:: Experimental

    .. versionadded:: 2.0
    """

    def __init__(self, jsqm):
        self._jsqm = jsqm

    @property
    @ignore_unicode_prefix
    @since(2.0)
    def active(self):
        """Returns a list of active queries associated with this SQLContext

        >>> sq = df.writeStream.format('memory').queryName('this_query').start()
        >>> sqm = spark.streams
        >>> # get the list of active streaming queries
        >>> [q.name for q in sqm.active]
        [u'this_query']
        >>> sq.stop()
        """
        return [StreamingQuery(jsq) for jsq in self._jsqm.active()]

    @ignore_unicode_prefix
    @since(2.0)
    def get(self, id):
        """Returns an active query from this SQLContext or throws exception if an active query
        with this name doesn't exist.

        >>> sq = df.writeStream.format('memory').queryName('this_query').start()
        >>> sq.name
        u'this_query'
        >>> sq = spark.streams.get(sq.id)
        >>> sq.isActive
        True
        >>> sq = sqlContext.streams.get(sq.id)
        >>> sq.isActive
        True
        >>> sq.stop()
        """
        if not isinstance(id, intlike):
            raise ValueError("The id for the query must be an integer. Got: %s" % id)
        return StreamingQuery(self._jsqm.get(id))

    @since(2.0)
    def awaitAnyTermination(self, timeout=None):
        """Wait until any of the queries on the associated SQLContext has terminated since the
        creation of the context, or since :func:`resetTerminated()` was called. If any query was
        terminated with an exception, then the exception will be thrown.
        If `timeout` is set, it returns whether the query has terminated or not within the
        `timeout` seconds.

        If a query has terminated, then subsequent calls to :func:`awaitAnyTermination()` will
        either return immediately (if the query was terminated by :func:`query.stop()`),
        or throw the exception immediately (if the query was terminated with exception). Use
        :func:`resetTerminated()` to clear past terminations and wait for new terminations.

        In the case where multiple queries have terminated since :func:`resetTermination()`
        was called, if any query has terminated with exception, then :func:`awaitAnyTermination()`
        will throw any of the exception. For correctly documenting exceptions across multiple
        queries, users need to stop all of them after any of them terminates with exception, and
        then check the `query.exception()` for each query.

        throws :class:`StreamingQueryException`, if `this` query has terminated with an exception
        """
        if timeout is not None:
            if not isinstance(timeout, (int, float)) or timeout < 0:
                raise ValueError("timeout must be a positive integer or float. Got %s" % timeout)
            return self._jsqm.awaitAnyTermination(int(timeout * 1000))
        else:
            return self._jsqm.awaitAnyTermination()

    @since(2.0)
    def resetTerminated(self):
        """Forget about past terminated queries so that :func:`awaitAnyTermination()` can be used
        again to wait for new terminations.

        >>> spark.streams.resetTerminated()
        """
        self._jsqm.resetTerminated()


class Trigger(object):
    """Used to indicate how often results should be produced by a :class:`StreamingQuery`.

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
        if type(interval) != str or len(interval.strip()) == 0:
            raise ValueError("interval should be a non empty interval string, e.g. '2 seconds'.")
        self.interval = interval

    def _to_java_trigger(self, sqlContext):
        return sqlContext._sc._jvm.org.apache.spark.sql.streaming.ProcessingTime.create(
            self.interval)


def _test():
    import doctest
    import os
    import tempfile
    from pyspark.sql import Row, SparkSession, SQLContext
    import pyspark.sql.streaming

    os.chdir(os.environ["SPARK_HOME"])

    globs = pyspark.sql.streaming.__dict__.copy()
    try:
        spark = SparkSession.builder.getOrCreate()
    except py4j.protocol.Py4JError:
        spark = SparkSession(sc)

    globs['tempfile'] = tempfile
    globs['os'] = os
    globs['spark'] = spark
    globs['sqlContext'] = SQLContext.getOrCreate(spark.sparkContext)
    globs['df'] = \
        globs['spark'].readStream.format('text').load('python/test_support/sql/streaming')

    (failure_count, test_count) = doctest.testmod(
        pyspark.sql.streaming, globs=globs,
        optionflags=doctest.ELLIPSIS | doctest.NORMALIZE_WHITESPACE | doctest.REPORT_NDIFF)
    globs['spark'].stop()
    if failure_count:
        exit(-1)


if __name__ == "__main__":
    _test()
