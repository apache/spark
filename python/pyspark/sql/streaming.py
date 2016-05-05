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
from pyspark.rdd import ignore_unicode_prefix

__all__ = ["ContinuousQuery"]


class ContinuousQuery(object):
    """
    A handle to a query that is executing continuously in the background as new data arrives.
    All these methods are thread-safe.

    .. note:: Experimental

    .. versionadded:: 2.0
    """

    def __init__(self, jcq):
        self._jcq = jcq

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
    def awaitTermination(self, timeout=None):
        """Waits for the termination of `this` query, either by :func:`query.stop()` or by an
        exception. If the query has terminated with an exception, then the exception will be thrown.
        If `timeout` is set, it returns whether the query has terminated or not within the
        `timeout` seconds.

        If the query has terminated, then all subsequent calls to this method will either return
        immediately (if the query was terminated by :func:`stop()`), or throw the exception
        immediately (if the query has terminated with exception).

        throws :class:`ContinuousQueryException`, if `this` query has terminated with an exception
        """
        if timeout is not None:
            if not isinstance(timeout, (int, float)) or timeout < 0:
                raise ValueError("timeout must be a positive integer or float. Got %s" % timeout)
            return self._jcq.awaitTermination(int(timeout * 1000))
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


class ContinuousQueryManager(object):
    """A class to manage all the :class:`ContinuousQuery` ContinuousQueries active
    on a :class:`SQLContext`.

    .. note:: Experimental

    .. versionadded:: 2.0
    """

    def __init__(self, jcqm):
        self._jcqm = jcqm

    @property
    @ignore_unicode_prefix
    @since(2.0)
    def active(self):
        """Returns a list of active queries associated with this SQLContext

        >>> cq = df.write.format('memory').queryName('this_query').startStream()
        >>> cqm = sqlContext.streams
        >>> # get the list of active continuous queries
        >>> [q.name for q in cqm.active]
        [u'this_query']
        >>> cq.stop()
        """
        return [ContinuousQuery(jcq) for jcq in self._jcqm.active()]

    @since(2.0)
    def get(self, name):
        """Returns an active query from this SQLContext or throws exception if an active query
        with this name doesn't exist.

        >>> df.write.format('memory').queryName('this_query').startStream()
        >>> cq = sqlContext.streams.get('this_query')
        >>> cq.isActive
        True
        >>> cq.stop()
        """
        if type(name) != str or len(name.strip()) == 0:
            raise ValueError("The name for the query must be a non-empty string. Got: %s" % name)
        return ContinuousQuery(self._jcqm.get(name))

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

        throws :class:`ContinuousQueryException`, if `this` query has terminated with an exception
        """
        if timeout is not None:
            if not isinstance(timeout, (int, float)) or timeout < 0:
                raise ValueError("timeout must be a positive integer or float. Got %s" % timeout)
            return self._jcqm.awaitAnyTermination(int(timeout * 1000))
        else:
            return self._jcqm.awaitAnyTermination()

    @since(2.0)
    def resetTerminated(self):
        """Forget about past terminated queries so that :func:`awaitAnyTermination()` can be used
        again to wait for new terminations.

        >>> sqlContext.streams.resetTerminated()
        """
        self._jcqm.resetTerminated()


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
        if type(interval) != str or len(interval.strip()) == 0:
            raise ValueError("interval should be a non empty interval string, e.g. '2 seconds'.")
        self.interval = interval

    def _to_java_trigger(self, sqlContext):
        return sqlContext._sc._jvm.org.apache.spark.sql.ProcessingTime.create(self.interval)


def _test():
    import doctest
    import os
    import tempfile
    from pyspark.context import SparkContext
    from pyspark.sql import Row, SQLContext, HiveContext
    import pyspark.sql.readwriter

    os.chdir(os.environ["SPARK_HOME"])

    globs = pyspark.sql.readwriter.__dict__.copy()
    sc = SparkContext('local[4]', 'PythonTest')

    globs['tempfile'] = tempfile
    globs['os'] = os
    globs['sc'] = sc
    globs['sqlContext'] = SQLContext(sc)
    globs['hiveContext'] = HiveContext._createForTesting(sc)
    globs['df'] = \
        globs['sqlContext'].read.format('text').stream('python/test_support/sql/streaming')

    (failure_count, test_count) = doctest.testmod(
        pyspark.sql.readwriter, globs=globs,
        optionflags=doctest.ELLIPSIS | doctest.NORMALIZE_WHITESPACE | doctest.REPORT_NDIFF)
    globs['sc'].stop()
    if failure_count:
        exit(-1)


if __name__ == "__main__":
    _test()
