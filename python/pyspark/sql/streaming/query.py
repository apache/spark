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

import json
import sys
from typing import Any, Dict, List, Optional

from py4j.java_gateway import JavaObject

from pyspark import since
from pyspark.sql.utils import StreamingQueryException


__all__ = ["StreamingQuery", "StreamingQueryManager"]


class StreamingQuery:
    """
    A handle to a query that is executing continuously in the background as new data arrives.
    All these methods are thread-safe.

    .. versionadded:: 2.0.0

    Notes
    -----
    This API is evolving.
    """

    def __init__(self, jsq: JavaObject) -> None:
        self._jsq = jsq

    @property  # type: ignore[misc]
    @since(2.0)
    def id(self) -> str:
        """Returns the unique id of this query that persists across restarts from checkpoint data.
        That is, this id is generated when a query is started for the first time, and
        will be the same every time it is restarted from checkpoint data.
        There can only be one query with the same id active in a Spark cluster.
        Also see, `runId`.
        """
        return self._jsq.id().toString()

    @property  # type: ignore[misc]
    @since(2.1)
    def runId(self) -> str:
        """Returns the unique id of this query that does not persist across restarts. That is, every
        query that is started (or restarted from checkpoint) will have a different runId.
        """
        return self._jsq.runId().toString()

    @property  # type: ignore[misc]
    @since(2.0)
    def name(self) -> str:
        """Returns the user-specified name of the query, or null if not specified.
        This name can be specified in the `org.apache.spark.sql.streaming.DataStreamWriter`
        as `dataframe.writeStream.queryName("query").start()`.
        This name, if set, must be unique across all active queries.
        """
        return self._jsq.name()

    @property  # type: ignore[misc]
    @since(2.0)
    def isActive(self) -> bool:
        """Whether this streaming query is currently active or not."""
        return self._jsq.isActive()

    @since(2.0)
    def awaitTermination(self, timeout: Optional[int] = None) -> Optional[bool]:
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

    @property  # type: ignore[misc]
    @since(2.1)
    def status(self) -> Dict[str, Any]:
        """
        Returns the current status of the query.
        """
        return json.loads(self._jsq.status().json())

    @property  # type: ignore[misc]
    @since(2.1)
    def recentProgress(self) -> List[Dict[str, Any]]:
        """Returns an array of the most recent [[StreamingQueryProgress]] updates for this query.
        The number of progress updates retained for each stream is configured by Spark session
        configuration `spark.sql.streaming.numRecentProgressUpdates`.
        """
        return [json.loads(p.json()) for p in self._jsq.recentProgress()]

    @property
    def lastProgress(self) -> Optional[Dict[str, Any]]:
        """
        Returns the most recent :class:`StreamingQueryProgress` update of this streaming query or
        None if there were no progress updates

        .. versionadded:: 2.1.0

        Returns
        -------
        dict
        """
        lastProgress = self._jsq.lastProgress()
        if lastProgress:
            return json.loads(lastProgress.json())
        else:
            return None

    def processAllAvailable(self) -> None:
        """Blocks until all available data in the source has been processed and committed to the
        sink. This method is intended for testing.

        .. versionadded:: 2.0.0

        Notes
        -----
        In the case of continually arriving data, this method may block forever.
        Additionally, this method is only guaranteed to block until data that has been
        synchronously appended data to a stream source prior to invocation.
        (i.e. `getOffset` must immediately reflect the addition).
        """
        return self._jsq.processAllAvailable()

    @since(2.0)
    def stop(self) -> None:
        """Stop this streaming query."""
        self._jsq.stop()

    def explain(self, extended: bool = False) -> None:
        """Prints the (logical and physical) plans to the console for debugging purpose.

        .. versionadded:: 2.1.0

        Parameters
        ----------
        extended : bool, optional
            default ``False``. If ``False``, prints only the physical plan.

        Examples
        --------
        >>> sq = sdf.writeStream.format('memory').queryName('query_explain').start()
        >>> sq.processAllAvailable() # Wait a bit to generate the runtime plans.
        >>> sq.explain()
        == Physical Plan ==
        ...
        >>> sq.explain(True)
        == Parsed Logical Plan ==
        ...
        == Analyzed Logical Plan ==
        ...
        == Optimized Logical Plan ==
        ...
        == Physical Plan ==
        ...
        >>> sq.stop()
        """
        # Cannot call `_jsq.explain(...)` because it will print in the JVM process.
        # We should print it in the Python process.
        print(self._jsq.explainInternal(extended))

    def exception(self) -> Optional[StreamingQueryException]:
        """
        .. versionadded:: 2.1.0

        Returns
        -------
        :class:`StreamingQueryException`
            the StreamingQueryException if the query was terminated by an exception, or None.
        """
        if self._jsq.exception().isDefined():
            je = self._jsq.exception().get()
            msg = je.toString().split(": ", 1)[1]  # Drop the Java StreamingQueryException type info
            stackTrace = "\n\t at ".join(map(lambda x: x.toString(), je.getStackTrace()))
            return StreamingQueryException(msg, stackTrace, je.getCause())
        else:
            return None


class StreamingQueryManager:
    """A class to manage all the :class:`StreamingQuery` StreamingQueries active.

    .. versionadded:: 2.0.0

    Notes
    -----
    This API is evolving.
    """

    def __init__(self, jsqm: JavaObject) -> None:
        self._jsqm = jsqm

    @property
    def active(self) -> List[StreamingQuery]:
        """Returns a list of active queries associated with this SQLContext

        .. versionadded:: 2.0.0

        Examples
        --------
        >>> sq = sdf.writeStream.format('memory').queryName('this_query').start()
        >>> sqm = spark.streams
        >>> # get the list of active streaming queries
        >>> [q.name for q in sqm.active]
        ['this_query']
        >>> sq.stop()
        """
        return [StreamingQuery(jsq) for jsq in self._jsqm.active()]

    def get(self, id: str) -> StreamingQuery:
        """Returns an active query from this SQLContext or throws exception if an active query
        with this name doesn't exist.

        .. versionadded:: 2.0.0

        Examples
        --------
        >>> sq = sdf.writeStream.format('memory').queryName('this_query').start()
        >>> sq.name
        'this_query'
        >>> sq = spark.streams.get(sq.id)
        >>> sq.isActive
        True
        >>> sq = sqlContext.streams.get(sq.id)
        >>> sq.isActive
        True
        >>> sq.stop()
        """
        return StreamingQuery(self._jsqm.get(id))

    @since(2.0)
    def awaitAnyTermination(self, timeout: Optional[int] = None) -> Optional[bool]:
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

    def resetTerminated(self) -> None:
        """Forget about past terminated queries so that :func:`awaitAnyTermination()` can be used
        again to wait for new terminations.

        .. versionadded:: 2.0.0

        Examples
        --------
        >>> spark.streams.resetTerminated()
        """
        self._jsqm.resetTerminated()


def _test() -> None:
    import doctest
    import os
    from pyspark.sql import SparkSession, SQLContext
    import pyspark.sql.streaming.query
    from py4j.protocol import Py4JError

    os.chdir(os.environ["SPARK_HOME"])

    globs = pyspark.sql.streaming.query.__dict__.copy()
    try:
        spark = SparkSession._getActiveSessionOrCreate()
    except Py4JError:  # noqa: F821
        spark = SparkSession(sc)  # type: ignore[name-defined] # noqa: F821

    globs["spark"] = spark
    globs["sqlContext"] = SQLContext.getOrCreate(spark.sparkContext)
    globs["sdf"] = spark.readStream.format("text").load("python/test_support/sql/streaming")

    (failure_count, test_count) = doctest.testmod(
        pyspark.sql.streaming.query,
        globs=globs,
        optionflags=doctest.ELLIPSIS | doctest.NORMALIZE_WHITESPACE | doctest.REPORT_NDIFF,
    )
    globs["spark"].stop()

    if failure_count:
        sys.exit(-1)


if __name__ == "__main__":
    _test()
