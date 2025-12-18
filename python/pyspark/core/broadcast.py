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

import gc
import os
import sys
from tempfile import NamedTemporaryFile
import threading
import pickle
from typing import (
    overload,
    Any,
    BinaryIO,
    Callable,
    Dict,
    Generic,
    IO,
    Iterator,
    Optional,
    Tuple,
    TypeVar,
    TYPE_CHECKING,
    Union,
)

from pyspark.serializers import ChunkedStream, pickle_protocol
from pyspark.util import print_exec, local_connect_and_auth
from pyspark.errors import PySparkRuntimeError

if TYPE_CHECKING:
    from pyspark import SparkContext


__all__ = ["Broadcast"]

T = TypeVar("T")


# Holds broadcasted data received from Java, keyed by its id.
_broadcastRegistry: Dict[int, "Broadcast[Any]"] = {}


def _from_id(bid: int) -> "Broadcast[Any]":
    from pyspark.core.broadcast import _broadcastRegistry

    if bid not in _broadcastRegistry:
        raise PySparkRuntimeError(
            errorClass="BROADCAST_VARIABLE_NOT_LOADED",
            messageParameters={
                "variable": str(bid),
            },
        )
    return _broadcastRegistry[bid]


class Broadcast(Generic[T]):

    """
    A broadcast variable created with :meth:`SparkContext.broadcast`.
    Access its value through :attr:`value`.

    Examples
    --------
    >>> b = spark.sparkContext.broadcast([1, 2, 3, 4, 5])
    >>> b.value
    [1, 2, 3, 4, 5]
    >>> spark.sparkContext.parallelize([0, 0]).flatMap(lambda x: b.value).collect()
    [1, 2, 3, 4, 5, 1, 2, 3, 4, 5]
    >>> b.unpersist()

    >>> large_broadcast = spark.sparkContext.broadcast(range(10000))
    """

    @overload  # On driver
    def __init__(
        self: "Broadcast[T]",
        sc: "SparkContext",
        value: T,
        pickle_registry: "BroadcastPickleRegistry",
    ):
        ...

    @overload  # On worker without decryption server
    def __init__(self: "Broadcast[Any]", *, path: str):
        ...

    @overload  # On worker with decryption server
    def __init__(self: "Broadcast[Any]", *, sock_file: str):
        ...

    def __init__(  # type: ignore[misc]
        self,
        sc: Optional["SparkContext"] = None,
        value: Optional[T] = None,
        pickle_registry: Optional["BroadcastPickleRegistry"] = None,
        path: Optional[str] = None,
        sock_file: Optional[BinaryIO] = None,
    ):
        """
        Should not be called directly by users -- use :meth:`SparkContext.broadcast`
        instead.
        """
        if sc is not None:
            # we're on the driver.  We want the pickled data to end up in a file (maybe encrypted)
            f = NamedTemporaryFile(delete=False, dir=sc._temp_dir)
            self._path = f.name
            self._sc: Optional["SparkContext"] = sc
            assert sc._jvm is not None
            self._python_broadcast = sc._jvm.PythonRDD.setupBroadcast(self._path)
            broadcast_out: Union[ChunkedStream, IO[bytes]]
            if sc._encryption_enabled:
                # with encryption, we ask the jvm to do the encryption for us, we send it data
                # over a socket
                conn_info, auth_secret = self._python_broadcast.setupEncryptionServer()
                (encryption_sock_file, _) = local_connect_and_auth(conn_info, auth_secret)
                broadcast_out = ChunkedStream(encryption_sock_file, 8192)
            else:
                # no encryption, we can just write pickled data directly to the file from python
                broadcast_out = f
            self.dump(value, broadcast_out)  # type: ignore[arg-type]
            if sc._encryption_enabled:
                self._python_broadcast.waitTillDataReceived()
            self._jbroadcast = sc._jsc.broadcast(self._python_broadcast)
            self._pickle_registry = pickle_registry
        else:
            # we're on an executor
            self._jbroadcast = None
            self._sc = None
            self._python_broadcast = None
            if sock_file is not None:
                # the jvm is doing decryption for us.  Read the value
                # immediately from the sock_file
                self._value = self.load(sock_file)
            else:
                # the jvm just dumps the pickled data in path -- we'll unpickle lazily when
                # the value is requested
                assert path is not None
                self._path = path

    def dump(self, value: T, f: BinaryIO) -> None:
        """
        Write a pickled representation of value to the open file or socket.
        The protocol pickle is HIGHEST_PROTOCOL.

        Parameters
        ----------
        value : T
            Value to write.

        f : :class:`BinaryIO`
            File or socket where the pickled value will be stored.

        Examples
        --------
        >>> import os
        >>> import tempfile

        >>> b = spark.sparkContext.broadcast([1, 2, 3, 4, 5])

        Write a pickled representation of `b` to the open temp file.

        >>> with tempfile.TemporaryDirectory(prefix="dump") as d:
        ...     path = os.path.join(d, "test.txt")
        ...     with open(path, "wb") as f:
        ...         b.dump(b.value, f)
        """
        try:
            pickle.dump(value, f, pickle_protocol)
        except pickle.PickleError:
            raise
        except Exception as e:
            msg = "Could not serialize broadcast: %s: %s" % (e.__class__.__name__, str(e))
            print_exec(sys.stderr)
            raise pickle.PicklingError(msg)
        f.close()

    def load_from_path(self, path: str) -> T:
        """
        Read the pickled representation of an object from the open file and
        return the reconstituted object hierarchy specified therein.

        Parameters
        ----------
        path : str
            File path where reads the pickled value.

        Returns
        -------
        T
            The object hierarchy specified therein reconstituted
            from the pickled representation of an object.

        Examples
        --------
        >>> import os
        >>> import tempfile

        >>> b = spark.sparkContext.broadcast([1, 2, 3, 4, 5])
        >>> c = spark.sparkContext.broadcast(1)

        Read the pickled representation of value from temp file.

        >>> with tempfile.TemporaryDirectory(prefix="load_from_path") as d:
        ...     path = os.path.join(d, "test.txt")
        ...     with open(path, "wb") as f:
        ...         b.dump(b.value, f)
        ...     c.load_from_path(path)
        [1, 2, 3, 4, 5]
        """
        with open(path, "rb", 1 << 20) as f:
            return self.load(f)

    def load(self, file: BinaryIO) -> T:
        """
        Read a pickled representation of value from the open file or socket.

        Parameters
        ----------
        file : :class:`BinaryIO`
            File or socket where the pickled value will be read.

        Returns
        -------
        T
            The object hierarchy specified therein reconstituted
            from the pickled representation of an object.

        Examples
        --------
        >>> import os
        >>> import tempfile

        >>> b = spark.sparkContext.broadcast([1, 2, 3, 4, 5])
        >>> c = spark.sparkContext.broadcast(1)

        Read the pickled representation of value from the open temp file.

        >>> with tempfile.TemporaryDirectory(prefix="load") as d:
        ...     path = os.path.join(d, "test.txt")
        ...     with open(path, "wb") as f:
        ...         b.dump(b.value, f)
        ...     with open(path, "rb") as f:
        ...         c.load(f)
        [1, 2, 3, 4, 5]
        """
        gc.disable()
        try:
            return pickle.load(file)
        finally:
            gc.enable()

    @property
    def value(self) -> T:
        """Return the broadcasted value"""
        if not hasattr(self, "_value") and self._path is not None:
            # we only need to decrypt it here when encryption is enabled and
            # if its on the driver, since executor decryption is handled already
            if self._sc is not None and self._sc._encryption_enabled:
                conn_info, auth_secret = self._python_broadcast.setupDecryptionServer()
                (decrypted_sock_file, _) = local_connect_and_auth(conn_info, auth_secret)
                self._python_broadcast.waitTillBroadcastDataSent()
                return self.load(decrypted_sock_file)
            else:
                self._value = self.load_from_path(self._path)
        return self._value

    def unpersist(self, blocking: bool = False) -> None:
        """
        Delete cached copies of this broadcast on the executors. If the
        broadcast is used after this is called, it will need to be
        re-sent to each executor.

        Parameters
        ----------
        blocking : bool, optional, default False
            Whether to block until unpersisting has completed.

        Examples
        --------
        >>> b = spark.sparkContext.broadcast([1, 2, 3, 4, 5])

        Delete cached copies of this broadcast on the executors

        >>> b.unpersist()
        """
        if self._jbroadcast is None:
            raise PySparkRuntimeError(
                errorClass="INVALID_BROADCAST_OPERATION",
                messageParameters={"operation": "unpersisted"},
            )
        self._jbroadcast.unpersist(blocking)

    def destroy(self, blocking: bool = False) -> None:
        """
        Destroy all data and metadata related to this broadcast variable.
        Use this with caution; once a broadcast variable has been destroyed,
        it cannot be used again.

        .. versionchanged:: 3.0.0
           Added optional argument `blocking` to specify whether to block until all
           blocks are deleted.

        Parameters
        ----------
        blocking : bool, optional, default False
            Whether to block until unpersisting has completed.

        Examples
        --------
        >>> b = spark.sparkContext.broadcast([1, 2, 3, 4, 5])

        Destroy all data and metadata related to this broadcast variable

        >>> b.destroy()
        """
        if self._jbroadcast is None:
            raise PySparkRuntimeError(
                errorClass="INVALID_BROADCAST_OPERATION",
                messageParameters={"operation": "destroyed"},
            )
        self._jbroadcast.destroy(blocking)
        os.unlink(self._path)

    def __reduce__(self) -> Tuple[Callable[[int], "Broadcast[T]"], Tuple[int]]:
        if self._jbroadcast is None:
            raise PySparkRuntimeError(
                errorClass="INVALID_BROADCAST_OPERATION",
                messageParameters={"operation": "serialized"},
            )
        assert self._pickle_registry is not None
        self._pickle_registry.add(self)
        return _from_id, (self._jbroadcast.id(),)


class BroadcastPickleRegistry(threading.local):
    """Thread-local registry for broadcast variables that have been pickled"""

    def __init__(self) -> None:
        self.__dict__.setdefault("_registry", set())

    def __iter__(self) -> Iterator[Broadcast[Any]]:
        for bcast in self._registry:
            yield bcast

    def add(self, bcast: Broadcast[Any]) -> None:
        self._registry.add(bcast)

    def clear(self) -> None:
        self._registry.clear()


def _test() -> None:
    import doctest
    from pyspark.sql import SparkSession
    import pyspark.core.broadcast

    globs = pyspark.core.broadcast.__dict__.copy()
    spark = SparkSession.builder.master("local[4]").appName("broadcast tests").getOrCreate()
    globs["spark"] = spark

    (failure_count, test_count) = doctest.testmod(pyspark.core.broadcast, globs=globs)
    spark.stop()
    if failure_count:
        sys.exit(-1)


if __name__ == "__main__":
    _test()
