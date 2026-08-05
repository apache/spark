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
import struct
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


_BROADCAST_FORMAT_MAGIC = b"PYSPARK_BROADCAST_V1"
_PICKLE_SERIALIZATION = b"P"
_ARROW_TABLE_SERIALIZATION = b"T"
_ARROW_RECORD_BATCH_SERIALIZATION = b"R"
_ARROW_ARRAY_SERIALIZATION = b"A"
_ARROW_CHUNKED_ARRAY_SERIALIZATION = b"C"
_CUSTOM_ARROW_SERIALIZATION = b"O"
_ARROW_COLUMN_NAME = "_value"
_ARROW_RECONSTRUCTOR_LENGTH_SIZE = 4


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
        use_arrow: bool = False,
    ): ...

    @overload  # On worker without decryption server
    def __init__(self: "Broadcast[Any]", *, path: str): ...

    @overload  # On worker with decryption server
    def __init__(self: "Broadcast[Any]", *, sock_file: str): ...

    def __init__(  # type: ignore[misc]
        self,
        sc: Optional["SparkContext"] = None,
        value: Optional[T] = None,
        pickle_registry: Optional["BroadcastPickleRegistry"] = None,
        path: Optional[str] = None,
        sock_file: Optional[BinaryIO] = None,
        use_arrow: bool = False,
    ):
        """
        Should not be called directly by users -- use :meth:`SparkContext.broadcast`
        instead.
        """
        if sc is not None:
            # We're on the driver. Write the serialized data to a file (maybe encrypted).
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
                encryption_sock_file, _ = local_connect_and_auth(conn_info, auth_secret)
                broadcast_out = ChunkedStream(encryption_sock_file, 8192)
            else:
                # Without encryption, write the serialized data directly to the file from Python.
                broadcast_out = f
            self._dump(value, broadcast_out, use_arrow)  # type: ignore[arg-type]
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
                # The JVM is doing decryption for us. Read the value immediately from the socket.
                self._value = self._load(sock_file)
            else:
                # The JVM writes the serialized data to path. Load it lazily when requested.
                assert path is not None
                self._path = path

    def _dump(self, value: T, f: BinaryIO, use_arrow: bool = False) -> None:
        arrow_serialization = self._get_arrow_serialization(value) if use_arrow else None
        f.write(_BROADCAST_FORMAT_MAGIC)
        if arrow_serialization is None:
            f.write(_PICKLE_SERIALIZATION)
            self.dump(value, f)
            return

        arrow_value, serialization, reconstructor = arrow_serialization
        if reconstructor is None:
            f.write(serialization)
        else:
            f.write(_CUSTOM_ARROW_SERIALIZATION)
            f.write(serialization)
            f.write(struct.pack("!I", len(reconstructor)))
            f.write(reconstructor)
        self._dump_arrow(arrow_value, f, serialization)

    @staticmethod
    def _get_arrow_serialization(
        value: Any,
    ) -> Optional[Tuple[Any, bytes, Optional[bytes]]]:
        try:
            import pyarrow as pa
        except ImportError:
            return None

        serialization = Broadcast._get_builtin_arrow_serialization(value, pa)
        if serialization is not None:
            return value, serialization, None

        from_arrow = getattr(type(value), "__from_arrow__", None)
        if not callable(from_arrow):
            return None

        to_arrow = getattr(value, "__to_arrow__", None)
        has_builtin_protocol = any(
            callable(getattr(value, protocol, None))
            for protocol in ("__arrow_c_stream__", "__arrow_c_array__", "__arrow_array__")
        )
        if not callable(to_arrow) and not has_builtin_protocol:
            return None

        try:
            reconstructor = pickle.dumps(type(value), pickle_protocol)
        except Exception:
            return None

        try:
            arrow_value = to_arrow() if callable(to_arrow) else value
            converted = Broadcast._convert_to_builtin_arrow(arrow_value, pa)
            if converted is None:
                raise TypeError(
                    "__to_arrow__ must return a native Arrow value or an object implementing "
                    "an Arrow array or stream protocol"
                )
            converted_value, serialization = converted
            return converted_value, serialization, reconstructor
        except Exception as e:
            msg = "Could not convert broadcast value to Arrow: %s: %s" % (
                e.__class__.__name__,
                str(e),
            )
            print_exec(sys.stderr)
            raise pickle.PicklingError(msg) from e

    @staticmethod
    def _get_builtin_arrow_serialization(value: Any, pa: Any) -> Optional[bytes]:
        if isinstance(value, pa.Table):
            return _ARROW_TABLE_SERIALIZATION
        if isinstance(value, pa.RecordBatch):
            return _ARROW_RECORD_BATCH_SERIALIZATION
        if isinstance(value, pa.Array):
            return _ARROW_ARRAY_SERIALIZATION
        if isinstance(value, pa.ChunkedArray):
            return _ARROW_CHUNKED_ARRAY_SERIALIZATION
        return None

    @staticmethod
    def _convert_to_builtin_arrow(value: Any, pa: Any) -> Optional[Tuple[Any, bytes]]:
        serialization = Broadcast._get_builtin_arrow_serialization(value, pa)
        if serialization is not None:
            return value, serialization
        if callable(getattr(value, "__arrow_c_stream__", None)):
            table = pa.RecordBatchReader.from_stream(value).read_all()
            return table, _ARROW_TABLE_SERIALIZATION
        if callable(getattr(value, "__arrow_c_array__", None)) or callable(
            getattr(value, "__arrow_array__", None)
        ):
            array = pa.array(value)
            serialization = Broadcast._get_builtin_arrow_serialization(array, pa)
            if serialization is not None:
                return array, serialization
        return None

    def _dump_arrow(self, value: Any, f: BinaryIO, serialization: bytes) -> None:
        import pyarrow as pa

        try:
            arrow_value: Any = value
            table = None
            if serialization == _ARROW_TABLE_SERIALIZATION:
                schema = arrow_value.schema
            elif serialization == _ARROW_RECORD_BATCH_SERIALIZATION:
                schema = arrow_value.schema
            elif serialization == _ARROW_ARRAY_SERIALIZATION:
                schema = pa.schema([pa.field(_ARROW_COLUMN_NAME, arrow_value.type)])
            else:
                assert serialization == _ARROW_CHUNKED_ARRAY_SERIALIZATION
                table = pa.Table.from_arrays([arrow_value], names=[_ARROW_COLUMN_NAME])
                schema = table.schema

            with pa.RecordBatchStreamWriter(f, schema) as writer:
                if serialization == _ARROW_TABLE_SERIALIZATION:
                    writer.write_table(arrow_value)
                elif serialization == _ARROW_RECORD_BATCH_SERIALIZATION:
                    writer.write_batch(arrow_value)
                elif serialization == _ARROW_ARRAY_SERIALIZATION:
                    writer.write_batch(
                        pa.RecordBatch.from_arrays([arrow_value], names=[_ARROW_COLUMN_NAME])
                    )
                else:
                    assert table is not None
                    writer.write_table(table)
        except Exception as e:
            msg = "Could not serialize broadcast with Arrow: %s: %s" % (
                e.__class__.__name__,
                str(e),
            )
            print_exec(sys.stderr)
            raise pickle.PicklingError(msg)
        f.close()

    def _load_from_path(self, path: str) -> T:
        with open(path, "rb", 1 << 20) as f:
            return self._load(f)

    def _load(self, file: BinaryIO) -> T:
        magic = self._read_exact(file, len(_BROADCAST_FORMAT_MAGIC))
        if magic != _BROADCAST_FORMAT_MAGIC:
            raise pickle.UnpicklingError("Invalid broadcast serialization format")

        serialization = self._read_exact(file, 1)
        if serialization == _PICKLE_SERIALIZATION:
            return self.load(file)

        reconstructor = None
        if serialization == _CUSTOM_ARROW_SERIALIZATION:
            serialization = self._read_exact(file, 1)
            reconstructor_length = struct.unpack(
                "!I", self._read_exact(file, _ARROW_RECONSTRUCTOR_LENGTH_SIZE)
            )[0]
            reconstructor = pickle.loads(self._read_exact(file, reconstructor_length))

        arrow_serializations = {
            _ARROW_TABLE_SERIALIZATION,
            _ARROW_RECORD_BATCH_SERIALIZATION,
            _ARROW_ARRAY_SERIALIZATION,
            _ARROW_CHUNKED_ARRAY_SERIALIZATION,
        }
        if serialization not in arrow_serializations:
            raise pickle.UnpicklingError("Unknown broadcast serialization format")

        arrow_value = self._load_arrow(file, serialization)
        if reconstructor is None:
            return arrow_value

        from_arrow = getattr(reconstructor, "__from_arrow__", None)
        if not callable(from_arrow):
            raise pickle.UnpicklingError(
                "Arrow broadcast reconstructor does not implement __from_arrow__"
            )
        try:
            return from_arrow(arrow_value)
        except Exception as e:
            raise pickle.UnpicklingError(
                "Could not reconstruct broadcast value with __from_arrow__"
            ) from e

    @staticmethod
    def _read_exact(file: BinaryIO, size: int) -> bytes:
        value = bytearray()
        while len(value) < size:
            chunk = file.read(size - len(value))
            if not chunk:
                raise pickle.UnpicklingError("Truncated broadcast serialization")
            value.extend(chunk)
        return bytes(value)

    @staticmethod
    def _load_arrow(file: BinaryIO, serialization: bytes) -> Any:
        import pyarrow as pa

        with pa.ipc.open_stream(file) as reader:
            if serialization == _ARROW_TABLE_SERIALIZATION:
                return reader.read_all()
            elif serialization == _ARROW_RECORD_BATCH_SERIALIZATION:
                batches = list(reader)
                if len(batches) != 1:
                    raise pickle.UnpicklingError("Invalid Arrow record batch broadcast")
                return batches[0]
            elif serialization == _ARROW_ARRAY_SERIALIZATION:
                batches = list(reader)
                if len(batches) != 1:
                    raise pickle.UnpicklingError("Invalid Arrow array broadcast")
                return batches[0].column(0)

            table = reader.read_all()
            return table.column(0)

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
                decrypted_sock_file, _ = local_connect_and_auth(conn_info, auth_secret)
                self._python_broadcast.waitTillBroadcastDataSent()
                return self._load(decrypted_sock_file)
            else:
                self._value = self._load_from_path(self._path)
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

    failure_count, test_count = doctest.testmod(pyspark.core.broadcast, globs=globs)
    spark.stop()
    if failure_count:
        sys.exit(-1)


if __name__ == "__main__":
    _test()
