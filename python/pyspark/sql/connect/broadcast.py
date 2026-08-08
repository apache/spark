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
"""
(SPARK-51705) Client-side broadcast proxy for Spark Connect.

``ConnectBroadcast`` mirrors the classic :class:`pyspark.core.broadcast.Broadcast` pickling
contract so that a UDF closure can reference a broadcast variable transparently. The value was
already uploaded to the server (cloudpickle -> cache artifact -> CreateBroadcastCommand) and a
driver-side broadcast id was returned; this proxy keeps a local copy of the value for driver-side
``.value`` reads, and on pickling emits ``(_from_id, (broadcast_id,))`` -- exactly what the classic
:class:`Broadcast` emits -- so the executor/worker resolves it from its ``_broadcastRegistry``
keyed by that same id. No JVM is available on the Connect client, so all lifecycle operations are
routed to the server as commands.
"""

import threading
from typing import Any, Generic, Tuple, TYPE_CHECKING, TypeVar

# NOTE: This reuses the classic ``pyspark.core.broadcast`` machinery so the pickled UDF closure is
# byte-identical to classic PySpark (``_from_id`` + the driver-side broadcast id). ``pyspark.core``
# is not available in a Spark Connect-only install (``pyspark-client``); this module is therefore
# imported lazily (from ``SparkSession.broadcast`` and ``PythonUDF.to_plan``), never at package
# import time, so the base Connect package still imports without the classic core present.
from pyspark.core.broadcast import Broadcast, _from_id

if TYPE_CHECKING:
    from pyspark.sql.connect.session import SparkSession

T = TypeVar("T")


class _BroadcastCaptureRegistry(threading.local):
    """Thread-local registry of broadcast ids captured while pickling a UDF closure.

    Thread-local is correct because ``CloudPickleSerializer().dumps(...)`` in
    ``PythonUDF.to_plan`` and the subsequent drain into ``PythonUDF.broadcast_ids`` happen
    synchronously on the same plan-building thread. This mirrors the classic
    :class:`pyspark.core.broadcast.BroadcastPickleRegistry`.
    """

    def __init__(self) -> None:
        self.__dict__.setdefault("_registry", set())

    def add(self, bid: int) -> None:
        self._registry.add(bid)

    def drain(self) -> "list[int]":
        """Return the captured ids and clear the registry (dumps-then-drain-then-clear)."""
        captured = list(self._registry)
        self._registry.clear()
        return captured


# Module-level thread-local registry, drained by ``PythonUDF.to_plan``.
_broadcast_capture_registry = _BroadcastCaptureRegistry()


class ConnectBroadcast(Broadcast, Generic[T]):
    """A broadcast variable created with :meth:`SparkSession.broadcast` over Spark Connect.

    See Also
    --------
    pyspark.core.broadcast.Broadcast
    """

    def __init__(self, session: "SparkSession", broadcast_id: int, value: T) -> None:
        # Intentionally does NOT call Broadcast.__init__ -- there is no live SparkContext on the
        # Connect client. We populate only the attributes needed for driver reads and pickling.
        self._session = session
        self._broadcast_id = broadcast_id
        self._value = value
        # Mirror the classic executor-side shape: no JVM broadcast / SparkContext handle.
        self._jbroadcast = None
        self._sc = None
        self._python_broadcast = None

    @property
    def value(self) -> T:
        """Return the broadcasted value (kept locally on the client for driver-side reads)."""
        return self._value

    def unpersist(self, blocking: bool = False) -> None:
        """Delete cached copies of this broadcast on the executors via the server."""
        self._session._client._unpersist_broadcast(
            self._broadcast_id, blocking=blocking, destroy=False
        )

    def destroy(self, blocking: bool = False) -> None:
        """Destroy all data and metadata related to this broadcast via the server."""
        self._session._client._unpersist_broadcast(
            self._broadcast_id, blocking=blocking, destroy=True
        )

    def __reduce__(self) -> Tuple[Any, Tuple[int]]:
        # Verbatim classic contract: reference the shared classic ``_from_id`` and the driver-side
        # broadcast id, so the worker resolves it from ``pyspark.core.broadcast._broadcastRegistry``
        # keyed by the same id. Also side-register the id so ``PythonUDF.to_plan`` can attach it to
        # the proto ``broadcast_ids`` field.
        _broadcast_capture_registry.add(self._broadcast_id)
        return _from_id, (self._broadcast_id,)
