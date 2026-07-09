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

import threading
from typing import Optional
from collections import deque


class ZeroCopyByteStream:
    """
    Accepts chunks of bytes as zero-copy memory views. Implements
    a file-like interface on top of the received chunks.

    read() calls that access bytes from a single chunk are served
    as zero-copy reads. If a read() call crosses chunk boundaries,
    memory copies are required. The later case is unexpected and only
    implemented for correctness.

    This implementation is thread-safe.
    """

    def __init__(self, initial_view: Optional[memoryview] = None):
        if not isinstance(initial_view, memoryview) and initial_view is not None:
            raise TypeError(
                "Only memoryview and None are allowed as the initial "
                + f"ZeroCopyByteStream view. Recveied type {type(initial_view)} instead."
            )

        self._chunks = deque[memoryview]()
        self._current_chunk = initial_view
        self._current_position = 0
        self._eof = False
        self._condition = threading.Condition()

    def add_next_chunk(self, chunk: memoryview) -> None:
        """
        Adds the next chunk as a read source.

        Chunks can only be added if the stream has not
        been finished before.

        The chunk to be added cannot be None.
        """
        if not isinstance(chunk, memoryview):
            raise TypeError(
                "Only memoryviews can be added to the ZeroCopyByteStreams. "
                + f"Received {type(chunk)} instead."
            )
        with self._condition:
            if self._eof:
                raise ValueError("Cannot add chunk after ZeroCopyByteStream has been finished")
            self._chunks.append(chunk)
            self._condition.notify()

    def finish(self) -> None:
        """
        Marks the stream as ended.

        Idempotent: can be called multiple times.
        """
        with self._condition:
            self._eof = True
            self._condition.notify()

    @property
    def finished(self) -> bool:
        """
        Returns whether the stream has been marked as finished and was fully
        consumed. If finished() == True, any attempts to read the stream
        will raise an `EOFError`.
        """
        with self._condition:
            # It is finished if: we read all content of the current chunk,
            # there are no remaining chunks, and the input has been marked as done
            return self._current_chunk is None and len(self._chunks) == 0 and self._eof

    def _try_read_bytes(self, size: int) -> Optional[memoryview]:
        """
        Reads up to ``size`` bytes from the current or next available chunk.
        Returns a zero-copy memoryview slice (may be shorter than ``size``
        if the current chunk doesn't have enough data), or None on EOF.

        Blocks until at least some data is available or EOF is reached.

        Internal, assumes to be run inside locked self._condition!
        """
        # Ensure we have a current chunk
        while self._current_chunk is None:
            try:
                self._current_chunk = self._chunks.popleft()
                self._current_position = 0
            except IndexError:
                # No chunks available - check for EOF
                if self._eof:
                    return None
                # Block until data arrives or EOF is signaled
                self._condition.wait()

        remaining = len(self._current_chunk) - self._current_position
        to_read = min(remaining, size)

        # Read slice from current chunk (zero-copy)
        result = self._current_chunk[self._current_position : self._current_position + to_read]
        self._current_position += to_read

        # If entire chunk consumed, clear it for next chunk
        assert self._current_position <= len(self._current_chunk), (
            f"Current position {self._current_position} was unexpectedly "
            + f"larger than max position {len(self._current_chunk)}"
        )
        if self._current_position == len(self._current_chunk):
            self._current_chunk = None
            self._current_position = 0

        return result

    def read(self, size: int) -> memoryview:
        """
        Reads size bytes. If the read fails because the
        stream was exhausted before size bytes could be read
        an `EOFError` will be raised.

        It is required that size >= 0.
        """
        if size < 0:
            raise ValueError(
                "ZeroCopyByteStream.read() cannot be called"
                + f" with negative size. Received {size}"
            )

        if size == 0:
            return memoryview(b"")

        with self._condition:
            first = self._try_read_bytes(size)

            # Zero-copy: either EOF or the read fits in a single chunk
            if first is None:
                raise EOFError(
                    f"ZeroCopyByteStream.read() tried to read {size} byte(s), "
                    + "however, the stream was exhausted after reading 0 byte."
                )
            elif len(first) == size:
                return first

            # Slow path: read crosses chunk boundaries (requires copy)
            buf = bytearray(size)
            buf[0 : len(first)] = first
            bytes_copied = len(first)

            while bytes_copied < size:
                chunk = self._try_read_bytes(size - bytes_copied)
                if chunk is None:
                    raise EOFError(
                        f"ZeroCopyByteStream.read() tried to read {size} byte(s), "
                        + "however, the stream was exhausted after reading "
                        + f"{bytes_copied} byte."
                    )
                buf[bytes_copied : bytes_copied + len(chunk)] = chunk
                bytes_copied += len(chunk)

            return memoryview(buf)
