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

from enum import Enum
from functools import wraps
from typing import BinaryIO, Callable, TypeVar
from abc import ABC, abstractmethod

from pyspark.messages.zero_copy_byte_stream import ZeroCopyByteStream


T = TypeVar("T", bound="SparkMessageReceiver")
R = TypeVar("R")


class MessageState(Enum):
    WAITING_FOR_INIT = 1
    WAITING_FOR_DATA = 2
    WAITING_FOR_FINISH = 3
    DONE = 4


class SparkMessageReceiver(ABC):
    """
    Generic class that implements receiving messages from Spark.
    Caution: This class is STATEFUL. It is expected, that the
    methods of this class are called in the following order:

    1. Init -> 2. Data stream -> 3. Finish

    This order is verified using assertions in the class. Each function
    can be called EXACTLY ONCE in the specified order.
    """

    def __init__(self) -> None:
        self._state = MessageState.WAITING_FOR_INIT

    @staticmethod
    def _state_transition(
        required_state: MessageState, next_state: MessageState
    ) -> Callable[[Callable[[T], R]], Callable[[T], R]]:
        """Decorator to enforce state transitions."""

        def decorator(func: Callable[[T], R]) -> Callable[[T], R]:
            @wraps(func)
            def wrapper(self: T) -> R:
                assert self._state == required_state
                result = func(self)
                self._state = next_state
                return result

            return wrapper

        return decorator

    @_state_transition(MessageState.WAITING_FOR_INIT, MessageState.WAITING_FOR_DATA)
    def get_init_message(self) -> ZeroCopyByteStream:
        """
        Returns:
            the binary contents of the initial message as a ZeroCopyByteStream.
        """
        return self._do_get_init_message()

    @_state_transition(MessageState.WAITING_FOR_DATA, MessageState.WAITING_FOR_FINISH)
    def get_data_stream(self) -> BinaryIO:
        """
        Returns:
            A binary stream containing the data to invoke the UDF on.
        """
        return self._do_get_data_stream()

    @_state_transition(MessageState.WAITING_FOR_FINISH, MessageState.DONE)
    def get_finish_signal_from_stream(self) -> None:
        """
        Consumes the finish message from the JVM and transitions to the DONE state.
        The finish message marks the end of the stream. Raises an `AssertionError` if
        the finish signal was not received correctly.
        """
        self._do_get_finish_signal_from_stream()

    @abstractmethod
    def _do_get_init_message(self) -> ZeroCopyByteStream:
        """
        Returns the contents of the init message
        as a 'ZeroCopyByteStream'.

        To be implemented by child classes.
        """
        ...

    @abstractmethod
    def _do_get_data_stream(self) -> BinaryIO:
        """
        Returns the Spark data stream.

        To be implemented by child classes.
        """
        ...

    @abstractmethod
    def _do_get_finish_signal_from_stream(self) -> None:
        """
        Consumes the finish signal from the stream.
        Raises an `AssertionError` if the signal is not received correctly.

        To be implemented by child classes.
        """
        ...
