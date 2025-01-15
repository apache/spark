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
import datetime
import json
from typing import Tuple, Optional

from pyspark.sql.types import Row, StructType, TimestampType
from pyspark.errors import PySparkTypeError, PySparkValueError, PySparkRuntimeError

__all__ = ["GroupState", "GroupStateTimeout"]


class GroupStateTimeout:
    """
    Represents the type of timeouts possible for the Dataset operations applyInPandasWithState.
    """

    NoTimeout: str = "NoTimeout"
    ProcessingTimeTimeout: str = "ProcessingTimeTimeout"
    EventTimeTimeout: str = "EventTimeTimeout"


class GroupState:
    """
    Wrapper class for interacting with per-group state data in `applyInPandasWithState`.
    """

    NO_TIMESTAMP: int = -1

    def __init__(
        self,
        # JVM Constructor
        optionalValue: Row,
        batchProcessingTimeMs: int,
        eventTimeWatermarkMs: int,
        timeoutConf: str,
        hasTimedOut: bool,
        watermarkPresent: bool,
        # JVM internal state.
        defined: bool,
        updated: bool,
        removed: bool,
        timeoutTimestamp: int,
        # Python internal state.
        keyAsUnsafe: bytes,
        valueSchema: StructType,
    ) -> None:
        self._keyAsUnsafe = keyAsUnsafe
        self._value = optionalValue
        self._batch_processing_time_ms = batchProcessingTimeMs
        self._event_time_watermark_ms = eventTimeWatermarkMs

        assert timeoutConf in [
            GroupStateTimeout.NoTimeout,
            GroupStateTimeout.ProcessingTimeTimeout,
            GroupStateTimeout.EventTimeTimeout,
        ]
        self._timeout_conf = timeoutConf

        self._has_timed_out = hasTimedOut
        self._watermark_present = watermarkPresent

        self._defined = defined
        self._updated = updated
        self._removed = removed
        self._timeout_timestamp = timeoutTimestamp
        # Python internal state.
        self._old_timeout_timestamp = timeoutTimestamp

        self._value_schema = valueSchema

    @property
    def exists(self) -> bool:
        """
        Whether state exists or not.
        """
        return self._defined

    @property
    def get(self) -> Tuple:
        """
        Get the state value if it exists, or throw ValueError.
        """
        if self.exists:
            return tuple(self._value)
        else:
            raise PySparkValueError(
                errorClass="STATE_NOT_EXISTS",
                messageParameters={},
            )

    @property
    def getOption(self) -> Optional[Tuple]:
        """
        Get the state value if it exists, or return None.
        """
        if self.exists:
            return tuple(self._value)
        else:
            return None

    @property
    def hasTimedOut(self) -> bool:
        """
        Whether the function has been called because the key has timed out.
        This can return true only when timeouts are enabled.
        """
        return self._has_timed_out

    # NOTE: this function is only available to PySpark implementation due to underlying
    # implementation, do not port to Scala implementation!
    @property
    def oldTimeoutTimestamp(self) -> int:
        return self._old_timeout_timestamp

    def update(self, newValue: Tuple) -> None:
        """
        Update the value of the state. The value of the state cannot be null.
        """
        from pyspark.testing.utils import have_numpy

        if newValue is None:
            raise PySparkTypeError(
                errorClass="CANNOT_BE_NONE",
                messageParameters={"arg_name": "newValue"},
            )

        converted = []
        if have_numpy:
            import numpy as np

            # In order to convert NumPy types to Python primitive types.
            for v in newValue:
                if isinstance(v, np.generic):
                    converted.append(v.tolist())
                # Address a couple of pandas dtypes too.
                elif hasattr(v, "to_pytimedelta"):
                    converted.append(v.to_pytimedelta())
                elif hasattr(v, "to_pydatetime"):
                    converted.append(v.to_pydatetime())
                else:
                    converted.append(v)
        else:
            converted = list(newValue)

        self._value = Row(*converted)
        self._defined = True
        self._updated = True
        self._removed = False

    def remove(self) -> None:
        """
        Remove this state.
        """
        self._defined = False
        self._updated = False
        self._removed = True

    def setTimeoutDuration(self, durationMs: int) -> None:
        """
        Set the timeout duration in ms for this key.
        Processing time timeout must be enabled.
        """
        if isinstance(durationMs, str):
            # TODO(SPARK-40437): Support string representation of durationMs.
            raise PySparkTypeError(
                errorClass="NOT_INT",
                messageParameters={
                    "arg_name": "durationMs",
                    "arg_type": type(durationMs).__name__,
                },
            )

        if self._timeout_conf != GroupStateTimeout.ProcessingTimeTimeout:
            raise PySparkRuntimeError(
                errorClass="CANNOT_WITHOUT",
                messageParameters={
                    "condition1": "set timeout duration",
                    "condition2": "enabling processing time timeout in applyInPandasWithState",
                },
            )

        if durationMs <= 0:
            raise PySparkValueError(
                errorClass="VALUE_NOT_POSITIVE",
                messageParameters={
                    "arg_name": "durationMs",
                    "arg_value": type(durationMs).__name__,
                },
            )
        self._timeout_timestamp = durationMs + self._batch_processing_time_ms

    # TODO(SPARK-40438): Implement additionalDuration parameter.
    def setTimeoutTimestamp(self, timestampMs: int) -> None:
        """
        Set the timeout timestamp for this key as milliseconds in epoch time.
        This timestamp cannot be older than the current watermark.
        Event time timeout must be enabled.
        """
        if self._timeout_conf != GroupStateTimeout.EventTimeTimeout:
            raise PySparkRuntimeError(
                errorClass="CANNOT_WITHOUT",
                messageParameters={
                    "condition1": "set timeout duration",
                    "condition2": "enabling processing time timeout in applyInPandasWithState",
                },
            )

        if isinstance(timestampMs, datetime.datetime):
            timestampMs = TimestampType().toInternal(timestampMs) / 1000

        if timestampMs <= 0:
            raise PySparkValueError(
                errorClass="VALUE_NOT_POSITIVE",
                messageParameters={
                    "arg_name": "timestampMs",
                    "arg_value": type(timestampMs).__name__,
                },
            )

        if (
            self._event_time_watermark_ms != GroupState.NO_TIMESTAMP
            and timestampMs < self._event_time_watermark_ms
        ):
            raise PySparkValueError(
                errorClass="INVALID_TIMEOUT_TIMESTAMP",
                messageParameters={
                    "timestamp": str(timestampMs),
                    "watermark": str(self._event_time_watermark_ms),
                },
            )

        self._timeout_timestamp = timestampMs

    def getCurrentWatermarkMs(self) -> int:
        """
        Get the current event time watermark as milliseconds in epoch time.
        In a streaming query, this can be called only when watermark is set.
        """
        if not self._watermark_present:
            raise PySparkRuntimeError(
                errorClass="CANNOT_WITHOUT",
                messageParameters={
                    "condition1": "get event time watermark timestamp",
                    "condition2": "setting watermark before applyInPandasWithState",
                },
            )
        return self._event_time_watermark_ms

    def getCurrentProcessingTimeMs(self) -> int:
        """
        Get the current processing time as milliseconds in epoch time.
        In a streaming query, this will return a constant value throughout the duration of a
        trigger, even if the trigger is re-executed.
        """
        return self._batch_processing_time_ms

    def __str__(self) -> str:
        if self.exists:
            return "GroupState(%s)" % (self.get,)
        else:
            return "GroupState(<undefined>)"

    def json(self) -> str:
        """
        Convert the internal values of instance into JSON. This is used to send out the update
        from Python worker to executor.
        """
        return json.dumps(
            {
                # Constructor
                "optionalValue": None,  # Note that optionalValue will be manually serialized.
                "batchProcessingTimeMs": self._batch_processing_time_ms,
                "eventTimeWatermarkMs": self._event_time_watermark_ms,
                "timeoutConf": self._timeout_conf,
                "hasTimedOut": self._has_timed_out,
                "watermarkPresent": self._watermark_present,
                # JVM internal state.
                "defined": self._defined,
                "updated": self._updated,
                "removed": self._removed,
                "timeoutTimestamp": self._timeout_timestamp,
            }
        )
