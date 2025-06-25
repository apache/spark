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
import itertools
from typing import Any, Iterator, Optional, TYPE_CHECKING, Union
from pyspark.sql.streaming.stateful_processor_api_client import (
    StatefulProcessorApiClient,
    StatefulProcessorHandleState,
)
from pyspark.sql.streaming.stateful_processor import (
    ExpiredTimerInfo,
    StatefulProcessor,
    StatefulProcessorHandle,
    TimerValues,
)
from pyspark.sql.streaming.stateful_processor_api_client import ExpiredTimerIterator
from pyspark.sql.types import Row

if TYPE_CHECKING:
    from pyspark.sql.pandas._typing import DataFrameLike as PandasDataFrameLike

# This file places the utilities for transformWithState in PySpark (Row, and Pandas); we have
# a separate file to avoid putting internal classes to the stateful_processor.py file which
# contains public APIs.


class TransformWithStateInPandasFuncMode(Enum):
    """
    Internal mode for python worker UDF mode for transformWithState in PySpark; external mode are
    in `StatefulProcessorHandleState` for public use purposes.

    NOTE: The class has `Pandas` in its name for compatibility purposes in Spark Connect.
    """

    PROCESS_DATA = 1
    PROCESS_TIMER = 2
    COMPLETE = 3
    PRE_INIT = 4


class TransformWithStateInPandasUdfUtils:
    """
    Internal Utility class used for python worker UDF for transformWithState in PySpark. This class
    is shared for both classic and spark connect mode.

    NOTE: The class has `Pandas` in its name for compatibility purposes in Spark Connect.
    """

    def __init__(self, stateful_processor: StatefulProcessor, time_mode: str):
        self._stateful_processor = stateful_processor
        self._time_mode = time_mode

    def transformWithStateUDF(
        self,
        stateful_processor_api_client: StatefulProcessorApiClient,
        mode: TransformWithStateInPandasFuncMode,
        key: Any,
        input_rows: Union[Iterator["PandasDataFrameLike"], Iterator[Row]],
    ) -> Union[Iterator["PandasDataFrameLike"], Iterator[Row]]:
        if mode == TransformWithStateInPandasFuncMode.PRE_INIT:
            return self._handle_pre_init(stateful_processor_api_client)

        handle = StatefulProcessorHandle(stateful_processor_api_client)

        if stateful_processor_api_client.handle_state == StatefulProcessorHandleState.CREATED:
            self._stateful_processor.init(handle)
            stateful_processor_api_client.set_handle_state(StatefulProcessorHandleState.INITIALIZED)

        if mode == TransformWithStateInPandasFuncMode.PROCESS_TIMER:
            stateful_processor_api_client.set_handle_state(
                StatefulProcessorHandleState.DATA_PROCESSED
            )
            result = self._handle_expired_timers(stateful_processor_api_client)
            return result
        elif mode == TransformWithStateInPandasFuncMode.COMPLETE:
            stateful_processor_api_client.set_handle_state(
                StatefulProcessorHandleState.TIMER_PROCESSED
            )
            stateful_processor_api_client.remove_implicit_key()
            self._stateful_processor.close()
            stateful_processor_api_client.set_handle_state(StatefulProcessorHandleState.CLOSED)
            return iter([])
        else:
            # mode == TransformWithStateInPandasFuncMode.PROCESS_DATA
            result = self._handle_data_rows(stateful_processor_api_client, key, input_rows)
            return result

    def transformWithStateWithInitStateUDF(
        self,
        stateful_processor_api_client: StatefulProcessorApiClient,
        mode: TransformWithStateInPandasFuncMode,
        key: Any,
        input_rows: Union[Iterator["PandasDataFrameLike"], Iterator[Row]],
        initial_states: Optional[Union[Iterator["PandasDataFrameLike"], Iterator[Row]]] = None,
    ) -> Union[Iterator["PandasDataFrameLike"], Iterator[Row]]:
        """
        UDF for TWS operator with non-empty initial states. Possible input combinations
        of inputRows and initialStates iterator:
        - Both `inputRows` and `initialStates` are non-empty. Both input rows and initial
         states contains the grouping key and data.
        - `InitialStates` is non-empty, while `inputRows` is empty. Only initial states
         contains the grouping key and data, and it is first batch.
        - `initialStates` is empty, while `inputRows` is non-empty. Only inputRows contains the
         grouping key and data, and it is first batch.
        - `initialStates` is None, while `inputRows` is not empty. This is not first batch.
         `initialStates` is initialized to the positional value as None.
        """
        if mode == TransformWithStateInPandasFuncMode.PRE_INIT:
            return self._handle_pre_init(stateful_processor_api_client)

        handle = StatefulProcessorHandle(stateful_processor_api_client)

        if stateful_processor_api_client.handle_state == StatefulProcessorHandleState.CREATED:
            self._stateful_processor.init(handle)
            stateful_processor_api_client.set_handle_state(StatefulProcessorHandleState.INITIALIZED)

        if mode == TransformWithStateInPandasFuncMode.PROCESS_TIMER:
            stateful_processor_api_client.set_handle_state(
                StatefulProcessorHandleState.DATA_PROCESSED
            )
            result = self._handle_expired_timers(stateful_processor_api_client)
            return result
        elif mode == TransformWithStateInPandasFuncMode.COMPLETE:
            stateful_processor_api_client.remove_implicit_key()
            self._stateful_processor.close()
            stateful_processor_api_client.set_handle_state(StatefulProcessorHandleState.CLOSED)
            return iter([])
        else:
            # mode == TransformWithStateInPandasFuncMode.PROCESS_DATA
            batch_timestamp, watermark_timestamp = stateful_processor_api_client.get_timestamps(
                self._time_mode
            )

        # only process initial state if first batch and initial state is not None
        if initial_states is not None:
            for cur_initial_state in initial_states:
                stateful_processor_api_client.set_implicit_key(key)
                self._stateful_processor.handleInitialState(
                    key, cur_initial_state, TimerValues(batch_timestamp, watermark_timestamp)
                )

        # if we don't have input rows for the given key but only have initial state
        # for the grouping key, the inputRows iterator could be empty
        input_rows_empty = False
        try:
            first = next(input_rows)
        except StopIteration:
            input_rows_empty = True
        else:
            input_rows = itertools.chain([first], input_rows)  # type: ignore

        if not input_rows_empty:
            result = self._handle_data_rows(stateful_processor_api_client, key, input_rows)
        else:
            result = iter([])

        return result

    def _handle_pre_init(
        self, stateful_processor_api_client: StatefulProcessorApiClient
    ) -> Union[Iterator["PandasDataFrameLike"], Iterator[Row]]:
        # Driver handle is different from the handle used on executors;
        # On JVM side, we will use `DriverStatefulProcessorHandleImpl` for driver handle which
        # will only be used for handling init() and get the state schema on the driver.
        driver_handle = StatefulProcessorHandle(stateful_processor_api_client)
        stateful_processor_api_client.set_handle_state(StatefulProcessorHandleState.PRE_INIT)
        self._stateful_processor.init(driver_handle)

        # This method is used for the driver-side stateful processor after we have collected
        # all the necessary schemas. This instance of the DriverStatefulProcessorHandleImpl
        # won't be used again on JVM.
        self._stateful_processor.close()

        # return a dummy result, no return value is needed for pre init
        return iter([])

    def _handle_data_rows(
        self,
        stateful_processor_api_client: StatefulProcessorApiClient,
        key: Any,
        input_rows: Optional[Union[Iterator["PandasDataFrameLike"], Iterator[Row]]] = None,
    ) -> Union[Iterator["PandasDataFrameLike"], Iterator[Row]]:
        stateful_processor_api_client.set_implicit_key(key)

        batch_timestamp, watermark_timestamp = stateful_processor_api_client.get_timestamps(
            self._time_mode
        )

        # process with data rows
        if input_rows is not None:
            data_iter = self._stateful_processor.handleInputRows(
                key, input_rows, TimerValues(batch_timestamp, watermark_timestamp)
            )
            return data_iter
        else:
            return iter([])

    def _handle_expired_timers(
        self,
        stateful_processor_api_client: StatefulProcessorApiClient,
    ) -> Union[Iterator["PandasDataFrameLike"], Iterator[Row]]:
        batch_timestamp, watermark_timestamp = stateful_processor_api_client.get_timestamps(
            self._time_mode
        )

        if self._time_mode.lower() == "processingtime":
            expiry_iter = ExpiredTimerIterator(stateful_processor_api_client, batch_timestamp)
        elif self._time_mode.lower() == "eventtime":
            expiry_iter = ExpiredTimerIterator(stateful_processor_api_client, watermark_timestamp)
        else:
            expiry_iter = iter([])  # type: ignore[assignment]

        # process with expiry timers, only timer related rows will be emitted
        for key_obj, expiry_timestamp in expiry_iter:
            stateful_processor_api_client.set_implicit_key(key_obj)
            for pd in self._stateful_processor.handleExpiredTimer(
                key=key_obj,
                timerValues=TimerValues(batch_timestamp, watermark_timestamp),
                expiredTimerInfo=ExpiredTimerInfo(expiry_timestamp),
            ):
                yield pd
            stateful_processor_api_client.delete_timer(expiry_timestamp)
