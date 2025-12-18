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

from abc import abstractmethod
import sys
from typing import (
    Iterator,
    NamedTuple,
)
import unittest
from pyspark.errors import PySparkRuntimeError
from pyspark.sql.streaming import StatefulProcessor, StatefulProcessorHandle
from pyspark.sql.types import (
    StringType,
    StructType,
    StructField,
    Row,
    IntegerType,
    TimestampType,
    LongType,
    BooleanType,
    FloatType,
    ArrayType,
    MapType,
)
from pyspark.testing.sqlutils import have_pandas

if have_pandas:
    import pandas as pd


class StatefulProcessorFactory:
    @abstractmethod
    def pandas(self):
        ...

    @abstractmethod
    def row(self):
        ...


# StatefulProcessor factory implementations


class SimpleStatefulProcessorWithInitialStateFactory(StatefulProcessorFactory):
    def pandas(self):
        return PandasSimpleStatefulProcessorWithInitialState()

    def row(self):
        return RowSimpleStatefulProcessorWithInitialState()


class StatefulProcessorWithInitialStateTimersFactory(StatefulProcessorFactory):
    def pandas(self):
        return PandasStatefulProcessorWithInitialStateTimers()

    def row(self):
        return RowStatefulProcessorWithInitialStateTimers()


class StatefulProcessorWithListStateInitialStateFactory(StatefulProcessorFactory):
    def pandas(self):
        return PandasStatefulProcessorWithListStateInitialState()

    def row(self):
        return RowStatefulProcessorWithListStateInitialState()


class EventTimeStatefulProcessorFactory(StatefulProcessorFactory):
    def pandas(self):
        return PandasEventTimeStatefulProcessor()

    def row(self):
        return RowEventTimeStatefulProcessor()


class ProcTimeStatefulProcessorFactory(StatefulProcessorFactory):
    def pandas(self):
        return PandasProcTimeStatefulProcessor()

    def row(self):
        return RowProcTimeStatefulProcessor()


class SimpleStatefulProcessorFactory(StatefulProcessorFactory):
    def pandas(self):
        return PandasSimpleStatefulProcessor()

    def row(self):
        return RowSimpleStatefulProcessor()


class StatefulProcessorChainingOpsFactory(StatefulProcessorFactory):
    def pandas(self):
        return PandasStatefulProcessorChainingOps()

    def row(self):
        return RowStatefulProcessorChainingOps()


class SimpleTTLStatefulProcessorFactory(StatefulProcessorFactory):
    def pandas(self):
        return PandasSimpleTTLStatefulProcessor()

    def row(self):
        return RowSimpleTTLStatefulProcessor()


class TTLStatefulProcessorFactory(StatefulProcessorFactory):
    def pandas(self):
        return PandasTTLStatefulProcessor()

    def row(self):
        return RowTTLStatefulProcessor()


class InvalidSimpleStatefulProcessorFactory(StatefulProcessorFactory):
    def pandas(self):
        return PandasInvalidSimpleStatefulProcessor()

    def row(self):
        return RowInvalidSimpleStatefulProcessor()


class ListStateProcessorFactory(StatefulProcessorFactory):
    def pandas(self):
        return PandasListStateProcessor()

    def row(self):
        return RowListStateProcessor()


class ListStateLargeListProcessorFactory(StatefulProcessorFactory):
    def pandas(self):
        return PandasListStateLargeListProcessor()

    def row(self):
        return RowListStateLargeListProcessor()


class ListStateLargeTTLProcessorFactory(StatefulProcessorFactory):
    def pandas(self):
        return PandasListStateLargeTTLProcessor()

    def row(self):
        return RowListStateLargeTTLProcessor()


class MapStateProcessorFactory(StatefulProcessorFactory):
    def pandas(self):
        return PandasMapStateProcessor()

    def row(self):
        return RowMapStateProcessor()


class MapStateLargeTTLProcessorFactory(StatefulProcessorFactory):
    def pandas(self):
        return PandasMapStateLargeTTLProcessor()

    def row(self):
        return RowMapStateLargeTTLProcessor()


class BasicProcessorFactory(StatefulProcessorFactory):
    def pandas(self):
        return PandasBasicProcessor()

    def row(self):
        return RowBasicProcessor()


class BasicProcessorNotNullableFactory(StatefulProcessorFactory):
    def pandas(self):
        return PandasBasicProcessorNotNullable()

    def row(self):
        return RowBasicProcessorNotNullable()


class AddFieldsProcessorFactory(StatefulProcessorFactory):
    def pandas(self):
        return PandasAddFieldsProcessor()

    def row(self):
        return RowAddFieldsProcessor()


class RemoveFieldsProcessorFactory(StatefulProcessorFactory):
    def pandas(self):
        return PandasRemoveFieldsProcessor()

    def row(self):
        return RowRemoveFieldsProcessor()


class ReorderedFieldsProcessorFactory(StatefulProcessorFactory):
    def pandas(self):
        return PandasReorderedFieldsProcessor()

    def row(self):
        return RowReorderedFieldsProcessor()


class UpcastProcessorFactory(StatefulProcessorFactory):
    def pandas(self):
        return PandasUpcastProcessor()

    def row(self):
        return RowUpcastProcessor()


class MinEventTimeStatefulProcessorFactory(StatefulProcessorFactory):
    def pandas(self):
        return PandasMinEventTimeStatefulProcessor()

    def row(self):
        return RowMinEventTimeStatefulProcessor()


class StatefulProcessorCompositeTypeFactory(StatefulProcessorFactory):
    def pandas(self):
        return PandasStatefulProcessorCompositeType()

    def row(self):
        return RowStatefulProcessorCompositeType()


class ChunkCountProcessorFactory(StatefulProcessorFactory):
    def pandas(self):
        return PandasChunkCountProcessor()


class ChunkCountProcessorWithInitialStateFactory(StatefulProcessorFactory):
    def pandas(self):
        return PandasChunkCountWithInitialStateProcessor()


class CompositeOutputProcessorFactory(StatefulProcessorFactory):
    def pandas(self):
        return PandasCompositeOutputProcessor()

    def row(self):
        return RowCompositeOutputProcessor()


class LargeValueStatefulProcessorFactory(StatefulProcessorFactory):
    def pandas(self):
        return PandasLargeValueStatefulProcessor()

    def row(self):
        return RowLargeValueStatefulProcessor()


# StatefulProcessor implementations


class PandasSimpleStatefulProcessorWithInitialState(StatefulProcessor):
    # this dict is the same as input initial state dataframe
    dict = {("0",): 789, ("3",): 987}

    def init(self, handle: StatefulProcessorHandle) -> None:
        state_schema = StructType([StructField("value", IntegerType(), True)])
        self.value_state = handle.getValueState("value_state", state_schema)
        self.handle = handle

    def handleInputRows(self, key, rows, timerValues) -> Iterator[pd.DataFrame]:
        exists = self.value_state.exists()
        if exists:
            value_row = self.value_state.get()
            existing_value = value_row[0]
        else:
            existing_value = 0

        accumulated_value = existing_value

        for pdf in rows:
            value = pdf["temperature"].astype(int).sum()
            accumulated_value += value

        self.value_state.update((accumulated_value,))

        if len(key) > 1:
            yield pd.DataFrame(
                {"id1": (key[0],), "id2": (key[1],), "value": str(accumulated_value)}
            )
        else:
            yield pd.DataFrame({"id": key, "value": str(accumulated_value)})

    def handleInitialState(self, key, initialState, timerValues) -> None:
        init_val = initialState.at[0, "initVal"]
        self.value_state.update((init_val,))
        if len(key) == 1:
            assert self.dict[key] == init_val

    def close(self) -> None:
        pass


class RowSimpleStatefulProcessorWithInitialState(StatefulProcessor):
    # this dict is the same as input initial state dataframe
    dict = {("0",): 789, ("3",): 987}

    def init(self, handle: StatefulProcessorHandle) -> None:
        state_schema = StructType([StructField("value", IntegerType(), True)])
        self.value_state = handle.getValueState("value_state", state_schema)
        self.handle = handle

    def handleInputRows(self, key, rows, timerValues) -> Iterator[Row]:
        exists = self.value_state.exists()
        if exists:
            value_row = self.value_state.get()
            existing_value = value_row[0]
        else:
            existing_value = 0

        accumulated_value = existing_value

        for row in rows:
            value = row.temperature
            accumulated_value += value

        self.value_state.update((accumulated_value,))

        if len(key) > 1:
            yield Row(id1=key[0], id2=key[1], value=str(accumulated_value))
        else:
            yield Row(id=key[0], value=str(accumulated_value))

    def handleInitialState(self, key, initialState, timerValues) -> None:
        init_val = initialState.initVal
        self.value_state.update((init_val,))
        if len(key) == 1:
            assert self.dict[key] == init_val

    def close(self) -> None:
        pass


class PandasStatefulProcessorWithInitialStateTimers(PandasSimpleStatefulProcessorWithInitialState):
    def handleExpiredTimer(self, key, timerValues, expiredTimerInfo) -> Iterator[pd.DataFrame]:
        self.handle.deleteTimer(expiredTimerInfo.getExpiryTimeInMs())
        str_key = f"{str(key[0])}-expired"
        yield pd.DataFrame({"id": (str_key,), "value": str(expiredTimerInfo.getExpiryTimeInMs())})

    def handleInitialState(self, key, initialState, timerValues) -> None:
        super().handleInitialState(key, initialState, timerValues)
        self.handle.registerTimer(timerValues.getCurrentProcessingTimeInMs() - 1)


class RowStatefulProcessorWithInitialStateTimers(RowSimpleStatefulProcessorWithInitialState):
    def handleExpiredTimer(self, key, timerValues, expiredTimerInfo) -> Iterator[Row]:
        self.handle.deleteTimer(expiredTimerInfo.getExpiryTimeInMs())
        str_key = f"{str(key[0])}-expired"
        yield Row(id=str_key, value=str(expiredTimerInfo.getExpiryTimeInMs()))

    def handleInitialState(self, key, initialState, timerValues) -> None:
        super().handleInitialState(key, initialState, timerValues)
        self.handle.registerTimer(timerValues.getCurrentProcessingTimeInMs() - 1)


class PandasStatefulProcessorWithListStateInitialState(
    PandasSimpleStatefulProcessorWithInitialState
):
    def init(self, handle: StatefulProcessorHandle) -> None:
        super().init(handle)
        list_ele_schema = StructType([StructField("value", IntegerType(), True)])
        self.list_state = handle.getListState("list_state", list_ele_schema)

    def handleInitialState(self, key, initialState, timerValues) -> None:
        for val in initialState["initVal"].tolist():
            self.list_state.appendValue((val,))


class RowStatefulProcessorWithListStateInitialState(RowSimpleStatefulProcessorWithInitialState):
    def init(self, handle: StatefulProcessorHandle) -> None:
        super().init(handle)
        list_ele_schema = StructType([StructField("value", IntegerType(), True)])
        self.list_state = handle.getListState("list_state", list_ele_schema)

    def handleInitialState(self, key, initialState, timerValues) -> None:
        self.list_state.appendValue((initialState.initVal,))


# A stateful processor that output the max event time it has seen. Register timer for
# current watermark. Clear max state if timer expires.
class PandasEventTimeStatefulProcessor(StatefulProcessor):
    def init(self, handle: StatefulProcessorHandle) -> None:
        state_schema = StructType([StructField("value", StringType(), True)])
        self.handle = handle
        self.max_state = handle.getValueState("max_state", state_schema)

    def handleExpiredTimer(self, key, timerValues, expiredTimerInfo) -> Iterator[pd.DataFrame]:
        self.max_state.clear()
        self.handle.deleteTimer(expiredTimerInfo.getExpiryTimeInMs())
        str_key = f"{str(key[0])}-expired"
        yield pd.DataFrame(
            {"id": (str_key,), "timestamp": str(expiredTimerInfo.getExpiryTimeInMs())}
        )

    def handleInputRows(self, key, rows, timerValues) -> Iterator[pd.DataFrame]:
        timestamp_list = []
        for pdf in rows:
            # int64 will represent timestamp in nanosecond, restore to second
            timestamp_list.extend((pdf["eventTime"].astype("int64") // 10**9).tolist())

        if self.max_state.exists():
            cur_max = int(self.max_state.get()[0])
        else:
            cur_max = 0
        max_event_time = str(max(cur_max, max(timestamp_list)))

        self.max_state.update((max_event_time,))
        self.handle.registerTimer(timerValues.getCurrentWatermarkInMs())

        yield pd.DataFrame({"id": key, "timestamp": max_event_time})

    def close(self) -> None:
        pass


# A stateful processor that output the max event time it has seen. Register timer for
# current watermark. Clear max state if timer expires.
class RowEventTimeStatefulProcessor(StatefulProcessor):
    def init(self, handle: StatefulProcessorHandle) -> None:
        state_schema = StructType([StructField("value", StringType(), True)])
        self.handle = handle
        self.max_state = handle.getValueState("max_state", state_schema)

    def handleExpiredTimer(self, key, timerValues, expiredTimerInfo) -> Iterator[Row]:
        self.max_state.clear()
        self.handle.deleteTimer(expiredTimerInfo.getExpiryTimeInMs())
        str_key = f"{str(key[0])}-expired"
        yield Row(id=str_key, timestamp=str(expiredTimerInfo.getExpiryTimeInMs()))

    def handleInputRows(self, key, rows, timerValues) -> Iterator[Row]:
        timestamp_list = []
        for row in rows:
            # timestamp is microsecond, restore to second
            timestamp_list.append(int(row.eventTime.timestamp()))

        if self.max_state.exists():
            cur_max = int(self.max_state.get()[0])
        else:
            cur_max = 0
        max_event_time = str(max(cur_max, max(timestamp_list)))

        self.max_state.update((max_event_time,))
        self.handle.registerTimer(timerValues.getCurrentWatermarkInMs())

        yield Row(id=key[0], timestamp=max_event_time)

    def close(self) -> None:
        pass


# A stateful processor that output the accumulation of count of input rows; register
# processing timer and clear the counter if timer expires.
class PandasProcTimeStatefulProcessor(StatefulProcessor):
    def init(self, handle: StatefulProcessorHandle) -> None:
        state_schema = StructType([StructField("value", StringType(), True)])
        self.handle = handle
        self.count_state = handle.getValueState("count_state", state_schema)

    def handleExpiredTimer(self, key, timerValues, expiredTimerInfo) -> Iterator[pd.DataFrame]:
        # reset count state each time the timer is expired
        timer_list_1 = [e for e in self.handle.listTimers()]
        timer_list_2 = []
        idx = 0
        for e in self.handle.listTimers():
            timer_list_2.append(e)
            # check multiple iterator on the same grouping key works
            assert timer_list_2[idx] == timer_list_1[idx]
            idx += 1

        if len(timer_list_1) > 0:
            assert len(timer_list_1) == 2
        self.count_state.clear()
        self.handle.deleteTimer(expiredTimerInfo.getExpiryTimeInMs())
        yield pd.DataFrame(
            {
                "id": key,
                "countAsString": str("-1"),
                "timeValues": str(expiredTimerInfo.getExpiryTimeInMs()),
            }
        )

    def handleInputRows(self, key, rows, timerValues) -> Iterator[pd.DataFrame]:
        if not self.count_state.exists():
            count = 0
        else:
            count = int(self.count_state.get()[0])

        if key == ("0",):
            self.handle.registerTimer(timerValues.getCurrentProcessingTimeInMs() + 1)

        rows_count = 0
        for pdf in rows:
            pdf_count = len(pdf)
            rows_count += pdf_count

        count = count + rows_count

        self.count_state.update((str(count),))
        timestamp = str(timerValues.getCurrentProcessingTimeInMs())

        yield pd.DataFrame({"id": key, "countAsString": str(count), "timeValues": timestamp})

    def close(self) -> None:
        pass


# A stateful processor that output the accumulation of count of input rows; register
# processing timer and clear the counter if timer expires.
class RowProcTimeStatefulProcessor(StatefulProcessor):
    def init(self, handle: StatefulProcessorHandle) -> None:
        state_schema = StructType([StructField("value", StringType(), True)])
        self.handle = handle
        self.count_state = handle.getValueState("count_state", state_schema)

    def handleExpiredTimer(self, key, timerValues, expiredTimerInfo) -> Iterator[Row]:
        # reset count state each time the timer is expired
        timer_list_1 = [e for e in self.handle.listTimers()]
        timer_list_2 = []
        idx = 0
        for e in self.handle.listTimers():
            timer_list_2.append(e)
            # check multiple iterator on the same grouping key works
            assert timer_list_2[idx] == timer_list_1[idx]
            idx += 1

        if len(timer_list_1) > 0:
            assert len(timer_list_1) == 2
        self.count_state.clear()
        self.handle.deleteTimer(expiredTimerInfo.getExpiryTimeInMs())
        yield Row(
            id=key[0], countAsString=str(-1), timeValues=str(expiredTimerInfo.getExpiryTimeInMs())
        )

    def handleInputRows(self, key, rows, timerValues) -> Iterator[Row]:
        if not self.count_state.exists():
            count = 0
        else:
            count = int(self.count_state.get()[0])

        if key == ("0",):
            self.handle.registerTimer(timerValues.getCurrentProcessingTimeInMs() + 1)

        rows_count = 0
        for row in rows:
            rows_count += 1

        count = count + rows_count

        self.count_state.update((str(count),))
        timestamp = str(timerValues.getCurrentProcessingTimeInMs())

        yield Row(id=key[0], countAsString=str(count), timeValues=timestamp)

    def close(self) -> None:
        pass


class PandasSimpleStatefulProcessor(StatefulProcessor, unittest.TestCase):
    dict = {0: {"0": 1, "1": 2}, 1: {"0": 4, "1": 3}}
    batch_id = 0

    def init(self, handle: StatefulProcessorHandle) -> None:
        # Test both string type and struct type schemas
        self.num_violations_state = handle.getValueState("numViolations", "value int")
        state_schema = StructType([StructField("value", IntegerType(), True)])
        self.temp_state = handle.getValueState("tempState", state_schema)
        handle.deleteIfExists("tempState")

    def handleInputRows(self, key, rows, timerValues) -> Iterator[pd.DataFrame]:
        with self.assertRaisesRegex(PySparkRuntimeError, "Error checking value state exists"):
            self.temp_state.exists()
        new_violations = 0
        count = 0
        key_str = key[0]
        exists = self.num_violations_state.exists()
        if exists:
            existing_violations_row = self.num_violations_state.get()
            existing_violations = existing_violations_row[0]
            assert existing_violations == self.dict[0][key_str]
            self.batch_id = 1
        else:
            existing_violations = 0
        for pdf in rows:
            pdf_count = pdf.count()
            count += pdf_count.get("temperature")
            violations_pdf = pdf.loc[pdf["temperature"] > 100]
            new_violations += violations_pdf.count().get("temperature")
        updated_violations = new_violations + existing_violations
        assert updated_violations == self.dict[self.batch_id][key_str]
        self.num_violations_state.update((updated_violations,))
        yield pd.DataFrame({"id": key, "countAsString": str(count)})

    def close(self) -> None:
        pass


class RowSimpleStatefulProcessor(StatefulProcessor, unittest.TestCase):
    dict = {0: {"0": 1, "1": 2}, 1: {"0": 4, "1": 3}}
    batch_id = 0

    def init(self, handle: StatefulProcessorHandle) -> None:
        # Test both string type and struct type schemas
        self.num_violations_state = handle.getValueState("numViolations", "value int")
        state_schema = StructType([StructField("value", IntegerType(), True)])
        self.temp_state = handle.getValueState("tempState", state_schema)
        handle.deleteIfExists("tempState")

    def handleInputRows(self, key, rows, timerValues) -> Iterator[Row]:
        with self.assertRaisesRegex(PySparkRuntimeError, "Error checking value state exists"):
            self.temp_state.exists()
        new_violations = 0
        count = 0
        key_str = key[0]
        exists = self.num_violations_state.exists()
        if exists:
            existing_violations_row = self.num_violations_state.get()
            existing_violations = existing_violations_row[0]
            assert existing_violations == self.dict[0][key_str]
            self.batch_id = 1
        else:
            existing_violations = 0
        for row in rows:
            # temperature should be non-NA to be counted
            temperature = row.temperature
            if temperature is not None:
                count += 1
                if temperature > 100:
                    new_violations += 1
        updated_violations = new_violations + existing_violations
        assert updated_violations == self.dict[self.batch_id][key_str]
        self.num_violations_state.update((updated_violations,))
        yield Row(id=key[0], countAsString=str(count))

    def close(self) -> None:
        pass


class PandasStatefulProcessorChainingOps(StatefulProcessor):
    def init(self, handle: StatefulProcessorHandle) -> None:
        pass

    def handleInputRows(self, key, rows, timerValues) -> Iterator[pd.DataFrame]:
        for pdf in rows:
            timestamp_list = pdf["eventTime"].tolist()
        yield pd.DataFrame({"id": key, "outputTimestamp": timestamp_list[0]})

    def close(self) -> None:
        pass


class RowStatefulProcessorChainingOps(StatefulProcessor):
    def init(self, handle: StatefulProcessorHandle) -> None:
        pass

    def handleInputRows(self, key, rows, timerValues) -> Iterator[Row]:
        timestamp_list = []
        for row in rows:
            timestamp_list.append(row.eventTime)
        yield Row(id=key[0], outputTimestamp=timestamp_list[0])

    def close(self) -> None:
        pass


# A stateful processor that inherit all behavior of SimpleStatefulProcessor except that it use
# ttl state with a large timeout.
class PandasSimpleTTLStatefulProcessor(PandasSimpleStatefulProcessor, unittest.TestCase):
    def init(self, handle: StatefulProcessorHandle) -> None:
        state_schema = StructType([StructField("value", IntegerType(), True)])
        self.num_violations_state = handle.getValueState("numViolations", state_schema, 30000)
        self.temp_state = handle.getValueState("tempState", state_schema)
        handle.deleteIfExists("tempState")


# A stateful processor that inherit all behavior of SimpleStatefulProcessor except that it use
# ttl state with a large timeout.
class RowSimpleTTLStatefulProcessor(RowSimpleStatefulProcessor, unittest.TestCase):
    def init(self, handle: StatefulProcessorHandle) -> None:
        state_schema = StructType([StructField("value", IntegerType(), True)])
        self.num_violations_state = handle.getValueState("numViolations", state_schema, 30000)
        self.temp_state = handle.getValueState("tempState", state_schema)
        handle.deleteIfExists("tempState")


class PandasTTLStatefulProcessor(StatefulProcessor):
    def init(self, handle: StatefulProcessorHandle) -> None:
        state_schema = StructType([StructField("value", IntegerType(), True)])
        user_key_schema = StructType([StructField("id", StringType(), True)])
        self.ttl_count_state = handle.getValueState("ttl-state", state_schema, 10000)
        self.count_state = handle.getValueState("state", state_schema)
        self.ttl_list_state = handle.getListState("ttl-list-state", state_schema, 10000)
        self.ttl_map_state = handle.getMapState(
            "ttl-map-state", user_key_schema, state_schema, 10000
        )

    def handleInputRows(self, key, rows, timerValues) -> Iterator[pd.DataFrame]:
        count = 0
        ttl_count = 0
        ttl_list_state_count = 0
        ttl_map_state_count = 0
        id = key[0]
        if self.count_state.exists():
            count = self.count_state.get()[0]
        if self.ttl_count_state.exists():
            ttl_count = self.ttl_count_state.get()[0]
        if self.ttl_list_state.exists():
            iter = self.ttl_list_state.get()
            for s in iter:
                ttl_list_state_count += s[0]
        if self.ttl_map_state.exists():
            ttl_map_state_count = self.ttl_map_state.getValue(key)[0]
        for pdf in rows:
            pdf_count = pdf.count().get("temperature")
            count += pdf_count
            ttl_count += pdf_count
            ttl_list_state_count += pdf_count
            ttl_map_state_count += pdf_count

        self.count_state.update((count,))
        # skip updating state for the 2nd batch so that ttl state expire
        if not (ttl_count == 2 and id == "0"):
            self.ttl_count_state.update((ttl_count,))
            self.ttl_list_state.put([(ttl_list_state_count,), (ttl_list_state_count,)])
            self.ttl_map_state.updateValue(key, (ttl_map_state_count,))
        yield pd.DataFrame(
            {
                "id": [
                    f"ttl-count-{id}",
                    f"count-{id}",
                    f"ttl-list-state-count-{id}",
                    f"ttl-map-state-count-{id}",
                ],
                "count": [ttl_count, count, ttl_list_state_count, ttl_map_state_count],
            }
        )


class RowTTLStatefulProcessor(StatefulProcessor):
    def init(self, handle: StatefulProcessorHandle) -> None:
        state_schema = StructType([StructField("value", IntegerType(), True)])
        user_key_schema = StructType([StructField("id", StringType(), True)])
        self.ttl_count_state = handle.getValueState("ttl-state", state_schema, 10000)
        self.count_state = handle.getValueState("state", state_schema)
        self.ttl_list_state = handle.getListState("ttl-list-state", state_schema, 10000)
        self.ttl_map_state = handle.getMapState(
            "ttl-map-state", user_key_schema, state_schema, 10000
        )

    def handleInputRows(self, key, rows, timerValues) -> Iterator[Row]:
        count = 0
        ttl_count = 0
        ttl_list_state_count = 0
        ttl_map_state_count = 0
        id = key[0]
        if self.count_state.exists():
            count = self.count_state.get()[0]
        if self.ttl_count_state.exists():
            ttl_count = self.ttl_count_state.get()[0]
        if self.ttl_list_state.exists():
            iter = self.ttl_list_state.get()
            for s in iter:
                ttl_list_state_count += s[0]
        if self.ttl_map_state.exists():
            ttl_map_state_count = self.ttl_map_state.getValue(key)[0]
        for row in rows:
            if row.temperature is not None:
                count += 1
                ttl_count += 1
                ttl_list_state_count += 1
                ttl_map_state_count += 1

        self.count_state.update((count,))
        # skip updating state for the 2nd batch so that ttl state expire
        if not (ttl_count == 2 and id == "0"):
            self.ttl_count_state.update((ttl_count,))
            self.ttl_list_state.put([(ttl_list_state_count,), (ttl_list_state_count,)])
            self.ttl_map_state.updateValue(key, (ttl_map_state_count,))

        ret = [
            Row(id=f"ttl-count-{id}", count=ttl_count),
            Row(id=f"count-{id}", count=count),
            Row(id=f"ttl-list-state-count-{id}", count=ttl_list_state_count),
            Row(id=f"ttl-map-state-count-{id}", count=ttl_map_state_count),
        ]
        return iter(ret)


class PandasInvalidSimpleStatefulProcessor(StatefulProcessor):
    def init(self, handle: StatefulProcessorHandle) -> None:
        state_schema = StructType([StructField("value", IntegerType(), True)])
        self.num_violations_state = handle.getValueState("numViolations", state_schema)

    def handleInputRows(self, key, rows, timerValues) -> Iterator[pd.DataFrame]:
        count = 0
        exists = self.num_violations_state.exists()
        assert not exists
        # try to get a state variable with no value
        assert self.num_violations_state.get() is None
        self.num_violations_state.clear()
        yield pd.DataFrame({"id": key, "countAsString": str(count)})


class RowInvalidSimpleStatefulProcessor(StatefulProcessor):
    def init(self, handle: StatefulProcessorHandle) -> None:
        state_schema = StructType([StructField("value", IntegerType(), True)])
        self.num_violations_state = handle.getValueState("numViolations", state_schema)

    def handleInputRows(self, key, rows, timerValues) -> Iterator[Row]:
        count = 0
        exists = self.num_violations_state.exists()
        assert not exists
        # try to get a state variable with no value
        assert self.num_violations_state.get() is None
        self.num_violations_state.clear()
        yield Row(id=key[0], countAsString=str(count))


class PandasListStateProcessor(StatefulProcessor):
    # Dict to store the expected results. The key represents the grouping key string, and the value
    # is a dictionary of pandas dataframe index -> expected temperature value. Since we set
    # maxRecordsPerBatch to 2, we expect the pandas dataframe dictionary to have 2 entries.
    dict = {0: 120, 1: 20}

    def init(self, handle: StatefulProcessorHandle) -> None:
        state_schema = StructType([StructField("temperature", IntegerType(), True)])
        timestamp_schema = StructType([StructField("time", TimestampType(), True)])
        self.list_state1 = handle.getListState("listState1", state_schema)
        self.list_state2 = handle.getListState("listState2", state_schema)
        self.list_state_timestamp = handle.getListState("listStateTimestamp", timestamp_schema)

    def handleInputRows(self, key, rows, timerValues) -> Iterator[pd.DataFrame]:
        import datetime

        count = 0
        time_list = []
        for pdf in rows:
            list_state_rows = [(120,), (20,)]
            self.list_state1.put(list_state_rows)
            self.list_state2.put(list_state_rows)
            self.list_state1.appendValue((111,))
            self.list_state2.appendValue((222,))
            self.list_state1.appendList(list_state_rows)
            self.list_state2.appendList(list_state_rows)
            pdf_count = pdf.count()
            count += pdf_count.get("temperature")
            current_processing_time = datetime.datetime.fromtimestamp(
                timerValues.getCurrentProcessingTimeInMs() / 1000
            )
            stored_time = current_processing_time + datetime.timedelta(minutes=1)
            time_list.append((stored_time,))
        iter1 = self.list_state1.get()
        iter2 = self.list_state2.get()
        # Mixing the iterator to test it we can resume from the correct point
        assert next(iter1)[0] == self.dict[0]
        assert next(iter2)[0] == self.dict[0]
        assert next(iter1)[0] == self.dict[1]
        assert next(iter2)[0] == self.dict[1]
        # Get another iterator for list_state1 to test if the 2 iterators (iter1 and iter3) don't
        # interfere with each other.
        iter3 = self.list_state1.get()
        assert next(iter3)[0] == self.dict[0]
        assert next(iter3)[0] == self.dict[1]
        # the second arrow batch should contain the appended value 111 for list_state1 and
        # 222 for list_state2
        assert next(iter1)[0] == 111
        assert next(iter2)[0] == 222
        assert next(iter3)[0] == 111
        # since we put another 2 rows after 111/222, check them here
        assert next(iter1)[0] == self.dict[0]
        assert next(iter2)[0] == self.dict[0]
        assert next(iter3)[0] == self.dict[0]
        assert next(iter1)[0] == self.dict[1]
        assert next(iter2)[0] == self.dict[1]
        assert next(iter3)[0] == self.dict[1]
        if time_list:
            # Validate timestamp type can work properly with arrow transmission
            self.list_state_timestamp.put(time_list)
        yield pd.DataFrame({"id": key, "countAsString": str(count)})

    def close(self) -> None:
        pass


class RowListStateProcessor(StatefulProcessor):
    # Dict to store the expected results. The key represents the grouping key string, and the value
    # is a dictionary of pandas dataframe index -> expected temperature value. Since we set
    # maxRecordsPerBatch to 2, we expect the pandas dataframe dictionary to have 2 entries.
    dict = {0: 120, 1: 20}

    def init(self, handle: StatefulProcessorHandle) -> None:
        state_schema = StructType([StructField("temperature", IntegerType(), True)])
        timestamp_schema = StructType([StructField("time", TimestampType(), True)])
        self.list_state1 = handle.getListState("listState1", state_schema)
        self.list_state2 = handle.getListState("listState2", state_schema)
        self.list_state_timestamp = handle.getListState("listStateTimestamp", timestamp_schema)

    def handleInputRows(self, key, rows, timerValues) -> Iterator[Row]:
        import datetime

        count = 0
        time_list = []
        for row in rows:
            list_state_rows = [(120,), (20,)]
            self.list_state1.put(list_state_rows)
            self.list_state2.put(list_state_rows)
            self.list_state1.appendValue((111,))
            self.list_state2.appendValue((222,))
            self.list_state1.appendList(list_state_rows)
            self.list_state2.appendList(list_state_rows)

            if row.temperature is not None:
                count += 1

            current_processing_time = datetime.datetime.fromtimestamp(
                timerValues.getCurrentProcessingTimeInMs() / 1000
            )
            stored_time = current_processing_time + datetime.timedelta(minutes=1)
            time_list.append((stored_time,))
        iter1 = self.list_state1.get()
        iter2 = self.list_state2.get()
        # Mixing the iterator to test it we can resume from the correct point
        assert next(iter1)[0] == self.dict[0]
        assert next(iter2)[0] == self.dict[0]
        assert next(iter1)[0] == self.dict[1]
        assert next(iter2)[0] == self.dict[1]
        # Get another iterator for list_state1 to test if the 2 iterators (iter1 and iter3) don't
        # interfere with each other.
        iter3 = self.list_state1.get()
        assert next(iter3)[0] == self.dict[0]
        assert next(iter3)[0] == self.dict[1]
        # the second arrow batch should contain the appended value 111 for list_state1 and
        # 222 for list_state2
        assert next(iter1)[0] == 111
        assert next(iter2)[0] == 222
        assert next(iter3)[0] == 111
        # since we put another 2 rows after 111/222, check them here
        assert next(iter1)[0] == self.dict[0]
        assert next(iter2)[0] == self.dict[0]
        assert next(iter3)[0] == self.dict[0]
        assert next(iter1)[0] == self.dict[1]
        assert next(iter2)[0] == self.dict[1]
        assert next(iter3)[0] == self.dict[1]
        if time_list:
            # Validate timestamp type can work properly with arrow transmission
            self.list_state_timestamp.put(time_list)
        yield Row(id=key[0], countAsString=str(count))

    def close(self) -> None:
        pass


class PandasListStateLargeListProcessor(StatefulProcessor):
    def init(self, handle: StatefulProcessorHandle) -> None:
        list_state_schema = StructType(
            [
                StructField("value", IntegerType(), True),
                StructField("valueNull", IntegerType(), True),
            ]
        )
        value_state_schema = StructType([StructField("size", IntegerType(), True)])
        self.list_state = handle.getListState("listState", list_state_schema)
        self.list_size_state = handle.getValueState("listSizeState", value_state_schema)

    def handleInputRows(self, key, rows, timerValues) -> Iterator[pd.DataFrame]:
        elements_iter = self.list_state.get()
        elements = list(elements_iter)

        # Use the magic number 100 to test with both inline proto case and Arrow case.
        # Now the magic number is not actually used, but this is to make this test be a regression
        # test of SPARK-53743.
        # Explicitly put 100 elements of list which triggered Arrow based list serialization before
        # SPARK-53743.

        if len(elements) == 0:
            # should be the first batch
            assert self.list_size_state.get() is None
            new_elements = [(i, None) for i in range(100)]
            if key == ("0",):
                self.list_state.put(new_elements)
            else:
                self.list_state.appendList(new_elements)
            self.list_size_state.update((len(new_elements),))
        else:
            # check the elements
            list_size = self.list_size_state.get()
            assert list_size is not None
            list_size = list_size[0]
            assert list_size == len(
                elements
            ), f"list_size ({list_size}) != len(elements) ({len(elements)})"

            expected_elements_in_state = [(i, None) for i in range(list_size)]
            assert (
                elements == expected_elements_in_state
            ), f"expected {expected_elements_in_state} but got {elements}"

            if key == ("0",):
                # Use the operation `put`
                new_elements = [(i, None) for i in range(list_size + 90)]
                self.list_state.put(new_elements)
                final_size = len(new_elements)
                self.list_size_state.update((final_size,))
            else:
                # Use the operation `appendList`
                new_elements = [(i, None) for i in range(list_size, list_size + 90)]
                self.list_state.appendList(new_elements)
                final_size = len(new_elements) + list_size
                self.list_size_state.update((final_size,))

        prev_elements = ",".join(map(lambda x: str(x[0]), elements))
        updated_elements = ",".join(map(lambda x: str(x[0]), self.list_state.get()))

        yield pd.DataFrame(
            {"id": key, "prevElements": prev_elements, "updatedElements": updated_elements}
        )


class RowListStateLargeListProcessor(StatefulProcessor):
    def init(self, handle: StatefulProcessorHandle) -> None:
        list_state_schema = StructType(
            [
                StructField("value", IntegerType(), True),
                StructField("valueNull", IntegerType(), True),
            ]
        )
        value_state_schema = StructType([StructField("size", IntegerType(), True)])
        self.list_state = handle.getListState("listState", list_state_schema)
        self.list_size_state = handle.getValueState("listSizeState", value_state_schema)

    def handleInputRows(self, key, rows, timerValues) -> Iterator[Row]:
        elements_iter = self.list_state.get()

        elements = list(elements_iter)

        # Use the magic number 100 to test with both inline proto case and Arrow case.
        # Now the magic number is not actually used, but this is to make this test be a regression
        # test of SPARK-53743.
        # Explicitly put 100 elements of list which triggered Arrow based list serialization before
        # SPARK-53743.

        if len(elements) == 0:
            # should be the first batch
            assert self.list_size_state.get() is None
            new_elements = [(i, None) for i in range(100)]
            if key == ("0",):
                self.list_state.put(new_elements)
            else:
                self.list_state.appendList(new_elements)
            self.list_size_state.update((len(new_elements),))
        else:
            # check the elements
            list_size = self.list_size_state.get()
            assert list_size is not None
            list_size = list_size[0]
            assert list_size == len(
                elements
            ), f"list_size ({list_size}) != len(elements) ({len(elements)})"

            expected_elements_in_state = [(i, None) for i in range(list_size)]
            assert (
                elements == expected_elements_in_state
            ), f"expected {expected_elements_in_state} but got {elements}"

            if key == ("0",):
                # Use the operation `put`
                new_elements = [(i, None) for i in range(list_size + 90)]
                self.list_state.put(new_elements)
                final_size = len(new_elements)
                self.list_size_state.update((final_size,))
            else:
                # Use the operation `appendList`
                new_elements = [(i, None) for i in range(list_size, list_size + 90)]
                self.list_state.appendList(new_elements)
                final_size = len(new_elements) + list_size
                self.list_size_state.update((final_size,))

        prev_elements = ",".join(map(lambda x: str(x[0]), elements))
        updated_elements = ",".join(map(lambda x: str(x[0]), self.list_state.get()))

        yield Row(id=key[0], prevElements=prev_elements, updatedElements=updated_elements)


class PandasListStateLargeTTLProcessor(PandasListStateProcessor):
    def init(self, handle: StatefulProcessorHandle) -> None:
        state_schema = StructType([StructField("temperature", IntegerType(), True)])
        timestamp_schema = StructType([StructField("time", TimestampType(), True)])
        self.list_state1 = handle.getListState("listState1", state_schema, 30000)
        self.list_state2 = handle.getListState("listState2", state_schema, 30000)
        self.list_state_timestamp = handle.getListState("listStateTimestamp", timestamp_schema)


class RowListStateLargeTTLProcessor(RowListStateProcessor):
    def init(self, handle: StatefulProcessorHandle) -> None:
        state_schema = StructType([StructField("temperature", IntegerType(), True)])
        timestamp_schema = StructType([StructField("time", TimestampType(), True)])
        self.list_state1 = handle.getListState("listState1", state_schema, 30000)
        self.list_state2 = handle.getListState("listState2", state_schema, 30000)
        self.list_state_timestamp = handle.getListState("listStateTimestamp", timestamp_schema)


class PandasMapStateProcessor(StatefulProcessor):
    def init(self, handle: StatefulProcessorHandle):
        # Test string type schemas
        self.map_state = handle.getMapState("mapState", "name string", "count int")

    def handleInputRows(self, key, rows, timerValues) -> Iterator[pd.DataFrame]:
        count = 0
        key1 = ("key1",)
        key2 = ("key2",)
        for pdf in rows:
            pdf_count = pdf.count()
            count += pdf_count.get("temperature")
        value1 = count
        value2 = count
        if self.map_state.exists():
            if self.map_state.containsKey(key1):
                value1 += self.map_state.getValue(key1)[0]
            if self.map_state.containsKey(key2):
                value2 += self.map_state.getValue(key2)[0]
        self.map_state.updateValue(key1, (value1,))
        self.map_state.updateValue(key2, (value2,))
        key_iter = self.map_state.keys()
        assert next(key_iter)[0] == "key1"
        assert next(key_iter)[0] == "key2"
        value_iter = self.map_state.values()
        assert next(value_iter)[0] == value1
        assert next(value_iter)[0] == value2
        map_iter = self.map_state.iterator()
        assert next(map_iter)[0] == key1
        assert next(map_iter)[1] == (value2,)
        self.map_state.removeKey(key1)
        assert not self.map_state.containsKey(key1)
        assert self.map_state.exists()
        self.map_state.clear()
        assert not self.map_state.exists()
        yield pd.DataFrame({"id": key, "countAsString": str(count)})

    def close(self) -> None:
        pass


class RowMapStateProcessor(StatefulProcessor):
    def init(self, handle: StatefulProcessorHandle):
        # Test string type schemas
        self.map_state = handle.getMapState("mapState", "name string", "count int")

    def handleInputRows(self, key, rows, timerValues) -> Iterator[Row]:
        count = 0
        key1 = ("key1",)
        key2 = ("key2",)
        for row in rows:
            if row.temperature is not None:
                count += 1
        value1 = count
        value2 = count
        if self.map_state.exists():
            if self.map_state.containsKey(key1):
                value1 += self.map_state.getValue(key1)[0]
            if self.map_state.containsKey(key2):
                value2 += self.map_state.getValue(key2)[0]
        self.map_state.updateValue(key1, (value1,))
        self.map_state.updateValue(key2, (value2,))
        key_iter = self.map_state.keys()
        assert next(key_iter)[0] == "key1"
        assert next(key_iter)[0] == "key2"
        value_iter = self.map_state.values()
        assert next(value_iter)[0] == value1
        assert next(value_iter)[0] == value2
        map_iter = self.map_state.iterator()
        assert next(map_iter)[0] == key1
        assert next(map_iter)[1] == (value2,)
        self.map_state.removeKey(key1)
        assert not self.map_state.containsKey(key1)
        assert self.map_state.exists()
        self.map_state.clear()
        assert not self.map_state.exists()
        yield Row(id=key[0], countAsString=str(count))

    def close(self) -> None:
        pass


# A stateful processor that inherit all behavior of MapStateProcessor except that it use
# ttl state with a large timeout.
class PandasMapStateLargeTTLProcessor(PandasMapStateProcessor):
    def init(self, handle: StatefulProcessorHandle) -> None:
        key_schema = StructType([StructField("name", StringType(), True)])
        value_schema = StructType([StructField("count", IntegerType(), True)])
        # Use a large timeout as long as 1 year
        self.map_state = handle.getMapState("mapState", key_schema, value_schema, 31536000000)
        self.list_state = handle.getListState("listState", key_schema)


# A stateful processor that inherit all behavior of MapStateProcessor except that it use
# ttl state with a large timeout.
class RowMapStateLargeTTLProcessor(RowMapStateProcessor):
    def init(self, handle: StatefulProcessorHandle) -> None:
        key_schema = StructType([StructField("name", StringType(), True)])
        value_schema = StructType([StructField("count", IntegerType(), True)])
        # Use a large timeout as long as 1 year
        self.map_state = handle.getMapState("mapState", key_schema, value_schema, 31536000000)
        self.list_state = handle.getListState("listState", key_schema)


class PandasBasicProcessor(StatefulProcessor):
    # Schema definitions
    state_schema = StructType(
        [StructField("id", IntegerType(), True), StructField("name", StringType(), True)]
    )

    def init(self, handle):
        self.state = handle.getValueState("state", self.state_schema)

    def handleInputRows(self, key, rows, timerValues) -> Iterator[pd.DataFrame]:
        for pdf in rows:
            pass
        id_val = int(key[0])
        name = f"name-{id_val}"
        self.state.update((id_val, name))
        yield pd.DataFrame({"id": [key[0]], "value": [{"id": id_val, "name": name}]})

    def close(self) -> None:
        pass


class RowBasicProcessor(StatefulProcessor):
    # Schema definitions
    state_schema = StructType(
        [StructField("id", IntegerType(), True), StructField("name", StringType(), True)]
    )

    def init(self, handle):
        self.state = handle.getValueState("state", self.state_schema)

    def handleInputRows(self, key, rows, timerValues) -> Iterator[Row]:
        for pdf in rows:
            pass
        id_val = int(key[0])
        name = f"name-{id_val}"
        self.state.update((id_val, name))
        yield Row(id=key[0], value={"id": id_val, "name": name})

    def close(self) -> None:
        pass


class PandasBasicProcessorNotNullable(StatefulProcessor):
    # Schema definitions
    state_schema = StructType(
        [StructField("id", IntegerType(), False), StructField("name", StringType(), False)]
    )

    def init(self, handle):
        self.state = handle.getValueState("state", self.state_schema)

    def handleInputRows(self, key, rows, timerValues) -> Iterator[pd.DataFrame]:
        for pdf in rows:
            pass
        id_val = int(key[0])
        name = f"name-{id_val}"
        self.state.update((id_val, name))
        yield pd.DataFrame({"id": [key[0]], "value": [{"id": id_val, "name": name}]})

    def close(self) -> None:
        pass


class RowBasicProcessorNotNullable(StatefulProcessor):
    # Schema definitions
    state_schema = StructType(
        [StructField("id", IntegerType(), False), StructField("name", StringType(), False)]
    )

    def init(self, handle):
        self.state = handle.getValueState("state", self.state_schema)

    def handleInputRows(self, key, rows, timerValues) -> Iterator[Row]:
        for pdf in rows:
            pass
        id_val = int(key[0])
        name = f"name-{id_val}"
        self.state.update((id_val, name))
        yield Row(id=key[0], value={"id": id_val, "name": name})

    def close(self) -> None:
        pass


class PandasAddFieldsProcessor(StatefulProcessor):
    state_schema = StructType(
        [
            StructField("id", IntegerType(), True),
            StructField("name", StringType(), True),
            StructField("count", IntegerType(), True),
            StructField("active", BooleanType(), True),
            StructField("score", FloatType(), True),
        ]
    )

    def init(self, handle):
        self.state = handle.getValueState("state", self.state_schema)

    def handleInputRows(self, key, rows, timerValues) -> Iterator[pd.DataFrame]:
        for pdf in rows:
            pass
        id_val = int(key[0])
        name = f"name-{id_val}"

        if self.state.exists():
            state_data = self.state.get()
            state_dict = {
                "id": state_data[0],
                "name": state_data[1],
                "count": state_data[2],
                "active": state_data[3],
                "score": state_data[4],
            }
        else:
            state_dict = {
                "id": id_val,
                "name": name,
                "count": 100,
                "active": True,
                "score": 99.9,
            }

        self.state.update(
            (
                state_dict["id"],
                state_dict["name"] + "0",
                state_dict["count"],
                state_dict["active"],
                state_dict["score"],
            )
        )
        yield pd.DataFrame({"id": [key[0]], "value": [state_dict]})

    def close(self) -> None:
        pass


class RowAddFieldsProcessor(StatefulProcessor):
    state_schema = StructType(
        [
            StructField("id", IntegerType(), True),
            StructField("name", StringType(), True),
            StructField("count", IntegerType(), True),
            StructField("active", BooleanType(), True),
            StructField("score", FloatType(), True),
        ]
    )

    def init(self, handle):
        self.state = handle.getValueState("state", self.state_schema)

    def handleInputRows(self, key, rows, timerValues) -> Iterator[Row]:
        for pdf in rows:
            pass
        id_val = int(key[0])
        name = f"name-{id_val}"

        if self.state.exists():
            state_data = self.state.get()
            state_dict = {
                "id": state_data[0],
                "name": state_data[1],
                "count": state_data[2],
                "active": state_data[3],
                "score": state_data[4],
            }
        else:
            state_dict = {
                "id": id_val,
                "name": name,
                "count": 100,
                "active": True,
                "score": 99.9,
            }

        self.state.update(
            (
                state_dict["id"],
                state_dict["name"] + "0",
                state_dict["count"],
                state_dict["active"],
                state_dict["score"],
            )
        )
        yield Row(id=key[0], value=state_dict)

    def close(self) -> None:
        pass


class PandasRemoveFieldsProcessor(StatefulProcessor):
    # Schema definitions
    state_schema = StructType(
        [StructField("id", IntegerType(), True), StructField("name", StringType(), True)]
    )

    def init(self, handle):
        self.state = handle.getValueState("state", self.state_schema)

    def handleInputRows(self, key, rows, timerValues) -> Iterator[pd.DataFrame]:
        for pdf in rows:
            pass
        id_val = int(key[0])
        name = f"name-{id_val}"
        if self.state.exists():
            name = self.state.get()[1]
        self.state.update((id_val, name))
        yield pd.DataFrame({"id": [key[0]], "value": [{"id": id_val, "name": name}]})

    def close(self) -> None:
        pass


class RowRemoveFieldsProcessor(StatefulProcessor):
    # Schema definitions
    state_schema = StructType(
        [StructField("id", IntegerType(), True), StructField("name", StringType(), True)]
    )

    def init(self, handle):
        self.state = handle.getValueState("state", self.state_schema)

    def handleInputRows(self, key, rows, timerValues) -> Iterator[Row]:
        for pdf in rows:
            pass
        id_val = int(key[0])
        name = f"name-{id_val}"
        if self.state.exists():
            name = self.state.get()[1]
        self.state.update((id_val, name))
        yield Row(id=key[0], value={"id": id_val, "name": name})

    def close(self) -> None:
        pass


class PandasReorderedFieldsProcessor(StatefulProcessor):
    state_schema = StructType(
        [
            StructField("name", StringType(), True),
            StructField("id", IntegerType(), True),
            StructField("score", FloatType(), True),
            StructField("count", IntegerType(), True),
            StructField("active", BooleanType(), True),
        ]
    )

    def init(self, handle):
        self.state = handle.getValueState("state", self.state_schema)

    def handleInputRows(self, key, rows, timerValues) -> Iterator[pd.DataFrame]:
        for pdf in rows:
            pass
        id_val = int(key[0])
        name = f"name-{id_val}"

        if self.state.exists():
            state_data = self.state.get()
            state_dict = {
                "name": state_data[0],
                "id": state_data[1],
                "score": state_data[2],
                "count": state_data[3],
                "active": state_data[4],
            }
        else:
            state_dict = {
                "name": name,
                "id": id_val,
                "score": 99.9,
                "count": 100,
                "active": True,
            }
        self.state.update(
            (
                state_dict["name"],
                state_dict["id"],
                state_dict["score"],
                state_dict["count"],
                state_dict["active"],
            )
        )
        yield pd.DataFrame({"id": [key[0]], "value": [state_dict]})

    def close(self) -> None:
        pass


class RowReorderedFieldsProcessor(StatefulProcessor):
    state_schema = StructType(
        [
            StructField("name", StringType(), True),
            StructField("id", IntegerType(), True),
            StructField("score", FloatType(), True),
            StructField("count", IntegerType(), True),
            StructField("active", BooleanType(), True),
        ]
    )

    def init(self, handle):
        self.state = handle.getValueState("state", self.state_schema)

    def handleInputRows(self, key, rows, timerValues) -> Iterator[Row]:
        for pdf in rows:
            pass
        id_val = int(key[0])
        name = f"name-{id_val}"

        if self.state.exists():
            state_data = self.state.get()
            state_dict = {
                "name": state_data[0],
                "id": state_data[1],
                "score": state_data[2],
                "count": state_data[3],
                "active": state_data[4],
            }
        else:
            state_dict = {
                "name": name,
                "id": id_val,
                "score": 99.9,
                "count": 100,
                "active": True,
            }
        self.state.update(
            (
                state_dict["name"],
                state_dict["id"],
                state_dict["score"],
                state_dict["count"],
                state_dict["active"],
            )
        )
        yield Row(id=key[0], value=state_dict)

    def close(self) -> None:
        pass


class PandasUpcastProcessor(StatefulProcessor):
    state_schema = StructType(
        [
            StructField("id", LongType(), True),  # Upcast from Int to Long
            StructField("name", StringType(), True),
        ]
    )

    def init(self, handle):
        self.state = handle.getValueState("state", self.state_schema)

    def handleInputRows(self, key, rows, timerValues) -> Iterator[pd.DataFrame]:
        for pdf in rows:
            pass
        id_val = int(key[0])
        name = f"name-{id_val}"
        if self.state.exists():
            id_val += self.state.get()[0] + 1
        self.state.update((id_val, name))
        yield pd.DataFrame({"id": [key[0]], "value": [{"id": id_val, "name": name}]})

    def close(self) -> None:
        pass


class RowUpcastProcessor(StatefulProcessor):
    state_schema = StructType(
        [
            StructField("id", LongType(), True),  # Upcast from Int to Long
            StructField("name", StringType(), True),
        ]
    )

    def init(self, handle):
        self.state = handle.getValueState("state", self.state_schema)

    def handleInputRows(self, key, rows, timerValues) -> Iterator[Row]:
        for pdf in rows:
            pass
        id_val = int(key[0])
        name = f"name-{id_val}"
        if self.state.exists():
            id_val += self.state.get()[0] + 1
        self.state.update((id_val, name))
        yield Row(id=key[0], value={"id": id_val, "name": name})

    def close(self) -> None:
        pass


class PandasMinEventTimeStatefulProcessor(StatefulProcessor):
    def init(self, handle: StatefulProcessorHandle) -> None:
        state_schema = StructType([StructField("value", StringType(), True)])
        self.handle = handle
        self.min_state = handle.getValueState("min_state", state_schema)

    def handleInputRows(self, key, rows, timerValues) -> Iterator[pd.DataFrame]:
        timestamp_list = []
        for pdf in rows:
            # int64 will represent timestamp in nanosecond, restore to second
            timestamp_list.extend((pdf["eventTime"].astype("int64") // 10**9).tolist())

        if self.min_state.exists():
            cur_min = int(self.min_state.get()[0])
        else:
            cur_min = sys.maxsize
        min_event_time = str(min(cur_min, min(timestamp_list)))

        self.min_state.update((min_event_time,))

        yield pd.DataFrame({"id": key, "timestamp": min_event_time})

    def close(self) -> None:
        pass


class RowMinEventTimeStatefulProcessor(StatefulProcessor):
    def init(self, handle: StatefulProcessorHandle) -> None:
        state_schema = StructType([StructField("value", StringType(), True)])
        self.handle = handle
        self.min_state = handle.getValueState("min_state", state_schema)

    def handleInputRows(self, key, rows, timerValues) -> Iterator[Row]:
        timestamp_list = []
        for row in rows:
            # timestamp is microsecond, restore to second
            timestamp_list.append(int(row.eventTime.timestamp()))

        if self.min_state.exists():
            cur_min = int(self.min_state.get()[0])
        else:
            cur_min = sys.maxsize
        min_event_time = str(min(cur_min, min(timestamp_list)))

        self.min_state.update((min_event_time,))

        yield Row(id=key[0], timestamp=min_event_time)

    def close(self) -> None:
        pass


# A stateful processor that contains composite python type inside Value, List and Map state variable
class PandasStatefulProcessorCompositeType(StatefulProcessor):
    class Address(NamedTuple):
        road_id: int
        city: str

    TAGS = [["dummy1", "dummy2"], ["dummy3"]]
    METADATA = [{"key": "env", "value": "prod"}, {"key": "region", "value": "us-west"}]
    ATTRIBUTES_MAP = {"key1": [1], "key2": [10]}
    CONFS_MAP = {"e1": {"e2": 5, "e3": 10}}
    ADDRESS = [Address(1, "Seattle"), Address(3, "SF")]

    def init(self, handle: StatefulProcessorHandle) -> None:
        obj_schema = StructType(
            [
                StructField("id", ArrayType(IntegerType())),
                StructField("tags", ArrayType(ArrayType(StringType()))),
                StructField(
                    "metadata",
                    ArrayType(
                        StructType(
                            [StructField("key", StringType()), StructField("value", StringType())]
                        )
                    ),
                ),
                StructField(
                    "address",
                    ArrayType(
                        StructType(
                            [
                                StructField("road_id", IntegerType()),
                                StructField("city", StringType()),
                            ]
                        )
                    ),
                ),
            ]
        )

        map_value_schema = StructType(
            [
                StructField("id", IntegerType(), True),
                StructField("attributes", MapType(StringType(), ArrayType(IntegerType())), True),
                StructField(
                    "confs", MapType(StringType(), MapType(StringType(), IntegerType()), True), True
                ),
            ]
        )

        self.obj_state = handle.getValueState("obj_state", obj_schema)
        self.list_state = handle.getListState("list_state", obj_schema)
        self.map_state = handle.getMapState("map_state", "name string", map_value_schema)

    def _update_obj_state(self, total_temperature):
        if self.obj_state.exists():
            ids, tags, metadata, address = self.obj_state.get()
            assert tags == self.TAGS, f"Tag mismatch: {tags}"
            assert metadata == [Row(**m) for m in self.METADATA], f"Metadata mismatch: {metadata}"
            assert address == [
                Row(**e._asdict()) for e in self.ADDRESS
            ], f"Address mismatch: {address}"
            ids = [int(x + total_temperature) for x in ids]
        else:
            ids = [0]
        self.obj_state.update((ids, self.TAGS, self.METADATA, self.ADDRESS))
        return ids

    def _update_list_state(self, total_temperature, initial_obj):
        existing_list = self.list_state.get()
        updated_list = []
        for ids, tags, metadata, address in existing_list:
            ids.append(total_temperature)
            updated_list.append((ids, tags, [row.asDict() for row in metadata], address))
        if not updated_list:
            updated_list.append(initial_obj)
        self.list_state.put(updated_list)
        return [id_val for ids, _, _, _ in updated_list for id_val in ids]

    def _update_map_state(self, key, total_temperature):
        if not self.map_state.containsKey(key):
            self.map_state.updateValue(key, (0, self.ATTRIBUTES_MAP, self.CONFS_MAP))
        else:
            id_val, attributes, confs = self.map_state.getValue(key)
            attributes[key] = [total_temperature]
            confs.setdefault("e1", {})[key] = total_temperature
            self.map_state.updateValue(key, (id_val, attributes, confs))
        return self.map_state.getValue(key)[1], self.map_state.getValue(key)[2]

    def handleInputRows(self, key, rows, timerValues) -> Iterator[pd.DataFrame]:
        key = key[0]
        total_temperature = sum(pdf["temperature"].astype(int).sum() for pdf in rows)

        updated_ids = self._update_obj_state(total_temperature)
        flattened_ids = self._update_list_state(
            total_temperature, (updated_ids, self.TAGS, self.METADATA, self.ADDRESS)
        )
        attributes_map, confs_map = self._update_map_state(key, total_temperature)

        import json
        import numpy as np

        def np_int64_to_int(x):
            if isinstance(x, np.int64):
                return int(x)
            return x

        yield pd.DataFrame(
            {
                "id": [key],
                "value_arr": [",".join(map(str, updated_ids))],
                "list_state_arr": [",".join(map(str, flattened_ids))],
                "map_state_arr": [
                    json.dumps(attributes_map, default=np_int64_to_int, sort_keys=True)
                ],
                "nested_map_state_arr": [
                    json.dumps(confs_map, default=np_int64_to_int, sort_keys=True)
                ],
            }
        )

    def close(self) -> None:
        pass


class RowStatefulProcessorCompositeType(StatefulProcessor):
    class Address(NamedTuple):
        road_id: int
        city: str

    TAGS = [["dummy1", "dummy2"], ["dummy3"]]
    METADATA = [{"key": "env", "value": "prod"}, {"key": "region", "value": "us-west"}]
    ATTRIBUTES_MAP = {"key1": [1], "key2": [10]}
    CONFS_MAP = {"e1": {"e2": 5, "e3": 10}}
    ADDRESS = [Address(1, "Seattle"), Address(3, "SF")]

    def init(self, handle: StatefulProcessorHandle) -> None:
        obj_schema = StructType(
            [
                StructField("id", ArrayType(IntegerType())),
                StructField("tags", ArrayType(ArrayType(StringType()))),
                StructField(
                    "metadata",
                    ArrayType(
                        StructType(
                            [StructField("key", StringType()), StructField("value", StringType())]
                        )
                    ),
                ),
                StructField(
                    "address",
                    ArrayType(
                        StructType(
                            [
                                StructField("road_id", IntegerType()),
                                StructField("city", StringType()),
                            ]
                        )
                    ),
                ),
            ]
        )

        map_value_schema = StructType(
            [
                StructField("id", IntegerType(), True),
                StructField("attributes", MapType(StringType(), ArrayType(IntegerType())), True),
                StructField(
                    "confs", MapType(StringType(), MapType(StringType(), IntegerType()), True), True
                ),
            ]
        )

        self.obj_state = handle.getValueState("obj_state", obj_schema)
        self.list_state = handle.getListState("list_state", obj_schema)
        self.map_state = handle.getMapState("map_state", "name string", map_value_schema)

    def _update_obj_state(self, total_temperature):
        if self.obj_state.exists():
            ids, tags, metadata, address = self.obj_state.get()
            assert tags == self.TAGS, f"Tag mismatch: {tags}"
            assert metadata == [Row(**m) for m in self.METADATA], f"Metadata mismatch: {metadata}"
            assert address == [
                Row(**e._asdict()) for e in self.ADDRESS
            ], f"Address mismatch: {address}"
            ids = [int(x + total_temperature) for x in ids]
        else:
            ids = [0]
        self.obj_state.update((ids, self.TAGS, self.METADATA, self.ADDRESS))
        return ids

    def _update_list_state(self, total_temperature, initial_obj):
        existing_list = self.list_state.get()
        updated_list = []
        for ids, tags, metadata, address in existing_list:
            ids.append(total_temperature)
            updated_list.append((ids, tags, [row.asDict() for row in metadata], address))
        if not updated_list:
            updated_list.append(initial_obj)
        self.list_state.put(updated_list)
        return [id_val for ids, _, _, _ in updated_list for id_val in ids]

    def _update_map_state(self, key, total_temperature):
        if not self.map_state.containsKey(key):
            self.map_state.updateValue(key, (0, self.ATTRIBUTES_MAP, self.CONFS_MAP))
        else:
            id_val, attributes, confs = self.map_state.getValue(key)
            attributes[key] = [total_temperature]
            confs.setdefault("e1", {})[key] = total_temperature
            self.map_state.updateValue(key, (id_val, attributes, confs))
        return self.map_state.getValue(key)[1], self.map_state.getValue(key)[2]

    def handleInputRows(self, key, rows, timerValues) -> Iterator[Row]:
        key = key[0]
        total_temperature = sum(int(row.temperature) for row in rows)

        updated_ids = self._update_obj_state(total_temperature)
        flattened_ids = self._update_list_state(
            total_temperature, (updated_ids, self.TAGS, self.METADATA, self.ADDRESS)
        )
        attributes_map, confs_map = self._update_map_state(key, total_temperature)

        import json

        yield Row(
            id=key,
            value_arr=",".join(map(str, updated_ids)),
            list_state_arr=",".join(map(str, flattened_ids)),
            map_state_arr=json.dumps(attributes_map, sort_keys=True),
            nested_map_state_arr=json.dumps(confs_map, sort_keys=True),
        )

    def close(self) -> None:
        pass


class PandasChunkCountProcessor(StatefulProcessor):
    def init(self, handle: StatefulProcessorHandle) -> None:
        pass

    def handleInputRows(self, key, rows, timerValues) -> Iterator[pd.DataFrame]:
        chunk_count = 0
        for _ in rows:
            chunk_count += 1
        yield pd.DataFrame({"id": [key[0]], "chunkCount": [chunk_count]})

    def close(self) -> None:
        pass


class PandasChunkCountWithInitialStateProcessor(StatefulProcessor):
    def init(self, handle: StatefulProcessorHandle) -> None:
        state_schema = StructType([StructField("value", IntegerType(), True)])
        self.value_state = handle.getValueState("value_state", state_schema)

    def handleInputRows(self, key, rows, timerValues) -> Iterator[pd.DataFrame]:
        chunk_count = 0
        for _ in rows:
            chunk_count += 1
        yield pd.DataFrame({"id": [key[0]], "chunkCount": [chunk_count]})

    def handleInitialState(self, key, initialState, timerValues) -> None:
        init_val = initialState.at[0, "initVal"]
        self.value_state.update((init_val,))

    def close(self) -> None:
        pass


# A Pandas stateful processor with a simple ValueState computation and composite output schema:
#
# primitiveValue: StringType
# listOfPrimitive: ArrayType(StringType)
# mapOfPrimitive: MapType(StringType, StringType)
# listOfComposite: ArrayType(InnerNestedClass)
# mapOfComposite: MapType(StringType, InnerNestedClass)
#
# where InnerNestedClass is a StructType with:
# intValue: IntegerType
# doubleValue: DoubleType
# arrayValue: ArrayType(StringType)
# mapValue: MapType(StringType, StringType)
class PandasCompositeOutputProcessor(StatefulProcessor):
    def init(self, handle: StatefulProcessorHandle) -> None:
        # Simple value state to track counts
        state_schema = StructType([StructField("value", IntegerType(), True)])
        self.count_state = handle.getValueState("count_state", state_schema)

    def handleInputRows(self, key, rows, timerValues) -> Iterator[pd.DataFrame]:
        # Calculate count from input rows
        count = 0
        if self.count_state.exists():
            count = self.count_state.get()[0]

        for pdf in rows:
            count += len(pdf)

        self.count_state.update((count,))

        # Build output matching the composite schema
        key_str = key[0]

        # primitiveValue: StringType
        primitive_value = f"key_{key_str}_count_{count}"

        # listOfPrimitive: ArrayType(StringType)
        list_of_primitive = [f"item_{i}" for i in range(count)]

        # mapOfPrimitive: MapType(StringType, StringType)
        map_of_primitive = {f"key{i}": f"value{i}" for i in range(count)}

        # listOfComposite: ArrayType(StructType)
        # Each Row schema matches the InnerNestedClass mentioned above.
        list_of_composite = [
            {
                "intValue": i,
                "doubleValue": float(i * 1.5),
                "arrayValue": [f"elem_{i}_{j}" for j in range(i + 1)],
                "mapValue": {f"map_{i}_{j}": f"val_{i}_{j}" for j in range(i + 1)},
            }
            for i in range(count)
        ]

        # mapOfComposite: MapType(StringType, StructType)
        # Each (value) Row schema matches the InnerNestedClass mentioned above.
        map_of_composite = {
            f"nested_key{i}": {
                "intValue": i * 10,
                "doubleValue": float(i * 2.5),
                "arrayValue": [f"elem_{i}_{j}" for j in range(i + 1)],
                "mapValue": {f"map_{i}_{j}": f"val_{i}_{j}" for j in range(i + 1)},
            }
            for i in range(count)
        }

        yield pd.DataFrame(
            {
                "primitiveValue": [primitive_value],
                "listOfPrimitive": [list_of_primitive],
                "mapOfPrimitive": [map_of_primitive],
                "listOfComposite": [list_of_composite],
                "mapOfComposite": [map_of_composite],
            }
        )

    def close(self) -> None:
        pass


# A Row stateful processor with a simple ValueState computation and composite output schema:
#
# primitiveValue: StringType
# listOfPrimitive: ArrayType(StringType)
# mapOfPrimitive: MapType(StringType, StringType)
# listOfComposite: ArrayType(InnerNestedClass)
# mapOfComposite: MapType(StringType, InnerNestedClass)
#
# where InnerNestedClass is a StructType with:
# intValue: IntegerType
# doubleValue: DoubleType
# arrayValue: ArrayType(StringType)
# mapValue: MapType(StringType, StringType)
class RowCompositeOutputProcessor(StatefulProcessor):
    def init(self, handle: StatefulProcessorHandle) -> None:
        # Simple value state to track counts
        state_schema = StructType([StructField("value", IntegerType(), True)])
        self.count_state = handle.getValueState("count_state", state_schema)

    def handleInputRows(self, key, rows, timerValues) -> Iterator[Row]:
        # Calculate count from input rows
        count = 0
        if self.count_state.exists():
            count = self.count_state.get()[0]

        for row in rows:
            count += 1

        self.count_state.update((count,))

        # Build output matching the composite schema
        key_str = key[0]

        # primitiveValue: StringType
        primitive_value = f"key_{key_str}_count_{count}"

        # listOfPrimitive: ArrayType(StringType)
        list_of_primitive = [f"item_{i}" for i in range(count)]

        # mapOfPrimitive: MapType(StringType, StringType)
        map_of_primitive = {f"key{i}": f"value{i}" for i in range(count)}

        # listOfComposite: ArrayType(StructType)
        # Each Row schema matches the InnerNestedClass mentioned above.
        list_of_composite = [
            Row(
                intValue=i,
                doubleValue=float(i * 1.5),
                arrayValue=[f"elem_{i}_{j}" for j in range(i + 1)],
                mapValue={f"map_{i}_{j}": f"val_{i}_{j}" for j in range(i + 1)},
            )
            for i in range(count)
        ]

        # mapOfComposite: MapType(StringType, StructType)
        # Each (value) Row schema matches the InnerNestedClass mentioned above.
        map_of_composite = {
            f"nested_key{i}": Row(
                intValue=i * 10,
                doubleValue=float(i * 2.5),
                arrayValue=[f"elem_{i}_{j}" for j in range(i + 1)],
                mapValue={f"map_{i}_{j}": f"val_{i}_{j}" for j in range(i + 1)},
            )
            for i in range(count)
        }

        yield Row(
            primitiveValue=primitive_value,
            listOfPrimitive=list_of_primitive,
            mapOfPrimitive=map_of_primitive,
            listOfComposite=list_of_composite,
            mapOfComposite=map_of_composite,
        )

    def close(self) -> None:
        pass


class PandasLargeValueStatefulProcessor(StatefulProcessor):
    def init(self, handle: StatefulProcessorHandle):
        # Test all three state types with large values
        value_state_schema = StructType([StructField("value", StringType(), True)])
        self.value_state = handle.getValueState("valueState", value_state_schema)

        list_state_schema = StructType([StructField("value", StringType(), True)])
        self.list_state = handle.getListState("listState", list_state_schema)

        self.map_state = handle.getMapState("mapState", "key string", "value string")

    def handleInputRows(self, key, rows, timerValues) -> Iterator[pd.DataFrame]:
        # Create a large string (512 KB)
        target_size_bytes = 512 * 1024
        large_string = "a" * target_size_bytes

        # Test ValueState with large string
        self.value_state.update((large_string,))
        value_retrieved = self.value_state.get()[0]

        # Test ListState with large strings
        self.list_state.put([(large_string,), (large_string + "b",), (large_string + "c",)])
        list_retrieved = list(self.list_state.get())
        list_elements = ",".join([elem[0] for elem in list_retrieved])

        # Test MapState with large strings
        map_key = ("large_string_key",)
        self.map_state.updateValue(map_key, (large_string,))
        map_retrieved = f"{map_key[0]}:{self.map_state.getValue(map_key)[0]}"

        yield pd.DataFrame(
            {
                "id": key,
                "valueStateResult": [value_retrieved],
                "listStateResult": [list_elements],
                "mapStateResult": [map_retrieved],
            }
        )

    def close(self) -> None:
        pass


class RowLargeValueStatefulProcessor(StatefulProcessor):
    def init(self, handle: StatefulProcessorHandle):
        # Test all three state types with large values
        value_state_schema = StructType([StructField("value", StringType(), True)])
        self.value_state = handle.getValueState("valueState", value_state_schema)

        list_state_schema = StructType([StructField("value", StringType(), True)])
        self.list_state = handle.getListState("listState", list_state_schema)

        self.map_state = handle.getMapState("mapState", "key string", "value string")

    def handleInputRows(self, key, rows, timerValues) -> Iterator[Row]:
        # Create a large string (512 KB)
        target_size_bytes = 512 * 1024
        large_string = "a" * target_size_bytes

        # Test ValueState with large string
        self.value_state.update((large_string,))
        value_retrieved = self.value_state.get()[0]

        # Test ListState with large strings
        self.list_state.put([(large_string,), (large_string + "b",), (large_string + "c",)])
        list_retrieved = list(self.list_state.get())
        list_elements = ",".join([elem[0] for elem in list_retrieved])

        # Test MapState with large strings
        map_key = ("large_string_key",)
        self.map_state.updateValue(map_key, (large_string,))
        map_retrieved = f"{map_key[0]}:{self.map_state.getValue(map_key)[0]}"

        yield Row(
            id=key[0],
            valueStateResult=value_retrieved,
            listStateResult=list_elements,
            mapStateResult=map_retrieved,
        )

    def close(self) -> None:
        pass
