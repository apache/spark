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

import sys
import os

# Required to run the script easily on PySpark's root directory on the Spark repo.
sys.path.append(os.getcwd())

import uuid
import time
import random
from typing import List

from pyspark.sql.types import (
    StringType,
    StructType,
    StructField,
)
from pyspark.sql.streaming.stateful_processor_api_client import (
    ListTimerIterator,
    StatefulProcessorApiClient,
)

from pyspark.sql.streaming.benchmark.utils import print_percentiles
from pyspark.sql.streaming.benchmark.tws_utils import get_list_state, get_map_state, get_value_state


def benchmark_value_state(api_client: StatefulProcessorApiClient, params: List[str]) -> None:
    data_size = int(params[0])

    value_state = get_value_state(
        api_client, "example_value_state", StructType([StructField("value", StringType(), True)])
    )

    measured_times_implicit_key = []
    measured_times_get = []
    measured_times_update = []

    uuid_long = []
    for i in range(int(data_size / 32) + 1):
        uuid_long.append(str(uuid.uuid4()))

    # TODO: Use streaming quantiles in Apache DataSketch if we want to run this longer
    for i in range(1000000):
        # Generate a random value
        random.shuffle(uuid_long)
        value = ("".join(uuid_long))[0:data_size]

        start_time_implicit_key_ns = time.perf_counter_ns()
        api_client.set_implicit_key(("example_grouping_key",))
        end_time_implicit_key_ns = time.perf_counter_ns()

        measured_times_implicit_key.append(
            (end_time_implicit_key_ns - start_time_implicit_key_ns) / 1000
        )

        # Measure the time taken for the get operation
        start_time_get_ns = time.perf_counter_ns()
        value_state.get()
        end_time_get_ns = time.perf_counter_ns()

        measured_times_get.append((end_time_get_ns - start_time_get_ns) / 1000)

        start_time_update_ns = time.perf_counter_ns()
        value_state.update((value,))
        end_time_update_ns = time.perf_counter_ns()

        measured_times_update.append((end_time_update_ns - start_time_update_ns) / 1000)

    print(" ==================== SET IMPLICIT KEY latency (micros) ======================")
    print_percentiles(measured_times_implicit_key, [50, 95, 99, 99.9, 100])

    print(" ==================== GET latency (micros) ======================")
    print_percentiles(measured_times_get, [50, 95, 99, 99.9, 100])

    print(" ==================== UPDATE latency (micros) ======================")
    print_percentiles(measured_times_update, [50, 95, 99, 99.9, 100])


def benchmark_list_state(api_client: StatefulProcessorApiClient, params: List[str]) -> None:
    data_size = int(params[0])
    list_length = int(params[1])

    # get and rewrite the list - the actual behavior depends on the server side implementation
    list_state = get_list_state(
        api_client, "example_list_state", StructType([StructField("value", StringType(), True)])
    )

    measured_times_implicit_key = []
    measured_times_get = []
    measured_times_put = []
    measured_times_clear = []
    measured_times_append_value = []

    uuid_long = []
    for i in range(int(data_size / 32) + 1):
        uuid_long.append(str(uuid.uuid4()))

    # TODO: Use streaming quantiles in Apache DataSketch if we want to run this longer
    for i in range(1000000):
        # Generate a random value
        random.shuffle(uuid_long)
        value = ("".join(uuid_long))[0:data_size]

        start_time_implicit_key_ns = time.perf_counter_ns()
        api_client.set_implicit_key(("example_grouping_key",))
        end_time_implicit_key_ns = time.perf_counter_ns()

        measured_times_implicit_key.append(
            (end_time_implicit_key_ns - start_time_implicit_key_ns) / 1000
        )

        # Measure the time taken for the get operation
        start_time_get_ns = time.perf_counter_ns()
        old_list_elements = list(list_state.get())
        end_time_get_ns = time.perf_counter_ns()

        measured_times_get.append((end_time_get_ns - start_time_get_ns) / 1000)

        if len(old_list_elements) > list_length:
            start_time_clear_ns = time.perf_counter_ns()
            list_state.clear()
            end_time_clear_ns = time.perf_counter_ns()
            measured_times_clear.append((end_time_clear_ns - start_time_clear_ns) / 1000)
        elif len(old_list_elements) % 2 == 0:
            start_time_put_ns = time.perf_counter_ns()
            old_list_elements.append((value,))
            list_state.put(old_list_elements)
            end_time_put_ns = time.perf_counter_ns()
            measured_times_put.append((end_time_put_ns - start_time_put_ns) / 1000)
        else:
            start_time_append_value_ns = time.perf_counter_ns()
            list_state.appendValue((value,))
            end_time_append_value_ns = time.perf_counter_ns()
            measured_times_append_value.append(
                (end_time_append_value_ns - start_time_append_value_ns) / 1000
            )

    print(" ==================== SET IMPLICIT KEY latency (micros) ======================")
    print_percentiles(measured_times_implicit_key, [50, 95, 99, 99.9, 100])

    print(" ==================== GET latency (micros) ======================")
    print_percentiles(measured_times_get, [50, 95, 99, 99.9, 100])

    print(" ==================== PUT latency (micros) ======================")
    print_percentiles(measured_times_put, [50, 95, 99, 99.9, 100])

    print(" ==================== CLEAR latency (micros) ======================")
    print_percentiles(measured_times_clear, [50, 95, 99, 99.9, 100])

    print(" ==================== APPEND VALUE latency (micros) ======================")
    print_percentiles(measured_times_append_value, [50, 95, 99, 99.9, 100])


def benchmark_map_state(api_client: StatefulProcessorApiClient, params: List[str]) -> None:
    data_size = int(params[0])
    map_length = int(params[1])

    map_state = get_map_state(
        api_client,
        "example_map_state",
        StructType(
            [
                StructField("key", StringType(), True),
            ]
        ),
        StructType([StructField("value", StringType(), True)]),
    )

    measured_times_implicit_key = []
    measured_times_keys = []
    measured_times_iterator = []
    measured_times_clear = []
    measured_times_contains_key = []
    measured_times_update_value = []
    measured_times_remove_key = []

    uuid_long = []
    for i in range(int(data_size / 32) + 1):
        uuid_long.append(str(uuid.uuid4()))

    # TODO: Use streaming quantiles in Apache DataSketch if we want to run this longer
    run_clear = False
    for i in range(1000000):
        # Generate a random value
        random.shuffle(uuid_long)
        value = ("".join(uuid_long))[0:data_size]

        start_time_implicit_key_ns = time.perf_counter_ns()
        api_client.set_implicit_key(("example_grouping_key",))
        end_time_implicit_key_ns = time.perf_counter_ns()

        measured_times_implicit_key.append(
            (end_time_implicit_key_ns - start_time_implicit_key_ns) / 1000
        )

        if i % 2 == 0:
            start_time_keys_ns = time.perf_counter_ns()
            keys = list(map_state.keys())
            end_time_keys_ns = time.perf_counter_ns()
            measured_times_keys.append((end_time_keys_ns - start_time_keys_ns) / 1000)
        else:
            start_time_iterator_ns = time.perf_counter_ns()
            kv_pairs = list(map_state.iterator())
            end_time_iterator_ns = time.perf_counter_ns()
            measured_times_iterator.append((end_time_iterator_ns - start_time_iterator_ns) / 1000)
            keys = [kv[0] for kv in kv_pairs]

        if len(keys) > map_length and run_clear:
            start_time_clear_ns = time.perf_counter_ns()
            map_state.clear()
            end_time_clear_ns = time.perf_counter_ns()
            measured_times_clear.append((end_time_clear_ns - start_time_clear_ns) / 1000)

            run_clear = False
        elif len(keys) > map_length:
            for key in keys:
                start_time_contains_key_ns = time.perf_counter_ns()
                map_state.containsKey(key)
                end_time_contains_key_ns = time.perf_counter_ns()
                measured_times_contains_key.append(
                    (end_time_contains_key_ns - start_time_contains_key_ns) / 1000
                )

                start_time_remove_key_ns = time.perf_counter_ns()
                map_state.removeKey(key)
                end_time_remove_key_ns = time.perf_counter_ns()
                measured_times_remove_key.append(
                    (end_time_remove_key_ns - start_time_remove_key_ns) / 1000
                )

            run_clear = True
        else:
            start_time_update_value_ns = time.perf_counter_ns()
            map_state.updateValue((str(uuid.uuid4()),), (value,))
            end_time_update_value_ns = time.perf_counter_ns()
            measured_times_update_value.append(
                (end_time_update_value_ns - start_time_update_value_ns) / 1000
            )

    print(" ==================== SET IMPLICIT KEY latency (micros) ======================")
    print_percentiles(measured_times_implicit_key, [50, 95, 99, 99.9, 100])

    print(" ==================== KEYS latency (micros) ======================")
    print_percentiles(measured_times_keys, [50, 95, 99, 99.9, 100])

    print(" ==================== ITERATOR latency (micros) ======================")
    print_percentiles(measured_times_iterator, [50, 95, 99, 99.9, 100])

    print(" ==================== CLEAR latency (micros) ======================")
    print_percentiles(measured_times_clear, [50, 95, 99, 99.9, 100])

    print(" ==================== CONTAINS KEY latency (micros) ======================")
    print_percentiles(measured_times_contains_key, [50, 95, 99, 99.9, 100])

    print(" ==================== UPDATE VALUE latency (micros) ======================")
    print_percentiles(measured_times_update_value, [50, 95, 99, 99.9, 100])

    print(" ==================== REMOVE KEY latency (micros) ======================")
    print_percentiles(measured_times_remove_key, [50, 95, 99, 99.9, 100])


def benchmark_timer(api_client: StatefulProcessorApiClient, params: List[str]) -> None:
    num_timers = int(params[0])

    measured_times_implicit_key = []
    measured_times_register = []
    measured_times_delete = []
    measured_times_list = []

    # TODO: Use streaming quantiles in Apache DataSketch if we want to run this longer
    for i in range(1000000):
        expiry_ts_ms = random.randint(0, 10000000)

        start_time_implicit_key_ns = time.perf_counter_ns()
        api_client.set_implicit_key(("example_grouping_key",))
        end_time_implicit_key_ns = time.perf_counter_ns()

        measured_times_implicit_key.append(
            (end_time_implicit_key_ns - start_time_implicit_key_ns) / 1000
        )

        start_time_list_ns = time.perf_counter_ns()
        timer_iter = ListTimerIterator(api_client)
        timers = list(timer_iter)
        end_time_list_ns = time.perf_counter_ns()
        measured_times_list.append((end_time_list_ns - start_time_list_ns) / 1000)

        if len(timers) > num_timers:
            start_time_delete_ns = time.perf_counter_ns()
            api_client.delete_timer(timers[0])
            end_time_delete_ns = time.perf_counter_ns()

            measured_times_delete.append((end_time_delete_ns - start_time_delete_ns) / 1000)

        start_time_register_ns = time.perf_counter_ns()
        api_client.register_timer(expiry_ts_ms)
        end_time_register_ns = time.perf_counter_ns()
        measured_times_register.append((end_time_register_ns - start_time_register_ns) / 1000)

    print(" ==================== SET IMPLICIT KEY latency (micros) ======================")
    print_percentiles(measured_times_implicit_key, [50, 95, 99, 99.9, 100])

    print(" ==================== REGISTER latency (micros) ======================")
    print_percentiles(measured_times_register, [50, 95, 99, 99.9, 100])

    print(" ==================== DELETE latency (micros) ======================")
    print_percentiles(measured_times_delete, [50, 95, 99, 99.9, 100])

    print(" ==================== LIST latency (micros) ======================")
    print_percentiles(measured_times_list, [50, 95, 99, 99.9, 100])


def main(state_server_port: str, benchmark_type: str) -> None:
    key_schema = StructType(
        [
            StructField("key", StringType(), True),
        ]
    )

    try:
        state_server_id = int(state_server_port)
    except ValueError:
        state_server_id = state_server_port  # type: ignore[assignment]

    api_client = StatefulProcessorApiClient(
        state_server_port=state_server_id,
        key_schema=key_schema,
    )

    benchmarks = {
        "value": benchmark_value_state,
        "list": benchmark_list_state,
        "map": benchmark_map_state,
        "timer": benchmark_timer,
    }

    benchmarks[benchmark_type](api_client, sys.argv[3:])


if __name__ == "__main__":
    """
    Instructions to run the benchmark:
    (assuming you installed required dependencies for PySpark)

    1. `cd python`
    2. `python3 pyspark/sql/streaming/benchmark/benchmark_tws_state_server.py
        <port/uds file of state server> <state type> <params if required>`

    Currently, state type can be one of the following:
    - value
    - list
    - map
    - timer

    Please take a look at the benchmark functions to see the parameters required for each state
    type.
    """
    print("Starting the benchmark code... state server port: " + sys.argv[1])
    main(sys.argv[1], sys.argv[2])
