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

from pyspark.sql.streaming.list_state_client import ListStateClient
from pyspark.sql.streaming.map_state_client import MapStateClient
from pyspark.sql.streaming.value_state_client import ValueStateClient
from pyspark.sql.streaming.stateful_processor import ListState, MapState, ValueState
from pyspark.sql.streaming.stateful_processor_api_client import StatefulProcessorApiClient
from pyspark.sql.types import StructType


def get_value_state(
    api_client: StatefulProcessorApiClient, state_name: str, value_schema: StructType
) -> ValueState:
    api_client.get_value_state(
        state_name,
        schema=value_schema,
        ttl_duration_ms=None,
    )

    value_state_client = ValueStateClient(api_client, schema=value_schema)

    return ValueState(value_state_client, state_name)


def get_list_state(
    api_client: StatefulProcessorApiClient, state_name: str, list_element_schema: StructType
) -> ListState:
    api_client.get_list_state(
        state_name,
        schema=list_element_schema,
        ttl_duration_ms=None,
    )

    list_state_client = ListStateClient(api_client, schema=list_element_schema)

    return ListState(list_state_client, state_name)


def get_map_state(
    api_client: StatefulProcessorApiClient,
    state_name: str,
    map_key_schema: StructType,
    map_value_schema: StructType,
) -> MapState:
    api_client.get_map_state(
        state_name,
        user_key_schema=map_key_schema,
        value_schema=map_value_schema,
        ttl_duration_ms=None,
    )

    map_state_client = MapStateClient(
        api_client,
        user_key_schema=map_key_schema,
        value_schema=map_value_schema,
    )

    return MapState(map_state_client, state_name)
