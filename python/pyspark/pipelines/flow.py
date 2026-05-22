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
from dataclasses import dataclass
from typing import Callable, Dict, List, Literal, Optional

from pyspark.sql import DataFrame
from pyspark.sql import Column
from pyspark.pipelines.source_code_location import SourceCodeLocation

QueryFunction = Callable[[], DataFrame]


@dataclass(frozen=True)
class Flow:
    """Definition of a flow in a pipeline dataflow graph. A flow defines how to update a particular
    dataset.

    :param name: The name of the flow.
    :param target: The name of the target dataset the flow writes to.
    :param spark_conf: A dict where the keys are the Spark configuration property names and the
        values are the property values. These properties will be set on the flow.
    :param source_code_location: The location of the source code that created this flow.
    :param func: The function that defines the flow. This function should return a DataFrame.
    """

    name: str
    target: str
    spark_conf: Dict[str, str]
    source_code_location: SourceCodeLocation
    func: QueryFunction


@dataclass(frozen=True)
class AutoCdcFlow:
    """Definition of an Auto CDC flow in a pipeline dataflow graph.

    An Auto CDC flow applies Change Data Capture (CDC) events from a source to a target
    streaming table.

    :param name: Optional name of the flow. When None, defaults to the target name.
    :param target: The name of the target streaming table.
    :param source: The name of the CDC source to stream from.
    :param keys: Column(s) that uniquely identify a row in source and target data.
    :param sequence_by: Expression used to order the source data.
    :param apply_as_deletes: Optional delete condition for the merged operation.
    :param column_list: Optional columns to include in the output table.
    :param except_column_list: Optional columns to exclude from the output table.
    :param stored_as_scd_type: Optional SCD type for the target table. Only 1 is supported.
    :param source_code_location: The location of the source code that created this flow.
    """

    name: Optional[str]
    target: str
    source: str
    keys: List[Column]
    sequence_by: Column
    apply_as_deletes: Optional[Column]
    column_list: Optional[List[Column]]
    except_column_list: Optional[List[Column]]
    stored_as_scd_type: Optional[Literal[1, "1"]]
    source_code_location: SourceCodeLocation
