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
from typing import Callable, Dict

from pyspark.sql import DataFrame
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
