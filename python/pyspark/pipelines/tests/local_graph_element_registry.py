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
from pathlib import Path
from typing import List, Sequence

from pyspark.pipelines.dataset import Dataset
from pyspark.pipelines.flow import Flow
from pyspark.pipelines.graph_element_registry import GraphElementRegistry


@dataclass(frozen=True)
class SqlFile:
    sql_text: str
    file_path: Path


class LocalGraphElementRegistry(GraphElementRegistry):
    def __init__(self) -> None:
        self._datasets: List[Dataset] = []
        self._flows: List[Flow] = []
        self._sql_files: List[SqlFile] = []

    def register_dataset(self, dataset: Dataset) -> None:
        self._datasets.append(dataset)

    def register_flow(self, flow: Flow) -> None:
        self._flows.append(flow)

    def register_sql(self, sql_text: str, file_path: Path) -> None:
        self._sql_files.append(SqlFile(sql_text, file_path))

    @property
    def datasets(self) -> Sequence[Dataset]:
        return self._datasets

    @property
    def flows(self) -> Sequence[Flow]:
        return self._flows

    @property
    def sql_files(self) -> Sequence[SqlFile]:
        return self._sql_files
