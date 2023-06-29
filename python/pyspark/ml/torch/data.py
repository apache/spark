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

import torch
import numpy as np
from typing import Any, Callable, Iterator
from pyspark.sql.types import StructType


class _SparkPartitionTorchDataset(torch.utils.data.IterableDataset):
    def __init__(self, arrow_file_path: str, schema: "StructType", num_samples: int):
        self.arrow_file_path = arrow_file_path
        self.num_samples = num_samples
        self.field_types = [field.dataType.simpleString() for field in schema]
        self.field_converters = [
            _SparkPartitionTorchDataset._get_field_converter(field_type)
            for field_type in self.field_types
        ]

    @staticmethod
    def _get_field_converter(field_type: str) -> Callable[[Any], Any]:
        if field_type == "vector":

            def converter(value: Any) -> Any:
                if value["type"] == 1:
                    # dense vector
                    return value["values"]
                if value["type"] == 0:
                    # sparse vector
                    size = int(value["size"])
                    sparse_array = np.zeros(size, dtype=np.float64)
                    sparse_array[value["indices"]] = value["values"]
                    return sparse_array

        elif field_type in [
            "float",
            "double",
            "int",
            "bigint",
            "smallint",
            "array<float>",
            "array<double>",
            "array<int>",
            "array<bigint>",
            "array<smallint>",
        ]:

            def converter(value: Any) -> Any:
                return value

        else:
            raise ValueError(
                "SparkPartitionTorchDataset does not support loading data from field of "
                f"type {field_type}."
            )
        return converter

    def __iter__(self) -> Iterator[Any]:
        from pyspark.sql.pandas.serializers import ArrowStreamSerializer

        serializer = ArrowStreamSerializer()

        worker_info = torch.utils.data.get_worker_info()
        if worker_info is not None and worker_info.num_workers > 1:
            raise RuntimeError(
                "SparkPartitionTorchDataset does not support multiple worker processes."
            )

        count = 0

        while count < self.num_samples:
            with open(self.arrow_file_path, "rb") as f:
                batch_iter = serializer.load_stream(f)
                for batch in batch_iter:
                    # TODO: we can optimize this further by directly extracting
                    #  field data from arrow batch without converting it to
                    #  pandas DataFrame.
                    batch_pdf = batch.to_pandas()
                    for row in batch_pdf.itertuples(index=False):
                        yield [
                            field_converter(value)
                            for value, field_converter in zip(row, self.field_converters)
                        ]
                        count += 1
                        if count == self.num_samples:
                            return
