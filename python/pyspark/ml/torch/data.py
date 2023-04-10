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


class SparkPartitionTorchDataset(torch.utils.data.IterableDataset):

    def __init__(self, arrow_file_path, schema, num_samples):
        self.arrow_file_path = arrow_file_path
        self.num_samples = num_samples
        self.field_types = [field.dataType.simpleString() for field in schema]

    @staticmethod
    def _extract_field_value(value, field_type):
        if field_type == "vector":
            if value['type'] == 1:
                # dense vector
                return value['values']
            if value['type'] == 0:
                # sparse vector
                size = int(value['size'])
                np_array = np.empty(size)
                for index, elem_value in zip(value['indices'], value['values']):
                    np_array[index] = elem_value
                return np_array
        if field_type == "double":
            return value

        raise ValueError(
            "SparkPartitionTorchDataset does not support loading data from field of "
            f"type {field_type}."
        )

    def __iter__(self):
        from pyspark.sql.pandas.serializers import ArrowStreamSerializer
        serializer = ArrowStreamSerializer()

        count = 0

        while count < self.num_samples:
            with open(self.arrow_file_path, "wb") as f:
                batch_iter = serializer.load_stream(f)
                for batch in batch_iter:
                    # TODO: we can optimize this further by directly extracting
                    #  field data from arrow batch without converting it to
                    #  pandas DataFrame.
                    batch_pdf = batch.to_pandas()
                    for row in batch_pdf.itertuples(index=False):
                        yield [
                            SparkPartitionTorchDataset._extract_field_value(value, field_type)
                            for value, field_type in zip(row, self.field_types)
                        ]
                        count += 1
                        if count == self.num_samples:
                            return
