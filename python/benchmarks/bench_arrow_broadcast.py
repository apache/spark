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

"""Benchmarks for pickle and Arrow PySpark broadcast serialization."""

import io

import numpy as np
import pyarrow as pa

from pyspark import Broadcast, SparkConf, SparkContext
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, udf


def _create_broadcast_value(n_rows):
    ids = pa.array(np.arange(n_rows, dtype=np.int64))
    payloads = pa.array(["x" * 32] * n_rows)
    return pa.Table.from_arrays([ids, payloads], names=["id", "payload"])


class _UncloseableBytesIO(io.BytesIO):
    def close(self):
        pass


class ExampleArrowBroadcastValue:
    """A float-list-backed value that can be reconstructed with an Arrow buffer."""

    def __init__(self, values=None, array=None):
        self.values = values
        self.array = array

    @classmethod
    def from_size(cls, n_values):
        return cls(values=[float(value) for value in range(n_values)])

    def __to_arrow__(self):
        if self.array is not None:
            return self.array
        return pa.array(self.values, type=pa.float64())

    @classmethod
    def __from_arrow__(cls, value):
        return cls(array=value)

    @property
    def num_values(self):
        if self.array is not None:
            return len(self.array)
        return len(self.values)

    def value_at(self, index):
        if self.array is not None:
            return self.array[index].as_py()
        return self.values[index]


class ArrowBroadcastSerdeBenchmark:
    """Benchmark the standalone driver and worker broadcast serde paths."""

    params = [
        [100_000, 1_000_000],
        ["pickle", "arrow"],
    ]
    param_names = ["n_rows", "serializer"]

    number = 1
    repeat = 3
    timeout = 60

    def setup(self, n_rows, serializer):
        self.value = _create_broadcast_value(n_rows)
        self._setup_serde(serializer)

    def _setup_serde(self, serializer):
        self.broadcast = Broadcast.__new__(Broadcast)
        self.use_arrow = serializer == "arrow"

        output = _UncloseableBytesIO()
        self.broadcast._dump(self.value, output, use_arrow=self.use_arrow)
        self.serialized = output.getvalue()
        actual = self.broadcast._load(io.BytesIO(self.serialized))
        self._check_deserialized_value(actual)

    def _check_deserialized_value(self, actual):
        assert actual.equals(self.value)

    def time_driver_serialize(self, n_rows, serializer):
        output = _UncloseableBytesIO()
        self.broadcast._dump(self.value, output, use_arrow=self.use_arrow)

    def time_worker_deserialize(self, n_rows, serializer):
        self.broadcast._load(io.BytesIO(self.serialized))

    def track_serialized_size(self, n_rows, serializer):
        return len(self.serialized)

    track_serialized_size.unit = "bytes"


class PythonObjectArrowBroadcastSerdeBenchmark(ArrowBroadcastSerdeBenchmark):
    """Benchmark serde for a float-list-backed object with an Arrow protocol."""

    def setup(self, n_rows, serializer):
        self.value = ExampleArrowBroadcastValue.from_size(n_rows)
        self._setup_serde(serializer)

    def _check_deserialized_value(self, actual):
        assert actual.num_values == self.value.num_values
        assert actual.value_at(0) == self.value.value_at(0)
        assert actual.value_at(-1) == self.value.value_at(-1)


class ArrowBroadcastEndToEndBenchmark:
    """Benchmark broadcast creation, distribution, and worker-side deserialization."""

    params = [
        [100_000, 1_000_000],
        ["pickle", "arrow"],
    ]
    param_names = ["n_rows", "serializer"]

    number = 1
    repeat = 3
    timeout = 180

    def setup(self, n_rows, serializer):
        conf = (
            SparkConf()
            .setMaster("local[4]")
            .setAppName("ArrowBroadcastEndToEndBenchmark")
            .set("spark.python.worker.reuse", "true")
            .set("spark.ui.enabled", "false")
        )
        self.sc = SparkContext(conf=conf)
        self.sc.setLogLevel("ERROR")
        self.broadcasts = []

        self.value = _create_broadcast_value(n_rows)
        self.expected = (n_rows, 0, n_rows - 1, "x" * 32)

        # Start all four Python workers and import PyArrow before the timed iteration.
        versions = (
            self.sc.parallelize(range(4), 4)
            .map(lambda _: __import__("pyarrow").__version__)
            .collect()
        )
        assert len(versions) == 4

    def teardown(self, n_rows, serializer):
        for broadcast in self.broadcasts:
            broadcast.destroy(blocking=True)
        self.sc.stop()

    def time_broadcast_and_read(self, n_rows, serializer):
        broadcast = self.sc.broadcast(self.value, useArrow=serializer == "arrow")
        self.broadcasts.append(broadcast)

        def read_broadcast(_):
            value = broadcast.value
            return (
                value.num_rows,
                value.column("id")[0].as_py(),
                value.column("id")[-1].as_py(),
                value.column("payload")[0].as_py(),
            )

        results = self.sc.parallelize(range(4), 4).map(read_broadcast).collect()
        assert results == [self.expected] * 4


class ArrowUDFBroadcastEndToEndBenchmark:
    """Benchmark an Arrow-optimized UDF with pickle and Arrow broadcasts."""

    params = [
        [100_000, 1_000_000],
        ["pickle", "arrow"],
    ]
    param_names = ["broadcast_rows", "broadcast_serializer"]

    number = 1
    repeat = 3
    timeout = 180

    def setup(self, broadcast_rows, broadcast_serializer):
        conf = (
            SparkConf()
            .setMaster("local[4]")
            .setAppName("ArrowUDFBroadcastEndToEndBenchmark")
            .set("spark.python.worker.reuse", "true")
            .set("spark.ui.enabled", "false")
        )
        self.spark = SparkSession.builder.config(conf=conf).getOrCreate()
        self.spark.sparkContext.setLogLevel("ERROR")
        self.broadcasts = []
        self.value = _create_broadcast_value(broadcast_rows)
        self.input = self.spark.range(4, numPartitions=4)
        self.expected = [broadcast_rows + value for value in range(4)]

        # Warm all four workers and the Arrow UDF path before measuring the broadcast.
        identity = udf(lambda value: value, "long", useArrow=True)
        results = [row[0] for row in self.input.select(identity(col("id"))).collect()]
        assert results == list(range(4))

    def teardown(self, broadcast_rows, broadcast_serializer):
        for broadcast in self.broadcasts:
            broadcast.destroy(blocking=True)
        self.spark.stop()
        SparkSession.builder._options = {}

    def time_arrow_udf_and_broadcast(self, broadcast_rows, broadcast_serializer):
        broadcast = self.spark.sparkContext.broadcast(
            self.value, useArrow=broadcast_serializer == "arrow"
        )
        self.broadcasts.append(broadcast)

        def add_broadcast_rows(value):
            return value + broadcast.value.num_rows

        read_broadcast = udf(add_broadcast_rows, "long", useArrow=True)
        results = [row[0] for row in self.input.select(read_broadcast(col("id"))).collect()]
        assert results == self.expected
