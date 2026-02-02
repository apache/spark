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
import ctypes
import unittest
from pyspark.testing.utils import (
    have_pyarrow,
    pyarrow_requirement_message,
)
from pyspark import pandas as ps
from pyspark.testing.sqlutils import SQLTestUtils

import pandas as pd


@unittest.skipIf(not have_pyarrow, pyarrow_requirement_message)
class ArrowInterfaceTestsMixin:
    def test_spark_arrow_c_streamer_arrow_consumer(self):
        import pyarrow as pa

        pdf = pd.DataFrame([[1, "a"], [2, "b"], [3, "c"], [4, "d"]], columns=["id", "value"])
        psdf = ps.from_pandas(pdf)

        capsule = psdf.__arrow_c_stream__()
        assert (
            ctypes.pythonapi.PyCapsule_IsValid(ctypes.py_object(capsule), b"arrow_array_stream")
            == 1
        )

        stream = pa.RecordBatchReader.from_stream(psdf)
        assert isinstance(stream, pa.RecordBatchReader)
        result = pa.Table.from_batches(stream)
        schema = pa.schema(
            [
                ("__index_level_0__", pa.int64(), False),
                ("id", pa.int64(), False),
                ("value", pa.string(), False),
            ]
        )
        expected = pa.Table.from_pandas(
            pd.DataFrame(
                [[0, 1, "a"], [1, 2, "b"], [2, 3, "c"], [3, 4, "d"]],
                columns=["__index_level_0__", "id", "value"],
            ),
            schema=schema,
        )
        self.assertEqual(result, expected)


class ArrowInterfaceTests(ArrowInterfaceTestsMixin, SQLTestUtils):
    pass


if __name__ == "__main__":
    from pyspark.testing import main

    main()
