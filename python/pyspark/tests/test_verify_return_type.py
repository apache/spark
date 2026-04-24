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
import unittest
from typing import Iterator

from pyspark.errors import PySparkTypeError
from pyspark.testing.utils import have_pyarrow, pyarrow_requirement_message
from pyspark.worker import verify_return_type


@unittest.skipIf(not have_pyarrow, pyarrow_requirement_message)
class VerifyReturnTypeTests(unittest.TestCase):
    def test_non_iterator_accepts_matching_type(self):
        import pyarrow as pa

        batch = pa.RecordBatch.from_arrays([pa.array([1, 2])], names=["x"])
        self.assertIs(verify_return_type(batch, pa.RecordBatch), batch)

    def test_non_iterator_rejects_wrong_type(self):
        import pyarrow as pa

        with self.assertRaises(PySparkTypeError) as ctx:
            verify_return_type(123, pa.RecordBatch)
        self.assertEqual(ctx.exception.getErrorClass(), "UDF_RETURN_TYPE")
        self.assertEqual(
            ctx.exception.getMessageParameters(),
            {"expected": "pyarrow.RecordBatch", "actual": "int"},
        )

    def test_iterator_accepts_and_is_lazy(self):
        import pyarrow as pa

        arrays = [pa.array([1]), pa.array([2])]
        verified = verify_return_type(iter(arrays), Iterator[pa.Array])
        self.assertEqual(list(verified), arrays)

    def test_iterator_rejects_non_iterable(self):
        import pyarrow as pa

        with self.assertRaises(PySparkTypeError) as ctx:
            verify_return_type(5, Iterator[pa.RecordBatch])
        self.assertEqual(ctx.exception.getErrorClass(), "UDF_RETURN_TYPE")
        self.assertEqual(
            ctx.exception.getMessageParameters(),
            {"expected": "iterator of pyarrow.RecordBatch", "actual": "int"},
        )

    def test_iterator_rejects_non_iterator_iterable(self):
        import pyarrow as pa

        # A list is Iterable but not an Iterator: per the UDF contract we reject it.
        with self.assertRaises(PySparkTypeError) as ctx:
            verify_return_type([pa.array([1])], Iterator[pa.Array])
        self.assertEqual(ctx.exception.getErrorClass(), "UDF_RETURN_TYPE")
        self.assertEqual(
            ctx.exception.getMessageParameters(),
            {"expected": "iterator of pyarrow.Array", "actual": "list"},
        )

    def test_iterator_rejects_wrong_element(self):
        import pyarrow as pa

        verified = verify_return_type(iter([1]), Iterator[pa.Array])
        with self.assertRaises(PySparkTypeError) as ctx:
            list(verified)
        self.assertEqual(ctx.exception.getErrorClass(), "UDF_RETURN_TYPE")
        self.assertEqual(
            ctx.exception.getMessageParameters(),
            {"expected": "iterator of pyarrow.Array", "actual": "iterator of int"},
        )


if __name__ == "__main__":
    from pyspark.testing import main

    main()
