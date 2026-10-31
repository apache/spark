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

import numpy as np

from pyspark.testing.utils import have_torch, torch_requirement_message

if have_torch:
    from pyspark.ml.torch.data import _SparkPartitionTorchDataset


@unittest.skipIf(not have_torch, torch_requirement_message)
class TestGetFieldConverterVectorType(unittest.TestCase):
    def setUp(self):
        self.converter = _SparkPartitionTorchDataset._get_field_converter("vector")

    def test_dense_vector(self):
        value = {"type": 1, "values": [1.0, 2.0, 3.5]}
        result = self.converter(value)
        np.testing.assert_array_equal(result, [1.0, 2.0, 3.5])

    def test_sparse_vector(self):
        value = {"type": 0, "size": 3, "indices": [1, 2], "values": [4.5, 5.5]}
        result = self.converter(value)
        np.testing.assert_array_equal(result, [0.0, 4.5, 5.5])

    def test_sparse_vector_empty(self):
        value = {"type": 0, "size": 4, "indices": [], "values": []}
        result = self.converter(value)
        np.testing.assert_array_equal(result, np.zeros(4))

    def test_unknown_vector_type_returns_none(self):
        value = {"type": 99, "values": [1.0, 2.0]}
        result = self.converter(value)
        self.assertIsNone(result)


@unittest.skipIf(not have_torch, torch_requirement_message)
class TestGetFieldConverterNumericTypes(unittest.TestCase):
    def test_scalar_types(self):
        for field_type, value in [
            ("float", 1.5),
            ("double", 3.14),
            ("int", 42),
            ("bigint", 123456789),
            ("smallint", 7),
        ]:
            with self.subTest(field_type=field_type):
                converter = _SparkPartitionTorchDataset._get_field_converter(field_type)
                self.assertEqual(converter(value), value)

    def test_array_types(self):
        for field_type, value in [
            ("array<float>", [1.0, 2.0, 3.0]),
            ("array<double>", [1.1, 2.2, 3.3]),
            ("array<int>", [1, 2, 3]),
            ("array<bigint>", [100, 200, 300]),
            ("array<smallint>", [1, 2, 3]),
        ]:
            with self.subTest(field_type=field_type):
                converter = _SparkPartitionTorchDataset._get_field_converter(field_type)
                self.assertEqual(converter(value), value)


@unittest.skipIf(not have_torch, torch_requirement_message)
class TestGetFieldConverterUnsupportedType(unittest.TestCase):
    def test_unsupported_type_raises(self):
        with self.assertRaises(ValueError):
            _SparkPartitionTorchDataset._get_field_converter("string")


if __name__ == "__main__":
    from pyspark.testing import main

    main()
