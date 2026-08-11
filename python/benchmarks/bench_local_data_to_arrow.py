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

"""
Microbenchmarks for local Python data to Arrow conversions.

``LocalDataToArrowConversion.convert`` turns local Python rows into a
``pyarrow.Table``; it is the hot path of Spark Connect ``createDataFrame`` and
Arrow-optimized Python UDF / DataSource output. Scalar ``string`` and ``binary``
columns are converted element by element through a per-element converter.

The per-element converter has a fast path when the element is already the target
Python type (``str`` / ``bytes``), and a slow path for values that need coercion
(``None``, ``bool``, ``int``, ``bytearray``, ...). To measure both paths -- and
guard against a fast-path optimization that merely shifts cost onto the slow path
-- these benchmarks sweep the fraction of non-target ("other") values in the
column from all-target to all-other, for both ``None`` (a common, legitimate
value in nullable columns) and a coerced type (``int`` for string, ``bytearray``
for binary; a schema-vs-data type mismatch).
"""


class LocalDataToArrowScalarBenchmark:
    """
    Benchmark ``LocalDataToArrowConversion.convert`` on a single ``string`` or
    ``binary`` column while sweeping the fraction of non-target values.

    - ``leaf``: the leaf type (``string`` / ``binary``), tested as a top-level
      scalar column and nested one level inside an ``array``.
    - ``other``: what the non-target values are -- ``none`` (legitimate null) or
      ``coerce`` (a type mismatch coerced to the leaf type: ``int`` for string,
      ``bytearray`` for binary).
    - ``other_frac``: fraction of elements that are the non-target value, from
      ``0`` (all fast path) to ``100`` (all slow path).
    """

    params = [
        [1000000],
        ["string_scalar", "string_array", "binary_scalar", "binary_array"],
        ["none", "coerce"],
        [0, 30, 70, 100],
    ]
    param_names = ["n_rows", "leaf", "other", "other_frac"]

    def setup(self, n_rows, leaf, other, other_frac):
        import random
        from pyspark.sql.conversion import LocalDataToArrowConversion
        from pyspark.sql.types import ArrayType, BinaryType, StringType, StructField, StructType

        self.convert = LocalDataToArrowConversion.convert

        is_string = leaf.startswith("string")
        nested = leaf.endswith("array")
        leaf_type = StringType() if is_string else BinaryType()

        def target(i):
            return f"s{i}" if is_string else b"s%d" % i

        if other == "none":

            def other_val(i):
                return None
        elif is_string:

            def other_val(i):
                return i  # int coerced to string
        else:

            def other_val(i):
                return bytearray(b"s%d" % i)  # bytearray coerced to bytes

        rnd = random.Random(0)

        def elem(i):
            return other_val(i) if rnd.random() * 100 < other_frac else target(i)

        if nested:
            self.schema = StructType([StructField("c", ArrayType(leaf_type), True)])
            self.data = [([elem(i), elem(i + 1), elem(i + 2)],) for i in range(n_rows)]
        else:
            self.schema = StructType([StructField("c", leaf_type, True)])
            self.data = [(elem(i),) for i in range(n_rows)]

    def time_convert(self, n_rows, leaf, other, other_frac):
        self.convert(self.data, self.schema, False)

    def peakmem_convert(self, n_rows, leaf, other, other_frac):
        self.convert(self.data, self.schema, False)
