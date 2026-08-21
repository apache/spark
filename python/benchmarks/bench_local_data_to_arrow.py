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
Microbenchmarks for ``LocalDataToArrowConversion.convert``, the hot path of
Spark Connect ``createDataFrame`` and Arrow-optimized Python UDF output.

``string`` / ``binary`` columns are converted element by element: the per-element
converter is fast when the element is already the target type (``str`` /
``bytes``) and slower when it needs coercion. Each benchmark sweeps the fraction
of non-target ("other") values from 0% (all fast path) to 100% (all slow path),
for a scalar column, a one-level ``array`` column, and a two-level
``array<array<...>>`` column (which drives the inlined array fast path hardest).
"""


def _build(convert, is_string, leaf, other_val, other_frac, n_rows):
    import random
    from pyspark.sql.types import ArrayType, BinaryType, StringType, StructField, StructType

    leaf_type = StringType() if is_string else BinaryType()

    def target(i):
        return f"s{i}" if is_string else b"s%d" % i

    rnd = random.Random(0)

    def elem(i):
        return other_val(i) if rnd.random() * 100 < other_frac else target(i)

    if leaf == "array2":
        schema = StructType([StructField("c", ArrayType(ArrayType(leaf_type)), True)])
        data = [([[elem(i), elem(i + 1)], [elem(i + 2), elem(i + 3)]],) for i in range(n_rows)]
    elif leaf == "array":
        schema = StructType([StructField("c", ArrayType(leaf_type), True)])
        data = [([elem(i), elem(i + 1), elem(i + 2)],) for i in range(n_rows)]
    else:  # scalar
        schema = StructType([StructField("c", leaf_type, True)])
        data = [(elem(i),) for i in range(n_rows)]
    return data, schema


class LocalDataToArrowStringBenchmark:
    """
    Benchmark ``convert`` on a ``string`` column, sweeping the kind and fraction
    of non-target values.

    - ``leaf``: ``scalar``, one-level ``array``, or two-level ``array2``
      (``array<array<string>>``).
    - ``other``: the non-target values -- ``none`` (null), ``int`` or ``bool``
      (a single non-str type, isolating one coercion branch), or ``mix`` (a
      rotation of int / float / bool / Decimal / date / datetime).
    - ``other_frac``: fraction of elements that are non-target, ``0`` to ``100``.
    """

    params = [
        [1000000],
        ["scalar", "array", "array2"],
        ["none", "int", "bool", "mix"],
        [0, 30, 70, 100],
    ]
    param_names = ["n_rows", "leaf", "other", "other_frac"]

    def setup(self, n_rows, leaf, other, other_frac):
        import datetime
        import decimal
        from pyspark.sql.conversion import LocalDataToArrowConversion

        self.convert = LocalDataToArrowConversion.convert

        if other == "none":

            def other_val(i):
                return None
        elif other == "int":

            def other_val(i):
                return i  # int coerced to string via str()
        elif other == "bool":

            def other_val(i):
                return i % 2 == 0  # bool coerced to "true" / "false"
        else:  # mix: a rotation of non-str types, each coerced to string
            _pool = [
                123,
                4.5,
                True,
                False,
                decimal.Decimal("1.50"),
                datetime.date(2020, 1, 1),
                datetime.datetime(2020, 1, 1, 3, 4, 5),
            ]

            def other_val(i):
                return _pool[i % len(_pool)]

        self.data, self.schema = _build(self.convert, True, leaf, other_val, other_frac, n_rows)

    def time_convert(self, n_rows, leaf, other, other_frac):
        self.convert(self.data, self.schema, False)

    def peakmem_convert(self, n_rows, leaf, other, other_frac):
        self.convert(self.data, self.schema, False)


class LocalDataToArrowBinaryBenchmark:
    """
    Benchmark ``convert`` on a ``binary`` column, sweeping the fraction of
    non-target values.

    - ``leaf``: ``scalar``, one-level ``array``, or two-level ``array2``
      (``array<array<binary>>``).
    - ``other``: ``none`` (null) or ``bytearray`` (the only non-bytes input
      binary accepts, copied to immutable bytes).
    - ``other_frac``: fraction of elements that are non-target, ``0`` to ``100``.
    """

    params = [
        [1000000],
        ["scalar", "array", "array2"],
        ["none", "bytearray"],
        [0, 30, 70, 100],
    ]
    param_names = ["n_rows", "leaf", "other", "other_frac"]

    def setup(self, n_rows, leaf, other, other_frac):
        from pyspark.sql.conversion import LocalDataToArrowConversion

        self.convert = LocalDataToArrowConversion.convert

        if other == "none":

            def other_val(i):
                return None
        else:  # bytearray coerced to immutable bytes

            def other_val(i):
                return bytearray(b"s%d" % i)

        self.data, self.schema = _build(self.convert, False, leaf, other_val, other_frac, n_rows)

    def time_convert(self, n_rows, leaf, other, other_frac):
        self.convert(self.data, self.schema, False)

    def peakmem_convert(self, n_rows, leaf, other, other_frac):
        self.convert(self.data, self.schema, False)
