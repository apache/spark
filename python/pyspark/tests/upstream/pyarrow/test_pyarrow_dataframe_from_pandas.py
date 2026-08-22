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
Golden-file tests for the PyArrow ``from_pandas`` constructors that take a whole pandas
DataFrame: ``pa.RecordBatch.from_pandas`` and ``pa.Schema.from_pandas`` (with
``pa.Table.from_pandas`` to follow in this file). These take a DataFrame, unlike
``pa.Array.from_pandas`` which takes a Series (covered by test_pyarrow_array_from_pandas_*).

Per-column type inference matches the Array tests, so these pin the DataFrame-level
behavior instead: whole-frame assembly, the pandas index under ``preserve_index``, and --
for RecordBatch -- num_rows preservation for a 0-column DataFrame. Spark calls
``RecordBatch.from_pandas`` bare at pandas/conversion.py:1026 and connect/session.py:632
(the createDataFrame 0-column branch) and stateful_processor_api_client.py:557, relying on
the default ``preserve_index=None`` to carry num_rows via the index metadata -- otherwise a
0-column relation loses its rows.

``Schema.from_pandas`` is inspected to build a Spark schema, and the two prod call sites
diverge on ``preserve_index``: classic pandas/conversion.py:971 passes ``False`` (index
dropped), Connect session.py:573 passes it bare/``None`` (a named or non-range index becomes
an extra field) -- so a named-index frame yields different field sets. Spark reads each
field's type AND nullability (conversion.py:989 / session.py:590), so the schema test pins
name/type/nullability across ``preserve_index``.

Regenerate with SPARK_GENERATE_GOLDEN_FILES=1.
"""

import datetime
import unittest

from pyspark.testing.utils import (
    have_pyarrow,
    have_pandas,
    pyarrow_requirement_message,
    pandas_requirement_message,
)
from pyspark.testing.goldenutils import GoldenFileTestMixin

if have_pandas:
    import pandas as pd
if have_pyarrow:
    import pyarrow as pa


class _PyArrowFromPandasFrameTestBase(GoldenFileTestMixin, unittest.TestCase):
    """
    Shared machinery for the DataFrame-input ``from_pandas`` constructors (RecordBatch and
    Schema here; Table as a followup). Owns the source-frame inventory and the index-aware
    input-cell rendering (both constructors depend on the index under ``preserve_index``);
    defines no ``test_*`` of its own.
    """

    @staticmethod
    def _index_desc(index) -> str:
        """Compact, deterministic description of a pandas index for the input cell."""
        if isinstance(index, pd.MultiIndex):
            return f"MultiIndex[names={list(index.names)}]"
        if isinstance(index, pd.RangeIndex):
            return f"RangeIndex[{index.start}:{index.stop}:{index.step}]"
        return f"{index.name!r}:{index.tolist()}"

    def _input_cell(self, df) -> str:
        """Input DataFrame repr, extended with its index (repr_value drops it)."""
        return f"{self.repr_value(df, max_len=0)}[index={self._index_desc(df.index)}]"

    def _build_source_frames(self):
        """Named pandas DataFrames covering shape x index-kind, plus a dtype sample."""
        dt = datetime.datetime(2020, 1, 1, 5, 30)
        named = pd.Index([100, 200, 300], name="idx")
        unnamed = pd.Index([10, 20, 30])
        frames = {}

        # =====================================================================
        # 0-column frames -- only the index carries the row count
        # =====================================================================
        frames["0-columns:range-index"] = pd.DataFrame(index=range(3))
        frames["0-columns:named-index"] = pd.DataFrame(index=named)
        frames["0-columns:unnamed-index"] = pd.DataFrame(index=unnamed)
        frames["0-columns:empty"] = pd.DataFrame(index=range(0))

        # =====================================================================
        # Single column -- a non-RangeIndex becomes an extra column
        # =====================================================================
        frames["single-column:range-index"] = pd.DataFrame({"a": [1, 2, 3]})
        frames["single-column:named-index"] = pd.DataFrame({"a": [1, 2, 3]}, index=named)
        frames["single-column:unnamed-index"] = pd.DataFrame({"a": [1, 2, 3]}, index=unnamed)

        # =====================================================================
        # Multi-column assembly. Drift-prone columns (object strings, datetime64[ns]) are
        # pinned so the Arrow output is stable across pandas 2/3; per-dtype inference itself
        # is already covered by test_pyarrow_array_from_pandas_default.
        # =====================================================================
        frames["multi-column:standard"] = pd.DataFrame(
            {
                "i": pd.Series([1, 2, 3], dtype="int64"),
                "f": pd.Series([1.5, 2.5, 3.5], dtype="float64"),
                "b": pd.Series([True, False, True], dtype=bool),
                "s": pd.Series(["a", "b", "c"], dtype=object),
                "t": pd.Series([dt, dt, dt], dtype="datetime64[ns]"),
            }
        )
        frames["multi-column:nullable"] = pd.DataFrame(
            {
                "f": pd.Series([1.5, None, 3.5], dtype="float64"),
                "b": pd.Series([True, None, False], dtype=object),
                "s": pd.Series(["a", None, "c"], dtype=object),
                "t": pd.Series([dt, None, dt], dtype="datetime64[ns]"),
            }
        )
        # Multiple columns but zero rows (an empty object column is omitted -- it would
        # infer to Arrow ``null`` rather than a concrete type).
        frames["multi-column:no-rows"] = pd.DataFrame(
            {
                "i": pd.Series([], dtype="int64"),
                "f": pd.Series([], dtype="float64"),
                "b": pd.Series([], dtype=bool),
                "t": pd.Series([], dtype="datetime64[ns]"),
            }
        )
        return frames


@unittest.skipIf(
    not have_pyarrow or not have_pandas,
    pyarrow_requirement_message or pandas_requirement_message,
)
class PyArrowRecordBatchFromPandasTests(_PyArrowFromPandasFrameTestBase):
    """Tests pa.RecordBatch.from_pandas() across preserve_index via golden file comparison."""

    def _from_pandas_cell(self, df, **kwargs) -> str:
        """
        Convert ``df`` via RecordBatch.from_pandas(**kwargs) and append num_rows -- the
        property this test pins, which a 0-column batch has no column to imply. Returns
        ERR@<ExceptionClass> if the conversion raises; a formatting error is a test bug.
        """
        try:
            batch = pa.RecordBatch.from_pandas(df, **kwargs)
        except Exception as e:
            return f"ERR@{type(e).__name__}"
        return f"{self.repr_value(batch, max_len=0)}[num_rows={batch.num_rows}]"

    def test_from_pandas(self):
        """Test pa.RecordBatch.from_pandas() across preserve_index against golden file."""
        sources = self._build_source_frames()
        row_names = list(sources.keys())
        preserve = {
            "preserve_index=None": None,
            "preserve_index=False": False,
            "preserve_index=True": True,
        }
        col_names = ["pandas dataframe", *preserve.keys()]

        # Version-specific expected values go here, keyed by (row, col), for known drift.
        overrides: dict[tuple[str, str], str] = {}

        def compute_cell(row_name, col_name):
            df = sources[row_name]
            if col_name == "pandas dataframe":
                return self._input_cell(df)
            return self._from_pandas_cell(df, preserve_index=preserve[col_name])

        self.compare_or_generate_golden_matrix(
            row_names=row_names,
            col_names=col_names,
            compute_cell=compute_cell,
            golden_file_prefix="golden_pyarrow_record_batch_from_pandas",
            index_name="test case",
            overrides=overrides,
        )


@unittest.skipIf(
    not have_pyarrow or not have_pandas,
    pyarrow_requirement_message or pandas_requirement_message,
)
class PyArrowSchemaFromPandasTests(_PyArrowFromPandasFrameTestBase):
    """Tests pa.Schema.from_pandas() across preserve_index via golden file comparison."""

    def _schema_source_frames(self):
        """Shared frames plus two MultiIndex rows -- a MultiIndex has several index levels,
        each becoming its own field, so these pin multi-level index-to-field naming at the
        schema. Level values are integers (stable int64 on pandas 2 and 3; strings drift)."""
        frames = self._build_source_frames()
        frames["single-column:multiindex"] = pd.DataFrame(
            {"a": [1, 2, 3]},
            index=pd.MultiIndex.from_tuples([(1, 10), (1, 20), (2, 30)], names=["g", "n"]),
        )
        frames["single-column:multiindex-partial-name"] = pd.DataFrame(
            {"a": [1, 2]},
            index=pd.MultiIndex.from_tuples([(1, 10), (2, 20)], names=["g", None]),
        )
        return frames

    def _from_pandas_cell(self, df, **kwargs) -> str:
        """
        Infer the schema via Schema.from_pandas(**kwargs) and render its fields with
        nullability -- the name/type/nullable Spark reads to build its StructType. Returns
        ERR@<ExceptionClass> if inference raises; a formatting error is a test bug.
        """
        try:
            schema = pa.Schema.from_pandas(df, **kwargs)
        except Exception as e:
            return f"ERR@{type(e).__name__}"
        return self.repr_value(schema, max_len=0)

    def test_from_pandas(self):
        """Test pa.Schema.from_pandas() across preserve_index against golden file."""
        sources = self._schema_source_frames()
        row_names = list(sources.keys())
        preserve = {
            "preserve_index=None": None,
            "preserve_index=False": False,
            "preserve_index=True": True,
        }
        col_names = ["pandas dataframe", *preserve.keys()]

        # Version-specific expected values go here, keyed by (row, col), for known drift.
        overrides: dict[tuple[str, str], str] = {}

        def compute_cell(row_name, col_name):
            df = sources[row_name]
            if col_name == "pandas dataframe":
                return self._input_cell(df)
            return self._from_pandas_cell(df, preserve_index=preserve[col_name])

        self.compare_or_generate_golden_matrix(
            row_names=row_names,
            col_names=col_names,
            compute_cell=compute_cell,
            golden_file_prefix="golden_pyarrow_schema_from_pandas",
            index_name="test case",
            overrides=overrides,
        )


if __name__ == "__main__":
    from pyspark.testing import main

    main()
