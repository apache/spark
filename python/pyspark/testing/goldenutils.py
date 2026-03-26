#
# Licensed to the Apache Software Foundation (ASF) under one or more
# contributor license agreements.  See the NOTICE file distributed with
# this work for additional information regarding copyright ownership.
# The ASF licenses this file to You under the Apache License, Version 2.0
# (the "License"); you may not use this file except in compliance with
# the License.  You may obtain a copy of the License at
#
# http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#

from typing import Any, Callable, List, Optional
import inspect
import os
import time

try:
    import numpy as np

    have_numpy = True
except ImportError:
    have_numpy = False

try:
    import pandas as pd

    have_pandas = True
except ImportError:
    have_pandas = False

try:
    import pyarrow as pa

    have_pyarrow = True
except ImportError:
    have_pyarrow = False


# PyArrow uses internal names ("halffloat", "float", "double") that differ from
# the commonly used names ("float16", "float32", "float64").  This mapping
# normalises the str() representation of Arrow DataType so that repr_type()
# returns the more intuitive names.
_ARROW_FLOAT_ALIASES = {
    "halffloat": "float16",
    "float": "float32",
    "double": "float64",
}


class GoldenFileTestMixin:
    """
    Mixin class providing utilities for golden file based testing.

    Golden files are CSV files that store expected test results. This mixin provides:
    - Timezone setup/teardown for deterministic results
    - Golden file read/write with SPARK_GENERATE_GOLDEN_FILES env var support
    - Result string cleaning utilities

    To regenerate golden files, set SPARK_GENERATE_GOLDEN_FILES=1 before running tests.

    Usage:
        class MyTest(GoldenFileTestMixin, ReusedSQLTestCase):
            def test_something(self):
                # Use helper methods from mixin
                if self.is_generating_golden():
                    self.save_golden(df, golden_csv, golden_md)
                else:
                    golden = self.load_golden_csv(golden_csv)
                    # compare results with golden
    """

    _tz_prev: Optional[str] = None

    def __init_subclass__(cls, **kwargs):
        """Verify correct inheritance order at class definition time."""
        super().__init_subclass__(**kwargs)
        # Check that GoldenFileTestMixin comes before any class with setUpClass in MRO.
        # This ensures setup_timezone() will be called after Spark session is created.
        # Correct:   class MyTest(GoldenFileTestMixin, ReusedSQLTestCase)
        # Incorrect: class MyTest(ReusedSQLTestCase, GoldenFileTestMixin)
        for base in cls.__mro__:
            if base is GoldenFileTestMixin:
                break
            # If we find a class with setUpClass before GoldenFileTestMixin, that's wrong
            if base is not cls and hasattr(base, "setUpClass") and "setUpClass" in base.__dict__:
                raise TypeError(
                    f"{cls.__name__} has incorrect inheritance order. "
                    f"GoldenFileTestMixin must be listed BEFORE {base.__name__}. "
                    f"Use: class {cls.__name__}(GoldenFileTestMixin, {base.__name__}, ...)"
                )

    @classmethod
    def setUpClass(cls) -> None:
        """Setup test class with timezone configuration."""
        super().setUpClass()
        cls.setup_timezone()

    @classmethod
    def tearDownClass(cls) -> None:
        """Teardown test class and restore timezone."""
        cls.teardown_timezone()
        super().tearDownClass()

    @classmethod
    def setup_timezone(cls, tz: str = "America/Los_Angeles") -> None:
        """
        Setup timezone for deterministic test results.

        Sets the OS-level TZ environment variable and, when a Spark session
        is available, synchronises the timezone with the JVM and Spark config.
        This allows the mixin to be used with both ReusedSQLTestCase (Spark)
        and plain unittest.TestCase (no Spark).
        """
        cls._tz_prev = os.environ.get("TZ", None)
        os.environ["TZ"] = tz
        time.tzset()

        # Sync with Spark / Java if a session is available.
        if hasattr(cls, "sc"):
            cls.sc.environment["TZ"] = tz
        if hasattr(cls, "spark"):
            cls.spark.conf.set("spark.sql.session.timeZone", tz)

    @classmethod
    def teardown_timezone(cls) -> None:
        """Restore original timezone."""
        if "TZ" in os.environ:
            del os.environ["TZ"]
        if cls._tz_prev is not None:
            os.environ["TZ"] = cls._tz_prev
        time.tzset()

    @staticmethod
    def is_generating_golden() -> bool:
        """Check if we are generating golden files (vs testing against them)."""
        return os.environ.get("SPARK_GENERATE_GOLDEN_FILES", "0") == "1"

    @staticmethod
    def load_golden_csv(golden_csv: str, use_index: bool = True) -> "pd.DataFrame":
        """
        Load golden file from CSV.

        Parameters
        ----------
        golden_csv : str
            Path to the golden CSV file.
        use_index : bool
            If True, use first column as index.
            If False, don't use index.

        Returns
        -------
        pd.DataFrame
            The loaded golden data with string dtype.
        """
        return pd.read_csv(
            golden_csv,
            sep="\t",
            index_col=0 if use_index else None,
            dtype="str",
            na_filter=False,
            engine="python",
        )

    @staticmethod
    def save_golden(df: "pd.DataFrame", golden_csv: str, golden_md: Optional[str] = None) -> None:
        """
        Save DataFrame as golden file (CSV and optionally Markdown).

        Parameters
        ----------
        df : pd.DataFrame
            The DataFrame to save.
        golden_csv : str
            Path to save the CSV file.
        golden_md : str, optional
            Path to save the Markdown file. Requires tabulate package.
        """
        df.to_csv(golden_csv, sep="\t", header=True, index=True)

        if golden_md is not None:
            try:
                df.to_markdown(golden_md, index=True, tablefmt="github")
            except Exception as e:
                import warnings

                warnings.warn(
                    f"Failed to write markdown file {golden_md}: {e}. "
                    "Install 'tabulate' package to generate markdown files."
                )

    @staticmethod
    def repr_type(t: Any) -> str:
        """
        Convert a type to a readable string representation.

        Handles different type representations:

        - Spark DataType: uses simpleString()
          (e.g. "int", "string", "array<int>")
        - PyArrow DataType: uses str(t) with float-name normalisation
          (e.g. "int8", "float32", "timestamp[s, tz=UTC]")
        - Python type: uses __name__
          (e.g. "int", "str", "list")
        - Other: falls back to str(t)

        Parameters
        ----------
        t : Any
            The type to represent.

        Returns
        -------
        str
            Human-readable string representation of the type.
        """
        # Spark DataType
        if hasattr(t, "simpleString"):
            return t.simpleString()
        # Python type (class)
        elif isinstance(t, type):
            return t.__name__
        else:
            s = str(t)
            # Normalise PyArrow float type names to be more intuitive:
            #   "halffloat" -> "float16", "float" -> "float32", "double" -> "float64"
            return _ARROW_FLOAT_ALIASES.get(s, s)

    @classmethod
    def repr_arrow_value(cls, value: Any, max_len: int = 32) -> str:
        """
        Format a PyArrow Array/ChunkedArray for golden file.

        Each element uses str(scalar) from PyArrow's own scalar formatting.

        Parameters
        ----------
        value : pa.Array or pa.ChunkedArray
            The PyArrow array to represent.
        max_len : int, default 32
            Maximum length for the value string portion.  0 means no limit.

        Returns
        -------
        str
            "[val1, val2, None]@arrow_type"
        """
        # Escape NULL bytes so the value can be safely stored in CSV files.
        elements = [str(scalar).replace("\x00", "\\0") for scalar in value]
        v_str = "[" + ", ".join(elements) + "]"
        if max_len > 0:
            v_str = v_str[:max_len]
        return f"{v_str}@{cls.repr_type(value.type)}"

    @classmethod
    def repr_pandas_value(cls, value: Any, max_len: int = 32) -> str:
        """
        Format a pandas DataFrame for golden file.

        Parameters
        ----------
        value : pd.DataFrame
            The pandas DataFrame to represent.
        max_len : int, default 32
            Maximum length for the value string portion.  0 means no limit.

        Returns
        -------
        str
            "value@Dataframe[schema]"
        """
        v_str = value.to_json().replace("\n", " ")
        if max_len > 0:
            v_str = v_str[:max_len]
        simple_schema = ", ".join([f"{t} {d.name}" for t, d in value.dtypes.items()])
        return f"{v_str}@Dataframe[{simple_schema}]"

    @classmethod
    def repr_numpy_value(cls, value: Any, max_len: int = 32) -> str:
        """
        Format a numpy ndarray for golden file.

        Parameters
        ----------
        value : np.ndarray
            The numpy ndarray to represent.
        max_len : int, default 32
            Maximum length for the value string portion.  0 means no limit.

        Returns
        -------
        str
            "value@ndarray[dtype]"
        """
        v_str = str(value).replace("\n", " ")
        if max_len > 0:
            v_str = v_str[:max_len]
        return f"{v_str}@ndarray[{value.dtype.name}]"

    @classmethod
    def repr_python_value(cls, value: Any, max_len: int = 32) -> str:
        """
        Format a plain Python value for golden file.

        Returns
        -------
        str
            "str(value)@class_name"
        """
        v_str = str(value).replace("\n", " ")
        if max_len > 0:
            v_str = v_str[:max_len]
        return f"{v_str}@{type(value).__name__}"

    @classmethod
    def repr_value(cls, value: Any, max_len: int = 32) -> str:
        """
        Format a value for golden file, dispatching to the appropriate repr
        based on the value's type.

        - PyArrow Array/ChunkedArray -> repr_arrow_value
        - pandas DataFrame -> repr_pandas_value
        - numpy ndarray -> repr_numpy_value
        - Everything else -> repr_python_value

        Parameters
        ----------
        value : Any
            The value to represent.
        max_len : int, default 32
            Maximum length for the value string portion.  0 means no limit.

        Returns
        -------
        str
            String representation in format "value@type".
        """
        if have_pyarrow and isinstance(value, (pa.Array, pa.ChunkedArray)):
            return cls.repr_arrow_value(value, max_len)

        if have_pandas and isinstance(value, pd.DataFrame):
            return cls.repr_pandas_value(value, max_len)
        if have_numpy and isinstance(value, np.ndarray):
            return cls.repr_numpy_value(value, max_len)

        return cls.repr_python_value(value, max_len)

    def compare_or_generate_golden_matrix(
        self,
        row_names: List[str],
        col_names: List[str],
        compute_cell: Callable[[str, str], str],
        golden_file_prefix: str,
        index_name: str = "source \\ target",
        overrides: Optional[dict[tuple[str, str], str]] = None,
    ) -> None:
        """
        Run a matrix of computations and compare against (or generate) a golden file.

        1. If SPARK_GENERATE_GOLDEN_FILES=1, compute every cell, build a
           DataFrame, and save it as the new golden CSV / Markdown file.
        2. Otherwise, load the existing golden file and assert that every cell
           matches the freshly computed value.

        Parameters
        ----------
        row_names : list[str]
            Ordered row labels (becomes the DataFrame index).
        col_names : list[str]
            Ordered column labels.
        compute_cell : (row_name, col_name) -> str
            Function that computes the string result for one cell.
        golden_file_prefix : str
            Prefix for the golden CSV/MD files (without extension).
            Files are placed in the same directory as the concrete test file.
        index_name : str, default "source \\ target"
            Name for the index column in the golden file.
        overrides : dict[(row, col) -> str], optional
            Version-specific expected values that take precedence over the golden
            file.  Use this to document known behavioral differences across
            library versions (e.g. PyArrow 18 vs 22) directly in the test code,
            so that the same golden file works for multiple versions.
        """
        generating = self.is_generating_golden()

        test_dir = os.path.dirname(inspect.getfile(type(self)))
        golden_csv = os.path.join(test_dir, f"{golden_file_prefix}.csv")
        golden_md = os.path.join(test_dir, f"{golden_file_prefix}.md")

        golden = None
        if not generating:
            golden = self.load_golden_csv(golden_csv)

        errors = []
        results = {}

        for row_name in row_names:
            for col_name in col_names:
                result = compute_cell(row_name, col_name)
                results[(row_name, col_name)] = result

                if not generating:
                    if overrides and (row_name, col_name) in overrides:
                        expected = overrides[(row_name, col_name)]
                    else:
                        expected = golden.loc[row_name, col_name]
                    if expected != result:
                        errors.append(
                            f"{row_name} -> {col_name}: expected '{expected}', got '{result}'"
                        )

        if generating:
            import pandas as pd

            index = pd.Index(row_names, name=index_name)
            df = pd.DataFrame(index=index)
            for col_name in col_names:
                df[col_name] = [results[(row, col_name)] for row in row_names]
            self.save_golden(df, golden_csv, golden_md)
        else:
            self.assertEqual(
                len(errors),
                0,
                f"\n{len(errors)} golden file mismatches:\n" + "\n".join(errors),
            )

    @staticmethod
    def clean_result(result: str) -> str:
        """Clean result string by removing newlines and extra whitespace."""
        return result.replace("\n", " ").replace("\r", " ").replace("\t", " ")
