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

from typing import Any, Optional
import os
import time

import pandas as pd

try:
    import numpy as np

    have_numpy = True
except ImportError:
    have_numpy = False


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
        Synchronizes timezone between Python and Java.
        """
        cls._tz_prev = os.environ.get("TZ", None)
        os.environ["TZ"] = tz
        time.tzset()

        cls.sc.environment["TZ"] = tz
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
        Convert a type to string representation.

        Handles different type representations:
        - Spark DataType: uses simpleString() (e.g., "int", "string", "array<int>")
        - Python type: uses __name__ (e.g., "int", "str", "list")
        - Other: uses str()

        Parameters
        ----------
        t : Any
            The type to represent. Can be Spark DataType or Python type.

        Returns
        -------
        str
            String representation of the type.
        """
        # Check if it's a Spark DataType (has simpleString method)
        if hasattr(t, "simpleString"):
            return t.simpleString()
        # Check if it's a Python type
        elif isinstance(t, type):
            return t.__name__
        else:
            return str(t)

    @classmethod
    def repr_value(cls, value: Any, max_len: int = 32) -> str:
        """
        Convert Python value to string representation for golden file.

        Default format: "value_str@type_name"
        Subclasses can override this method for custom representations.

        Parameters
        ----------
        value : Any
            The Python value to represent.
        max_len : int, default 32
            Maximum length for the value string portion.

        Returns
        -------
        str
            String representation in format "value@type".
        """
        v_str = str(value)[:max_len]
        return f"{v_str}@{type(value).__name__}"

    @classmethod
    def repr_pandas_value(cls, value: Any, max_len: int = 32) -> str:
        """
        Convert Python/Pandas value to string representation for golden file.

        Extended version that handles pandas DataFrame and numpy ndarray specially.

        Parameters
        ----------
        value : Any
            The Python value to represent.
        max_len : int, default 32
            Maximum length for the value string portion.

        Returns
        -------
        str
            String representation in format "value@type[dtype]".
        """
        if isinstance(value, pd.DataFrame):
            v_str = value.to_json()
        else:
            v_str = str(value)
        v_str = v_str.replace("\n", " ")[:max_len]

        if have_numpy and isinstance(value, np.ndarray):
            return f"{v_str}@ndarray[{value.dtype.name}]"
        elif isinstance(value, pd.DataFrame):
            simple_schema = ", ".join([f"{t} {d.name}" for t, d in value.dtypes.items()])
            return f"{v_str}@Dataframe[{simple_schema}]"
        return f"{v_str}@{type(value).__name__}"

    @staticmethod
    def clean_result(result: str) -> str:
        """Clean result string by removing newlines and extra whitespace."""
        return result.replace("\n", " ").replace("\r", " ").replace("\t", " ")
