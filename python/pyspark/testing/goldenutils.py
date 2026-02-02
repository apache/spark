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

from typing import Any, Callable, Iterable, Optional, TYPE_CHECKING
import concurrent.futures
import inspect
import os
import re
import time

from pyspark.testing.utils import have_pandas, have_numpy

if have_pandas:
    import pandas as pd
if have_numpy:
    import numpy as np

if TYPE_CHECKING:
    from pyspark.sql.types import DataType


class GoldenFileTestMixin:
    """
    Mixin class providing utilities for golden file based testing.

    Golden files are CSV files that store expected test results. This mixin provides:
    - Timezone setup/teardown for deterministic results
    - Golden file read/write with SPARK_GENERATE_GOLDEN_FILES env var support
    - Result string cleaning utilities

    To regenerate golden files, set SPARK_GENERATE_GOLDEN_FILES=1 before running tests.
    """

    _tz_prev: Optional[str] = None

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
            If True, use first column as index (for matrix format).
            If False, don't use index (for row list format).

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
    def repr_spark_type(spark_type: "DataType") -> str:
        """Convert Spark type to string representation."""
        return spark_type.simpleString()

    @staticmethod
    def repr_value(
        value: Any,
        max_len: int = 32,
        type_override: Optional[str] = None,
    ) -> str:
        """
        Convert Python value to string representation for golden file.

        Format: "value_str@type_info"
        - For numpy.ndarray: includes dtype, e.g., "[1 2]@ndarray[int64]"
        - For pandas.DataFrame: includes schema, e.g., "{...}@DataFrame[_1 int64]"
        - For list: includes element types, e.g., "[1, 2]@List[int]"
        - For other types: uses type name, e.g., "True@bool"

        Java object hash codes are normalized (e.g., @69420149 -> @<hash>)
        for deterministic test results.

        Parameters
        ----------
        value : Any
            The Python value to represent.
        max_len : int, default 32
            Maximum length for the value string portion.
        type_override : str, optional
            If provided, use this as the type string instead of auto-detecting.

        Returns
        -------
        str
            String representation in format "value@type".
        """
        if type_override is not None:
            v_str = str(value)
            type_str = type_override
        elif have_pandas and isinstance(value, pd.DataFrame):
            v_str = value.to_json()
            schema = ", ".join([f"{col} {dtype.name}" for col, dtype in value.dtypes.items()])
            type_str = f"DataFrame[{schema}]"
        elif have_numpy and isinstance(value, np.ndarray):
            v_str = str(value)
            type_str = f"ndarray[{value.dtype.name}]"
        elif have_pandas and isinstance(value, pd.Series):
            v_str = str(value.tolist())
            type_str = f"Series[{value.dtype.name}]"
        elif isinstance(value, list):
            v_str = str(value)
            # Format as List[element_types]
            if len(value) == 0:
                type_str = "List"
            else:
                elem_types = sorted(set(type(x).__name__ for x in value))
                if len(elem_types) == 1:
                    type_str = f"List[{elem_types[0]}]"
                else:
                    type_str = f"List[{' | '.join(elem_types)}]"
        else:
            v_str = str(value)
            type_str = type(value).__name__

        # Clean up: replace newlines, normalize Java hash codes, then truncate
        v_str = v_str.replace("\n", " ")
        v_str = re.sub(r"@[a-fA-F0-9]+", "@<hash>", v_str)
        v_str = v_str[:max_len]
        return f"{v_str}@{type_str}"

    @staticmethod
    def format_container(types: list, container: str = "List") -> str:
        """
        Format a list of type strings into a container type.

        Parameters
        ----------
        types : list
            List of type name strings (e.g., ['int', 'int'] or ['int64', 'object']).
        container : str, default "List"
            Container type name (e.g., "List", "Series").

        Returns
        -------
        str
            Formatted type string, e.g., "List[int]" or "Series[int64 | object]".
        """
        unique_types = sorted(set(types))
        if len(unique_types) == 0:
            return container
        elif len(unique_types) == 1:
            return f"{container}[{unique_types[0]}]"
        else:
            return f"{container}[{' | '.join(unique_types)}]"

    @staticmethod
    def clean_result(result: str) -> str:
        """Clean result string by removing newlines and extra whitespace."""
        return result.replace("\n", " ").replace("\r", " ").replace("\t", " ")

    def _golden_path(self, name: str) -> str:
        """
        Get the full path for a golden file.

        Uses the test class's file location and prefix property to construct
        the path: {test_file_dir}/{prefix}_{name}

        Parameters
        ----------
        name : str
            The suffix for the golden file name (e.g., "vanilla", "base").

        Returns
        -------
        str
            Full path without extension (e.g., "/path/to/golden_test_vanilla").
        """
        test_file = inspect.getfile(self.__class__)
        return os.path.join(os.path.dirname(test_file), f"{self.prefix}_{name}")

    def _compare_or_generate_golden(
        self,
        golden_file: str,
        row_keys: list[str],
        column_names: list[str],
        results: list[tuple[str, str, str, Optional[str]]],
        index_name: str = "Source Value \\ Target Type",
    ) -> None:
        """
        Compare test results against golden file, or generate new golden file.

        This method provides a common framework for matrix-style golden file tests.
        Each test produces a matrix where rows are source values and columns are
        target types (or other categories).

        Parameters
        ----------
        golden_file : str
            Full base path of golden file (without extension). Will generate both
            .csv and .md files. E.g., "/path/to/golden_test_name"
        row_keys : list[str]
            Index values (row labels) for the DataFrame.
        column_names : list[str]
            Column names for the DataFrame.
        results : list[tuple[str, str, str, Optional[str]]]
            List of (row_key, column_name, cell_value, error_or_none) tuples.
            Each tuple represents one cell in the result matrix.
        index_name : str
            Name for the index column in the golden file.
        """
        generating = self.is_generating_golden()

        golden_csv = f"{golden_file}.csv"
        golden_md = f"{golden_file}.md"

        if not generating:
            golden = self.load_golden_csv(golden_csv)
            errs = []
            for row_key, col_name, value, err in results:
                if err is not None:
                    errs.append(err)
                else:
                    expected = golden.loc[row_key, col_name]
                    if expected != value:
                        errs.append(f"{row_key} => {col_name}: expects {expected} but got {value}")
            self.assertTrue(len(errs) == 0, "\n" + "\n".join(errs) + "\n")
        else:
            index = pd.Index(row_keys, name=index_name)
            new_golden = pd.DataFrame("?", index=index, columns=column_names)

            for row_key, col_name, value, _ in results:
                new_golden.loc[row_key, col_name] = value

            self.save_golden(new_golden, golden_csv, golden_md)

    # ==================== Golden Test Framework ====================

    def _run_golden_tests(
        self,
        golden_name: str,
        test_items: Iterable[Any],
        run_test: Callable[[Any], tuple[str, list[tuple[str, str]]]],
        column_names: list[str],
        parallel: bool = False,
    ) -> None:
        """
        Run golden file tests with a common framework.

        This method handles test execution, result collection, and golden file
        comparison/generation. Subclasses only need to provide the test items
        and a function to run each test.

        Parameters
        ----------
        golden_name : str
            Suffix for golden file name (e.g., "vanilla", "base").
            Will be combined with prefix property to form full path.
        test_items : Iterable[Any]
            Iterable of test items to process.
        run_test : Callable[[Any], tuple[str, list[tuple[str, str]]]]
            Function that takes a test item and returns:
            - row_key: The row index for this result
            - results: List of (column_name, cell_value) tuples
            Should return ("X", [...]) or similar on exception.
        column_names : list[str]
            Column names for the golden file.
        parallel : bool
            If True, execute tests in parallel using ThreadPoolExecutor.
        """

        def safe_run_test(item):
            """Wrapper that catches exceptions."""
            try:
                return run_test(item)
            except Exception:
                return None

        # Execute tests
        if parallel:
            with concurrent.futures.ThreadPoolExecutor(max_workers=4) as executor:
                raw_results = list(executor.map(safe_run_test, test_items))
        else:
            raw_results = [safe_run_test(item) for item in test_items]

        # Collect results
        row_keys = []
        all_results = []
        row_key_set = set()

        for result in raw_results:
            if result is None:
                continue
            row_key, col_values = result
            if row_key not in row_key_set:
                row_keys.append(row_key)
                row_key_set.add(row_key)
            for col_name, value in col_values:
                all_results.append((row_key, col_name, self.clean_result(value), None))

        # Compare or generate golden file
        golden_file = self._golden_path(golden_name)
        self._compare_or_generate_golden(golden_file, row_keys, column_names, all_results)
