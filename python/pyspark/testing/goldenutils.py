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

    @classmethod
    def repr_value(
        cls,
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
        # Get value string representation
        if have_pandas and isinstance(value, pd.DataFrame):
            v_str = value.to_json()
        elif have_pandas and isinstance(value, pd.Series):
            v_str = str(value.tolist())
        else:
            v_str = str(value)

        # Get type string
        type_str = type_override if type_override is not None else cls.repr_type(value)

        # Clean up: replace newlines, normalize Java hash codes, then truncate
        v_str = v_str.replace("\n", " ")
        v_str = re.sub(r"@[a-fA-F0-9]+", "@<hash>", v_str)
        v_str = v_str[:max_len]
        return f"{v_str}@{type_str}"

    @classmethod
    def repr_type(cls, value: Any) -> str:
        """
        Get the type representation string for a value.

        Parameters
        ----------
        value : Any
            The value to get type representation for.

        Returns
        -------
        str
            Type string, e.g., "int", "List[int | NoneType]", "DataFrame[col1 int64]".
        """
        if have_pandas and isinstance(value, pd.DataFrame):
            schema = ", ".join([f"{col} {dtype.name}" for col, dtype in value.dtypes.items()])
            return f"DataFrame[{schema}]"
        elif have_numpy and isinstance(value, np.ndarray):
            return f"ndarray[{value.dtype.name}]"
        elif have_pandas and isinstance(value, pd.Series):
            return f"Series[{value.dtype.name}]"
        elif isinstance(value, list):
            if len(value) == 0:
                return "List"
            elem_types = [type(x).__name__ for x in value]
            return cls._repr_container(elem_types, container="List")
        else:
            return type(value).__name__

    @staticmethod
    def _repr_container(types: list, container: str = "List") -> str:
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
            NoneType is always placed at the end if present.
        """
        unique_types = set(types)
        # Sort with NoneType at the end
        has_none = "NoneType" in unique_types
        other_types = sorted(t for t in unique_types if t != "NoneType")
        if has_none:
            other_types.append("NoneType")
        if len(other_types) == 0:
            return container
        elif len(other_types) == 1:
            return f"{container}[{other_types[0]}]"
        else:
            return f"{container}[{' | '.join(other_types)}]"

    @staticmethod
    def clean_result(result: str) -> str:
        """Clean result string by removing newlines and extra whitespace."""
        return result.replace("\n", " ").replace("\r", " ").replace("\t", " ")

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

    @property
    def column_names(self) -> list[str]:
        """Column names for the golden file. Override in subclass."""
        raise NotImplementedError("Subclass must define column_names property")

    def run_single_test(self, test_item: Any) -> tuple[str, list[tuple[str, str]]]:
        """
        Run a single test item and return results.

        Override in subclass to implement test logic.

        Returns
        -------
        tuple[str, list[tuple[str, str]]]
            (row_key, [(column_name, cell_value), ...])
        """
        raise NotImplementedError("Subclass must implement run_single_test method")

    @property
    def test_cases(self) -> Iterable[Any]:
        """Test cases to iterate over. Override in subclass."""
        raise NotImplementedError("Subclass must define test_cases property")

    def run_tests(self, golden_name: str) -> None:
        """
        Run golden file tests using class properties.

        Uses self.test_cases, self.column_names, and self.run_single_test.
        """
        self._run_golden_tests(
            golden_name=golden_name,
            test_items=self.test_cases,
            run_test=self.run_single_test,
            column_names=self.column_names,
            parallel=True,
        )

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
            assert row_key not in row_key_set, f"Duplicate test case name: {row_key}"
            row_keys.append(row_key)
            row_key_set.add(row_key)
            for col_name, value in col_values:
                all_results.append((row_key, col_name, self.clean_result(value), None))

        # Compare or generate golden file
        test_file = inspect.getfile(self.__class__)
        golden_file = os.path.join(os.path.dirname(test_file), f"{self.prefix}_{golden_name}")
        self._compare_or_generate_golden(golden_file, row_keys, column_names, all_results)
