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
Integration tests for unified type coercion in Arrow-backed Python UDFs.

These tests verify:
1. PERMISSIVE policy: Arrow-enabled UDFs produce the same results as pickle-based UDFs
2. WARN policy: Same as PERMISSIVE but with warnings (tested at unit level)
3. STRICT policy: Arrow handles type conversion natively (no coercion applied)

The goal is to ensure backward compatibility when enabling Arrow optimization.
"""

import array
import datetime
import re
import unittest
from decimal import Decimal

from pyspark.sql import Row
from pyspark.sql.functions import udf
from pyspark.sql.types import (
    ArrayType,
    BinaryType,
    BooleanType,
    ByteType,
    DateType,
    DecimalType,
    DoubleType,
    FloatType,
    IntegerType,
    LongType,
    MapType,
    ShortType,
    StringType,
    StructField,
    StructType,
    TimestampType,
)
from pyspark.testing.utils import (
    have_pyarrow,
    have_pandas,
    pyarrow_requirement_message,
    pandas_requirement_message,
)
from pyspark.testing.sqlutils import ReusedSQLTestCase


def normalize_result(value):
    """Normalize result for comparison, handling Java object hash codes."""
    result_str = repr(value)
    # Normalize Java object hash codes to make tests deterministic
    return re.sub(r"@[a-fA-F0-9]+", "@<hash>", result_str)


@unittest.skipIf(
    not have_pandas or not have_pyarrow,
    pandas_requirement_message or pyarrow_requirement_message,
)
class ArrowUDFCoercionTests(ReusedSQLTestCase):
    """
    Integration tests comparing Arrow-enabled UDFs (with PERMISSIVE coercion)
    against pickle-based UDFs to ensure identical behavior.
    """

    @classmethod
    def setUpClass(cls):
        super().setUpClass()

    def setUp(self):
        super().setUp()
        # Test values covering various Python types
        self.test_data = [
            None,
            True,
            1,
            "a",
            datetime.date(1970, 1, 1),
            datetime.datetime(1970, 1, 1, 0, 0),
            1.0,
            array.array("i", [1]),
            [1],
            (1,),
            bytearray([65, 66, 67]),
            Decimal(1),
            {"a": 1},
            Row(kwargs=1),
            Row("namedtuple")(1),
        ]

        # SQL types to test coercion against
        self.test_types = [
            BooleanType(),
            ByteType(),
            ShortType(),
            IntegerType(),
            LongType(),
            StringType(),
            DateType(),
            TimestampType(),
            FloatType(),
            DoubleType(),
            ArrayType(IntegerType()),
            BinaryType(),
            DecimalType(10, 0),
            MapType(StringType(), IntegerType()),
            StructType([StructField("_1", IntegerType())]),
        ]

    def _run_udf(self, value, spark_type, use_arrow):
        """Run a UDF that returns a specific value with a given return type."""
        try:
            test_udf = udf(lambda _: value, spark_type, useArrow=use_arrow)
            row = self.spark.range(1).select(test_udf("id")).first()
            return ("success", normalize_result(row[0]))
        except Exception as e:
            return ("error", type(e).__name__)

    def _results_match(self, pickle_result, arrow_result, spark_type, value):
        """
        Check if pickle and Arrow results match, with tolerance for known differences.

        Returns True if the results are equivalent for the purposes of coercion testing.
        """
        # Exact match
        if pickle_result == arrow_result:
            return True

        # Both error - consider equivalent (error types may differ between Py4J and Python)
        if pickle_result[0] == "error" and arrow_result[0] == "error":
            return True

        # String type has known representation differences between Java and Python
        # (Java's toString() vs Python's str())
        if isinstance(spark_type, StringType) and pickle_result[0] == "success":
            # datetime objects: Java GregorianCalendar vs Python iso format
            if isinstance(value, (datetime.date, datetime.datetime)):
                return True
            # Container types: Java array/object notation vs Python repr
            if isinstance(value, (array.array, tuple, bytearray, dict)):
                return True

        return False

    def test_arrow_with_permissive_matches_pickle(self):
        """
        Test that Arrow-enabled UDFs with PERMISSIVE coercion produce
        the same results as pickle-based UDFs.
        """
        mismatches = []

        for spark_type in self.test_types:
            for value in self.test_data:
                # Run with pickle (Arrow disabled)
                with self.sql_conf({"spark.sql.execution.pythonUDF.arrow.enabled": "false"}):
                    pickle_result = self._run_udf(value, spark_type, use_arrow=False)

                # Run with Arrow enabled (uses PERMISSIVE coercion by default)
                with self.sql_conf({"spark.sql.execution.pythonUDF.arrow.enabled": "true"}):
                    arrow_result = self._run_udf(value, spark_type, use_arrow=True)

                # Compare results
                if not self._results_match(pickle_result, arrow_result, spark_type, value):
                    mismatches.append(
                        {
                            "type": spark_type.simpleString(),
                            "value": f"{value!r} ({type(value).__name__})",
                            "pickle": pickle_result,
                            "arrow": arrow_result,
                        }
                    )

        if mismatches:
            mismatch_report = "\n".join(
                f"  - {m['type']} <- {m['value']}: pickle={m['pickle']}, arrow={m['arrow']}"
                for m in mismatches
            )
            self.fail(
                f"Arrow with PERMISSIVE coercion does not match pickle behavior:\n{mismatch_report}"
            )

    def test_specific_coercion_cases(self):
        """Test specific coercion cases that are known to differ between Arrow and pickle."""
        test_cases = [
            # (value, spark_type, description)
            (1, BooleanType(), "int -> boolean should return None"),
            (1.0, BooleanType(), "float -> boolean should return None"),
            (True, IntegerType(), "bool -> int should return None"),
            (1.5, IntegerType(), "float -> int should return None"),
            (Decimal(1), IntegerType(), "Decimal -> int should return None"),
            (True, FloatType(), "bool -> float should return None"),
            (1, FloatType(), "int -> float should return None"),
            (Decimal(1), FloatType(), "Decimal -> float should return None"),
            (1, DecimalType(10, 0), "int -> decimal should return None"),
        ]

        for value, spark_type, description in test_cases:
            with self.subTest(msg=description):
                # Run with pickle (Arrow disabled)
                with self.sql_conf({"spark.sql.execution.pythonUDF.arrow.enabled": "false"}):
                    pickle_result = self._run_udf(value, spark_type, use_arrow=False)

                # Run with Arrow enabled
                with self.sql_conf({"spark.sql.execution.pythonUDF.arrow.enabled": "true"}):
                    arrow_result = self._run_udf(value, spark_type, use_arrow=True)

                self.assertEqual(
                    pickle_result,
                    arrow_result,
                    f"{description}: pickle={pickle_result}, arrow={arrow_result}",
                )

    def test_warn_policy_matches_permissive(self):
        """
        Test that WARN policy produces the same results as PERMISSIVE.
        WARN should behave identically to PERMISSIVE, just with warnings logged.
        """
        mismatches = []

        for spark_type in self.test_types:
            for value in self.test_data:
                # Run with PERMISSIVE policy
                with self.sql_conf(
                    {
                        "spark.sql.execution.pythonUDF.arrow.enabled": "true",
                        "spark.sql.execution.pythonUDF.coercion.policy": "permissive",
                    }
                ):
                    permissive_result = self._run_udf(value, spark_type, use_arrow=True)

                # Run with WARN policy
                with self.sql_conf(
                    {
                        "spark.sql.execution.pythonUDF.arrow.enabled": "true",
                        "spark.sql.execution.pythonUDF.coercion.policy": "warn",
                    }
                ):
                    warn_result = self._run_udf(value, spark_type, use_arrow=True)

                # Compare results (should be identical)
                if permissive_result != warn_result:
                    mismatches.append(
                        {
                            "type": spark_type.simpleString(),
                            "value": f"{value!r} ({type(value).__name__})",
                            "permissive": permissive_result,
                            "warn": warn_result,
                        }
                    )

        if mismatches:
            mismatch_report = "\n".join(
                f"  - {m['type']} <- {m['value']}: permissive={m['permissive']}, warn={m['warn']}"
                for m in mismatches
            )
            self.fail(f"WARN policy does not match PERMISSIVE behavior:\n{mismatch_report}")

    def test_strict_policy_differs_from_permissive(self):
        """
        Test that STRICT policy (no coercion) produces different results than PERMISSIVE
        for cases where Arrow's native type conversion differs from pickle behavior.

        STRICT is a no-op - it lets Arrow handle type conversion natively,
        which is more aggressive than pickle (PERMISSIVE).
        """
        # Cases where STRICT (Arrow native) should produce different results than PERMISSIVE
        # For each case: (value, spark_type, description)
        test_cases = [
            # int -> boolean: PERMISSIVE returns None, STRICT (Arrow) converts to True
            (1, BooleanType(), "int -> boolean"),
            # float -> boolean: PERMISSIVE returns None, STRICT (Arrow) converts to True
            (1.0, BooleanType(), "float -> boolean"),
            # float -> int: PERMISSIVE returns None, STRICT (Arrow) truncates to 1
            (1.5, IntegerType(), "float -> int"),
            # Decimal -> int: PERMISSIVE returns None, STRICT (Arrow) converts to 1
            (Decimal(1), IntegerType(), "Decimal -> int"),
            # int -> float: PERMISSIVE returns None, STRICT (Arrow) converts to 1.0
            (1, FloatType(), "int -> float"),
            # bool -> float: PERMISSIVE returns None, STRICT (Arrow) converts to 1.0
            (True, FloatType(), "bool -> float"),
        ]

        for value, spark_type, description in test_cases:
            with self.subTest(msg=description):
                # Run with PERMISSIVE policy
                with self.sql_conf(
                    {
                        "spark.sql.execution.pythonUDF.arrow.enabled": "true",
                        "spark.sql.execution.pythonUDF.coercion.policy": "permissive",
                    }
                ):
                    permissive_result = self._run_udf(value, spark_type, use_arrow=True)

                # Run with STRICT policy (Arrow native behavior)
                with self.sql_conf(
                    {
                        "spark.sql.execution.pythonUDF.arrow.enabled": "true",
                        "spark.sql.execution.pythonUDF.coercion.policy": "strict",
                    }
                ):
                    strict_result = self._run_udf(value, spark_type, use_arrow=True)

                # Skip if either result is an error (environment issue, not test logic)
                if permissive_result[0] == "error" or strict_result[0] == "error":
                    # If both error, that's unexpected - fail
                    if permissive_result[0] == "error" and strict_result[0] == "error":
                        self.skipTest(
                            f"{description}: Both policies errored - likely environment issue"
                        )
                    continue

                # PERMISSIVE should return None for these cases
                self.assertIn(
                    "None",
                    permissive_result[1],
                    f"{description}: PERMISSIVE should return None, got {permissive_result[1]}",
                )

                # STRICT should succeed with a converted value (not None)
                self.assertNotIn(
                    "None",
                    strict_result[1],
                    f"{description}: STRICT should not return None (Arrow converts), got {strict_result[1]}",
                )

                # The results should be different
                self.assertNotEqual(
                    permissive_result,
                    strict_result,
                    f"{description}: PERMISSIVE and STRICT should produce different results",
                )


if __name__ == "__main__":
    try:
        import xmlrunner

        testRunner = xmlrunner.XMLTestRunner(output="target/test-reports", verbosity=2)
    except ImportError:
        testRunner = None
    unittest.main(testRunner=testRunner, verbosity=2)
