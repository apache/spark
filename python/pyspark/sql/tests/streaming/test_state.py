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

from pyspark.errors import PySparkRuntimeError, PySparkValueError
from pyspark.sql.streaming.state import GroupState, GroupStateTimeout, _parse_timeout_duration
from pyspark.sql.types import StructType, StructField, IntegerType, Row
from pyspark.testing.utils import PySparkErrorTestUtils

_DAY_MS = 24 * 60 * 60 * 1_000
_MONTH_MS = 31 * _DAY_MS  # Spark uses 31 days/month (streaming watermark convention)
_YEAR_MS = 12 * _MONTH_MS


def _make_state(timeout_conf=GroupStateTimeout.ProcessingTimeTimeout, batch_time_ms=1000):
    return GroupState(
        optionalValue=Row(value=5),
        batchProcessingTimeMs=batch_time_ms,
        eventTimeWatermarkMs=-1,
        timeoutConf=timeout_conf,
        hasTimedOut=False,
        watermarkPresent=False,
        defined=True,
        updated=False,
        removed=False,
        timeoutTimestamp=-1,
        keyAsUnsafe=b"",
        valueSchema=StructType([StructField("value", IntegerType())]),
    )


class ParseTimeoutDurationTests(unittest.TestCase, PySparkErrorTestUtils):
    # -- basic unit parsing --

    def test_single_integer_units(self):
        self.assertEqual(_parse_timeout_duration("1 second"), 1_000)
        self.assertEqual(_parse_timeout_duration("2 seconds"), 2_000)
        self.assertEqual(_parse_timeout_duration("1 minute"), 60_000)
        self.assertEqual(_parse_timeout_duration("3 minutes"), 180_000)
        self.assertEqual(_parse_timeout_duration("1 hour"), 3_600_000)
        self.assertEqual(_parse_timeout_duration("2 hours"), 7_200_000)
        self.assertEqual(_parse_timeout_duration("1 day"), _DAY_MS)
        self.assertEqual(_parse_timeout_duration("1 week"), 7 * _DAY_MS)
        self.assertEqual(_parse_timeout_duration("500 milliseconds"), 500)
        self.assertEqual(_parse_timeout_duration("1000 microseconds"), 1)

    def test_month_and_year_use_31_days_per_month(self):
        # Spark's IntervalUtils.getDuration uses daysPerMonth=31 by default.
        self.assertEqual(_parse_timeout_duration("1 month"), _MONTH_MS)
        self.assertEqual(_parse_timeout_duration("1 months"), _MONTH_MS)
        self.assertEqual(_parse_timeout_duration("1 year"), _YEAR_MS)
        self.assertEqual(_parse_timeout_duration("1 years"), _YEAR_MS)

    # -- Scala parity: -1 month + 31 days + 1 second = 1 second --

    def test_negative_month_offset_by_days(self):
        # Mirrors the Scala test: batchProcessingTimeMs=1000, duration=1000ms => timestamp=2000.
        # -1 month (=-31 days) + 31 days cancels out, leaving 1 second = 1000ms.
        self.assertEqual(_parse_timeout_duration("-1 month 31 days 1 second"), 1_000)

    # -- fractional seconds --

    def test_fractional_seconds(self):
        self.assertEqual(_parse_timeout_duration("1.5 seconds"), 1_500)
        self.assertEqual(_parse_timeout_duration("0.5 seconds"), 500)
        self.assertEqual(_parse_timeout_duration("1.5 second"), 1_500)

    def test_fractional_non_seconds_raises(self):
        for bad in ("1.5 hours", "1.5 minutes", "1.5 days", "1.5 weeks", "1.5 milliseconds"):
            with self.assertRaises(PySparkValueError) as pe:
                _parse_timeout_duration(bad)
            self.check_error(
                exception=pe.exception,
                errorClass="INVALID_TIMEOUT_DURATION_STRING",
                messageParameters={"duration": bad},
            )

    # -- optional 'interval' keyword prefix --

    def test_interval_keyword_prefix(self):
        self.assertEqual(_parse_timeout_duration("interval 5 seconds"), 5_000)
        self.assertEqual(_parse_timeout_duration("interval 1 hour"), 3_600_000)

    # -- per-component sign --

    def test_per_component_sign(self):
        # Negative component mixed with positive: net result still positive.
        self.assertEqual(_parse_timeout_duration("-30 minutes 1 hour"), 1_800_000)

    # -- compound strings --

    def test_compound_duration(self):
        self.assertEqual(
            _parse_timeout_duration("1 hour 30 minutes"),
            3_600_000 + 1_800_000,
        )
        self.assertEqual(
            _parse_timeout_duration("1 day 2 hours 30 minutes"),
            _DAY_MS + 7_200_000 + 1_800_000,
        )

    # -- leading-dot decimals and explicit positive sign --

    def test_leading_dot_fractional_seconds(self):
        # ".5 seconds" is valid in Scala (SIGN state accepts '.' -> VALUE_FRACTIONAL_PART).
        self.assertEqual(_parse_timeout_duration(".5 seconds"), 500)

    def test_explicit_positive_sign(self):
        self.assertEqual(_parse_timeout_duration("+5 seconds"), 5_000)

    def test_space_between_sign_and_number(self):
        # "- 5 seconds" is valid syntax in Scala (TRIM_BEFORE_VALUE skips whitespace after sign).
        # The net result is negative, so setTimeoutDuration raises VALUE_NOT_POSITIVE,
        # but _parse_timeout_duration itself returns the value.
        self.assertEqual(_parse_timeout_duration("- 5 seconds"), -5_000)

    def test_no_space_between_quantity_and_unit_raises(self):
        # "1seconds" (no whitespace) is rejected by Scala's VALUE state (INVALID_VALUE error).
        with self.assertRaises(PySparkValueError) as pe:
            _parse_timeout_duration("1seconds")
        self.check_error(
            exception=pe.exception,
            errorClass="INVALID_TIMEOUT_DURATION_STRING",
            messageParameters={"duration": "1seconds"},
        )

    # -- case-insensitivity --

    def test_case_insensitive(self):
        self.assertEqual(_parse_timeout_duration("5 SECONDS"), 5_000)
        self.assertEqual(_parse_timeout_duration("5 Hours"), 5 * 3_600_000)

    # -- invalid inputs --

    def test_invalid_string_raises(self):
        for bad in ("not a duration", "", "abc", "5"):
            with self.assertRaises(PySparkValueError) as pe:
                _parse_timeout_duration(bad)
            self.check_error(
                exception=pe.exception,
                errorClass="INVALID_TIMEOUT_DURATION_STRING",
                messageParameters={"duration": bad},
            )


class SetTimeoutDurationStringTests(unittest.TestCase, PySparkErrorTestUtils):
    def test_string_duration_sets_correct_timestamp(self):
        # Mirrors Scala: batchProcessingTimeMs=1000, "2 second" => timestamp=3000.
        state = _make_state(batch_time_ms=1000)
        state.setTimeoutDuration("2 second")
        self.assertEqual(state._timeout_timestamp, 3_000)

    def test_negative_month_offset_by_days(self):
        # Mirrors Scala: batchProcessingTimeMs=1000, "-1 month 31 days 1 second" => timestamp=2000.
        state = _make_state(batch_time_ms=1000)
        state.setTimeoutDuration("-1 month 31 days 1 second")
        self.assertEqual(state._timeout_timestamp, 2_000)

    def test_fractional_seconds(self):
        state = _make_state(batch_time_ms=0)
        state.setTimeoutDuration("1.5 seconds")
        self.assertEqual(state._timeout_timestamp, 1_500)

    def test_interval_prefix(self):
        state = _make_state(batch_time_ms=0)
        state.setTimeoutDuration("interval 10 seconds")
        self.assertEqual(state._timeout_timestamp, 10_000)

    def test_compound_duration(self):
        state = _make_state(batch_time_ms=0)
        state.setTimeoutDuration("1 hour 30 minutes")
        self.assertEqual(state._timeout_timestamp, 3_600_000 + 1_800_000)

    def test_int_duration_still_works(self):
        state = _make_state(batch_time_ms=1000)
        state.setTimeoutDuration(5000)
        self.assertEqual(state._timeout_timestamp, 6_000)

    def test_zero_int_duration_raises(self):
        state = _make_state()
        with self.assertRaises(PySparkValueError) as pe:
            state.setTimeoutDuration(0)
        self.check_error(
            exception=pe.exception,
            errorClass="VALUE_NOT_POSITIVE",
            messageParameters={"arg_name": "durationMs", "arg_value": "int"},
        )

    def test_negative_int_duration_raises(self):
        state = _make_state()
        with self.assertRaises(PySparkValueError) as pe:
            state.setTimeoutDuration(-1000)
        self.check_error(
            exception=pe.exception,
            errorClass="VALUE_NOT_POSITIVE",
            messageParameters={"arg_name": "durationMs", "arg_value": "int"},
        )

    def test_negative_string_duration_raises(self):
        # "-2 second" is negative => raises.
        state = _make_state()
        with self.assertRaises(PySparkValueError) as pe:
            state.setTimeoutDuration("-2 second")
        self.check_error(
            exception=pe.exception,
            errorClass="VALUE_NOT_POSITIVE",
            messageParameters={"arg_name": "durationMs", "arg_value": "int"},
        )

    def test_negative_month_raises(self):
        # "-1 month" is purely negative (-31 days) => raises.
        state = _make_state()
        with self.assertRaises(PySparkValueError) as pe:
            state.setTimeoutDuration("-1 month")
        self.check_error(
            exception=pe.exception,
            errorClass="VALUE_NOT_POSITIVE",
            messageParameters={"arg_name": "durationMs", "arg_value": "int"},
        )

    def test_zero_net_duration_raises(self):
        # "1 month -31 day" => 31 days - 31 days = 0 => raises.
        state = _make_state()
        with self.assertRaises(PySparkValueError) as pe:
            state.setTimeoutDuration("1 month -31 day")
        self.check_error(
            exception=pe.exception,
            errorClass="VALUE_NOT_POSITIVE",
            messageParameters={"arg_name": "durationMs", "arg_value": "int"},
        )

    def test_wrong_timeout_conf_raises(self):
        state = _make_state(timeout_conf=GroupStateTimeout.EventTimeTimeout)
        with self.assertRaises(PySparkRuntimeError) as pe:
            state.setTimeoutDuration("5 seconds")
        self.check_error(
            exception=pe.exception,
            errorClass="CANNOT_WITHOUT",
            messageParameters={
                "condition1": "set timeout duration",
                "condition2": "enabling processing time timeout in applyInPandasWithState",
            },
        )

    def test_invalid_duration_string_raises(self):
        state = _make_state()
        with self.assertRaises(PySparkValueError) as pe:
            state.setTimeoutDuration("not a duration")
        self.check_error(
            exception=pe.exception,
            errorClass="INVALID_TIMEOUT_DURATION_STRING",
            messageParameters={"duration": "not a duration"},
        )


if __name__ == "__main__":
    from pyspark.testing import main

    main()
