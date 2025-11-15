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

from pyspark.sql.datasource import DataSource, DataSourceStreamReader, InputPartition


class MockStreamReaderWithAdmissionControl(DataSourceStreamReader):
    """Mock stream reader that implements admission control."""

    def __init__(self):
        self.initial_offset_val = 0
        self.latest_offset_val = 1000
        self.latest_offset_calls = []
        self.report_latest_offset_calls = []

    def initialOffset(self):
        return {"offset": self.initial_offset_val}

    def latestOffset(self, start_offset=None, read_limit=None):
        """Implementation with admission control support."""
        self.latest_offset_calls.append({"start_offset": start_offset, "read_limit": read_limit})

        if start_offset is None:
            start = self.initial_offset_val
        else:
            start = start_offset["offset"]

        # Apply limit if provided
        if read_limit and read_limit.get("type") == "maxRows":
            max_rows = read_limit["maxRows"]
            end = min(start + max_rows, self.latest_offset_val)
        else:
            end = self.latest_offset_val

        return {"offset": end}

    def reportLatestOffset(self):
        """Report true latest without limits."""
        self.report_latest_offset_calls.append(True)
        return {"offset": self.latest_offset_val}

    def partitions(self, start, end):
        return [InputPartition(b"partition")]

    def read(self, partition):
        return iter([])


class DataSourceStreamReaderAdmissionControlTests(unittest.TestCase):
    """Tests for DataSourceStreamReader admission control functionality."""

    def test_latest_offset_with_parameters(self):
        """Test that latestOffset can accept start_offset and read_limit parameters."""
        reader = MockStreamReaderWithAdmissionControl()

        # Test without parameters (backward compatibility)
        offset = reader.latestOffset()
        self.assertEqual(offset, {"offset": 1000})
        self.assertEqual(len(reader.latest_offset_calls), 1)
        self.assertIsNone(reader.latest_offset_calls[0]["start_offset"])
        self.assertIsNone(reader.latest_offset_calls[0]["read_limit"])

    def test_latest_offset_with_start_offset(self):
        """Test latestOffset with start_offset parameter."""
        reader = MockStreamReaderWithAdmissionControl()

        offset = reader.latestOffset(start_offset={"offset": 500})
        self.assertEqual(offset, {"offset": 1000})
        self.assertEqual(reader.latest_offset_calls[-1]["start_offset"], {"offset": 500})

    def test_latest_offset_with_max_rows_limit(self):
        """Test latestOffset respects maxRows limit."""
        reader = MockStreamReaderWithAdmissionControl()

        limit = {"type": "maxRows", "maxRows": 100}
        offset = reader.latestOffset(start_offset={"offset": 500}, read_limit=limit)

        # Should cap at 500 + 100 = 600, not 1000
        self.assertEqual(offset, {"offset": 600})
        self.assertEqual(reader.latest_offset_calls[-1]["read_limit"], limit)

    def test_latest_offset_limit_larger_than_available(self):
        """Test latestOffset when limit exceeds available data."""
        reader = MockStreamReaderWithAdmissionControl()

        limit = {"type": "maxRows", "maxRows": 2000}  # Larger than available
        offset = reader.latestOffset(start_offset={"offset": 900}, read_limit=limit)

        # Should return latest available (1000), not 900 + 2000
        self.assertEqual(offset, {"offset": 1000})

    def test_latest_offset_with_all_available_limit(self):
        """Test latestOffset with allAvailable limit."""
        reader = MockStreamReaderWithAdmissionControl()

        limit = {"type": "allAvailable"}
        offset = reader.latestOffset(start_offset={"offset": 500}, read_limit=limit)

        # Should return all available data
        self.assertEqual(offset, {"offset": 1000})

    def test_report_latest_offset(self):
        """Test reportLatestOffset returns true latest."""
        reader = MockStreamReaderWithAdmissionControl()

        # First call latestOffset with limit
        limit = {"type": "maxRows", "maxRows": 50}
        capped_offset = reader.latestOffset(start_offset={"offset": 100}, read_limit=limit)
        self.assertEqual(capped_offset, {"offset": 150})  # Capped

        # Now report latest should return true latest, not capped
        latest = reader.reportLatestOffset()
        self.assertEqual(latest, {"offset": 1000})
        self.assertEqual(len(reader.report_latest_offset_calls), 1)

    def test_report_latest_offset_default_none(self):
        """Test that reportLatestOffset returns None by default."""

        class MinimalReader(DataSourceStreamReader):
            def initialOffset(self):
                return {"offset": 0}

            def latestOffset(self, start_offset=None, read_limit=None):
                return {"offset": 100}

            def partitions(self, start, end):
                return []

            def read(self, partition):
                return iter([])

        reader = MinimalReader()
        # Should return None if not overridden
        self.assertIsNone(reader.reportLatestOffset())

    def test_read_limit_dictionary_formats(self):
        """Test various read limit dictionary formats work correctly."""
        reader = MockStreamReaderWithAdmissionControl()

        # Test maxFiles format (reader doesn't use it, but format should be accepted)
        limit = {"type": "maxFiles", "maxFiles": 10}
        offset = reader.latestOffset(start_offset={"offset": 0}, read_limit=limit)
        self.assertEqual(offset, {"offset": 1000})  # No maxFiles logic, returns all

        # Test maxBytes format
        limit = {"type": "maxBytes", "maxBytes": 1048576}
        offset = reader.latestOffset(start_offset={"offset": 0}, read_limit=limit)
        self.assertEqual(offset, {"offset": 1000})  # No maxBytes logic, returns all

        # Test minRows format
        limit = {"type": "minRows", "minRows": 100, "maxTriggerDelayMs": 30000}
        offset = reader.latestOffset(start_offset={"offset": 0}, read_limit=limit)
        self.assertEqual(offset, {"offset": 1000})  # No minRows logic, returns all

        # Test composite format
        limit = {
            "type": "composite",
            "limits": [
                {"type": "minRows", "minRows": 100, "maxTriggerDelayMs": 30000},
                {"type": "maxRows", "maxRows": 1000},
            ],
        }
        offset = reader.latestOffset(start_offset={"offset": 0}, read_limit=limit)
        self.assertEqual(offset, {"offset": 1000})  # No composite logic, returns all


class StreamReaderBackwardCompatibilityTests(unittest.TestCase):
    """Tests for backward compatibility with existing stream readers."""

    def test_old_signature_still_works(self):
        """Test that old latestOffset() signature without parameters still works."""

        class LegacyReader(DataSourceStreamReader):
            def initialOffset(self):
                return {"offset": 0}

            def latestOffset(self):
                """Old signature without parameters."""
                return {"offset": 100}

            def partitions(self, start, end):
                return []

            def read(self, partition):
                return iter([])

        reader = LegacyReader()
        offset = reader.latestOffset()
        self.assertEqual(offset, {"offset": 100})


if __name__ == "__main__":
    from pyspark.sql.tests.streaming.test_streaming_datasource_admission_control import (  # noqa: F401,E501
        *  # noqa: F401
    )

    try:
        import xmlrunner

        testRunner = xmlrunner.XMLTestRunner(output="target/test-reports", verbosity=2)
    except ImportError:
        testRunner = None
    unittest.main(testRunner=testRunner, verbosity=2)