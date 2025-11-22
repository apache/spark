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
from typing import Iterator

from pyspark.sql.streaming import StatefulProcessor
from pyspark.sql.streaming.tws_tester import TwsTester
from pyspark.sql.types import StructType, StructField, StringType, IntegerType
from pyspark.testing.sqlutils import (
    ReusedSQLTestCase,
    have_pandas,
    have_pyarrow,
    pandas_requirement_message,
    pyarrow_requirement_message,
)

if have_pandas:
    import pandas as pd

    output_schema = StructType([
        StructField("key", StringType(), True),
        StructField("count", IntegerType(), True)
    ])
    state_schema = StructType([
        StructField("value", IntegerType(), True)
    ])

    class RunningCountProcessor(StatefulProcessor):
        def init(self, handle) -> None:
            self.handle = handle
            self.count = handle.getValueState("count", state_schema)
        
        def handleInputRows(self, key, rows, timerValues) -> Iterator[pd.DataFrame]:
            count = self.count.get()[0] if self.count.exists() else 0

            for row_df in rows:
                for _, row in row_df.iterrows():
                    count += 1
            
            self.count.update((count,))
            
            yield pd.DataFrame({
                "key": [str(key)],
                "count": [count],
            })
        
        def close(self) -> None:
            pass

 
@unittest.skipIf(
    not have_pandas or not have_pyarrow,
    pandas_requirement_message or pyarrow_requirement_message or "",
)
class TwsTesterTests(ReusedSQLTestCase):
    
    def test_running_count_processor(self):
        input = [
            ("key1", pd.DataFrame({"value": ["a"]})),
            ("key2", pd.DataFrame({"value": ["b"]})),
            ("key1", pd.DataFrame({"value": ["c"]})),
            ("key2", pd.DataFrame({"value": ["b"]})),
            ("key1", pd.DataFrame({"value": ["c"]})),
            ("key1", pd.DataFrame({"value": ["c"]})),
            ("key3", pd.DataFrame({"value": ["q"]}))
        ]
        
        tester = TwsTester(RunningCountProcessor())
        ans1 = tester.test(input)
        
        expected = [
            {"key": "key1", "count": 4},
            {"key": "key2", "count": 2},
            {"key": "key3", "count": 1}
        ]
        
        ans1_sorted = sorted(ans1, key=lambda x: x["key"])
        expected_sorted = sorted(expected, key=lambda x: x["key"])
        
        self.assertEqual(ans1_sorted, expected_sorted)
        
        self.assertEqual(tester.peekValueState("count", "key1"), (4,))
        self.assertEqual(tester.peekValueState("count", "key2"), (2,))
        self.assertEqual(tester.peekValueState("count", "key3"), (1,))
        self.assertIsNone(tester.peekValueState("count", "key4"))
        
        ans2 = tester.test([("key1", pd.DataFrame({"value": ["q"]}))])
        self.assertEqual(ans2, [{"key": "key1", "count": 5}])
        self.assertEqual(tester.peekValueState("count", "key1"), (5,))
        self.assertEqual(tester.peekValueState("count", "key2"), (2,))
        
        ans3 = tester.test([
            ("key1", pd.DataFrame({"value": ["a"]})),
            ("key2", pd.DataFrame({"value": ["a"]}))
        ])
        ans3_sorted = sorted(ans3, key=lambda x: x["key"])
        expected3 = [{"key": "key1", "count": 6}, {"key": "key2", "count": 3}]
        self.assertEqual(ans3_sorted, expected3)
    
    def test_direct_access_to_value_state(self):
        processor = RunningCountProcessor()
        tester = TwsTester(processor)
        tester.setValueState("count", "foo", (5,))
        tester.test([("foo", pd.DataFrame({"value": ["a"]}))])
        self.assertEqual(tester.peekValueState("count", "foo"), (6,))


if __name__ == "__main__":
    from pyspark.sql.tests.pandas.streaming.test_tws_tester import *  # noqa: F401

    try:
        import xmlrunner

        testRunner = xmlrunner.XMLTestRunner(output="target/test-reports", verbosity=2)
    except ImportError:
        testRunner = None
    unittest.main(testRunner=testRunner, verbosity=2)

