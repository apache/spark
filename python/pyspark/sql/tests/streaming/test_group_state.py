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

from pyspark.errors import PySparkRuntimeError
from pyspark.sql import Row
from pyspark.sql.streaming.state import GroupState, GroupStateTimeout
from pyspark.sql.types import StructType
from pyspark.testing.utils import PySparkErrorTestUtils


class GroupStateTests(unittest.TestCase, PySparkErrorTestUtils):
    """GroupState is plain Python and raises its errors before touching any JVM state,
    so these are exercised directly without a SparkSession."""

    @staticmethod
    def _group_state(timeout_conf):
        return GroupState(
            optionalValue=Row(),
            batchProcessingTimeMs=1000,
            eventTimeWatermarkMs=GroupState.NO_TIMESTAMP,
            timeoutConf=timeout_conf,
            hasTimedOut=False,
            watermarkPresent=False,
            defined=False,
            updated=False,
            removed=True,
            timeoutTimestamp=-1,
            keyAsUnsafe=b"",
            valueSchema=StructType(),
        )

    def test_set_timeout_timestamp_requires_event_time_timeout(self):
        state = self._group_state(GroupStateTimeout.NoTimeout)
        with self.assertRaises(PySparkRuntimeError) as pe:
            state.setTimeoutTimestamp(1000)
        self.check_error(
            pe.exception,
            errorClass="CANNOT_WITHOUT",
            messageParameters={
                "condition1": "set timeout timestamp",
                "condition2": "enabling event time timeout in applyInPandasWithState",
            },
        )


if __name__ == "__main__":
    from pyspark.testing import main

    main()
