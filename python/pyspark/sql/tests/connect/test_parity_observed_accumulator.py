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

from pyspark.sql.tests.test_observed_accumulator import ObservedAccumulatorTestsMixin
from pyspark.testing.connectutils import ReusedConnectTestCase


class ObservedAccumulatorParityTests(ObservedAccumulatorTestsMixin, ReusedConnectTestCase):
    @unittest.skip("Uses a SparkContext accumulator to count invocations; not available on Connect")
    def test_udf_evaluated_once_per_row(self):
        pass

    @unittest.skip("Cross-checks against a SparkContext accumulator; not available on Connect")
    def test_matches_sparkcontext_accumulator(self):
        pass

    # Exercises the classic session-keyed JVM registry; Connect scopes state to the client store, a
    # separate mechanism covered elsewhere.
    @unittest.skip("Classic JVM-registry session isolation; not applicable to the Connect client")
    def test_same_named_accumulators_across_sessions_isolated(self):
        pass


if __name__ == "__main__":
    from pyspark.testing import main

    main()
