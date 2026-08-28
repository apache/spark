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

import sys
import unittest

from pyspark.graphframes import GraphFrame
from pyspark.graphframes.examples import BeliefPropagation, Graphs
from pyspark.graphframes.lib import AggregateMessages, Pregel
from pyspark.graphframes.lib.pregel import Pregel as PregelFromSubmodule
from pyspark.util import is_remote_only


class GraphFramesClientImportTests(unittest.TestCase):
    def test_public_imports(self) -> None:
        self.assertTrue(GraphFrame)
        self.assertTrue(AggregateMessages)
        self.assertTrue(BeliefPropagation)
        self.assertTrue(Graphs)
        self.assertTrue(Pregel)
        self.assertIs(Pregel, PregelFromSubmodule)
        if is_remote_only():
            self.assertNotIn("pyspark.graphframes.classic.pregel", sys.modules)


if __name__ == "__main__":
    from pyspark.testing.unittestutils import main

    main()
