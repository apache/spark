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
import sys
import typing
import unittest


_unittest_main = None

if sys.version_info >= (3, 12) and _unittest_main is None:
    _unittest_main = unittest.main

    def unittest_main(*args, **kwargs):
        exit = kwargs.pop("exit", True)
        kwargs["exit"] = False
        res = _unittest_main(*args, **kwargs)

        if exit:
            if not res.result.wasSuccessful():
                sys.exit(1)
            elif res.result.testsRun == 0 and len(res.result.skipped) == 0:
                sys.exit(5)
            else:
                sys.exit(0)

        return res

    unittest.main = unittest_main


from pyspark.testing.utils import assertDataFrameEqual, assertSchemaEqual

__all__ = ["assertDataFrameEqual", "assertSchemaEqual"]
