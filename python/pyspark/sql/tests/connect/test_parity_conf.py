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
from unittest.mock import Mock

from pyspark.errors import PySparkValueError
from pyspark.sql.tests.test_conf import ConfTestsMixin
from pyspark.testing.connectutils import (
    ReusedConnectTestCase,
    connect_requirement_message,
    should_test_connect,
)
from pyspark.testing.utils import PySparkErrorTestUtils

if should_test_connect:
    from pyspark.sql.connect.client import SparkConnectClient
    from pyspark.sql.connect.client.core import ConfigResult
    from pyspark.sql.connect.conf import RuntimeConf


class ConfParityTests(ConfTestsMixin, ReusedConnectTestCase):
    pass


@unittest.skipIf(not should_test_connect, connect_requirement_message)
class RuntimeConfTests(unittest.TestCase, PySparkErrorTestUtils):
    def setUp(self):
        self.client = Mock(spec=SparkConnectClient)
        self.conf = RuntimeConf(self.client)

    def test_set_and_unset_warnings(self):
        self.client.config.return_value = ConfigResult([], ["configuration warning"])
        for operation in ["set", "unset"]:
            with self.subTest(operation=operation):
                with self.assertWarnsRegex(UserWarning, "configuration warning"):
                    if operation == "set":
                        self.conf.set("key", "value")
                    else:
                        self.conf.unset("key")
                request = self.client.config.call_args.args[0]
                self.assertTrue(request.HasField(operation))
                if operation == "set":
                    self.assertEqual(
                        [(p.key, p.value) for p in request.set.pairs], [("key", "value")]
                    )
                else:
                    self.assertEqual(list(request.unset.keys), ["key"])

    def test_set_all_value_conversion_and_warning(self):
        self.client.config.return_value = ConfigResult([], ["configuration warning"])
        for silent in [False, True]:
            with self.subTest(silent=silent):
                with self.assertWarnsRegex(UserWarning, "configuration warning"):
                    self.conf._set_all(
                        {"enabled": True, "disabled": False, "count": 3, "text": "value"}, silent
                    )
                request = self.client.config.call_args.args[0]
                self.assertTrue(request.HasField("set"))
                self.assertEqual(request.set.silent, silent)
                self.assertEqual(
                    {p.key: p.value for p in request.set.pairs},
                    {"enabled": "true", "disabled": "false", "count": "3", "text": "value"},
                )

    def test_is_modifiable_invalid_response(self):
        for value in [None, "", "TRUE", "unexpected"]:
            with self.subTest(value=value):
                self.client.config.return_value = ConfigResult([("key", value)], [])
                with self.assertRaises(PySparkValueError) as error:
                    self.conf.isModifiable("key")
                self.check_error(
                    error.exception,
                    errorClass="VALUE_NOT_ALLOWED",
                    messageParameters={"arg_name": "result", "allowed_values": "'true' or 'false'"},
                )
                request = self.client.config.call_args.args[0]
                self.assertTrue(request.HasField("is_modifiable"))
                self.assertEqual(list(request.is_modifiable.keys), ["key"])


if __name__ == "__main__":
    from pyspark.testing import main

    main()
