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
from pyspark.sql.protobuf.functions import _read_descriptor_set_file
from pyspark.testing.utils import PySparkErrorTestUtils


class ProtobufFunctionsTests(unittest.TestCase, PySparkErrorTestUtils):
    def test_read_descriptor_set_file_not_found(self):
        with self.assertRaises(PySparkRuntimeError) as pe:
            _read_descriptor_set_file("/nonexistent/path/file.desc")

        self.check_error(
            exception=pe.exception,
            errorClass="PROTOBUF_DESCRIPTOR_FILE_NOT_FOUND",
            messageParameters={"filePath": "/nonexistent/path/file.desc"},
        )


if __name__ == "__main__":
    from pyspark.testing import main

    main()
