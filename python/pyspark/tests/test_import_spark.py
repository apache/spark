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

import subprocess
import sys
import unittest


class ImportSparkTest(unittest.TestCase):
    def test_import_spark_libraries(self):
        """
        We want to ensure "import pyspark" is fast. It matters when we spawn
        workers and it provides a good user experience.

        It's difficult to compare the exact time, for this test, we test if any
        unnecessary 3rd party libraries are imported. We only expect py4j to
        be imported when we do "import pyspark".
        """
        ALLOWED_PACKAGES = set(sys.stdlib_module_names) | set(["py4j"])

        stdout = subprocess.check_output(
            ["python", "-X", "importtime", "-c", "import pyspark"],
            stderr=subprocess.STDOUT,
            text=True,
        )
        # reverse the lines to follow the dependency order
        # the first line is the header so we skip it
        lines = reversed(stdout.split("\n")[1:])
        module_lines = [line.split("|")[-1] for line in lines if line.strip()]
        skip_indentation = None
        for module_line in module_lines:
            module = module_line.lstrip()
            indentation = len(module_line) - len(module)
            if skip_indentation is not None:
                if indentation > skip_indentation:
                    continue
                else:
                    skip_indentation = None

            if module.startswith("pyspark"):
                continue

            package = module.split(".")[0]
            if package in ALLOWED_PACKAGES:
                skip_indentation = indentation
            else:
                self.fail(
                    f"Unexpected 3rd party package '{package}' imported during 'import pyspark'"
                )


if __name__ == "__main__":
    from pyspark.testing import main

    main()
