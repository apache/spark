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
import os
import tempfile

from pyspark.sql.tests.connect.client.test_artifact import ArtifactTestsMixin
from pyspark.testing.sqlutils import ReusedSQLTestCase
from pyspark.errors import PySparkRuntimeError


class ArtifactTests(ArtifactTestsMixin, ReusedSQLTestCase):
    @classmethod
    def root(cls):
        from pyspark.core.files import SparkFiles

        return SparkFiles.getRootDirectory()

    def test_add_pyfile(self):
        self.check_add_pyfile(self.spark)

        # Test multi sessions. Should be able to add the same
        # file from different session.
        self.check_add_pyfile(self.spark.newSession())

    def test_add_file(self):
        self.check_add_file(self.spark)

        # Test multi sessions. Should be able to add the same
        # file from different session.
        self.check_add_file(self.spark.newSession())

    def test_add_archive(self):
        self.check_add_archive(self.spark)

        # Test multi sessions. Should be able to add the same
        # file from different session.
        self.check_add_file(self.spark.newSession())

    def test_artifacts_cannot_be_overwritten(self):
        with tempfile.TemporaryDirectory(prefix="test_artifacts_cannot_be_overwritten") as d:
            pyfile_path = os.path.join(d, "my_pyfile.py")
            with open(pyfile_path, "w+") as f:
                f.write("my_func = lambda: 10")

            self.spark.addArtifacts(pyfile_path, pyfile=True)

            # Writing the same file twice is fine, and should not throw.
            self.spark.addArtifacts(pyfile_path, pyfile=True)

            with open(pyfile_path, "w+") as f:
                f.write("my_func = lambda: 11")

            with self.assertRaises(PySparkRuntimeError) as pe:
                self.spark.addArtifacts(pyfile_path, pyfile=True)

            self.check_error(
                exception=pe.exception,
                errorClass="DUPLICATED_ARTIFACT",
                messageParameters={"normalized_path": pyfile_path},
            )

    def test_add_zipped_package(self):
        self.check_add_zipped_package(self.spark)

        # Test multi sessions. Should be able to add the same
        # file from different session.
        self.check_add_file(self.spark.newSession())


if __name__ == "__main__":
    from pyspark.sql.tests.test_artifact import *  # noqa: F401

    try:
        import xmlrunner  # type: ignore[import]

        testRunner = xmlrunner.XMLTestRunner(output="target/test-reports", verbosity=2)
    except ImportError:
        testRunner = None
    unittest.main(testRunner=testRunner, verbosity=2)
