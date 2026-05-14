#
# Licensed to the Apache Software Foundation (ASF) under one or more
# contributor license agreements.  See the NOTICE file distributed with
# this work for additional information regarding copyright ownership.
#
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

import os
import tempfile
import unittest

from pyspark.errors import PySparkNotImplementedError
from pyspark.sql import SparkSession
from pyspark.sql.tests.connect.client.test_artifact import ArtifactViaSparkContextCheckMixin
from pyspark.testing.connectutils import ReusedConnectTestCase
from pyspark.util import is_remote_only


@unittest.skipIf(is_remote_only(), "Requires JVM access")
class SparkContextArtifactParityTests(ArtifactViaSparkContextCheckMixin, ReusedConnectTestCase):
    """Parity checks for Spark Connect :class:`SparkContext` artifact APIs using shared checks.

    These reuse one logical Spark Connect backend session (:class:`ReusedConnectTestCase`).
    Unlike :class:`~pyspark.sql.tests.connect.client.test_artifact.ArtifactTestsMixin`,
    we do not open a second remote session per test: artifact paths are global on that
    backend and a duplicate normalized path raises ``ARTIFACT_ALREADY_EXISTS``.
    """

    @classmethod
    def root(cls):
        from pyspark.core.files import SparkFiles

        jvm = SparkSession._instantiatedSession._jvm
        current_uuid = (
            getattr(
                getattr(
                    jvm.org.apache.spark,  # type: ignore[union-attr]
                    "JobArtifactSet$",
                ),
                "MODULE$",
            )
            .lastSeenState()
            .get()
            .uuid()
        )
        return os.path.join(SparkFiles.getRootDirectory(), current_uuid)

    def test_add_pyfile(self):
        self.check_add_pyfile_via_spark_context(self.spark)

    def test_add_file(self):
        self.check_add_file_via_spark_context(self.spark)

    def test_add_archive(self):
        self.check_add_archive_via_spark_context(self.spark)

    def test_add_zipped_package(self):
        self.check_add_zipped_package_via_spark_context(self.spark)

    def test_spark_connect_add_file_recursive_via_spark_context_not_supported(self):
        with tempfile.TemporaryDirectory(prefix="recursive_add_sc") as d:
            nested = os.path.join(d, "nested.txt")
            with open(nested, "w") as f:
                f.write("x")
            with self.assertRaises(PySparkNotImplementedError):
                self.spark.sparkContext.addFile(d, recursive=True)


if __name__ == "__main__":
    from pyspark.testing import main

    main()
