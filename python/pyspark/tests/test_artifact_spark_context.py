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

from pyspark.sql.tests.connect.client.test_artifact import ArtifactViaSparkContextCheckMixin
from pyspark.testing.sqlutils import ReusedSQLTestCase


class ArtifactSparkContextTests(ArtifactViaSparkContextCheckMixin, ReusedSQLTestCase):
    @classmethod
    def root(cls):
        from pyspark.core.files import SparkFiles

        return SparkFiles.getRootDirectory()

    def test_add_pyfile_via_spark_context(self):
        self.check_add_pyfile_via_spark_context(self.spark)

        self.check_add_pyfile_via_spark_context(self.spark.newSession())

    def test_add_file_via_spark_context(self):
        self.check_add_file_via_spark_context(self.spark)

        self.check_add_file_via_spark_context(self.spark.newSession())

    def test_add_archive_via_spark_context(self):
        self.check_add_archive_via_spark_context(self.spark)

        self.check_add_archive_via_spark_context(self.spark.newSession())

    def test_add_zipped_package_via_spark_context(self):
        self.check_add_zipped_package_via_spark_context(self.spark)

        self.check_add_zipped_package_via_spark_context(self.spark.newSession())


if __name__ == "__main__":
    from pyspark.testing import main

    main()
