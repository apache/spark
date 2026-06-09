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
import os
import tempfile

from pyspark.sql.tests.connect.client.test_artifact import ArtifactTestsMixin
from pyspark.testing.sqlutils import ReusedSQLTestCase
from pyspark.errors import PySparkRuntimeError
from pyspark.sql.functions import assert_true, lit, udf


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

    def test_add_multiple_pyfiles(self):
        def check_add_multiple_pyfiles(spark_session):
            with tempfile.TemporaryDirectory(prefix="check_add_multiple_pyfiles") as d:
                pyfile_paths = []
                for name, value in [
                    ("my_pyfile_a.py", 1),
                    ("my_pyfile_b.py", 2),
                    ("my_pyfile_c.py", 3),
                ]:
                    pyfile_path = os.path.join(d, name)
                    with open(pyfile_path, "w") as f:
                        f.write(f"my_func = lambda: {value}")
                    pyfile_paths.append(pyfile_path)

                @udf("int")
                def func(x):
                    import my_pyfile_a
                    import my_pyfile_b
                    import my_pyfile_c

                    return my_pyfile_a.my_func() + my_pyfile_b.my_func() + my_pyfile_c.my_func()

                spark_session.addArtifacts(*pyfile_paths, pyfile=True)
                spark_session.range(1).select(assert_true(func("id") == lit(6))).show()

        check_add_multiple_pyfiles(self.spark)

        # Test multi sessions. Should be able to add the same
        # files from different session.
        check_add_multiple_pyfiles(self.spark.newSession())

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
    from pyspark.testing import main

    main()
