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
import hashlib
import shutil
import tempfile
import unittest
import os

from pyspark.util import is_remote_only
from pyspark.sql import SparkSession
from pyspark.testing.connectutils import ReusedConnectTestCase, should_test_connect
from pyspark.testing.sqlutils import SPARK_HOME
from pyspark.sql.functions import udf, assert_true, lit

if should_test_connect:
    from pyspark.sql.connect.client.artifact import ArtifactManager
    from pyspark.sql.connect.client import DefaultChannelBuilder
    from pyspark.errors import SparkRuntimeException


class ArtifactTestsMixin:
    def check_add_pyfile(self, spark_session):
        with tempfile.TemporaryDirectory(prefix="check_add_pyfile") as d:
            pyfile_path = os.path.join(d, "my_pyfile.py")
            with open(pyfile_path, "w") as f:
                f.write("my_func = lambda: 10")

            @udf("int")
            def func(x):
                import my_pyfile

                return my_pyfile.my_func()

            spark_session.addArtifacts(pyfile_path, pyfile=True)
            spark_session.range(1).select(assert_true(func("id") == lit(10))).show()

    def test_add_pyfile(self):
        self.check_add_pyfile(self.spark)

        # Test multi sessions. Should be able to add the same
        # file from different session.
        self.check_add_pyfile(
            SparkSession.builder.remote(
                f"sc://localhost:{DefaultChannelBuilder.default_port()}"
            ).create()
        )

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

            with self.assertRaises(SparkRuntimeException) as pe:
                self.spark.addArtifacts(pyfile_path, pyfile=True)

            self.check_error(
                exception=pe.exception,
                errorClass="ARTIFACT_ALREADY_EXISTS",
                messageParameters={"normalizedRemoteRelativePath": "pyfiles/my_pyfile.py"},
            )

    def check_add_zipped_package(self, spark_session):
        with tempfile.TemporaryDirectory(prefix="check_add_zipped_package") as d:
            package_path = os.path.join(d, "my_zipfile")
            os.mkdir(package_path)
            pyfile_path = os.path.join(package_path, "__init__.py")
            with open(pyfile_path, "w") as f:
                _ = f.write("my_func = lambda: 5")
            shutil.make_archive(package_path, "zip", d, "my_zipfile")

            @udf("long")
            def func(x):
                import my_zipfile

                return my_zipfile.my_func()

            spark_session.addArtifacts(f"{package_path}.zip", pyfile=True)
            spark_session.range(1).select(assert_true(func("id") == lit(5))).show()

    def test_add_zipped_package(self):
        self.check_add_zipped_package(self.spark)

        # Test multi sessions. Should be able to add the same
        # file from different session.
        self.check_add_zipped_package(
            SparkSession.builder.remote(
                f"sc://localhost:{DefaultChannelBuilder.default_port()}"
            ).create()
        )

    def check_add_archive(self, spark_session):
        with tempfile.TemporaryDirectory(prefix="check_add_archive") as d:
            archive_path = os.path.join(d, "my_archive")
            os.mkdir(archive_path)
            pyfile_path = os.path.join(archive_path, "my_file.txt")
            with open(pyfile_path, "w") as f:
                _ = f.write("hello world!")
            shutil.make_archive(archive_path, "zip", d, "my_archive")

            # Should addArtifact first to make sure state is set,
            # and 'root' can be found properly.
            spark_session.addArtifacts(f"{archive_path}.zip#my_files", archive=True)

            root = self.root()

            @udf("string")
            def func(x):
                with open(
                    os.path.join(root, "my_files", "my_archive", "my_file.txt"),
                    "r",
                ) as my_file:
                    return my_file.read().strip()

            spark_session.range(1).select(assert_true(func("id") == lit("hello world!"))).show()

    def test_add_archive(self):
        self.check_add_archive(self.spark)

        # Test multi sessions. Should be able to add the same
        # file from different session.
        self.check_add_archive(
            SparkSession.builder.remote(
                f"sc://localhost:{DefaultChannelBuilder.default_port()}"
            ).create()
        )

    def check_add_file(self, spark_session):
        with tempfile.TemporaryDirectory(prefix="check_add_file") as d:
            file_path = os.path.join(d, "my_file.txt")
            with open(file_path, "w") as f:
                f.write("Hello world!!")

            # Should addArtifact first to make sure state is set,
            # and 'root' can be found properly.
            spark_session.addArtifacts(file_path, file=True)

            root = self.root()

            @udf("string")
            def func(x):
                with open(os.path.join(root, "my_file.txt"), "r") as my_file:
                    return my_file.read().strip()

            spark_session.range(1).select(assert_true(func("id") == lit("Hello world!!"))).show()

    def test_add_file(self):
        self.check_add_file(self.spark)

        # Test multi sessions. Should be able to add the same
        # file from different session.
        self.check_add_file(
            SparkSession.builder.remote(
                f"sc://localhost:{DefaultChannelBuilder.default_port()}"
            ).create()
        )


@unittest.skipIf(is_remote_only(), "Requires JVM access")
class ArtifactTests(ReusedConnectTestCase, ArtifactTestsMixin):
    @classmethod
    def root(cls):
        from pyspark.core.files import SparkFiles

        # In local mode, the file location is the same as Driver
        # The executors are running in a thread.
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

    @classmethod
    def setUpClass(cls):
        super(ArtifactTests, cls).setUpClass()
        cls.artifact_manager: ArtifactManager = cls.spark._client._artifact_manager
        cls.base_resource_dir = os.path.join(SPARK_HOME, "data")
        cls.artifact_file_path = os.path.join(
            cls.base_resource_dir,
            "artifact-tests",
        )
        cls.artifact_crc_path = os.path.join(
            cls.artifact_file_path,
            "crc",
        )

    @classmethod
    def conf(cls):
        conf = super().conf()
        conf.set("spark.sql.artifact.copyFromLocalToFs.allowDestLocal", "true")
        return conf

    def test_basic_requests(self):
        file_name = "smallJar"
        small_jar_path = os.path.join(self.artifact_file_path, f"{file_name}.jar")
        response = self.artifact_manager._retrieve_responses(
            self.artifact_manager._create_requests(
                small_jar_path, pyfile=False, archive=False, file=False
            )
        )
        self.assertTrue(response.artifacts[0].name.endswith(f"{file_name}.jar"))

    def test_single_chunk_artifact(self):
        file_name = "smallJar"
        small_jar_path = os.path.join(self.artifact_file_path, f"{file_name}.jar")
        small_jar_crc_path = os.path.join(self.artifact_crc_path, f"{file_name}.txt")

        requests = list(
            self.artifact_manager._create_requests(
                small_jar_path, pyfile=False, archive=False, file=False
            )
        )
        self.assertEqual(len(requests), 1)

        request = requests[0]
        self.assertIsNotNone(request.batch)

        batch = request.batch
        self.assertEqual(len(batch.artifacts), 1)

        single_artifact = batch.artifacts[0]
        self.assertTrue(single_artifact.name.endswith(".jar"))

        self.assertEqual(os.path.join("jars", f"{file_name}.jar"), single_artifact.name)
        with open(small_jar_crc_path) as f1, open(small_jar_path, "rb") as f2:
            self.assertEqual(single_artifact.data.crc, int(f1.readline()))
            self.assertEqual(single_artifact.data.data, f2.read())

    def test_chunked_artifacts(self):
        file_name = "junitLargeJar"
        large_jar_path = os.path.join(self.artifact_file_path, f"{file_name}.jar")
        large_jar_crc_path = os.path.join(self.artifact_crc_path, f"{file_name}.txt")

        requests = list(
            self.artifact_manager._create_requests(
                large_jar_path, pyfile=False, archive=False, file=False
            )
        )
        # Expected chunks = roundUp( file_size / chunk_size) = 12
        # File size of `junitLargeJar.jar` is 384581 bytes.
        large_jar_size = os.path.getsize(large_jar_path)
        expected_chunks = int(
            (large_jar_size + (ArtifactManager.CHUNK_SIZE - 1)) / ArtifactManager.CHUNK_SIZE
        )
        self.assertEqual(len(requests), expected_chunks)
        request = requests[0]
        self.assertIsNotNone(request.begin_chunk)
        begin_chunk = request.begin_chunk
        self.assertEqual(begin_chunk.name, os.path.join("jars", f"{file_name}.jar"))
        self.assertEqual(begin_chunk.total_bytes, large_jar_size)
        self.assertEqual(begin_chunk.num_chunks, expected_chunks)
        other_requests = requests[1:]
        data_chunks = [begin_chunk.initial_chunk] + [req.chunk for req in other_requests]

        with open(large_jar_crc_path) as f1, open(large_jar_path, "rb") as f2:
            cscs = [chunk.crc for chunk in data_chunks]
            expected_cscs = [int(line.rstrip()) for line in f1]
            self.assertEqual(cscs, expected_cscs)

            binaries = [chunk.data for chunk in data_chunks]
            expected_binaries = list(iter(lambda: f2.read(ArtifactManager.CHUNK_SIZE), b""))
            self.assertEqual(binaries, expected_binaries)

    def test_batched_artifacts(self):
        file_name = "smallJar"
        small_jar_path = os.path.join(self.artifact_file_path, f"{file_name}.jar")
        small_jar_crc_path = os.path.join(self.artifact_crc_path, f"{file_name}.txt")

        requests = list(
            self.artifact_manager._create_requests(
                small_jar_path, small_jar_path, pyfile=False, archive=False, file=False
            )
        )
        # Single request containing 2 artifacts.
        self.assertEqual(len(requests), 1)

        request = requests[0]
        self.assertIsNotNone(request.batch)

        batch = request.batch
        self.assertEqual(len(batch.artifacts), 2)

        artifact1 = batch.artifacts[0]
        self.assertTrue(artifact1.name.endswith(".jar"))
        artifact2 = batch.artifacts[1]
        self.assertTrue(artifact2.name.endswith(".jar"))

        self.assertEqual(os.path.join("jars", f"{file_name}.jar"), artifact1.name)
        with open(small_jar_crc_path) as f1, open(small_jar_path, "rb") as f2:
            crc = int(f1.readline())
            data = f2.read()
            self.assertEqual(artifact1.data.crc, crc)
            self.assertEqual(artifact1.data.data, data)
            self.assertEqual(artifact2.data.crc, crc)
            self.assertEqual(artifact2.data.data, data)

    def test_single_chunked_and_chunked_artifact(self):
        file_name1 = "smallJar"
        file_name2 = "junitLargeJar"
        small_jar_path = os.path.join(self.artifact_file_path, f"{file_name1}.jar")
        small_jar_crc_path = os.path.join(self.artifact_crc_path, f"{file_name1}.txt")
        large_jar_path = os.path.join(self.artifact_file_path, f"{file_name2}.jar")
        large_jar_crc_path = os.path.join(self.artifact_crc_path, f"{file_name2}.txt")
        large_jar_size = os.path.getsize(large_jar_path)

        requests = list(
            self.artifact_manager._create_requests(
                small_jar_path,
                large_jar_path,
                small_jar_path,
                small_jar_path,
                pyfile=False,
                archive=False,
                file=False,
            )
        )
        # There are a total of 14 requests.
        # The 1st request contains a single artifact - smallJar.jar (There are no
        # other artifacts batched with it since the next one is large multi-chunk artifact)
        # Requests 2-13 (1-indexed) belong to the transfer of junitLargeJar.jar. This includes
        # the first "beginning chunk" and the subsequent data chunks.
        # The last request (14) contains two smallJar.jar batched
        # together.
        self.assertEqual(len(requests), 1 + 12 + 1)

        first_req_batch = requests[0].batch.artifacts
        self.assertEqual(len(first_req_batch), 1)
        self.assertEqual(first_req_batch[0].name, os.path.join("jars", f"{file_name1}.jar"))
        with open(small_jar_crc_path) as f1, open(small_jar_path, "rb") as f2:
            self.assertEqual(first_req_batch[0].data.crc, int(f1.readline()))
            self.assertEqual(first_req_batch[0].data.data, f2.read())

        second_req_batch = requests[1]
        self.assertIsNotNone(second_req_batch.begin_chunk)
        begin_chunk = second_req_batch.begin_chunk
        self.assertEqual(begin_chunk.name, os.path.join("jars", f"{file_name2}.jar"))
        self.assertEqual(begin_chunk.total_bytes, large_jar_size)
        self.assertEqual(begin_chunk.num_chunks, 12)
        other_requests = requests[2:-1]
        data_chunks = [begin_chunk.initial_chunk] + [req.chunk for req in other_requests]

        with open(large_jar_crc_path) as f1, open(large_jar_path, "rb") as f2:
            cscs = [chunk.crc for chunk in data_chunks]
            expected_cscs = [int(line.rstrip()) for line in f1]
            self.assertEqual(cscs, expected_cscs)

            binaries = [chunk.data for chunk in data_chunks]
            expected_binaries = list(iter(lambda: f2.read(ArtifactManager.CHUNK_SIZE), b""))
            self.assertEqual(binaries, expected_binaries)

        last_request = requests[-1]
        self.assertIsNotNone(last_request.batch)

        batch = last_request.batch
        self.assertEqual(len(batch.artifacts), 2)

        artifact1 = batch.artifacts[0]
        self.assertTrue(artifact1.name.endswith(".jar"))
        artifact2 = batch.artifacts[1]
        self.assertTrue(artifact2.name.endswith(".jar"))

        self.assertEqual(os.path.join("jars", f"{file_name1}.jar"), artifact1.name)
        with open(small_jar_crc_path) as f1, open(small_jar_path, "rb") as f2:
            crc = int(f1.readline())
            data = f2.read()
            self.assertEqual(artifact1.data.crc, crc)
            self.assertEqual(artifact1.data.data, data)
            self.assertEqual(artifact2.data.crc, crc)
            self.assertEqual(artifact2.data.data, data)

    def test_copy_from_local_to_fs(self):
        with tempfile.TemporaryDirectory(prefix="test_copy_from_local_to_fs1") as d:
            with tempfile.TemporaryDirectory(prefix="test_copy_from_local_to_fs2") as d2:
                file_path = os.path.join(d, "file1")
                dest_path = os.path.join(d2, "file1_dest")
                file_content = "test_copy_from_local_to_FS"

                with open(file_path, "w") as f:
                    f.write(file_content)

                self.spark.copyFromLocalToFs(file_path, dest_path)

                with open(dest_path, "r") as f:
                    self.assertEqual(f.read(), file_content)

    def test_cache_artifact(self):
        s = "Hello, World!"
        blob = bytearray(s, "utf-8")
        expected_hash = hashlib.sha256(blob).hexdigest()
        self.assertEqual(self.artifact_manager.is_cached_artifact(expected_hash), False)
        actualHash = self.artifact_manager.cache_artifact(blob)
        self.assertEqual(actualHash, expected_hash)
        self.assertEqual(self.artifact_manager.is_cached_artifact(expected_hash), True)

    def test_add_not_existing_artifact(self):
        with tempfile.TemporaryDirectory(prefix="test_add_not_existing_artifact") as d:
            with self.assertRaises(FileNotFoundError):
                self.artifact_manager.add_artifacts(
                    os.path.join(d, "not_existing"), file=True, pyfile=False, archive=False
                )
