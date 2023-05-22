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

from pyspark.testing.connectutils import ReusedConnectTestCase, should_test_connect
from pyspark.testing.utils import SPARK_HOME

if should_test_connect:
    from pyspark.sql.connect.client.artifact import ArtifactManager


class ArtifactTests(ReusedConnectTestCase):
    @classmethod
    def setUpClass(cls):
        super(ArtifactTests, cls).setUpClass()
        cls.artifact_manager: ArtifactManager = cls.spark._client._artifact_manager
        cls.base_resource_dir = os.path.join(
            SPARK_HOME, "connector", "connect", "common", "src", "test", "resources"
        )
        cls.artifact_file_path = os.path.join(
            cls.base_resource_dir,
            "artifact-tests",
        )
        cls.artifact_crc_path = os.path.join(
            cls.artifact_file_path,
            "crc",
        )

    def test_basic_requests(self):
        file_name = "smallJar"
        small_jar_path = os.path.join(self.artifact_file_path, f"{file_name}.jar")
        response = self.artifact_manager._retrieve_responses(
            self.artifact_manager._create_requests(small_jar_path)
        )
        self.assertTrue(response.artifacts[0].name.endswith(f"{file_name}.jar"))

    def test_single_chunk_artifact(self):
        file_name = "smallJar"
        small_jar_path = os.path.join(self.artifact_file_path, f"{file_name}.jar")
        small_jar_crc_path = os.path.join(self.artifact_crc_path, f"{file_name}.txt")

        requests = list(self.artifact_manager._create_requests(small_jar_path))
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

        requests = list(self.artifact_manager._create_requests(large_jar_path))
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

        requests = list(self.artifact_manager._create_requests(small_jar_path, small_jar_path))
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
                small_jar_path, large_jar_path, small_jar_path, small_jar_path
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


if __name__ == "__main__":
    from pyspark.sql.tests.connect.client.test_artifact import *  # noqa: F401

    try:
        import xmlrunner  # type: ignore

        testRunner = xmlrunner.XMLTestRunner(output="target/test-reports", verbosity=2)
    except ImportError:
        testRunner = None
    unittest.main(testRunner=testRunner, verbosity=2)
