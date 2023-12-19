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
import pickle
import sys
import subprocess
import tempfile
import unittest
from pyspark.sql import SparkSession
from pyspark.sql.chunk_api import persist_dataframe_as_chunks, read_chunk, unpersist_chunks


class ChunkReadApiTests(unittest.TestCase):
    def setUp(self):
        self._old_sys_path = list(sys.path)
        class_name = self.__class__.__name__
        self.spark = (
            SparkSession.builder.master("local-cluster[2, 1, 1024]")
            .appName(class_name)
            .config("spark.python.dataFrameChunkRead.enabled", "true")
            .config("spark.task.maxFailures", "1")
            .getOrCreate()
        )
        self.sc = self.spark.sparkContext

        self.test_df = self.spark.range(0, 16, 1, 2)
        self.chunks = persist_dataframe_as_chunks(self.test_df, 3)
        self.chunk_ids = [chunk.id for chunk in self.chunks]
        self.expected_chunk_data_list = [
            [0, 1, 2],
            [3, 4, 5],
            [6, 7],
            [8, 9, 10],
            [11, 12, 13],
            [14, 15],
        ]
        assert len(self.chunks) == len(self.expected_chunk_data_list)

        self.child_proc_test_code = """
import sys
from pyspark.sql.chunk_api import read_chunk
chunk_ids = sys.argv[1].split(",")
for chunk_id in chunk_ids:
    chunk_pd = read_chunk(chunk_id).to_pandas()
    chunk_pd.to_pickle(f"{chunk_id}.pkl")
"""

    def tearDown(self):
        self.spark.stop()
        sys.path = self._old_sys_path

    def test_read_chunk_in_driver(self):
        for i, chunk in enumerate(self.chunks):
            chunk_data = list(read_chunk(chunk.id).to_pandas().id)
            self.assertEqual(chunk_data, self.expected_chunk_data_list[i])

    def test_read_chunk_in_executor(self):
        def mapper(chunk_id):
            return list(read_chunk(chunk_id).to_pandas().id)

        chunk_data_list = self.sc.parallelize(self.chunk_ids, 4).map(mapper).collect()
        self.assertEqual(chunk_data_list, self.expected_chunk_data_list)

    def _assert_saved_chunk_data_correct(self, dir_path):
        for chunk_id, expected_chunk_data in zip(self.chunk_ids, self.expected_chunk_data_list):
            with open(os.path.join(dir_path, f"{chunk_id}.pkl"), "rb") as f:
                pdf = pickle.load(f)
                chunk_data = list(pdf.id)
                self.assertEqual(chunk_data, expected_chunk_data)

    def test_read_chunk_in_driver_child_proc(self):
        with tempfile.TemporaryDirectory() as tmp_dir:
            with open(os.path.join(tmp_dir, "read_chunk_and_save.py"), "w") as f:
                f.write(self.child_proc_test_code)

            subprocess.check_call(
                ["python", "./read_chunk_and_save.py", ",".join(self.chunk_ids)],
                cwd=tmp_dir,
            )

            self._assert_saved_chunk_data_correct(tmp_dir)

    def test_read_chunk_in_udf_worker_child_proc(self):
        with tempfile.TemporaryDirectory() as tmp_dir:
            with open(os.path.join(tmp_dir, "read_chunk_and_save.py"), "w") as f:
                f.write(self.child_proc_test_code)

            def mapper(chunk_id):
                subprocess.check_call(
                    ["python", "./read_chunk_and_save.py", chunk_id],
                    cwd=tmp_dir,
                )
                return True

            self.sc.parallelize(self.chunk_ids, 4).map(mapper).collect()
            self._assert_saved_chunk_data_correct(tmp_dir)

    def test_unpersist_chunk(self):
        df = self.spark.range(16)
        chunks = persist_dataframe_as_chunks(df, 16)
        unpersist_chunks([chunks[0].id])
        with self.assertRaisesRegex(RuntimeError, "cache does not exist or has been removed"):
            read_chunk(chunks[0].id)
