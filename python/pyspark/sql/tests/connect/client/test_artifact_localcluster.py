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
import unittest

from pyspark.sql.tests.connect.client.test_artifact import (
    ArtifactTestsMixin,
    GlobalPyFilesPrecedenceTestsMixin,
    GlobalPyFilesTestsMixin,
)
from pyspark.testing.connectutils import ReusedConnectTestCase
from pyspark.util import is_remote_only


class LocalClusterArtifactTests(ArtifactTestsMixin, ReusedConnectTestCase):
    @classmethod
    def conf(cls):
        return (
            super().conf().set("spark.driver.memory", "512M").set("spark.executor.memory", "512M")
        )

    @classmethod
    def root(cls):
        # In local cluster, we can mimic the production usage.
        return "."

    @classmethod
    def master(cls):
        return os.environ.get("SPARK_CONNECT_TESTING_REMOTE", "local-cluster[2,2,512]")


@unittest.skipIf(is_remote_only(), "Requires JVM access")
class LocalClusterGlobalPyFilesTests(
    GlobalPyFilesTestsMixin, GlobalPyFilesPrecedenceTestsMixin, ReusedConnectTestCase
):
    @classmethod
    def conf(cls):
        return cls.with_global_py_files(
            super().conf().set("spark.driver.memory", "512M").set("spark.executor.memory", "512M")
        )

    @classmethod
    def master(cls):
        return os.environ.get("SPARK_CONNECT_TESTING_REMOTE", "local-cluster[2,2,512]")


if __name__ == "__main__":
    from pyspark.testing import main

    main()
