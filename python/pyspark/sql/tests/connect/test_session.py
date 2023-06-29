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
from typing import Optional

from pyspark.sql.connect.client import ChannelBuilder
from pyspark.sql.connect.session import SparkSession as RemoteSparkSession


class CustomChannelBuilder(ChannelBuilder):
    @property
    def userId(self) -> Optional[str]:
        return "abc"


class SparkSessionTestCase(unittest.TestCase):
    def test_fails_to_create_session_without_remote_and_channel_builder(self):
        with self.assertRaises(ValueError):
            RemoteSparkSession.builder.getOrCreate()

    def test_fails_to_create_when_both_remote_and_channel_builder_are_specified(self):
        with self.assertRaises(ValueError):
            (
                RemoteSparkSession.builder.channelBuilder(CustomChannelBuilder("sc://localhost"))
                .remote("sc://localhost")
                .getOrCreate()
            )

    def test_creates_session_with_channel_builder(self):
        test_session = RemoteSparkSession.builder.channelBuilder(
            CustomChannelBuilder("sc://other")
        ).getOrCreate()
        host = test_session.client.host
        test_session.stop()

        self.assertEqual("other", host)

    def test_creates_session_with_remote(self):
        test_session = RemoteSparkSession.builder.remote("sc://other").getOrCreate()
        host = test_session.client.host
        test_session.stop()

        self.assertEqual("other", host)
