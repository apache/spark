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
import time
import unittest

from pyspark import StorageLevel
from pyspark.streaming.kinesis import KinesisUtils, InitialPositionInStream
from pyspark.testing.streamingutils import (
    should_test_kinesis,
    kinesis_requirement_message,
    PySparkStreamingTestCase,
)


@unittest.skipIf(not should_test_kinesis, kinesis_requirement_message)
class KinesisStreamTests(PySparkStreamingTestCase):
    def test_kinesis_stream_api(self):
        # Don't start the StreamingContext because we cannot test it in Jenkins
        KinesisUtils.createStream(
            self.ssc,
            "myAppNam",
            "mySparkStream",
            "https://kinesis.us-west-2.amazonaws.com",
            "us-west-2",
            InitialPositionInStream.LATEST,
            2,
            StorageLevel.MEMORY_AND_DISK_2,
        )
        KinesisUtils.createStream(
            self.ssc,
            "myAppNam",
            "mySparkStream",
            "https://kinesis.us-west-2.amazonaws.com",
            "us-west-2",
            InitialPositionInStream.LATEST,
            2,
            StorageLevel.MEMORY_AND_DISK_2,
            "awsAccessKey",
            "awsSecretKey",
        )

    def test_kinesis_stream(self):
        import random

        kinesisAppName = "KinesisStreamTests-%d" % abs(random.randint(0, 10000000))
        kinesisTestUtils = self.ssc._jvm.org.apache.spark.streaming.kinesis.KinesisTestUtils(2)
        try:
            kinesisTestUtils.createStream()
            aWSCredentials = kinesisTestUtils.getAWSCredentials()
            stream = KinesisUtils.createStream(
                self.ssc,
                kinesisAppName,
                kinesisTestUtils.streamName(),
                kinesisTestUtils.endpointUrl(),
                kinesisTestUtils.regionName(),
                InitialPositionInStream.LATEST,
                10,
                StorageLevel.MEMORY_ONLY,
                aWSCredentials.getAWSAccessKeyId(),
                aWSCredentials.getAWSSecretKey(),
            )

            outputBuffer = []

            def get_output(_, rdd):
                for e in rdd.collect():
                    outputBuffer.append(e)

            stream.foreachRDD(get_output)
            self.ssc.start()

            testData = [i for i in range(1, 11)]
            expectedOutput = set([str(i) for i in testData])
            start_time = time.time()
            while time.time() - start_time < 120:
                kinesisTestUtils.pushData(testData)
                if expectedOutput == set(outputBuffer):
                    break
                time.sleep(10)
            self.assertEqual(expectedOutput, set(outputBuffer))
        except BaseException:
            import traceback

            traceback.print_exc()
            raise
        finally:
            self.ssc.stop(False)
            kinesisTestUtils.deleteStream()
            kinesisTestUtils.deleteDynamoDBTable(kinesisAppName)


if __name__ == "__main__":
    from pyspark.streaming.tests.test_kinesis import *  # noqa: F401

    try:
        import xmlrunner  # type: ignore[import]

        testRunner = xmlrunner.XMLTestRunner(output="target/test-reports", verbosity=2)
    except ImportError:
        testRunner = None
    unittest.main(testRunner=testRunner, verbosity=2)
