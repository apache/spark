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
from pyspark.streaming import StreamingListener
from pyspark.testing.streamingutils import PySparkStreamingTestCase


class StreamingListenerTests(PySparkStreamingTestCase):

    duration = .5

    class BatchInfoCollector(StreamingListener):

        def __init__(self):
            super(StreamingListener, self).__init__()
            self.batchInfosCompleted = []
            self.batchInfosStarted = []
            self.batchInfosSubmitted = []
            self.streamingStartedTime = []

        def onStreamingStarted(self, streamingStarted):
            self.streamingStartedTime.append(streamingStarted.time)

        def onBatchSubmitted(self, batchSubmitted):
            self.batchInfosSubmitted.append(batchSubmitted.batchInfo())

        def onBatchStarted(self, batchStarted):
            self.batchInfosStarted.append(batchStarted.batchInfo())

        def onBatchCompleted(self, batchCompleted):
            self.batchInfosCompleted.append(batchCompleted.batchInfo())

    def test_batch_info_reports(self):
        batch_collector = self.BatchInfoCollector()
        self.ssc.addStreamingListener(batch_collector)
        input = [[1], [2], [3], [4]]

        def func(dstream):
            return dstream.map(int)
        expected = [[1], [2], [3], [4]]
        self._test_func(input, func, expected)

        batchInfosSubmitted = batch_collector.batchInfosSubmitted
        batchInfosStarted = batch_collector.batchInfosStarted
        batchInfosCompleted = batch_collector.batchInfosCompleted
        streamingStartedTime = batch_collector.streamingStartedTime

        self.wait_for(batchInfosCompleted, 4)

        self.assertEqual(len(streamingStartedTime), 1)

        self.assertGreaterEqual(len(batchInfosSubmitted), 4)
        for info in batchInfosSubmitted:
            self.assertGreaterEqual(info.batchTime().milliseconds(), 0)
            self.assertGreaterEqual(info.submissionTime(), 0)

            for streamId in info.streamIdToInputInfo():
                streamInputInfo = info.streamIdToInputInfo()[streamId]
                self.assertGreaterEqual(streamInputInfo.inputStreamId(), 0)
                self.assertGreaterEqual(streamInputInfo.numRecords, 0)
                for key in streamInputInfo.metadata():
                    self.assertIsNotNone(streamInputInfo.metadata()[key])
                self.assertIsNotNone(streamInputInfo.metadataDescription())

            for outputOpId in info.outputOperationInfos():
                outputInfo = info.outputOperationInfos()[outputOpId]
                self.assertGreaterEqual(outputInfo.batchTime().milliseconds(), 0)
                self.assertGreaterEqual(outputInfo.id(), 0)
                self.assertIsNotNone(outputInfo.name())
                self.assertIsNotNone(outputInfo.description())
                self.assertGreaterEqual(outputInfo.startTime(), -1)
                self.assertGreaterEqual(outputInfo.endTime(), -1)
                self.assertIsNone(outputInfo.failureReason())

            self.assertEqual(info.schedulingDelay(), -1)
            self.assertEqual(info.processingDelay(), -1)
            self.assertEqual(info.totalDelay(), -1)
            self.assertEqual(info.numRecords(), 0)

        self.assertGreaterEqual(len(batchInfosStarted), 4)
        for info in batchInfosStarted:
            self.assertGreaterEqual(info.batchTime().milliseconds(), 0)
            self.assertGreaterEqual(info.submissionTime(), 0)

            for streamId in info.streamIdToInputInfo():
                streamInputInfo = info.streamIdToInputInfo()[streamId]
                self.assertGreaterEqual(streamInputInfo.inputStreamId(), 0)
                self.assertGreaterEqual(streamInputInfo.numRecords, 0)
                for key in streamInputInfo.metadata():
                    self.assertIsNotNone(streamInputInfo.metadata()[key])
                self.assertIsNotNone(streamInputInfo.metadataDescription())

            for outputOpId in info.outputOperationInfos():
                outputInfo = info.outputOperationInfos()[outputOpId]
                self.assertGreaterEqual(outputInfo.batchTime().milliseconds(), 0)
                self.assertGreaterEqual(outputInfo.id(), 0)
                self.assertIsNotNone(outputInfo.name())
                self.assertIsNotNone(outputInfo.description())
                self.assertGreaterEqual(outputInfo.startTime(), -1)
                self.assertGreaterEqual(outputInfo.endTime(), -1)
                self.assertIsNone(outputInfo.failureReason())

            self.assertGreaterEqual(info.schedulingDelay(), 0)
            self.assertEqual(info.processingDelay(), -1)
            self.assertEqual(info.totalDelay(), -1)
            self.assertEqual(info.numRecords(), 0)

        self.assertGreaterEqual(len(batchInfosCompleted), 4)
        for info in batchInfosCompleted:
            self.assertGreaterEqual(info.batchTime().milliseconds(), 0)
            self.assertGreaterEqual(info.submissionTime(), 0)

            for streamId in info.streamIdToInputInfo():
                streamInputInfo = info.streamIdToInputInfo()[streamId]
                self.assertGreaterEqual(streamInputInfo.inputStreamId(), 0)
                self.assertGreaterEqual(streamInputInfo.numRecords, 0)
                for key in streamInputInfo.metadata():
                    self.assertIsNotNone(streamInputInfo.metadata()[key])
                self.assertIsNotNone(streamInputInfo.metadataDescription())

            for outputOpId in info.outputOperationInfos():
                outputInfo = info.outputOperationInfos()[outputOpId]
                self.assertGreaterEqual(outputInfo.batchTime().milliseconds(), 0)
                self.assertGreaterEqual(outputInfo.id(), 0)
                self.assertIsNotNone(outputInfo.name())
                self.assertIsNotNone(outputInfo.description())
                self.assertGreaterEqual(outputInfo.startTime(), 0)
                self.assertGreaterEqual(outputInfo.endTime(), 0)
                self.assertIsNone(outputInfo.failureReason())

            self.assertGreaterEqual(info.schedulingDelay(), 0)
            self.assertGreaterEqual(info.processingDelay(), 0)
            self.assertGreaterEqual(info.totalDelay(), 0)
            self.assertEqual(info.numRecords(), 0)


if __name__ == "__main__":
    import unittest
    from pyspark.streaming.tests.test_listener import *  # noqa: F401

    try:
        import xmlrunner  # type: ignore[import]
        testRunner = xmlrunner.XMLTestRunner(output='target/test-reports', verbosity=2)
    except ImportError:
        testRunner = None
    unittest.main(testRunner=testRunner, verbosity=2)
