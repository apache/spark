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

__all__ = ["StreamingListener"]


class StreamingListener(object):

    def __init__(self):
        pass

    def onReceiverStarted(self, receiverStarted):
        """
        Called when a receiver has been started
        """
        pass

    def onReceiverError(self, receiverError):
        """
        Called when a receiver has reported an error
        """
        pass

    def onReceiverStopped(self, receiverStopped):
        """
        Called when a receiver has been stopped
        """
        pass

    def onBatchSubmitted(self, batchSubmitted):
        """
        Called when a batch of jobs has been submitted for processing.
        """
        pass

    def onBatchStarted(self, batchStarted):
        """
        Called when processing of a batch of jobs has started.
        """
        pass

    def onBatchCompleted(self, batchCompleted):
        """
        Called when processing of a batch of jobs has completed.
        """
        pass

    def onOutputOperationStarted(self, outputOperationStarted):
        """
        Called when processing of a job of a batch has started.
        """
        pass

    def onOutputOperationCompleted(self, outputOperationCompleted):
        """
        Called when processing of a job of a batch has completed
        """
        pass

    def getEventInfo(self, event):
        """
        :param event: StreamingListenerEvent
        :return Returns a BatchInfo, OutputOperationInfo, or ReceiverInfo based on
                event passed.
        """
        event_name = event.getClass().getSimpleName()
        if 'Batch' in event_name:
            return BatchInfo(event.batchInfo())

        elif 'Output' in event_name:
            return OutputOperationInfo(event.outputOperationInfo())

        elif 'Receiver' in event_name:
            return ReceiverInfo(event.receiverInfo())

    class Java:
        implements = ["org.apache.spark.streaming.scheduler.StreamingListener"]


class BatchInfo(object):

    def __init__(self, javaBatchInfo):

        self.processingStartTime = None
        self.processingEndTime = None

        self.batchTime = javaBatchInfo.batchTime()
        self.streamIdToInputInfo = self._map2dict(javaBatchInfo.streamIdToInputInfo())

        self.submissionTime = javaBatchInfo.submissionTime()
        if javaBatchInfo.processingStartTime().isEmpty() is False:
            self.processingStartTime = javaBatchInfo.processingStartTime().get()
        if javaBatchInfo.processingEndTime().isEmpty() is False:
            self.processingEndTime = javaBatchInfo.processingEndTime().get()

        self.outputOperationInfos = self._map2dict(javaBatchInfo.outputOperationInfos())


    def schedulingDelay(self):
        """
        Time taken for the first job of this batch to start processing from the time this batch
        was submitted to the streaming scheduler.
        """
        if self.processingStartTime is None:
            return None
        else:
            return self.processingStartTime - self.submissionTime

    def processingDelay(self):
        """
        Time taken for the all jobs of this batch to finish processing from the time they started
        processing.
        """
        if self.processingEndTime is None or self.processingStartTime is None:
            return None
        else:
            return self.processingEndTime - self.processingStartTime

    def totalDelay(self):
        """
        Time taken for all the jobs of this batch to finish processing from the time they
        were submitted
        """
        if self.processingEndTime is None or self.processingStartTime is None:
            return None
        else:
            return self.processingDelay() + self.schedulingDelay()

    def numRecords(self):
        """
        The number of recorders received by the receivers in this batch.
        """
        return len(self.streamIdToInputInfo)

    def _map2dict(self, javaMap):
        """
        Converts a scala.collection.immutable.Map to a Python dict
        """
        mapping = dict()
        map_iterator = javaMap.iterator()
        while map_iterator.hasNext():
            entry = map_iterator.next()
            mapping[entry._1()] = entry._2()
        return mapping


class OutputOperationInfo(object):

    def __init__(self, outputOperationInfo):
        self.batchTime = outputOperationInfo.batchTime()
        self.id = outputOperationInfo.id()
        self.name = outputOperationInfo.name()
        self.startTime = None
        if outputOperationInfo.startTime().isEmpty() is False:
            self.startTime = outputOperationInfo.startTime().get()
        self.endTime = None
        if outputOperationInfo.endTime().isEmpty() is False:
            self.endTime = outputOperationInfo.endTime().get()
        self.failureReason = None
        if outputOperationInfo.failureReason().isEmpty() is False:
            self.failureReason = outputOperationInfo.failureReason().get()

    def duration(self):
        if self.endTime is None or self.startTime is None:
            return None
        else:
            return self.endTime - self.startTime


class ReceiverInfo(object):

    def __init__(self, receiverInfo):
        self.streamId = receiverInfo.streamId()
        self.name = receiverInfo.name()
        self.active = receiverInfo.active()
        self.location = receiverInfo.location()
        self.lastErrorMessage = receiverInfo.lastErrorMessage()
        self.lastError = receiverInfo.lastError()
        self.lastErrorTime = receiverInfo.lastErrorTime()
