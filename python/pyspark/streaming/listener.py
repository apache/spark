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


class StreamingListenerEvent(object):

    def __init__(self):
        pass


class StreamingListenerBatchSubmitted(StreamingListenerEvent):

    def __init__(self, batchInfo):
        super(StreamingListenerEvent, self).__init__()
        self.batchInfo = batchInfo


class StreamingListenerBatchCompleted(StreamingListenerEvent):

    def __init__(self, batchInfo):
        super(StreamingListenerEvent, self).__init__()
        self.batchInfo = batchInfo


class StreamingListenerBatchStarted(StreamingListenerEvent):

    def __init__(self, batchInfo):
        super(StreamingListenerEvent, self).__init__()
        self.batchInfo = batchInfo


class StreamingListenerOutputOperationStarted(StreamingListenerEvent):

    def __init__(self, outputOperationInfo):
        super(StreamingListenerEvent, self).__init__()
        self.outputOperationInfo = outputOperationInfo


class StreamingListenerOutputOperationCompleted(StreamingListenerEvent):

    def __init__(self, outputOperationInfo):
        super(StreamingListenerEvent, self).__init__()
        self.outputOperationInfo = outputOperationInfo


class StreamingListenerReceieverStarted(StreamingListenerEvent):

    def __init__(self, receiverInfo):
        super(StreamingListenerEvent, self).__init__()
        self.receiverInfo = receiverInfo


class StreamingListenerReceiverError(StreamingListenerEvent):

    def __init__(self, receiverInfo):
        super(StreamingListenerEvent, self).__init__()
        self.receiverInfo = receiverInfo


class StreamingListenerReceiverStopped(StreamingListenerEvent):

    def __init__(self, receiverInfo):
        super(StreamingListenerEvent, self).__init__()
        self.receiverInfo = receiverInfo


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

    class Java:
        implements = ["org.apache.spark.streaming.scheduler.StreamingListener"]


class StreamingListenerAdapter(StreamingListener):

    def __init__(self, streamingListener):
        super(StreamingListener, self).__init__()
        self.userStreamingListener = streamingListener

    def onReceiverStarted(self, receiverStarted):
        receiver_info = ReceiverInfo(receiverStarted.receiverInfo())
        receiver_started = StreamingListenerReceieverStarted(receiver_info)
        self.userStreamingListener.onReceiverStarted(receiver_started)

    def onReceiverError(self, receiverError):
        receiver_info = ReceiverInfo(receiverError.receiverInfo())
        receiver_error = StreamingListenerReceiverError(receiver_info)
        self.userStreamingListener.onReceiverError(receiver_error)

    def onReceiverStopped(self, receiverStopped):
        receiver_info = ReceiverInfo(receiverStopped.receiverInfo())
        receiver_stopped = StreamingListenerReceiverStopped(receiver_info)
        self.userStreamingListener.onReceiverStopped(receiver_stopped)

    def onBatchSubmitted(self, batchSubmitted):
        batch_info = BatchInfo(batchSubmitted.batchInfo())
        batch_submitted = StreamingListenerBatchSubmitted(batch_info)
        self.userStreamingListener.onBatchSubmitted(batch_submitted)

    def onBatchStarted(self, batchStarted):
        batch_info = BatchInfo(batchStarted.batchInfo())
        batch_started = StreamingListenerBatchStarted(batch_info)
        self.userStreamingListener.onBatchStarted(batch_started)

    def onBatchCompleted(self, batchCompleted):
        batch_info = BatchInfo(batchCompleted.batchInfo())
        batch_completed = StreamingListenerBatchCompleted(batch_info)
        self.userStreamingListener.onBatchCompleted(batch_completed)

    def onOutputOperationStarted(self, outputOperationStarted):
        output_op_info = OutputOperationInfo(outputOperationStarted.outputOperationInfo())
        output_operation_started = StreamingListenerOutputOperationStarted(output_op_info)
        self.userStreamingListener.onOutputOperationStarted(output_operation_started)

    def onOutputOperationCompleted(self, outputOperationCompleted):
        output_op_info = OutputOperationInfo(outputOperationCompleted.outputOperationInfo())
        output_operation_completed = StreamingListenerOutputOperationCompleted(output_op_info)
        self.userStreamingListener.onOutputOperationCompleted(output_operation_completed)


class BatchInfo(object):

    def __init__(self, javaBatchInfo):

        self.processingStartTime = None
        self.processingEndTime = None

        self.batchTime = javaBatchInfo.batchTime()
        self.streamIdToInputInfo = _map2dict(javaBatchInfo.streamIdToInputInfo(),
                                             StreamInputInfo)
        self.submissionTime = javaBatchInfo.submissionTime()
        if javaBatchInfo.processingStartTime().isEmpty() is False:
            self.processingStartTime = javaBatchInfo.processingStartTime().get()
        if javaBatchInfo.processingEndTime().isEmpty() is False:
            self.processingEndTime = javaBatchInfo.processingEndTime().get()

        self.outputOperationInfos = _map2dict(javaBatchInfo.outputOperationInfos(),
                                              OutputOperationInfo)

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


class StreamInputInfo(object):

    def __init__(self, streamInputInfo):
        self.inputStreamId = streamInputInfo.inputStreamId()
        self.numRecords = streamInputInfo.numRecords()
        self.metadata = _map2dict(streamInputInfo.metadata())
        self.metadataDescription = None
        if streamInputInfo.metadataDescription().isEmpty() is False:
            self.metadataDescription = streamInputInfo.metadataDescription().get()


def _map2dict(scalaMap, constructor=None):
    """
    Converts a scala.collection.immutable.Map to a Python dict.
    Creates an instance of an object as the value if a constructor
    is passed.
    """
    mapping = dict()
    map_iterator = scalaMap.iterator()
    while map_iterator.hasNext():
        entry = map_iterator.next()
        if constructor is not None:
            info = constructor(entry._2())
            mapping[entry._1()] = info
        else:
            mapping[entry._1()] = entry._2()
    return mapping
