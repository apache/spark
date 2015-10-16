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

    # Called when a receiver has been started.
    def onReceiverStarted(self, receiverStarted):
        pass

    # Called when a receiver has reported an error.
    def onReceiverError(self, receiverError):
        pass

    # Called when a receiver has been stopped.
    def onReceiverStopped(self, receiverStopped):
        pass

    # Called when a batch of jobs has been submitted for processing.
    def onBatchSubmitted(self, batchSubmitted):
        pass

    # Called when processing of a batch of jobs has started.
    def onBatchStarted(self, batchStarted):
        pass

    # Called when processing of a batch of jobs has completed.
    def onBatchCompleted(self, batchCompleted):
        pass

    # Called when processing of a job of a batch has started.
    def onOutputOperationStarted(self, outputOperationStarted):
        pass

    # Called when processing of a job of a batch has completed
    def onOutputOperationCompleted(self, outputOperationCompleted):
        pass

    def getEventInfo(self, event):
        """
        :param event: StreamingListenerEvent
        :return Returns a BatchInfo, OutputOperationInfo, or ReceiverInfo based on
                event passed.
        """
        event_name = event.getClass().getSimpleName()
        if 'Batch' in event_name:
            return event.batchInfo()

        elif 'Output' in event_name:
            return event.outputOperationInfo()

        elif 'Receiver' in event_name:
            return event.receiverInfo()

    class Java:
        implements = ["org.apache.spark.streaming.scheduler.StreamingListener"]
