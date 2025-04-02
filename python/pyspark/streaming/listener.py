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

from typing import Any


__all__ = ["StreamingListener"]


class StreamingListener:
    def __init__(self) -> None:
        pass

    def onStreamingStarted(self, streamingStarted: Any) -> None:
        """
        Called when the streaming has been started.
        """
        pass

    def onReceiverStarted(self, receiverStarted: Any) -> None:
        """
        Called when a receiver has been started
        """
        pass

    def onReceiverError(self, receiverError: Any) -> None:
        """
        Called when a receiver has reported an error
        """
        pass

    def onReceiverStopped(self, receiverStopped: Any) -> None:
        """
        Called when a receiver has been stopped
        """
        pass

    def onBatchSubmitted(self, batchSubmitted: Any) -> None:
        """
        Called when a batch of jobs has been submitted for processing.
        """
        pass

    def onBatchStarted(self, batchSubmitted: Any) -> None:
        """
        Called when processing of a batch of jobs has started.
        """
        pass

    def onBatchCompleted(self, batchCompleted: Any) -> None:
        """
        Called when processing of a batch of jobs has completed.
        """
        pass

    def onOutputOperationStarted(self, outputOperationStarted: Any) -> None:
        """
        Called when processing of a job of a batch has started.
        """
        pass

    def onOutputOperationCompleted(self, outputOperationCompleted: Any) -> None:
        """
        Called when processing of a job of a batch has completed
        """
        pass

    class Java:
        implements = ["org.apache.spark.streaming.api.java.PythonStreamingListener"]
