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

from pyspark.context import SparkContext
from pyspark.serializers import PickleSerializer


class Receiver(object):
    """
    :: DeveloperApi ::
    Abstract class of a receiver that can be run on worker nodes to receive
    external data. A custom receiver can be defined by defining the functions
    `onStart()` and `onStop()`. `onStart()` should define the setup steps
    necessary to start receiving data, and `onStop()` should define the cleanup
    steps necessary to stop receiving data. Exceptions while receiving can
    be handled either by restarting the receiver with `restart(...)`
    or stopped completely by `stop(...)`.

    A custom receiver would look like this.

    >>> class MyReceiver(Receiver):
    ...     def onStart(self):
    ...         # Setup stuff (start threads, open sockets, etc.) to start receiving data.
    ...         # Must start new thread to receive data, as onStart() must be non-blocking.
    ...
    ...         # Call store(...) in those threads to store received data into Spark's memory.
    ...
    ...         # Call stop(...), restart(...) or reportError(...) on any thread based on how
    ...         # different errors needs to be handled.
    ...
    ...         # See corresponding method documentation for more details
    ...         pass
    ...
    ...     def onStop(self):
    ...         pass # Cleanup stuff (stop threads, close sockets, etc.) to stop receiving data.
    """

    def __init__(self, storageLevel):
        self.storageLevel = storageLevel
        self.sc = SparkContext._active_spark_context
        self.ser = PickleSerializer()
        self._jreceiver = None

    def onStart(self):
        """
        This method is called by the system when the receiver is started. This function
        must initialize all resources (threads, buffers, etc.) necessary for receiving data.
        This function must be non-blocking, so receiving the data must occur on a different
        thread. Received data can be stored with Spark by calling `store(data)`.

        If there are errors in threads started here, then following options can be done
        (i) `reportError(...)` can be called to report the error to the driver.
        The receiving of data will continue uninterrupted.
        (ii) `stop(...)` can be called to stop receiving data. This will call `onStop()` to
        clear up all resources allocated (threads, buffers, etc.) during `onStart()`.
        (iii) `restart(...)` can be called to restart the receiver. This will call `onStop()`
        immediately, and then `onStart()` after a delay.
        """

    def onStop(self):
        """
        This method is called by the system when the receiver is stopped. All resources
        (threads, buffers, etc.) setup in `onStart()` must be cleaned up in this method.
        """

    def preferredLocation(self):
        """
        Override this to specify a preferred location (hostname).

        :return: preferred location (hostname)
        """

    def store(self, object, metadata=None):
        """
        Store a single item of received data to Spark's memory.
        These single items will be aggregated together into data blocks before
        being pushed into Spark's memory.
        """
        return self.storeMulti([object], metadata)

    def storeMulti(self, objects, metadata=None):
        """
        Store an list of received data as a data block into Spark's memory.
        The metadata will be associated with this block of data
        for being used in the corresponding InputDStream.
        """
        return self._jreceiver.store(bytearray(self.ser.dumps(objects)))

    def reportError(self, message, exc=None):
        """ Report exceptions in receiving data."""
        return self._jreceiver.reportError(message, exc or str(exc))

    def restart(self, message, exc=None, timeout=None):
        """
        Restart the receiver. This method schedules the restart and returns
        immediately. The stopping and subsequent starting of the receiver
        (by calling `onStop()` and `onStart()`) is performed asynchronously
        in a background thread. The delay between the stopping and the starting
        is defined by the Spark configuration `spark.streaming.receiverRestartDelay`.
        The `message` and `exception` will be reported to the driver.

        If `timeout` is specified, This method schedules the restart and returns
        immediately. The stopping and subsequent starting of the receiver
        (by calling `onStop()` and `onStart()`) is performed asynchronously
        in a background thread.
        """
        return self._jreceiver.restart(message, exc and str(exc),
                                       timeout and long(timeout * 1000) or 0)

    def stop(self, message, exc=None):
        """Stop the receiver completely due to an exception"""
        return self._jreceiver.stop(message, exc and str(exc))

    def isStarted(self):
        """Check if the receiver has started or not."""
        return self._jreceiver.isStarted()

    def isStopped(self):
        """
        Check if receiver has been marked for stopping. Use this to identify when
        the receiving of data should be stopped.
        """
        return self._jreceiver.isStopped()

    @property
    def streamId(self):
        """
        Get the unique identifier the receiver input stream that this
        receiver is associated with.
        """
        return self._jreceiver.streamId()

    class Java:
        implements = ['org.apache.spark.streaming.api.python.PythonReceiver']
