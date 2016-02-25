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

"""
 Create a queue of RDDs that will be mapped/reduced one at a time in
 1 second intervals.

 To run this example use
    `$ bin/spark-submit examples/src/main/python/streaming/queue_stream.py
"""
import sys
import time
import tempfile
from shutil import rmtree

from pyspark import SparkContext
from pyspark.streaming import StreamingContext
from pyspark.mllib.stat.test import BinarySample, StreamingTest

if __name__ == "__main__":

    sc = SparkContext(appName="PythonStreamingTestExample")
    ssc = StreamingContext(sc, 1)

    checkpointPath = tempfile.mkdtemp()
    ssc.checkpoint(checkpointPath)

    # Create the queue through which RDDs can be pushed to
    # a QueueInputDStream
    rddQueue = []
    for i in range(5):
        rddQueue += [ssc.sparkContext.parallelize(
            [BinarySample(True, j) for j in range(1, 1001)], 10)]

    # Create the QueueInputDStream and use it do some processing
    inputStream = ssc.queueStream(rddQueue)

    model = StreamingTest()
    test_result = model.registerStream(inputStream)

    test_result.pprint()

    ssc.start()
    time.sleep(12)
    ssc.stop(stopSparkContext=True, stopGraceFully=True)
    try:
        rmtree(checkpointPath)
    except OSError:
        pass
