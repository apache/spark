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
Create a DStream that contains several RDDs to show the StreamingTest of PySpark.
"""
import time
import tempfile
from shutil import rmtree

from pyspark import SparkContext
from pyspark.streaming import StreamingContext
from pyspark.mllib.stat.test import BinarySample, StreamingTest

if __name__ == "__main__":

    sc = SparkContext(appName="PythonStreamingTestExample")
    ssc = StreamingContext(sc, 1)

    checkpoint_path = tempfile.mkdtemp()
    ssc.checkpoint(checkpoint_path)

    # Create the queue through which RDDs can be pushed to a QueueInputDStream.
    rdd_queue = []
    for i in range(5):
        rdd_queue += [ssc.sparkContext.parallelize(
            [BinarySample(True, j) for j in range(1, 1001)], 10)]

    # Create the QueueInputDStream and use it do some processing.
    input_stream = ssc.queueStream(rdd_queue)

    model = StreamingTest()
    test_result = model.registerStream(input_stream)

    test_result.pprint()

    ssc.start()
    time.sleep(12)
    ssc.stop(stopSparkContext=True, stopGraceFully=True)
    try:
        rmtree(checkpoint_path)
    except OSError:
        pass
