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
A sample wordcount using ZeroMQ in Python via reflection, based on the scala
ZeroMQWordCount example.

To work with zeroMQ, some native libraries have to be installed.
Install zeroMQ (release 2.1) core libraries. [ZeroMQ Install guide]
(http://www.zeromq.org/intro:get-the-software)

Usage: zeromq_wordcount.py <zeroMQurl> <topic>
   <zeroMQurl> and <topic> describe where zeroMq publisher is running.

To run this locally, first launch the publisher from the scala ZeroMQWordCount example:
    `$ bin/run-example \
      org.apache.spark.examples.streaming.SimpleZeroMQPublisher tcp://127.0.1.1:1234 foo.bar`
 and then run the subscriber, making sure to add the external jar with zeromq support to the
 classpath via the spark-submit --jars flag:
    `$ bin/spark-submit \
    --jars external/zeromq/target/spark-streaming-zeromq_2.10-1.3.0-SNAPSHOT.jar \
    examples/src/main/python/streaming/zeromq_wordcount.py tcp://127.0.1.1:1234 foo`
"""

import sys

from pyspark import SparkContext
from pyspark.streaming import StreamingContext
from pyspark.serializers import UTF8Deserializer

if __name__ == "__main__":
    if len(sys.argv) != 3:
        print >> sys.stderr, "Usage: zeromq_wordcount.py <zeromqUrl> <topic>"
        exit(-1)
    sc = SparkContext(appName="PythonZeroMQWordCount")
    ssc = StreamingContext(sc, 1)

    lines = ssc.reflectedStream(
        "org.apache.spark.streaming.zeromq.ReflectedZeroMQStreamFactory",
        UTF8Deserializer(),
        sys.argv[1], sys.argv[2])
    counts = lines.flatMap(lambda line: line.strip().split()) \
        .map(lambda word: (word, 1)) \
        .reduceByKey(lambda a, b: a+b)
    counts.pprint()

    ssc.start()
    ssc.awaitTermination()