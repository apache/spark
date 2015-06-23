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
 A sample wordcount with MqttStream stream
 Usage: mqtt_wordcount.py <broker url> <topic>
 To work with Mqtt, Mqtt Message broker/server required.
 Mosquitto (http://mosquitto.org/) is an open source Mqtt Broker
 In ubuntu mosquitto can be installed using the command  `$ sudo apt-get install mosquitto`
 Run Mqtt publisher as
  `$ bin/run-example \
       org.apache.spark.examples.streaming.MQTTPublisher tcp://localhost:1883 foo`
 and then run the example as
    `$ bin/spark-submit --jars external/mqtt-assembly/target/scala-*/\
      spark-streaming-mqtt-assembly-*.jar examples/src/main/python/streaming/mqtt_wordcount.py \
      tcp://localhost:1883 foo`
"""

import sys

from pyspark import SparkContext
from pyspark.streaming import StreamingContext
from pyspark.streaming.mqtt import MQTTUtils

if __name__ == "__main__":
    if len(sys.argv) != 3:
        print >> sys.stderr, "Usage: mqtt_wordcount.py <broker url> <topic>"
        exit(-1)

    sc = SparkContext(appName="PythonStreamingMQTTWordCount")
    ssc = StreamingContext(sc, 1)

    brokerUrl = sys.argv[1]
    topic = sys.argv[2]

    lines = MQTTUtils.createStream(ssc, brokerUrl, topic)
    counts = lines.flatMap(lambda line: line.split(" ")) \
        .map(lambda word: (word, 1)) \
        .reduceByKey(lambda a, b: a+b)
    counts.pprint()

    ssc.start()
    ssc.awaitTermination()
