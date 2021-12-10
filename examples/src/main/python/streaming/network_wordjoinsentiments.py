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

r"""
 Shows the most positive words in UTF8 encoded, '\n' delimited text directly received the network
 every 5 seconds. The streaming data is joined with a static RDD of the AFINN word list
 (http://neuro.imm.dtu.dk/wiki/AFINN)

 Usage: network_wordjoinsentiments.py <hostname> <port>
   <hostname> and <port> describe the TCP server that Spark Streaming would connect to receive data.

 To run this on your local machine, you need to first run a Netcat server
    `$ nc -lk 9999`
 and then run the example
    `$ bin/spark-submit examples/src/main/python/streaming/network_wordjoinsentiments.py \
    localhost 9999`
"""

import sys
from typing import Tuple

from pyspark import SparkContext
from pyspark.streaming import DStream, StreamingContext


def print_happiest_words(rdd):
    top_list = rdd.take(5)
    print("Happiest topics in the last 5 seconds (%d total):" % rdd.count())
    for tuple in top_list:
        print("%s (%d happiness)" % (tuple[1], tuple[0]))

if __name__ == "__main__":
    if len(sys.argv) != 3:
        print("Usage: network_wordjoinsentiments.py <hostname> <port>", file=sys.stderr)
        sys.exit(-1)

    sc = SparkContext(appName="PythonStreamingNetworkWordJoinSentiments")
    ssc = StreamingContext(sc, 5)

    def line_to_tuple(line: str) -> Tuple[str, str]:
        try:
            k, v = line.split(" ")
            return k, v
        except ValueError:
            return "", ""

    # Read in the word-sentiment list and create a static RDD from it
    word_sentiments_file_path = "data/streaming/AFINN-111.txt"
    word_sentiments = ssc.sparkContext.textFile(word_sentiments_file_path) \
        .map(line_to_tuple)

    lines = ssc.socketTextStream(sys.argv[1], int(sys.argv[2]))

    word_counts = lines.flatMap(lambda line: line.split(" ")) \
        .map(lambda word: (word, 1)) \
        .reduceByKey(lambda a, b: a + b)

    # Determine the words with the highest sentiment values by joining the streaming RDD
    # with the static RDD inside the transform() method and then multiplying
    # the frequency of the words by its sentiment value
    happiest_words: DStream[Tuple[float, str]] = word_counts \
        .transform(lambda rdd: word_sentiments.join(rdd)) \
        .map(lambda word_tuples: (word_tuples[0], float(word_tuples[1][0]) * word_tuples[1][1])) \
        .map(lambda word_happiness: (word_happiness[1], word_happiness[0])) \
        .transform(lambda rdd: rdd.sortByKey(False))

    happiest_words.foreachRDD(print_happiest_words)

    ssc.start()
    ssc.awaitTermination()
