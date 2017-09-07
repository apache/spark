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

# Counts words in UTF8 encoded, '\n' delimited text received from the network.

# To run this on your local machine, you need to first run a Netcat server
# $ nc -lk 9999
# and then run the example
# ./bin/spark-submit examples/src/main/r/streaming/structured_network_wordcount.R localhost 9999

# Load SparkR library into your R session
library(SparkR)

# Initialize SparkSession
sparkR.session(appName = "SparkR-Streaming-structured-network-wordcount-example")

args <- commandArgs(trailing = TRUE)

if (length(args) != 2) {
  print("Usage: structured_network_wordcount.R <hostname> <port>")
  print("<hostname> and <port> describe the TCP server that Structured Streaming")
  print("would connect to receive data.")
  q("no")
}

hostname <- args[[1]]
port <- as.integer(args[[2]])

# Create DataFrame representing the stream of input lines from connection to localhost:9999
lines <- read.stream("socket", host = hostname, port = port)

# Split the lines into words
words <- selectExpr(lines, "explode(split(value, ' ')) as word")

# Generate running word count
wordCounts <- count(groupBy(words, "word"))

# Start running the query that prints the running counts to the console
query <- write.stream(wordCounts, "console", outputMode = "complete")

awaitTermination(query)

sparkR.session.stop()
