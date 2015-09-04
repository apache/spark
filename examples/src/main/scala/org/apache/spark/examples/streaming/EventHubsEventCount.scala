/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.spark.examples.streaming

import org.apache.spark.SparkConf
import org.apache.spark.streaming.eventhubs.EventHubsUtils
import org.apache.spark.streaming.{Minutes, Seconds, StreamingContext}

/**
 * Count messages from EventHubs partitions.
 * Usage: EventCount <checkpointDirectory> <policy> <key> <namespace>
 *   <name> <partitionCount> <consumerGroup> <outputDirectory>
 *   <checkpointDirectory> is the DFS location to store context checkpoint
 *   <policyname> is the EventHubs policy name
 *   <policykey> is the EventHubs policy key
 *   <namespace> is the EventHubs namespace
 *   <name> is the name of the EventHubs
 *   <partitionCount> is the partition count of the EventHubs
 *   <consumerGroup> is the name of consumer group
 *   <outputDirectory> is the DFS location to store the output
 *
 * Example:
 *    `%SPARK_HOME%\bin\spark-submit.cmd
 *    --class org.apache.spark.streaming.eventhubs.example.EventCount
 *    --master spark://headnodehost:7077
 *    c:\sparktest\spark-streaming-eventhubs*.jar
 *    wasb://{container}@{account}.blob.core.windows.net/sparkcheckpoint
 *    root
 *    "pHUoYy8dvBHuXciNuD3NpVpDPL4mwycWhWQ9DVHASTM="
 *    myeh-ns
 *    myeh
 *    4
 *    "$default"
 *    wasb://{container}@{account}.blob.core.windows.net/sparkoutput/ehcount`
 */
object EventCount {
  def createContext(checkpointDir: String, ehParams: Map[String, String], outputDir: String) : StreamingContext = {
    println("Creating new StreamingContext")

    // Set max number of cores to double the partition count
    val partitionCount = ehParams("eventhubs.partition.count").toInt
    val sparkConf = new SparkConf().setAppName("EventCount").set("spark.cores.max",
      (partitionCount*2).toString)

    // Set batch size
    val ssc =  new StreamingContext(sparkConf, Seconds(5))
    ssc.checkpoint(checkpointDir)

    // Create a unioned stream for all partitions
    val stream = EventHubsUtils.createUnionStream(ssc, ehParams)

    // Create a single stream for one partition
    // val stream = EventHubsUtils.createStream(ssc, ehParams, "0")

    // Set checkpoint interval
    stream.checkpoint(Seconds(10))

    // Count number of events in the past minute
    // val counts = stream.countByWindow(Minutes(1), Seconds(5))

    // Count number of events in the past batch
    val counts = stream.count()

    counts.saveAsTextFiles(outputDir)
    counts.print()

    ssc
  }

  def main(args: Array[String]) {
    if (args.length < 8) {
      System.err.println("Usage: EventCount <checkpointDirectory> <policyname> <policykey>"
        + "<namespace> <name> <partitionCount> <consumerGroup> <outputDirectory>")
      System.exit(1)
    }
    val Array(checkpointDir, policy, key, namespace, name,
      partitionCount, consumerGroup, outputDir) = args
    val ehParams = Map[String, String](
      "eventhubs.policyname" -> policy,
      "eventhubs.policykey" -> key,
      "eventhubs.namespace" -> namespace,
      "eventhubs.name" -> name,
      "eventhubs.partition.count" -> partitionCount,
      "eventhubs.consumergroup" -> consumerGroup,
      "eventhubs.checkpoint.dir" -> checkpointDir,
      "eventhubs.checkpoint.interval" -> "10"
    )

    // Get StreamingContext from checkpoint directory, or create a new one
    val ssc = StreamingContext.getOrCreate(checkpointDir,
      () => {
        createContext(checkpointDir, ehParams, outputDir)
      })

    ssc.start()
    ssc.awaitTermination()
  }


}