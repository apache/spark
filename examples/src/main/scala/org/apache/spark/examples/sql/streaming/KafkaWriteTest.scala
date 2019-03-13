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

// scalastyle:off println
package org.apache.spark.examples.sql.streaming

import java.util.UUID

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.streaming.Trigger


object KafkaWriteTest {
  def main(args: Array[String]): Unit = {

    val spark = SparkSession
      .builder
      .appName("KafkaWriteTest")
      .getOrCreate()
    import spark.implicits._

    // Create DataSet representing the stream of input lines from kafka
    val lines = spark
      .readStream
      .format("kafka")
      .option("kafka.bootstrap.servers", "localhost:9092")
      .option("subscribe", "test")
      .load()
      .selectExpr("CAST(key AS STRING)", "CAST(value AS STRING)")

    val q = lines.writeStream.format("kafka")
      .option("kafka.bootstrap.servers", "localhost:9092")
      .option("topic", "test1").trigger(Trigger.ProcessingTime("2 second"))
      .option("checkpointLocation", "/tmp/temporary-" + UUID.randomUUID.toString)
      .start()
    q.awaitTermination()
  }

}
