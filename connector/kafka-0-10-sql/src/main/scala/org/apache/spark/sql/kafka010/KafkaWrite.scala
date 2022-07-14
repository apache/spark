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
package org.apache.spark.sql.kafka010

import java.{util => ju}

import org.apache.spark.sql.connector.write.{BatchWrite, Write}
import org.apache.spark.sql.connector.write.streaming.StreamingWrite
import org.apache.spark.sql.types.StructType

case class KafkaWrite(
    topic: Option[String],
    producerParams: ju.Map[String, Object],
    schema: StructType) extends Write {

  override def description(): String = "Kafka"

  override def toBatch: BatchWrite = {
    assert(schema != null)
    new KafkaBatchWrite(topic, producerParams, schema)
  }

  override def toStreaming: StreamingWrite = {
    assert(schema != null)
    new KafkaStreamingWrite(topic, producerParams, schema)
  }
}
