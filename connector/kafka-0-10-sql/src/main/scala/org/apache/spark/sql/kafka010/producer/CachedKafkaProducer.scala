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

package org.apache.spark.sql.kafka010.producer

import java.{util => ju}

import scala.util.control.NonFatal

import org.apache.kafka.clients.producer.KafkaProducer

import org.apache.spark.internal.Logging

private[kafka010] class CachedKafkaProducer(
    val cacheKey: Seq[(String, Object)],
    val producer: KafkaProducer[Array[Byte], Array[Byte]]) extends Logging {
  val id: String = ju.UUID.randomUUID().toString

  private[producer] def close(): Unit = {
    try {
      logInfo(s"Closing the KafkaProducer with id: $id.")
      producer.close()
    } catch {
      case NonFatal(e) => logWarning("Error while closing kafka producer.", e)
    }
  }
}
