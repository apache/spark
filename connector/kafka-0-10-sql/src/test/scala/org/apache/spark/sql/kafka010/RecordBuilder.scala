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

import java.lang.{Integer => JInt, Long => JLong}

import scala.jdk.CollectionConverters._

import org.apache.kafka.clients.producer.ProducerRecord
import org.apache.kafka.common.header.Header
import org.apache.kafka.common.header.internals.RecordHeader

class RecordBuilder(topic: String, value: String) {
  var _partition: Option[JInt] = None
  var _timestamp: Option[JLong] = None
  var _key: Option[String] = None
  var _headers: Option[Seq[(String, Array[Byte])]] = None

  def partition(part: JInt): RecordBuilder = {
    _partition = Some(part)
    this
  }

  def partition(part: Int): RecordBuilder = {
    _partition = Some(part.intValue())
    this
  }

  def timestamp(ts: JLong): RecordBuilder = {
    _timestamp = Some(ts)
    this
  }

  def timestamp(ts: Long): RecordBuilder = {
    _timestamp = Some(ts.longValue())
    this
  }

  def key(k: String): RecordBuilder = {
    _key = Some(k)
    this
  }

  def headers(hdrs: Seq[(String, Array[Byte])]): RecordBuilder = {
    _headers = Some(hdrs)
    this
  }

  def build(): ProducerRecord[String, String] = {
    val part = _partition.orNull
    val ts = _timestamp.orNull
    val k = _key.orNull
    val hdrs = _headers.map { h =>
      h.map { case (k, v) => new RecordHeader(k, v).asInstanceOf[Header] }
    }.map(_.asJava).orNull

    new ProducerRecord[String, String](topic, part, ts, k, value, hdrs)
  }
}
