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

package org.apache.spark.streaming.kafka

import java.lang.{Integer => JInt}
import java.util.{Map => JMap}

import scala.collection.JavaConverters._

abstract class KafkaTopicFilter extends Serializable {}

object KafkaTopicFilter {
  def apply (
    regexFilter: String,
    numStreams: Int,
    blackList: Boolean = false
  ) : KafkaTopicFilter = new KafkaRegexTopicFilter(regexFilter, numStreams, blackList)

  def apply (topics: Map[String, Int]) : KafkaTopicFilter = new KafkaPlainTopicFilter(topics)

  def apply (topics: JMap[String, JInt]) : KafkaTopicFilter = new KafkaPlainTopicFilter(topics)
}

class KafkaRegexTopicFilter  (
   val regexFilter: String,
   val numStreams: Int,
   val blackList: Boolean
 ) extends KafkaTopicFilter {

}

class KafkaPlainTopicFilter  (
   val topics: Map[String, Int]
 ) extends KafkaTopicFilter {

  def this(topics: JMap[String, JInt]) {
    this(Map(topics.asScala.mapValues(_.intValue()).toSeq: _*));
  }
}
