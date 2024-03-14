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

package org.apache.spark.sql.connect.common

import com.google.protobuf.Message

private[connect] object ProtoUtils {
  def abbreviate[T <: Message](message: T, maxStringSize: Int = 1024): T = {
    abbreviate[T](message, Map("STRING" -> maxStringSize))
  }

  def abbreviate[T <: Message](message: T, thresholds: Map[String, Int]): T = {
    new Abbreviator(thresholds).abbreviate[T](message)
  }

  // Because Spark Connect operation tags are also set as SparkContext Job tags, they cannot contain
  // SparkContext.SPARK_JOB_TAGS_SEP
  private val SPARK_JOB_TAGS_SEP = ',' // SparkContext.SPARK_JOB_TAGS_SEP

  /**
   * Validate if a tag for ExecutePlanRequest.tags is valid. Throw IllegalArgumentException if
   * not.
   */
  def throwIfInvalidTag(tag: String): Unit = {
    // Same format rules apply to Spark Connect execution tags as to SparkContext job tags,
    // because the Spark Connect job tag is also used as part of SparkContext job tag.
    // See SparkContext.throwIfInvalidTag and ExecuteHolderSessionTag
    if (tag == null) {
      throw new IllegalArgumentException("Spark Connect tag cannot be null.")
    }
    if (tag.contains(SPARK_JOB_TAGS_SEP)) {
      throw new IllegalArgumentException(
        s"Spark Connect tag cannot contain '$SPARK_JOB_TAGS_SEP'.")
    }
    if (tag.isEmpty) {
      throw new IllegalArgumentException("Spark Connect tag cannot be an empty string.")
    }
  }
}
