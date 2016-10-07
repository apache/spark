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

package org.apache.spark.sql.streaming

import org.apache.spark.annotation.Experimental

/**
 * :: Experimental ::
 * Status and metrics of a streaming sink.
 *
 * @param description Description of the source corresponding to this status
 * @param offsetDesc Description of the current offset up to which data has been written by the sink
 * @since 2.0.0
 */
@Experimental
class SinkStatus private(
    val description: String,
    val offsetDesc: String) {

  override def toString: String =
    "SinkStatus:\n" + prettyStrings.map("    " + _).mkString("\n")

  private[sql] def prettyStrings: Array[String] = {
    s"""Description: $description
       |Committed offsets: $offsetDesc
       |""".stripMargin.split("\n")
  }
}

/** Companion object, primarily for creating SinkStatus instances internally */
private[sql] object SinkStatus {
  def apply(desc: String, offsetDesc: String): SinkStatus = new SinkStatus(desc, offsetDesc)
}
