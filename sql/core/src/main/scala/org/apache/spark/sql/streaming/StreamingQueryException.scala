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

import org.apache.spark.annotation.{Experimental, InterfaceStability}

/**
 * :: Experimental ::
 * Exception that stopped a [[StreamingQuery]]. Use `cause` get the actual exception
 * that caused the failure.
 * @param message     Message of this exception
 * @param cause       Internal cause of this exception
 * @param startOffset Starting offset in json of the range of data in which exception occurred
 * @param endOffset   Ending offset in json of the range of data in exception occurred
 * @since 2.0.0
 */
@Experimental
@InterfaceStability.Evolving
class StreamingQueryException private[sql](
    private val queryDebugString: String,
    val message: String,
    val cause: Throwable,
    val startOffset: String,
    val endOffset: String)
  extends Exception(message, cause) {

  /** Time when the exception occurred */
  val time: Long = System.currentTimeMillis

  override def toString(): String =
    s"""${classOf[StreamingQueryException].getName}: ${cause.getMessage}
       |$queryDebugString""".stripMargin
}
