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

import org.apache.spark.SparkThrowable
import org.apache.spark.annotation.Evolving

/**
 * Exception that stopped a [[StreamingQuery]] in Spark Connect. Currently not all fields in the
 * original StreamingQueryException are supported.
 * @param message
 *   Maps to return value of original StreamingQueryException's toString method
 * @param errorClass
 *   Error class of this exception
 * @param stackTrace
 *   Stack trace of this exception
 * @since 3.5.0
 */
@Evolving
class StreamingQueryException private[sql] (
    message: String,
    errorClass: String,
    stackTrace: String)
    extends Exception(message)
    with SparkThrowable {

  override def getErrorClass: String = errorClass

  override def toString: String = s"""$message
    |JVM stacktrace: $stackTrace
    |""".stripMargin
}
