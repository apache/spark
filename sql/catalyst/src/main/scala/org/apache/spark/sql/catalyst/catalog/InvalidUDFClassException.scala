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

package org.apache.spark.sql.catalyst.catalog

import org.apache.spark.SparkThrowableHelper
import org.apache.spark.sql.AnalysisException

/**
 * Thrown when a query failed for invalid function class, usually because a SQL
 * function's class does not follow the rules of the UDF/UDAF/UDTF class definition.
 */
class InvalidUDFClassException private[sql](
    message: String,
    errorClass: Option[String] = None)
  extends AnalysisException(message = message, errorClass = errorClass) {

  def this(errorClass: String, messageParameters: Array[String]) =
    this(SparkThrowableHelper.getMessage(errorClass, null, messageParameters), Some(errorClass))
}
