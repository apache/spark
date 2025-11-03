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

package org.apache.spark.sql.connector.catalog

import org.apache.spark.SparkException
import org.apache.spark.annotation.Experimental

@Experimental
class CatalogNotFoundException(
    errorClass: String,
    messageParameters: Map[String, String],
    cause: Throwable)
  extends SparkException(errorClass, messageParameters, cause) {

  def this(errorClass: String, messageParameters: Map[String, String]) =
    this(errorClass, messageParameters, null)
}
