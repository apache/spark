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
package org.apache.spark.sql.execution.datasources.xml

import org.apache.spark.sql.types.{ArrayType, DataType, StringType, StructField, StructType}

private[xml] object TestUtils {

  def buildSchema(fields: StructField*): StructType = StructType(fields)

  def field(name: String, dataType: DataType = StringType, nullable: Boolean = true): StructField =
    StructField(name, dataType, nullable)

  def structField(fields: StructField*): StructType = buildSchema(fields: _*)

  def structField(name: String, fields: StructField*): StructField =
    field(name, structField(fields: _*))

  def structArray(name: String, fields: StructField*): StructField =
    field(name, ArrayType(structField(fields: _*)))

  def arrayField(name: String, dataType: DataType): StructField = field(name, ArrayType(dataType))

}
