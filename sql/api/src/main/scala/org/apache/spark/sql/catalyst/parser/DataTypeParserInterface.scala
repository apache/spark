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
package org.apache.spark.sql.catalyst.parser

import org.apache.spark.sql.types.{DataType, StructType}

/**
 * Interface for [[DataType]] parsing functionality.
 */
trait DataTypeParserInterface {

  /**
   * Parse a string to a [[StructType]]. The passed SQL string should be a comma separated list of
   * field definitions which will preserve the correct Hive metadata.
   */
  @throws[ParseException]("Text cannot be parsed to a schema")
  def parseTableSchema(sqlText: String): StructType

  /**
   * Parse a string to a [[DataType]].
   */
  @throws[ParseException]("Text cannot be parsed to a DataType")
  def parseDataType(sqlText: String): DataType
}
