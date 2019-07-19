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

package org.apache.spark.sql.execution.datasources.v2.jdbc

import java.sql.{Connection, PreparedStatement}

import org.apache.spark.internal.Logging
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.execution.datasources.jdbc.{JdbcOptionsInWrite, JdbcUtils}
import org.apache.spark.sql.execution.datasources.jdbc.JdbcUtils.{getCommonJDBCType, getJdbcType}
import org.apache.spark.sql.jdbc.{JdbcDialect, JdbcDialects, JdbcType}
import org.apache.spark.sql.types.{BooleanType, ByteType, DataType, DoubleType, FloatType, IntegerType, LongType, ShortType, StringType, StructType}

/* Misc utils

 */
object Utils extends Logging{
  def logSchema(prefix: String, schema: Option[StructType]) : Unit = {
    schema match {
      case Some(i) =>
        val schemaString = i.printTreeString()
        logInfo(s"***dsv2-flows*** $prefix schema exists" )
        logInfo(s"***dsv2-flows*** $prefix schemaInDB schema is $schemaString")
      case None =>
        logInfo(s"***dsv2-flows*** $prefix schemaInDB schema is None" )
    }
  }

  def createTable(structType: StructType): Unit = {
    /* Create table per passed schema. Raise exception on any failure.
     */
  }

  def strictSchemaCheck(schemaInSpark: StructType) : StructType = {
    // TODO : Raise exception if fwPassedSchema is not same as schemaInDB.
    schemaInSpark
  }





}
