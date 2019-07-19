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

import java.sql.{Connection, DriverManager}

import org.apache.spark.internal.Logging
import org.apache.spark.sql.SaveMode
import org.apache.spark.sql.execution.datasources.jdbc.{JdbcOptionsInWrite, JdbcUtils}
import org.apache.spark.sql.sources.{AlwaysTrue$, Filter}
import org.apache.spark.sql.sources.v2.writer.{BatchWrite, SupportsOverwrite, WriteBuilder}
import org.apache.spark.sql.types.StructType

class JDBCWriteBuilder(options: JdbcOptionsInWrite,
                       userSchema: Option[StructType])
  extends SupportsOverwrite with Logging{
  // TODO : Check, The default mode is assumed as Append. Refer physical plans to
  // overwrite and append data i.e. OverwriteByExpressionExec and AppendDataExec
  // respectively(Truncate and overwrite are called explicitly)
  private var writeMode : SaveMode = SaveMode.Append
  private var isTruncate : Boolean = false
  private var fwPassedSchema : StructType = _
  private val conn : Connection = JdbcUtils.createConnectionFactory(options)()

  override def withQueryId(queryId: String): WriteBuilder = {
    logInfo("***dsv2-flows*** withQueryId called with queryId" + queryId)
    // TODO : Check, Possible for his object to handles multiple queries on same table.
    this
  }

  override def withInputDataSchema(schema: StructType): WriteBuilder = {
    logInfo("***dsv2-flows*** withInputDataSchema called with schema")
    logInfo("***dsv2-flows*** schema is " + schema.printTreeString())
    fwPassedSchema = schema
    this
  }

  override def buildForBatch : BatchWrite = {
    logInfo("***dsv2-flows*** buildForBatch called")
    writeMode match {
      case SaveMode.Overwrite =>
        processOverwrite()
      case SaveMode.Append =>
        processAppend()
    }
    new JDBCBatchWrite(options, fwPassedSchema)
  }

  override def overwrite(filters: Array[Filter]): WriteBuilder = {
    logInfo("***dsv2-flows*** overwrite called ")
    writeMode = SaveMode.Overwrite
    this
  }

  override def truncate(): WriteBuilder = {
    logInfo("***dsv2-flows*** overwrite called ")
    writeMode = SaveMode.Overwrite
    isTruncate = true
    this
  }

  def processOverwrite() : Boolean = {
    /* Overwrite table logic
        1. Check if table exists. If not create it here. Should create be done??
        2. If table exists and isTruncate, then just truncate existing table
        3. If table exists and !isTruncate, then recreate table with new schema
        Post table creation, send requests to executors to insert data.

        check filters.
    */
    logInfo("***dsv2-flows*** Overwrite table with new schema")
    false
  }

  def processAppend() : Unit = {
    /* Append table logic
     * 1. Check is table exists. Create if not. Step4.
     * 2. If table exists and schema does not match, raise exception.
     * 3. If table exists and schema match. Step4
     * 4. Send to executors for data insert
     */
    logInfo("***dsv2-flows*** Append to table")
    // log schemas received.
    Utils.logSchema("userSchema", userSchema)
    Utils.logSchema("fwPassedSchema", Option(fwPassedSchema))

    JdbcUtils.tableExists(conn, options) match {
      case true =>
        logInfo("***dsv2-flows*** Table exists" )
        Utils.strictSchemaCheck(fwPassedSchema)
        logInfo("***dsv2-flows*** schema check done. Good to go." )
      case _ =>
        logInfo("***dsv2-flows*** Table does not exists." )
        // TODO : Check scemantics, Raise exception Or Create it.
        Utils.createTable(fwPassedSchema)
    }
  }
}
