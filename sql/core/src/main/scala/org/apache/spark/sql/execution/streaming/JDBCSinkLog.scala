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
package org.apache.spark.sql.execution.streaming

import java.sql.{Connection, PreparedStatement, ResultSet}

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.execution.datasources.jdbc.{JDBCOptions, JdbcUtils}
import org.apache.spark.sql.types.{LongType, StringType, StructField, StructType}



class JDBCSinkLog(parameters: Map[String, String], sparkSession: SparkSession) {

  val tableName = parameters(JDBCOptions.JDBC_TABLE_NAME)
  val options = new JDBCOptions(parameters)
  val logTableName = tableName + "$_STREAM_LOG"
  val logParams : Map[String, String] = parameters + (JDBCOptions.JDBC_TABLE_NAME -> logTableName)
  val logOptions = new JDBCOptions(logParams)
  val batchIdCol = parameters.get("batchIdCol")

  val BATCH_ID = "batchId"
  val STATUS = "status"
  def isBatchCommitted(batchId: Long, connection: Connection ): Boolean = {
    createTableIfNotExists(connection)
    return status(connection, batchId).contains("COMMITTED")

  }
  def startBatch(batchId: Long, connection: Connection ): Unit = {

    createTableIfNotExists(connection)
    if(status(connection, batchId).contains("UNCOMMITTED")) {

      deleteIncompleteBatch(connection, batchId)
    }
    updateStatus(connection, batchId, "UNCOMMITTED")
  }
  def commitBatch(batchId: Long, connection: Connection ): Unit = {

    createTableIfNotExists(connection)
    updateStatus(connection, batchId, "COMMITTED")
  }

  private def createTableIfNotExists(conn: Connection): Unit = {
    if (!JdbcUtils.tableExists(conn, logOptions)) {
      JdbcUtils.createTable(conn, StructType(
                                    List(StructField(BATCH_ID, LongType, true)
                                      , StructField(STATUS, StringType, true))
                                  )
                            , sparkSession, logOptions)
    }
  }

  def quote(colName: String): String = {
    s""""$colName""""
  }

  private def status(conn: Connection, batchId: Long): Option[String] = {
    val ps: PreparedStatement = conn.prepareStatement(s"Select ${quote(STATUS)}" +
                                  s" from $logTableName where ${quote(BATCH_ID)}=?" )
    try {
      ps.setLong(1, batchId)
      val rs = ps.executeQuery()
      try {
        if (rs.next()) Some(rs.getString(1)) else None
      } finally {
        rs.close()
      }
    } finally {
      ps.close()
    }
  }

  private def deleteIncompleteBatch(conn: Connection, batchId: Long): Unit = {
    batchIdCol.foreach(b => {
      // Sink table has a batch ID column. This means we can support exactly once semantic
      // this function is called when an incomplete batch is being re executed
      // so we will delete the existing contents of the table
      val ps: PreparedStatement = conn.prepareStatement(s"delete from $tableName" +
                                                      s" where ${quote(b)}=?" )
      try {
        ps.setLong(1, batchId)
        ps.executeUpdate()
      } finally {
        ps.close()
      }
    })
    // now delete the row from log table
    val ps: PreparedStatement = conn.prepareStatement(s"delete from $logTableName" +
                                                s" where ${quote(BATCH_ID)}=?" )
    try {
      ps.setLong(1, batchId)
      ps.executeUpdate()
    } finally {
      ps.close()
    }
  }

  private def updateStatus(conn: Connection, batchId: Long, newStatus: String): Unit = {

    status(conn, batchId) match {
      case None =>
        // insert row
        val ps: PreparedStatement = conn.prepareStatement(s"insert into $logTableName" +
                                            s" (${quote(BATCH_ID)}, ${quote(STATUS)})" +
                                            s" values (?,?)" )
        try {
          ps.setLong(1, batchId)
          ps.setString(2, newStatus)
          ps.executeUpdate()
        } finally {
          ps.close()
        }
      case Some(existingStatus) if (existingStatus != newStatus) =>
        // update row
        val ps: PreparedStatement = conn.prepareStatement(
                                      s"update $logTableName set ${quote(STATUS)}=?" +
                                        s" where ${quote(BATCH_ID)}=?" )
        try {
          ps.setString(1, newStatus)
          ps.setLong(2, batchId)
          ps.executeUpdate()
        } finally {
          ps.close()
        }

    }

  }



}
