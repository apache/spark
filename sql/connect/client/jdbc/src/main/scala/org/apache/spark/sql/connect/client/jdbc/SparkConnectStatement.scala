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

package org.apache.spark.sql.connect.client.jdbc

import java.sql.{Array => _, _}

import org.apache.spark.sql.connect.client.SparkResult

class SparkConnectStatement(conn: SparkConnectConnection) extends Statement {

  private var operationId: String = _
  private var resultSet: SparkConnectResultSet = _

  private var maxRows: Int = 0

  @volatile private var closed: Boolean = false

  override def isClosed: Boolean = closed

  override def close(): Unit = synchronized {
    if (!closed) {
      if (operationId != null) {
        try {
          conn.spark.interruptOperation(operationId)
        } catch {
          case _: java.net.ConnectException =>
            // Ignore ConnectExceptions during cleanup as the operation may have already completed
            // or the server may be unavailable. The important part is marking this statement
            // as closed to prevent further use.
        }
        operationId = null
      }
      if (resultSet != null) {
        resultSet.close()
        resultSet = null
      }
      closed = true
    }
  }

  private[jdbc] def checkOpen(): Unit = {
    if (closed) {
      throw new SQLException("JDBC Statement is closed.")
    }
  }

  override def executeQuery(sql: String): ResultSet = {
    val hasResultSet = execute(sql)
    if (hasResultSet) {
      assert(resultSet != null)
      resultSet
    } else {
      throw new SQLException("The query does not produce a ResultSet.")
    }
  }

  override def executeUpdate(sql: String): Int = {
    val hasResultSet = execute(sql)
    if (hasResultSet) {
      // user are not expected to access the result set in this case,
      // we must close it to avoid memory leak.
      resultSet.close()
      throw new SQLException("The query produces a ResultSet.")
    } else {
      assert(resultSet == null)
      getUpdateCount
    }
  }

  private def hasResultSet(sparkResult: SparkResult[_]): Boolean = {
    // suppose this works in most cases
    sparkResult.schema.length > 0
  }

  override def execute(sql: String): Boolean = {
    checkOpen()

    // stmt can be reused to execute more than one queries,
    // reset before executing new query
    operationId = null
    resultSet = null

    var df = conn.spark.sql(sql)
    if (maxRows > 0) {
      df = df.limit(maxRows)
    }
    val sparkResult = df.collectResult()
    operationId = sparkResult.operationId
    if (hasResultSet(sparkResult)) {
      resultSet = new SparkConnectResultSet(sparkResult, this)
      true
    } else {
      sparkResult.close()
      false
    }
  }

  override def getResultSet: ResultSet = {
    checkOpen()
    resultSet
  }

  override def getMaxFieldSize: Int =
    throw new SQLFeatureNotSupportedException

  override def setMaxFieldSize(max: Int): Unit =
    throw new SQLFeatureNotSupportedException

  override def getMaxRows: Int = {
    checkOpen()
    this.maxRows
  }

  override def setMaxRows(max: Int): Unit = {
    checkOpen()

    if (max < 0) {
      throw new SQLException("The max rows must be zero or a positive integer.")
    }
    this.maxRows = max
  }

  override def setEscapeProcessing(enable: Boolean): Unit =
    throw new SQLFeatureNotSupportedException

  override def getQueryTimeout: Int = {
    checkOpen()
    0
  }

  override def setQueryTimeout(seconds: Int): Unit =
    throw new SQLFeatureNotSupportedException

  override def cancel(): Unit = {
    checkOpen()

    if (operationId != null) {
      conn.spark.interruptOperation(operationId)
    }
  }

  override def getWarnings: SQLWarning = null

  override def clearWarnings(): Unit = {}

  override def setCursorName(name: String): Unit =
    throw new SQLFeatureNotSupportedException

  override def getUpdateCount: Int = {
    checkOpen()

    if (resultSet != null) {
      -1
    } else {
      0 // always return 0 because affected rows is not supported yet
    }
  }

  override def getMoreResults: Boolean =
    throw new SQLFeatureNotSupportedException

  override def setFetchDirection(direction: Int): Unit =
    throw new SQLFeatureNotSupportedException

  override def getFetchDirection: Int =
    throw new SQLFeatureNotSupportedException

  override def setFetchSize(rows: Int): Unit =
    throw new SQLFeatureNotSupportedException

  override def getFetchSize: Int =
    throw new SQLFeatureNotSupportedException

  override def getResultSetConcurrency: Int = {
    checkOpen()
    ResultSet.CONCUR_READ_ONLY
  }

  override def getResultSetType: Int =
    throw new SQLFeatureNotSupportedException

  override def addBatch(sql: String): Unit =
    throw new SQLFeatureNotSupportedException

  override def clearBatch(): Unit =
    throw new SQLFeatureNotSupportedException

  override def executeBatch(): Array[Int] =
    throw new SQLFeatureNotSupportedException

  override def getConnection: Connection = {
    checkOpen()
    conn
  }

  override def getMoreResults(current: Int): Boolean =
    throw new SQLFeatureNotSupportedException

  override def getGeneratedKeys: ResultSet =
    throw new SQLFeatureNotSupportedException

  override def executeUpdate(sql: String, autoGeneratedKeys: Int): Int =
    throw new SQLFeatureNotSupportedException

  override def executeUpdate(sql: String, columnIndexes: Array[Int]): Int =
    throw new SQLFeatureNotSupportedException

  override def executeUpdate(sql: String, columnNames: Array[String]): Int =
    throw new SQLFeatureNotSupportedException

  override def execute(sql: String, autoGeneratedKeys: Int): Boolean =
    throw new SQLFeatureNotSupportedException

  override def execute(sql: String, columnIndexes: Array[Int]): Boolean =
    throw new SQLFeatureNotSupportedException

  override def execute(sql: String, columnNames: Array[String]): Boolean =
    throw new SQLFeatureNotSupportedException

  override def getResultSetHoldability: Int =
    throw new SQLFeatureNotSupportedException

  override def setPoolable(poolable: Boolean): Unit = {
    checkOpen()

    if (poolable) {
      throw new SQLFeatureNotSupportedException("Poolable statement is not supported")
    }
  }

  override def isPoolable: Boolean = {
    checkOpen()
    false
  }

  override def closeOnCompletion(): Unit = {
    checkOpen()
  }

  override def isCloseOnCompletion: Boolean = {
    checkOpen()
    false
  }

  override def unwrap[T](iface: Class[T]): T = if (isWrapperFor(iface)) {
    iface.asInstanceOf[T]
  } else {
    throw new SQLException(s"${this.getClass.getName} not unwrappable from ${iface.getName}")
  }

  override def isWrapperFor(iface: Class[_]): Boolean = iface.isInstance(this)
}
