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

package org.apache.spark.sql.execution.datasources.jdbc

import java.sql.Connection

import scala.util.control.NonFatal

import org.apache.spark.internal.Logging

/**
 * Object that contains metadata about the external database.
 * The metadata is static database information such as the version, or the version
 * of the JDBC driver.
 *
 * This object is stored in JDBCRDD.
 */
case class JDBCDatabaseMetadata(
   databaseMajorVersion: Option[Int],
   databaseMinorVersion: Option[Int],
   databaseDriverMajorVersion: Option[Int],
   databaseDriverMinorVersion: Option[Int]
 )

/**
 * Companion object for DatabaseMetadata.
 * Contains factory methods to build instances.
 */
object JDBCDatabaseMetadata extends Logging {

  /**
   * Safely retrieves a piece of metadata.
   *
   * @param f A function that retrieves an integer value from DatabaseMetaData.
   * @return Some(value) on success, None on failure.
   */
  private def safeGet(f: => Int): Option[Int] = {
    try {
      Some(f)
    } catch {
      case NonFatal(e) =>
        logWarning(log"Exception while getting specific database metadata", e)
        None
    }
  }

  /**
   * Creates a DatabaseMetadata instance from a JDBC Connection,
   * handling errors for each field individually.
   *
   * @param getConnection A JDBC connection factory.
   * @return A new instance of DatabaseMetadata containing the version metadata.
   */
  def fromJDBCConnectionFactory(getConnection: Int => Connection): JDBCDatabaseMetadata = {
    var conn: Connection = null

    def closeConnection(): Unit = {
      try {
        if (null != conn) {
          conn.close()
        }
        logInfo("closed connection during metadata fetch")
      } catch {
        case e: Exception => logWarning("Exception closing connection during metadata fetch", e)
      }
    }

    try {
      conn = getConnection(-1)
      // getMetaData itself can throw, so we catch that and return None for all fields
      val databaseMetadata = conn.getMetaData

      JDBCDatabaseMetadata(
        databaseMajorVersion = safeGet(databaseMetadata.getDatabaseMajorVersion),
        databaseMinorVersion = safeGet(databaseMetadata.getDatabaseMinorVersion),
        databaseDriverMajorVersion = safeGet(databaseMetadata.getDriverMajorVersion),
        databaseDriverMinorVersion = safeGet(databaseMetadata.getDriverMinorVersion)
      )
    } catch {
      case NonFatal(e) =>
        logWarning(log"Exception while getting database metadata object from connection", e)
        JDBCDatabaseMetadata(None, None, None, None)
    } finally {
      closeConnection()
    }
  }
}
