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

package org.apache.spark.sql.hive.thriftserver

import org.apache.hive.service.cli.{HiveSQLException, OperationState}
import org.apache.hive.service.cli.operation.Operation

import org.apache.spark.SparkContext
import org.apache.spark.internal.{Logging, MDC}
import org.apache.spark.internal.LogKeys.{HIVE_OPERATION_TYPE, STATEMENT_ID}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.CurrentUserContext.CURRENT_USER
import org.apache.spark.sql.catalyst.catalog.{CatalogTableType, SessionCatalog}
import org.apache.spark.sql.catalyst.catalog.CatalogTableType.{EXTERNAL, MANAGED, VIEW}
import org.apache.spark.sql.internal.{SessionState, SharedState, SQLConf}
import org.apache.spark.util.Utils

/**
 * Utils for Spark operations.
 */
private[hive] trait SparkOperation extends Operation with Logging {

  protected def session: SparkSession

  protected var statementId = getHandle().getHandleIdentifier().getPublicId().toString()

  protected def cleanup(): Unit = () // noop by default

  abstract override def run(): Unit = {
    withLocalProperties {
      super.run()
    }
  }

  private def sessionState: SessionState = session.sessionState

  final protected def catalog: SessionCatalog = sessionState.catalog

  final protected def conf: SQLConf = sessionState.conf

  final protected def sparkContext: SparkContext = session.sparkContext

  final protected def withClassLoader(f: ClassLoader => Unit): Unit = {
    val sharedState: SharedState = session.sharedState
    val executionHiveClassLoader = sharedState.jarClassLoader
    Thread.currentThread().setContextClassLoader(executionHiveClassLoader)
    f(executionHiveClassLoader)
  }

  abstract override def close(): Unit = {
    super.close()
    cleanup()
    logInfo(log"Close statement with ${MDC(STATEMENT_ID, statementId)}")
    HiveThriftServer2.eventManager.onOperationClosed(statementId)
  }

  // Set thread local properties for the execution of the operation.
  // This method should be applied during the execution of the operation, by all the child threads.
  // The original spark context local properties will be restored after the operation.
  //
  // It is used to:
  // - set appropriate SparkSession
  // - set scheduler pool for the operation
  def withLocalProperties[T](f: => T): T = {
    val originalProps = Utils.cloneProperties(sparkContext.getLocalProperties)
    val originalSession = SparkSession.getActiveSession

    try {
      // Set active SparkSession
      SparkSession.setActiveSession(session)

      // Set scheduler pool
      session.conf.getOption(SQLConf.THRIFTSERVER_POOL.key) match {
        case Some(pool) =>
          sparkContext.setLocalProperty(SparkContext.SPARK_SCHEDULER_POOL, pool)
        case None =>
      }
      CURRENT_USER.set(getParentSession.getUserName)
      // run the body
      f
    } finally {
      CURRENT_USER.remove()
      // reset local properties, will also reset SPARK_SCHEDULER_POOL
      sparkContext.setLocalProperties(originalProps)

      originalSession match {
        case Some(session) => SparkSession.setActiveSession(session)
        case None => SparkSession.clearActiveSession()
      }
    }
  }

  def tableTypeString(tableType: CatalogTableType): String = tableType match {
    case EXTERNAL | MANAGED => "TABLE"
    case VIEW => "VIEW"
    case t =>
      throw new IllegalArgumentException(s"Unknown table type is found: $t")
  }

  protected def onError(): PartialFunction[Throwable, Unit] = {
    case e: Throwable =>
      logError(log"Error operating ${MDC(HIVE_OPERATION_TYPE, getType)} with " +
        log"${MDC(STATEMENT_ID, statementId)}", e)
      super.setState(OperationState.ERROR)
      HiveThriftServer2.eventManager.onStatementError(
        statementId, e.getMessage, Utils.exceptionString(e))
      e match {
        case _: HiveSQLException => throw e
        case _ => throw HiveThriftServerErrors.hiveOperatingError(getType, e)
      }
  }
}
