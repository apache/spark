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

package org.apache.spark.sql.util

import scala.collection.JavaConverters._

import org.apache.spark.annotation.DeveloperApi
import org.apache.spark.internal.Logging
import org.apache.spark.scheduler.{SparkListener, SparkListenerEvent}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.errors.QueryExecutionErrors
import org.apache.spark.sql.execution.QueryExecution
import org.apache.spark.sql.execution.ui.SparkListenerSQLExecutionEnd
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.internal.StaticSQLConf._
import org.apache.spark.util.{ListenerBus, Utils}

/**
 * The interface of query execution listener that can be used to analyze execution metrics.
 *
 * @note Implementations should guarantee thread-safety as they can be invoked by
 * multiple different threads.
 */
trait QueryExecutionListener {

  /**
   * A callback function that will be called when a query executed successfully.
   *
   * @param funcName name of the action that triggered this query.
   * @param qe the QueryExecution object that carries detail information like logical plan,
   *           physical plan, etc.
   * @param durationNs the execution time for this query in nanoseconds.
   *
   * @note This can be invoked by multiple different threads.
   */
  @DeveloperApi
  def onSuccess(funcName: String, qe: QueryExecution, durationNs: Long): Unit

  /**
   * A callback function that will be called when a query execution failed.
   *
   * @param funcName the name of the action that triggered this query.
   * @param qe the QueryExecution object that carries detail information like logical plan,
   *           physical plan, etc.
   * @param exception the exception that failed this query. If `java.lang.Error` is thrown during
   *                  execution, it will be wrapped with an `Exception` and it can be accessed by
   *                  `exception.getCause`.
   * @note This can be invoked by multiple different threads.
   */
  @DeveloperApi
  def onFailure(funcName: String, qe: QueryExecution, exception: Exception): Unit
}


/**
 * Manager for [[QueryExecutionListener]]. See `org.apache.spark.sql.SQLContext.listenerManager`.
 */
// The `session` is used to indicate which session carries this listener manager, and we only
// catch SQL executions which are launched by the same session.
// The `loadExtensions` flag is used to indicate whether we should load the pre-defined,
// user-specified listeners during construction. We should not do it when cloning this listener
// manager, as we will copy all listeners to the cloned listener manager.
class ExecutionListenerManager private[sql](
    session: SparkSession,
    sqlConf: SQLConf,
    loadExtensions: Boolean)
  extends Logging {

  // SPARK-39864: lazily create the listener bus on the first register() call in order to
  // avoid listener overheads when QueryExecutionListeners aren't used:
  private val listenerBusInitializationLock = new Object()
  @volatile private var listenerBus: Option[ExecutionListenerBus] = None

  if (loadExtensions) {
    val conf = session.sparkContext.conf
    conf.get(QUERY_EXECUTION_LISTENERS).foreach { classNames =>
      SQLConf.withExistingConf(sqlConf) {
        Utils.loadExtensions(classOf[QueryExecutionListener], classNames, conf).foreach(register)
      }
    }
  }

  /**
   * Registers the specified [[QueryExecutionListener]].
   */
  @DeveloperApi
  def register(listener: QueryExecutionListener): Unit = {
    listenerBusInitializationLock.synchronized {
      if (listenerBus.isEmpty) {
        listenerBus = Some(new ExecutionListenerBus(this, session))
      }
    }
    listenerBus.get.addListener(listener)
  }

  /**
   * Unregisters the specified [[QueryExecutionListener]].
   */
  @DeveloperApi
  def unregister(listener: QueryExecutionListener): Unit = {
    listenerBus.foreach(_.removeListener(listener))
  }

  /**
   * Removes all the registered [[QueryExecutionListener]].
   */
  @DeveloperApi
  def clear(): Unit = {
    listenerBus.foreach(_.removeAllListeners())
  }

  /** Only exposed for testing. */
  private[sql] def listListeners(): Array[QueryExecutionListener] = {
    listenerBus.map(_.listeners.asScala.toArray).getOrElse(Array.empty[QueryExecutionListener])
  }

  /**
   * Get an identical copy of this listener manager.
   */
  private[sql] def clone(session: SparkSession, sqlConf: SQLConf): ExecutionListenerManager = {
    val newListenerManager =
      new ExecutionListenerManager(session, sqlConf, loadExtensions = false)
    listenerBus.foreach(_.listeners.asScala.foreach(newListenerManager.register))
    newListenerManager
  }
}

private[sql] class ExecutionListenerBus private(sessionUUID: String)
  extends SparkListener with ListenerBus[QueryExecutionListener, SparkListenerSQLExecutionEnd] {

  def this(manager: ExecutionListenerManager, session: SparkSession) = {
    this(session.sessionUUID)
    session.sparkContext.listenerBus.addToSharedQueue(this)
    session.sparkContext.cleaner.foreach { cleaner =>
      cleaner.registerSparkListenerForCleanup(manager, this)
    }
  }

  override def onOtherEvent(event: SparkListenerEvent): Unit = event match {
    case e: SparkListenerSQLExecutionEnd => postToAll(e)
    case _ =>
  }

  override protected def doPostEvent(
      listener: QueryExecutionListener,
      event: SparkListenerSQLExecutionEnd): Unit = {
    if (shouldReport(event)) {
      val funcName = event.executionName.get
      event.executionFailure match {
        case Some(ex) =>
          val exception = ex match {
            case e: Exception => e
            case other: Throwable =>
              QueryExecutionErrors.failedToExecuteQueryError(other)
          }
          listener.onFailure(funcName, event.qe, exception)
        case _ =>
          listener.onSuccess(funcName, event.qe, event.duration)
      }
    }
  }

  private def shouldReport(e: SparkListenerSQLExecutionEnd): Boolean = {
    // Only catch SQL execution with a name, and triggered by the same spark session that this
    // listener manager belongs.
    e.executionName.isDefined && e.qe.sparkSession.sessionUUID == sessionUUID
  }
}
