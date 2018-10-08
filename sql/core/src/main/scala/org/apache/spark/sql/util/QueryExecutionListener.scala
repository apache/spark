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

import java.util.concurrent.CopyOnWriteArrayList

import scala.collection.JavaConverters._

import org.apache.spark.annotation.{DeveloperApi, Experimental, InterfaceStability}
import org.apache.spark.internal.Logging
import org.apache.spark.scheduler.{SparkListener, SparkListenerEvent}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.execution.QueryExecution
import org.apache.spark.sql.execution.ui.SparkListenerSQLExecutionEnd
import org.apache.spark.sql.internal.StaticSQLConf._
import org.apache.spark.util.Utils

/**
 * :: Experimental ::
 * The interface of query execution listener that can be used to analyze execution metrics.
 *
 * @note Implementations should guarantee thread-safety as they can be invoked by
 * multiple different threads.
 */
@Experimental
@InterfaceStability.Evolving
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
   * @param exception the exception that failed this query.
   *
   * @note This can be invoked by multiple different threads.
   */
  @DeveloperApi
  def onFailure(funcName: String, qe: QueryExecution, exception: Exception): Unit
}


/**
 * :: Experimental ::
 *
 * Manager for [[QueryExecutionListener]]. See `org.apache.spark.sql.SQLContext.listenerManager`.
 */
@Experimental
@InterfaceStability.Evolving
class ExecutionListenerManager private[sql](session: SparkSession, loadExtensions: Boolean)
  extends SparkListener with Logging {

  private[this] val listeners = new CopyOnWriteArrayList[QueryExecutionListener]

  if (loadExtensions) {
    val conf = session.sparkContext.conf
    conf.get(QUERY_EXECUTION_LISTENERS).foreach { classNames =>
      Utils.loadExtensions(classOf[QueryExecutionListener], classNames, conf).foreach(register)
    }
  }

  session.sparkContext.listenerBus.addToSharedQueue(this)

  /**
   * Registers the specified [[QueryExecutionListener]].
   */
  @DeveloperApi
  def register(listener: QueryExecutionListener): Unit = {
    listeners.add(listener)
  }

  /**
   * Unregisters the specified [[QueryExecutionListener]].
   */
  @DeveloperApi
  def unregister(listener: QueryExecutionListener): Unit = {
    listeners.remove(listener)
  }

  /**
   * Removes all the registered [[QueryExecutionListener]].
   */
  @DeveloperApi
  def clear(): Unit = {
    listeners.clear()
  }

  /**
   * Get an identical copy of this listener manager.
   */
  private[sql] def clone(session: SparkSession): ExecutionListenerManager = {
    val newListenerManager = new ExecutionListenerManager(session, loadExtensions = false)
    listeners.iterator().asScala.foreach(newListenerManager.register)
    newListenerManager
  }

  override def onOtherEvent(event: SparkListenerEvent): Unit = event match {
    case e: SparkListenerSQLExecutionEnd if shouldCatchEvent(e) =>
      val funcName = e.executionName.get
      e.executionFailure match {
        case Some(ex) =>
          listeners.iterator().asScala.foreach(_.onFailure(funcName, e.qe, ex))
        case _ =>
          listeners.iterator().asScala.foreach(_.onSuccess(funcName, e.qe, e.duration))
      }

    case _ => // Ignore
  }

  private def shouldCatchEvent(e: SparkListenerSQLExecutionEnd): Boolean = {
    // Only catch SQL execution with a name, and triggered by the same spark session that this
    // listener manager belongs.
    e.executionName.isDefined && e.qe.sparkSession.eq(this.session)
  }
}
