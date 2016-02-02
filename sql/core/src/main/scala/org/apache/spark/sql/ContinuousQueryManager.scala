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

package org.apache.spark.sql

import scala.collection.mutable

import org.apache.spark.annotation.Experimental
import org.apache.spark.sql.execution.streaming.{Sink, StreamExecution}

/**
 * :: Experimental ::
 * A class to manage all the [[org.apache.spark.sql.ContinuousQuery ContinuousQueries]] active
 * on a [[SQLContext]].
 */
@Experimental
class ContinuousQueryManager {

  private val activeQueries = new mutable.HashMap[String, ContinuousQuery]
  private val activeQueriesLock = new Object
  private val awaitTerminationLock = new Object

  @volatile
  private var lastTerminatedQuery: ContinuousQuery = null

  /** Returns a list of active queries associated with this SQLContext */
  def active: Array[ContinuousQuery] = activeQueriesLock.synchronized {
    activeQueries.values.toArray
  }

  /** Returns an active query from this SQLContext or throws exception if bad name */
  def get(name: String): ContinuousQuery = activeQueriesLock.synchronized {
    activeQueries.get(name).getOrElse {
      throw new IllegalArgumentException(s"There is no active query with name $name")
    }
  }

  /**
   * Wait until any of the queries on this SQLContext is terminated, with or without
   * exceptions. Returns the query that has been terminated.
   */
  def awaitAnyTermination(): ContinuousQuery = {
    awaitTerminationLock.synchronized {
      lastTerminatedQuery = null
      while (lastTerminatedQuery == null) {
        awaitTerminationLock.wait(10)
      }
      lastTerminatedQuery
    }
  }

  /**
   * Wait until any of the queries on this SQLContext is terminated.
   * Returns the stopped query if any query was terminated.
   */
  def awaitAnyTermination(timeoutMs: Long): Option[ContinuousQuery] = {
    val endTime = System.currentTimeMillis + timeoutMs
    def timeLeft = math.max(endTime - System.currentTimeMillis, 0)

    awaitTerminationLock.synchronized {
      lastTerminatedQuery = null
      while (timeLeft > 0 && lastTerminatedQuery == null) {
        awaitTerminationLock.wait(10)
      }
      Option(lastTerminatedQuery)
    }
  }

  private[sql] def startQuery(name: String, df: DataFrame, sink: Sink): ContinuousQuery = {
    activeQueriesLock.synchronized {
      if(active.contains(name)) {
        throw new IllegalArgumentException(
          s"Cannot start query with name $name as a query with that name is already active")
      }
      val query = new StreamExecution(df.sqlContext, name, df.logicalPlan, sink)
      activeQueries.put(name, query)
      query
    }
  }

  private[sql] def notifyQueryTermination(terminatedQuery: ContinuousQuery): Unit = {
    activeQueriesLock.synchronized {
      activeQueries -= terminatedQuery.name
      awaitTerminationLock.synchronized {
        lastTerminatedQuery = terminatedQuery
        awaitTerminationLock.notifyAll()
      }
    }
  }
}
