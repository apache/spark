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

import org.apache.spark.annotation.Experimental
import org.apache.spark.sql.ContinuousQuery
import org.apache.spark.sql.util.ContinuousQueryListener._

/**
 * :: Experimental ::
 * Interface for listening to events related to [[ContinuousQuery ContinuousQueries]].
 * @note The methods are not thread-safe as they may be called from different threads.
 */
@Experimental
abstract class ContinuousQueryListener {

  /**
   * Called when a query is started.
   * @note This is called synchronously with
   *       [[org.apache.spark.sql.DataFrameWriter `DataFrameWriter.startStream()`]],
   *       that is, `onQueryStart` will be called on all listeners before
   *       `DataFrameWriter.startStream()` returns the corresponding [[ContinuousQuery]]. Please
   *       don't block this method as it will block your query.
   */
  def onQueryStarted(queryStarted: QueryStarted): Unit

  /**
   * Called when there is some status update (ingestion rate updated, etc.)
   *
   * @note This method is asynchronous. The status in [[ContinuousQuery]] will always be
   *       latest no matter when this method is called. Therefore, the status of [[ContinuousQuery]]
   *       may be changed before/when you process the event. E.g., you may find [[ContinuousQuery]]
   *       is terminated when you are processing [[QueryProgress]].
   */
  def onQueryProgress(queryProgress: QueryProgress): Unit

  /** Called when a query is stopped, with or without error */
  def onQueryTerminated(queryTerminated: QueryTerminated): Unit
}


/**
 * :: Experimental ::
 * Companion object of [[ContinuousQueryListener]] that defines the listener events.
 */
@Experimental
object ContinuousQueryListener {

  /** Base type of [[ContinuousQueryListener]] events */
  trait Event

  /** Event representing the start of a query */
  class QueryStarted private[sql](val query: ContinuousQuery) extends Event

  /** Event representing any progress updates in a query */
  class QueryProgress private[sql](val query: ContinuousQuery) extends Event

  /** Event representing that termination of a query */
  class QueryTerminated private[sql](val query: ContinuousQuery) extends Event
}
