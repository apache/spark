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

/**
 * These classes in this package are intentionally placed to the outer package of spark,
 * because IsolatedClientLoader leverages Spark classloader for shared classess including
 * spark package, and the test should fail if Spark initializes these listeners with
 * IsolatedClientLoader.
 */
package test.custom.listener

import org.apache.spark.sql.execution.QueryExecution
import org.apache.spark.sql.streaming.StreamingQueryListener
import org.apache.spark.sql.util.QueryExecutionListener

class DummyQueryExecutionListener extends QueryExecutionListener {
  override def onSuccess(funcName: String, qe: QueryExecution, durationNs: Long): Unit = {}
  override def onFailure(funcName: String, qe: QueryExecution, exception: Exception): Unit = {}
}

class DummyStreamingQueryListener extends StreamingQueryListener {
  override def onQueryStarted(event: StreamingQueryListener.QueryStartedEvent): Unit = {}
  override def onQueryProgress(event: StreamingQueryListener.QueryProgressEvent): Unit = {}
  override def onQueryTerminated(event: StreamingQueryListener.QueryTerminatedEvent): Unit = {}
}
