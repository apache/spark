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
package org.apache.spark.sql.execution

import scala.concurrent._
import scala.concurrent.duration._

import org.apache.spark.broadcast
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.Attribute
import org.apache.spark.sql.catalyst.plans.physical.BroadcastMode
import org.apache.spark.util.ThreadUtils

/**
 * A broadcast collects, transforms and finally broadcasts the result of a transformed SparkPlan.
 */
case class Broadcast(
    mode: BroadcastMode,
    child: SparkPlan) extends UnaryNode {

  override def output: Seq[Attribute] = child.output

  val timeout: Duration = {
    val timeoutValue = sqlContext.conf.broadcastTimeout
    if (timeoutValue < 0) {
      Duration.Inf
    } else {
      timeoutValue.seconds
    }
  }

  @transient
  private lazy val relationFuture: Future[broadcast.Broadcast[Any]] = {
    // broadcastFuture is used in "doExecute". Therefore we can get the execution id correctly here.
    val executionId = sparkContext.getLocalProperty(SQLExecution.EXECUTION_ID_KEY)
    Future {
      // This will run in another thread. Set the execution id so that we can connect these jobs
      // with the correct execution.
      SQLExecution.withExecutionId(sparkContext, executionId) {
        // Note that we use .execute().collect() because we don't want to convert data to Scala
        // types
        val input: Array[InternalRow] = child.execute().map { row =>
          row.copy()
        }.collect()

        // Construct and broadcast the relation.
        sparkContext.broadcast(mode(input))
      }
    }(Broadcast.executionContext)
  }

  override protected def doPrepare(): Unit = {
    // Materialize the future.
    relationFuture
  }

  override protected def doExecute(): RDD[InternalRow] = {
    child.execute() // TODO throw an Exception here?
  }

  override protected[sql] def doExecuteBroadcast[T](): broadcast.Broadcast[T] = {
    val result = Await.result(relationFuture, timeout)
    result.asInstanceOf[broadcast.Broadcast[T]]
  }
}

object Broadcast {
  private[execution] val executionContext = ExecutionContext.fromExecutorService(
    ThreadUtils.newDaemonCachedThreadPool("broadcast", 128))
}
