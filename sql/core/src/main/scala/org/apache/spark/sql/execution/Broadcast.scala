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
import org.apache.spark.rdd.{EmptyRDD, RDD}
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.Attribute
import org.apache.spark.sql.catalyst.expressions.codegen.CodegenContext
import org.apache.spark.sql.execution.metric.SQLMetrics
import org.apache.spark.util.ThreadUtils

/**
 * A broadcaster collects transforms and broadcasts the result of an underlying spark plan.
 *
 * TODO whole stage codegen.
 */
case class Broadcast(
    f: Iterable[InternalRow] => Any,
    child: SparkPlan)
  extends UnaryNode with CodegenSupport {

  override def output: Seq[Attribute] = child.output

  override private[sql] lazy val metrics = Map(
    "numRows" -> SQLMetrics.createLongMetric(sparkContext, "number of rows")
  )

  val timeout: Duration = {
    val timeoutValue = sqlContext.conf.broadcastTimeout
    if (timeoutValue < 0) {
      Duration.Inf
    } else {
      timeoutValue.seconds
    }
  }

  @transient
  private lazy val relation: broadcast.Broadcast[Any] = {
    val numBuildRows = longMetric("numRows")

    // broadcastFuture is used in "doExecute". Therefore we can get the execution id correctly here.
    val executionId = sparkContext.getLocalProperty(SQLExecution.EXECUTION_ID_KEY)
    val future = Future {
      // This will run in another thread. Set the execution id so that we can connect these jobs
      // with the correct execution.
      SQLExecution.withExecutionId(sparkContext, executionId) {
        // Note that we use .execute().collect() because we don't want to convert data to Scala
        // types
        val input: Array[InternalRow] = child.execute().map { row =>
          numBuildRows += 1
          row.copy()
        }.collect()

        // Construct and broadcast the relation.
        sparkContext.broadcast(f(input))
      }
    }(Broadcast.executionContext)
    Await.result(future, timeout)
  }

  override def upstream(): RDD[InternalRow] = {
    child.asInstanceOf[CodegenSupport].upstream()
  }

  override def doProduce(ctx: CodegenContext): String = ""

  override protected def doPrepare(): Unit = {
    // Materialize the relation.
    relation
  }

  override protected def doExecute(): RDD[InternalRow] = {
    // Return an empty RDD.
    // TODO this might violate the principle of least surprise.
    new EmptyRDD[InternalRow](sparkContext)
  }

  /** Get the constructed relation. */
  def broadcastRelation[T]: broadcast.Broadcast[T] = relation.asInstanceOf[broadcast.Broadcast[T]]
}

object Broadcast {
  def broadcastRelation[T](plan: SparkPlan): broadcast.Broadcast[T] = plan match {
    case builder: Broadcast => builder.broadcastRelation
    case _ => sys.error("The given plan is not a Broadcast")
  }

  private[execution] val executionContext = ExecutionContext.fromExecutorService(
    ThreadUtils.newDaemonCachedThreadPool("build-broadcast", 128))
}
