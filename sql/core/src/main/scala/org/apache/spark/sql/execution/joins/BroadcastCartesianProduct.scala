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

package org.apache.spark.sql.execution.joins

import scala.concurrent._
import scala.concurrent.duration._

import org.apache.spark.annotation.DeveloperApi
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.{Attribute, JoinedRow}
import org.apache.spark.sql.execution.{BinaryNode, SparkPlan}
import org.apache.spark.util.ThreadUtils

/**
 * :: DeveloperApi ::
 */
@DeveloperApi
case class BroadcastCartesianProduct(
    left: SparkPlan,
    right: SparkPlan,
    buildSide: BuildSide) extends BinaryNode {
  override def output: Seq[Attribute] = left.output ++ right.output

  private val (streamed, broadcast) = buildSide match {
    case BuildRight => (left, right)
    case BuildLeft => (right, left)
  }

  private val timeout: Duration = {
    val timeoutValue = sqlContext.conf.broadcastTimeout
    if (timeoutValue < 0) {
      Duration.Inf
    } else {
      timeoutValue.seconds
    }
  }

  @transient
  private val broadcastFuture = future {
    val input = broadcast.execute().map(_.copy()).collect()
    sparkContext.broadcast(input)
  }(BroadcastCartesianProduct.broadcastCartesianProductExecutionContext)

  protected override def doExecute(): RDD[InternalRow] = {
    val leftResults = streamed.execute().map(_.copy())
    val rightResults = Await.result(broadcastFuture, timeout)

    leftResults.mapPartitions { streamedIter =>
      for (x <- streamedIter; y <- rightResults.value)
        yield {
          val joinedRow = new JoinedRow
          buildSide match {
            case BuildRight => joinedRow(x, y)
            case BuildLeft => joinedRow(y, x)
          }
        }
    }
  }
}

object BroadcastCartesianProduct {
  private val broadcastCartesianProductExecutionContext = ExecutionContext.fromExecutorService(
    ThreadUtils.newDaemonCachedThreadPool("broadcast-cartesian-product", 128))
}
