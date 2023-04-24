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

import org.apache.spark.annotation.DeveloperApi
import org.apache.spark.connect.proto
import org.apache.spark.sql.catalyst.expressions.Attribute
import org.apache.spark.sql.catalyst.plans.logical.{FlatMapGroupsWithState, LogicalGroupState}
import org.apache.spark.sql.expressions.ScalarUserDefinedFunction
import org.apache.spark.sql.functions.col
import org.apache.spark.sql.streaming.{GroupState, GroupStateTimeout, OutputMode}

/**
 * A [[Dataset]] has been logically grouped by a user specified grouping key.  Users should not
 * construct a [[KeyValueGroupedDataset]] directly, but should instead call `groupByKey` on
 * an existing [[Dataset]].
 *
 * @since 2.0.0
 */
class KeyValueGroupedDataset[K, V] private[sql](
    kEncoder: Encoder[K],
    vEncoder: Encoder[V],
    val sparkSession: SparkSession,
    @DeveloperApi val plan: proto.Plan,
    private val dataAttributes: Seq[Attribute],
    private val groupingAttributes: Seq[Attribute]) extends Serializable {

  // Make sure we don't forget to set plan id.
  assert(plan.getRoot.getCommon.hasPlanId)

  // Similar to [[Dataset]], we turn the passed in encoder to `ExpressionEncoder` explicitly.
  private implicit val kExprEnc = encoderFor(kEncoder)
  private implicit val vExprEnc = encoderFor(vEncoder)

  /**
   * (Scala-specific)
   * Applies the given function to each group of data, while maintaining a user-defined per-group
   * state. The result Dataset will represent the objects returned by the function.
   * For a static batch Dataset, the function will be invoked once per group. For a streaming
   * Dataset, the function will be invoked for each group repeatedly in every trigger, and
   * updates to each group's state will be saved across invocations.
   * See [[org.apache.spark.sql.streaming.GroupState]] for more details.
   *
   * @tparam S The type of the user-defined state. Must be encodable to Spark SQL types.
   * @tparam U The type of the output objects. Must be encodable to Spark SQL types.
   * @param func Function to be called on every group.
   *
   * See [[Encoder]] for more details on what types are encodable to Spark SQL.
   * @since 2.2.0
   */
  def mapGroupsWithState[S: Encoder, U: Encoder](
      func: (K, Iterator[V], GroupState[S]) => U): Dataset[U] = {
    val flatMapFunc = (key: K, it: Iterator[V], s: GroupState[S]) => Iterator(func(key, it, s))
    val outputEncoder = encoderFor[U]
    val udf = ScalarUserDefinedFunction(
      function = flatMapFunc,
      inputEncoders = encoderFor[S] :: Nil,
      outputEncoder = outputEncoder)
    sparkSession.newDataset(outputEncoder) { builder =>
      builder.getFlatMapGroupsWithStateBuilder
        .setInput(plan.getRoot)
        .setFunc(udf.apply(col("*")).expr.getCommonInlineUserDefinedFunction)
        .setIsMapGroupsWithState(true)
    }
  }
}
