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

package org.apache.spark.sql.pipelines.util

import scala.util.Success

import org.apache.spark.sql.{QueryTest, Row}
import org.apache.spark.sql.catalyst.TableIdentifier
import org.apache.spark.sql.pipelines.graph.{
  FlowFunction,
  FlowFunctionResult,
  Input,
  QueryContext,
  QueryOrigin,
  ResolvedFlow,
  StreamingFlow,
  UntypedFlow
}
import org.apache.spark.sql.test.SharedSparkSession
import org.apache.spark.sql.types.{IntegerType, StringType, StructType}

/** Tests for the flow ordering used by [[SchemaInferenceUtils.inferSchemaFromFlows]]. */
class InferSchemaFromFlowsSuite extends QueryTest with SharedSparkSession {

  /** A [[FlowFunction]] that throws if invoked; these tests build already-resolved flows. */
  private val noOpFlowFunction: FlowFunction = new FlowFunction {
    override def call(
        allInputs: Set[TableIdentifier],
        availableInputs: Seq[Input],
        configuration: Map[String, String],
        queryContext: QueryContext,
        queryOrigin: QueryOrigin): FlowFunctionResult =
      throw new UnsupportedOperationException(
        "noOpFlowFunction.call should not be invoked from InferSchemaFromFlowsSuite tests")
  }

  private val queryContext = QueryContext(currentCatalog = Some("c"), currentDatabase = Some("d"))

  /** A resolved flow with the given identifier and output schema, writing to `destination`. */
  private def resolvedFlow(
      identifier: TableIdentifier,
      destination: TableIdentifier,
      schema: StructType): ResolvedFlow = {
    val df = spark.createDataFrame(spark.sparkContext.emptyRDD[Row], schema)
    val flow = UntypedFlow(
      identifier = identifier,
      destinationIdentifier = destination,
      func = noOpFlowFunction,
      queryContext = queryContext,
      sqlConf = Map.empty,
      once = false,
      origin = QueryOrigin.empty)
    new StreamingFlow(
      flow,
      FlowFunctionResult(
        requestedInputs = Set.empty,
        batchInputs = Set.empty,
        streamingInputs = Set.empty,
        usedExternalInputs = Set.empty,
        dataFrame = Success(df),
        sqlConf = Map.empty))
  }

  test("inferSchemaFromFlows merges deterministically for identifiers that collide under " +
    "unquotedString") {
    // The merge order decides which spelling of a case-only-differing column survives, so it has to
    // be deterministic and independent of the incoming flow order (which is the nondeterministic
    // flow-resolution completion order). inferSchemaFromFlows sorts on the identifier's
    // `quotedString`. Sorting on `unquotedString` would collapse these two DISTINCT identifiers to
    // the same key ("c.a.b.x"), and a stable sort would then fall back to the incoming order --
    // flipping the surviving column casing when the flows happen to arrive swapped.
    val destination = TableIdentifier("t", Some("d"), Some("c"))
    // Both identifiers render to "c.a.b.x" under unquotedString, but differ under quotedString.
    // The first character where the two quoted strings differ is '.' (in flowLow) vs '`' (in
    // flowHigh), and '.' < '`', so flowLow sorts first:
    //   flowLow  -> `c`.`a.b`.`x`
    //   flowHigh -> `c`.`a`.`b.x`
    val flowLow = resolvedFlow(
      identifier = TableIdentifier("x", Some("a.b"), Some("c")),
      destination = destination,
      schema = new StructType().add("id", IntegerType).add("value", StringType))
    val flowHigh = resolvedFlow(
      identifier = TableIdentifier("b.x", Some("a"), Some("c")),
      destination = destination,
      schema = new StructType().add("id", IntegerType).add("Value", StringType))

    // flowLow has the lower `quotedString`, so its spelling ("value") wins the case-insensitive
    // fold regardless of the order the flows are passed in.
    val expected = new StructType().add("id", IntegerType).add("value", StringType)
    Seq(Seq(flowLow, flowHigh), Seq(flowHigh, flowLow)).foreach { flows =>
      val inferred = SchemaInferenceUtils.inferSchemaFromFlows(
        tableIdentifier = destination,
        flows = flows,
        userSpecifiedSchema = None,
        sessionCaseSensitive = false)
      assert(inferred === expected, s"unexpected schema for input order $flows")
    }
  }
}
