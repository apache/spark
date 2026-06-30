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

package org.apache.spark.sql.pipelines.graph

import scala.collection.mutable
import scala.collection.mutable.ListBuffer

import org.apache.spark.sql.catalyst.TableIdentifier
import org.apache.spark.sql.classic.SparkSession
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.pipelines.AnalysisWarning

/**
 * A context used when evaluating a `Flow`'s query into a concrete DataFrame.
 *
 * @param allInputs            Set of identifiers for all `Input`s defined in the DataflowGraph.
 * @param availableInputs      Inputs available to be referenced with `read` or `readStream`.
 * @param queryContext         The context of the query being evaluated.
 * @param requestedInputs      A mutable buffer populated with names of all inputs that were
 *                             requested.
 * @param spark                The (shared) spark session to be used.
 * @param flowConf             A private [[SQLConf]] holding this flow's per-flow confs. It is
 *                             installed for the analyzing thread via `SQLConf.withExistingConf`
 *                             (see `FlowAnalysis.createFlowFunctionFromLogicalPlan`) so per-flow
 *                             confs stay isolated from concurrently resolving flows and from the
 *                             shared session, without cloning the session.
 * @param externalInputs The names of external inputs that were used to evaluate
 *                                 the flow's query.
 */
private[pipelines] case class FlowAnalysisContext(
    allInputs: Set[TableIdentifier],
    availableInputs: Seq[Input],
    queryContext: QueryContext,
    batchInputs: mutable.HashSet[ResolvedInput] = mutable.HashSet.empty,
    streamingInputs: mutable.HashSet[ResolvedInput] = mutable.HashSet.empty,
    requestedInputs: mutable.HashSet[TableIdentifier] = mutable.HashSet.empty,
    shouldLowerCaseNames: Boolean = false,
    analysisWarnings: mutable.Buffer[AnalysisWarning] = new ListBuffer[AnalysisWarning],
    spark: SparkSession,
    flowConf: SQLConf,
    externalInputs: mutable.HashSet[TableIdentifier] = mutable.HashSet.empty
) {

  /** Map from `Input` name to the actual `Input` */
  val availableInput: Map[TableIdentifier, Input] =
    availableInputs.map(i => i.identifier -> i).toMap

  /**
   * Sets a Spark conf for this flow's analysis. It is set on the per-flow [[flowConf]], which is
   * active for the analyzing thread only, so it does not leak to other flows or to the shared
   * session.
   */
  def setConf(key: String, value: String): Unit = {
    flowConf.setConfString(key, value)
  }
}
