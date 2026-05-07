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
import org.apache.spark.sql.pipelines.AnalysisWarning

/**
 * A context used when evaluating a `Flow`'s query into a concrete DataFrame.
 *
 * @param allInputs            Set of identifiers for all `Input`s defined in the DataflowGraph.
 * @param availableInputs      Inputs available to be referenced with `read` or `readStream`.
 * @param queryContext         The context of the query being evaluated.
 * @param requestedInputs      A mutable buffer populated with names of all inputs that were
 *                             requested.
 * @param spark                the spark session to be used.
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
    externalInputs: mutable.HashSet[TableIdentifier] = mutable.HashSet.empty
) {

  /** Map from `Input` name to the actual `Input` */
  val availableInput: Map[TableIdentifier, Input] =
    availableInputs.map(i => i.identifier -> i).toMap

  /** The confs set in this context that should be undone when exiting this context. */
  private val confsToRestore = mutable.HashMap[String, Option[String]]()

  /** Sets a Spark conf within this context that will be undone by `restoreOriginalConf`. */
  def setConf(key: String, value: String): Unit = {
    if (!confsToRestore.contains(key)) {
      confsToRestore.put(key, spark.conf.getOption(key))
    }
    spark.conf.set(key, value)
  }

  /** Restores the Spark conf to its state when this context was creating by undoing confs set. */
  def restoreOriginalConf(): Unit = confsToRestore.foreach {
    case (k, Some(v)) => spark.conf.set(k, v)
    case (k, None) => spark.conf.unset(k)
  }
}
