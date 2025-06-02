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

import org.apache.spark.SparkException
import org.apache.spark.sql.AnalysisException
import org.apache.spark.sql.catalyst.TableIdentifier

/**
 * Exception raised when a flow tries to read from a dataset that exists but is unresolved
 *
 * @param identifier The identifier of the dataset
 */
case class UnresolvedDatasetException(identifier: TableIdentifier)
    extends AnalysisException(
      s"Failed to read dataset '${identifier.unquotedString}'. Dataset is defined in the " +
      s"pipeline but could not be resolved."
    )

/**
 * Exception raised when a flow fails to read from a table defined within the pipeline
 *
 * @param name The name of the table
 * @param cause The cause of the failure
 */
case class LoadTableException(name: String, cause: Option[Throwable])
    extends SparkException(
      errorClass = "INTERNAL_ERROR",
      messageParameters = Map("message" -> s"Failed to load table '$name'"),
      cause = cause.orNull
    )

/**
 * Exception raised when a pipeline has one or more flows that cannot be resolved
 *
 * @param directFailures     Mapping between the name of flows that failed to resolve (due to an
 *                           error in that flow) and the error that occurred when attempting to
 *                           resolve them
 * @param downstreamFailures Mapping between the name of flows that failed to resolve (because they
 *                           failed to read from other unresolved flows) and the error that occurred
 *                           when attempting to resolve them
 */
case class UnresolvedPipelineException(
    graph: DataflowGraph,
    directFailures: Map[TableIdentifier, Throwable],
    downstreamFailures: Map[TableIdentifier, Throwable],
    additionalHint: Option[String] = None)
    extends AnalysisException(
      s"""
       |Failed to resolve flows in the pipeline.
       |
       |A flow can fail to resolve because the flow itself contains errors or because it reads
       |from an upstream flow which failed to resolve.
       |${additionalHint.getOrElse("")}
       |Flows with errors: ${directFailures.keys.map(_.unquotedString).toSeq.sorted.mkString(", ")}
       |Flows that failed due to upstream errors: ${downstreamFailures.keys
           .map(_.unquotedString)
           .toSeq
           .sorted
           .mkString(", ")}
       |
       |To view the exceptions that were raised while resolving these flows, look for flow
       |failures that precede this log.""".stripMargin
    )

/**
 * Raised when there's a circular dependency in the current pipeline. That is, a downstream
 * table is referenced while creating a upstream table.
 */
case class CircularDependencyException(
    downstreamTable: TableIdentifier,
    upstreamDataset: TableIdentifier)
    extends AnalysisException(
      s"The downstream table '${downstreamTable.unquotedString}' is referenced when " +
      s"creating the upstream table or view '${upstreamDataset.unquotedString}'. " +
      s"Circular dependencies are not supported in a pipeline. Please remove the dependency " +
      s"between '${upstreamDataset.unquotedString}' and '${downstreamTable.unquotedString}'."
    )
