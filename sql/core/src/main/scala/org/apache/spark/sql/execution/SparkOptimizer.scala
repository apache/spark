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

import org.apache.spark.sql.ExperimentalMethods
import org.apache.spark.sql.catalyst.catalog.SessionCatalog
import org.apache.spark.sql.catalyst.optimizer.Optimizer
import org.apache.spark.sql.execution.datasources.PruneFileSourcePartitions
import org.apache.spark.sql.execution.datasources.SchemaPruning
import org.apache.spark.sql.execution.python.{ExtractPythonUDFFromAggregate, ExtractPythonUDFs}

class SparkOptimizer(
    catalog: SessionCatalog,
    experimentalMethods: ExperimentalMethods)
  extends Optimizer(catalog) {

  override def defaultBatches: Seq[Batch] = (preOptimizationBatches ++ super.defaultBatches :+
    Batch("Optimize Metadata Only Query", Once, OptimizeMetadataOnlyQuery(catalog)) :+
    Batch("Extract Python UDFs", Once,
      Seq(ExtractPythonUDFFromAggregate, ExtractPythonUDFs): _*) :+
    Batch("Prune File Source Table Partitions", Once, PruneFileSourcePartitions) :+
    Batch("Schema Pruning", Once, SchemaPruning)) ++
    postHocOptimizationBatches :+
    Batch("User Provided Optimizers", fixedPoint, experimentalMethods.extraOptimizations: _*)

  override def nonExcludableRules: Seq[String] =
    super.nonExcludableRules :+ ExtractPythonUDFFromAggregate.ruleName

  /**
   * Optimization batches that are executed before the regular optimization batches (also before
   * the finish analysis batch).
   */
  def preOptimizationBatches: Seq[Batch] = Nil

  /**
   * Optimization batches that are executed after the regular optimization batches, but before the
   * batch executing the [[ExperimentalMethods]] optimizer rules. This hook can be used to add
   * custom optimizer batches to the Spark optimizer.
   */
   def postHocOptimizationBatches: Seq[Batch] = Nil
}
