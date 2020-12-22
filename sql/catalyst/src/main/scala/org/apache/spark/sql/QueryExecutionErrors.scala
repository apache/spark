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

package org.apache.spark.sql.errors

import org.apache.spark.sql.catalyst.analysis.UnresolvedGenerator
import org.apache.spark.sql.catalyst.plans.logical.{LeafNode, LogicalPlan}

/**
 * Object for grouping all error messages of the query runtime.
 * Currently it includes all SparkExceptions and RuntimeExceptions(e.g.
 * UnsupportedOperationException, IllegalStateException).
 */
object QueryExecutionErrors {

  def columnChangeUnsupportedError(): Throwable = {
    new UnsupportedOperationException("Please add an implementation for a column change here")
  }

  def unexpectedPlanReturnError(plan: LogicalPlan, methodName: String): Throwable = {
    new IllegalStateException(s"[BUG] unexpected plan returned by `$methodName`: $plan")
  }

  def logicalHintOperatorNotRemovedDuringAnalysisError(): Throwable = {
    new IllegalStateException(
      "Internal error: logical hint operator should have been removed during analysis")
  }

  def logicalPlanHaveOutputOfCharOrVarcharError(leaf: LeafNode): Throwable = {
    new IllegalStateException(
      s"[BUG] logical plan should not have output of char/varchar type: $leaf")
  }

  def cannotEvaluateGeneratorError(generator: UnresolvedGenerator): Throwable = {
    new UnsupportedOperationException(s"Cannot evaluate expression: $generator")
  }

  def cannotGenerateCodeForGeneratorError(generator: UnresolvedGenerator): Throwable = {
    new UnsupportedOperationException(s"Cannot generate code for expression: $generator")
  }

  def cannotTerminateGeneratorError(generator: UnresolvedGenerator): Throwable = {
    new UnsupportedOperationException(s"Cannot terminate expression: $generator")
  }
}
