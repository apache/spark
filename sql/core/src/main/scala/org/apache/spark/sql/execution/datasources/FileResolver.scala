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

package org.apache.spark.sql.execution.datasources

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.analysis.resolver.ResolverExtension
import org.apache.spark.sql.catalyst.plans.logical.{AnalysisHelper, LogicalPlan}

/**
 * The [[FileResolver]] is a [[MetadataResolver]] extension that resolves [[UnresolvedRelation]]
 * which is created out of file. It reuses the code from [[ResolveSQLOnFile]] to resolve it
 * properly.
 *
 * We have it as an extension to avoid cyclic dependencies between [[resolver]] and [[datasources]]
 * packages.
 */
class FileResolver(sparkSession: SparkSession) extends ResolverExtension {
  private val resolveSQLOnFile = new ResolveSQLOnFile(sparkSession)

  /**
   * [[ResolveSQLOnFile]] code that is reused to resolve [[UnresolvedRelation]] has
   * [[ExpressionEncoder.resolveAndBind]] on its path which introduces another call to
   * the analyzer which is acceptable as it is called on the leaf node of the plan. That's why we
   * have to allow invoking transforms in the single-pass analyzer.
   */
  object UnresolvedRelationResolution {
    def unapply(operator: LogicalPlan): Option[LogicalPlan] =
      AnalysisHelper.allowInvokingTransformsInAnalyzer {
        resolveSQLOnFile.UnresolvedRelationResolution.unapply(operator)
      }
  }

  /**
   * Reuse [[ResolveSQLOnFile]] code to resolve [[UnresolvedRelation]] made out of file.
   */
  override def resolveOperator: PartialFunction[LogicalPlan, LogicalPlan] = {
    case UnresolvedRelationResolution(resolvedRelation) =>
      resolvedRelation
  }
}
