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

package org.apache.spark.sql.catalyst.analysis

import org.apache.spark.sql.catalyst.plans.logical.{LogicalPlan, View}
import org.apache.spark.sql.errors.QueryCompilationErrors
import org.apache.spark.sql.internal.SQLConf

object ViewResolution {
  def resolve(
      view: View,
      resolveChild: LogicalPlan => LogicalPlan,
      checkAnalysis: LogicalPlan => Unit): View = {
    // The view's child should be a logical plan parsed from the `desc.viewText`, the variable
    // `viewText` should be defined, or else we throw an error on the generation of the View
    // operator.

    // Resolve all the UnresolvedRelations and Views in the child.
    val newChild = AnalysisContext.withAnalysisContext(view.desc) {
      val nestedViewDepth = AnalysisContext.get.nestedViewDepth
      val maxNestedViewDepth = AnalysisContext.get.maxNestedViewDepth
      if (nestedViewDepth > maxNestedViewDepth) {
        throw QueryCompilationErrors.viewDepthExceedsMaxResolutionDepthError(
          view.desc.identifier,
          maxNestedViewDepth,
          view
        )
      }
      SQLConf.withExistingConf(View.effectiveSQLConf(view.desc.viewSQLConfigs, view.isTempView)) {
        resolveChild(view.child)
      }
    }

    // Fail the analysis eagerly because outside AnalysisContext, the unresolved operators
    // inside a view maybe resolved incorrectly.
    checkAnalysis(newChild)

    view.copy(child = newChild)
  }
}
