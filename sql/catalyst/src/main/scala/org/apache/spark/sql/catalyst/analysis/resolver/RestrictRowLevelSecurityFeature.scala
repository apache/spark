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

package org.apache.spark.sql.catalyst.analysis.resolver

import com.databricks.sql.catalyst.plans.logical.SupportsRowColumnControlLogicalPlanning

import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan

/**
 * [[RestrictRowLevelSecurityFeature]] is used to detect RLS/CM features on logical plan relations
 * and to throw [[ExplicitlyUnsupportedResolverFeature]] is those are found.
 */
object RestrictRowLevelSecurityFeature {
  def apply(operator: LogicalPlan): Unit = {
    operator match {
      case supportsRlsCm: SupportsRowColumnControlLogicalPlanning
          if (supportsRlsCm.rowColumnControlProperties
            .exists(_.governanceMetadata.hasEffectivePolicies)) =>
        throw new ExplicitlyUnsupportedResolverFeature("RowColumnLevelAccessControlProperties")
      case _ =>
    }
  }
}
