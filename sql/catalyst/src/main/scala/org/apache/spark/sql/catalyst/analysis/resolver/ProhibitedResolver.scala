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

import org.apache.spark.SparkException
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan

/**
 * This is a dummy [[LogicalPlanResolver]] whose [[resolve]] is not implemented and throws
 * [[SparkException]].
 *
 * It's used by the [[MetadataResolver]] to pass it as an argument to
 * [[tryDelegateResolutionToExtensions]], because unresolved subtree resolution doesn't make sense
 * during metadata resolution traversal.
 */
class ProhibitedResolver extends LogicalPlanResolver {
  def resolve(plan: LogicalPlan): LogicalPlan = {
    throw SparkException.internalError("Resolver cannot be used here")
  }
}
