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

package org.apache.spark.sql.execution.joins

import org.apache.spark.sql.catalyst.plans.logical.{LogicalPlan, PreparedExternalLookupJoin}
import org.apache.spark.sql.execution.{SparkPlan, SparkStrategy}

/** Plans resolved external lookup joins. */
object ExternalLookupJoinStrategy extends SparkStrategy {
  override def apply(plan: LogicalPlan): Seq[SparkPlan] = plan match {
    case lookup: PreparedExternalLookupJoin =>
      ExternalLookupJoinExec(
        lookup.tableName,
        lookup.lookupParameters,
        lookup.lookupInput,
        lookup.lookupOutput,
        lookup.nullSafeParameters,
        lookup.joinType,
        lookup.residualCondition,
        lookup.scan,
        lookup.readerFactory,
        lookup.maxParameterRowsPerRequest,
        planLater(lookup.child)) :: Nil
    case _ => Nil
  }
}
