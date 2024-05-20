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

package org.apache.spark.sql.catalyst.parser

import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan

sealed trait BatchPlanStatement

// Statement that is supposed to be executed against Spark.
// This can also be a Spark expression that is wrapped in a statement.
case class SparkStatementWithPlan(
    parsedPlan: LogicalPlan,
    sourceStart: Int,
    sourceEnd: Int)
  extends BatchPlanStatement {

  def getText(batch: String): String = batch.substring(sourceStart, sourceEnd)
}

case class BatchBody(collection: List[BatchPlanStatement]) extends BatchPlanStatement