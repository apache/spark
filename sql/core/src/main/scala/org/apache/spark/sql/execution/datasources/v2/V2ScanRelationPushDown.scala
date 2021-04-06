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
package org.apache.spark.sql.execution.datasources.v2

import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan
import org.apache.spark.sql.catalyst.rules.Rule
import org.apache.spark.sql.connector.read.{Scan, V1Scan}
import org.apache.spark.sql.execution.datasources.v2.pushdown.PushQuery
import org.apache.spark.sql.sources
import org.apache.spark.sql.types.StructType

object V2ScanRelationPushDown extends Rule[LogicalPlan] {
  override def apply(plan: LogicalPlan): LogicalPlan = plan transform {
    case PushQuery(pushed: PushQuery) =>
      pushed.push()
  }
}

// A wrapper for v1 scan to carry the translated filters and the handled ones. This is required by
// the physical v1 scan node.
case class V1ScanWrapper(
    v1Scan: V1Scan,
    translatedFilters: Seq[sources.Filter],
    handledFilters: Seq[sources.Filter]) extends Scan {
  override def readSchema(): StructType = v1Scan.readSchema()
}
