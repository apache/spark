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

import org.apache.spark.sql.catalyst.analysis.RelationTimeTravel
import org.apache.spark.sql.catalyst.expressions.Expression
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan

/**
 * Result of parsing a table name that may carry an '@' time travel suffix:
 * `name@v<version>` reads a specific version of the table, and `name@<yyyyMMddHHmmssSSS>`
 * reads the table as of a timestamp. `timestamp` is an already-converted TimestampType
 * literal (the digits are interpreted in the session time zone at parse time). At most one
 * of `timestamp` and `version` is set.
 */
case class TemporalIdentifier(
    nameParts: Seq[String],
    timestamp: Option[Expression],
    version: Option[String]) {

  def isTemporal: Boolean = timestamp.isDefined || version.isDefined

  /** Wraps `plan` in a [[RelationTimeTravel]] if a time travel suffix was specified. */
  def wrapTimeTravel(plan: LogicalPlan): LogicalPlan = {
    if (isTemporal) RelationTimeTravel(plan, timestamp, version) else plan
  }
}
