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
import org.apache.spark.sql.catalyst.trees.{CurrentOrigin, Origin, WithOrigin}

/**
 * Trait for all SQL Scripting logical operators that are product of parsing phase.
 * These operators will be used by the SQL Scripting interpreter to generate execution nodes.
 */
sealed trait CompoundPlanStatement

/**
 * Logical operator representing result of parsing a single SQL statement
 *   that is supposed to be executed against Spark.
 * @param parsedPlan Result of SQL statement parsing.
 */
case class SingleStatement(parsedPlan: LogicalPlan)
  extends CompoundPlanStatement
  with WithOrigin {

  override val origin: Origin = CurrentOrigin.get

  /**
   * Get the SQL query text corresponding to this statement.
   * @return
   *   SQL query text.
   */
  def getText: String = {
    assert(origin.sqlText.isDefined && origin.startIndex.isDefined && origin.stopIndex.isDefined)
    origin.sqlText.get.substring(origin.startIndex.get, origin.stopIndex.get + 1)
  }
}

/**
 * Logical operator for a compound body. Contains all statements within the compound body.
 * @param collection Collection of statements within the compound body.
 * @param label Label set to CompoundBody by user or UUID otherwise.
 *              It can be None in case when CompoundBody is not part of BeginEndCompoundBlock
 *              for example when CompoundBody is inside loop or conditional block.
 */
case class CompoundBody(
    collection: Seq[CompoundPlanStatement],
    label: Option[String]) extends CompoundPlanStatement
