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

package org.apache.spark.sql.catalyst.plans.logical

import org.apache.spark.sql.catalyst.expressions.{AttributeReference, Attribute}
import org.apache.spark.sql.types.StringType

/**
 * A logical node that represents a non-query command to be executed by the system.  For example,
 * commands can be used by parsers to represent DDL operations.  Commands, unlike queries, are
 * eagerly executed.
 */
trait Command

/**
 * Returned for the "DESCRIBE [EXTENDED] FUNCTION functionName" command.
 * @param functionName The function to be described.
 * @param isExtended True if "DESCRIBE EXTENDED" is used. Otherwise, false.
 */
private[sql] case class DescribeFunction(
    functionName: String,
    isExtended: Boolean) extends LogicalPlan with Command {

  override def children: Seq[LogicalPlan] = Seq.empty
  override val output: Seq[Attribute] = Seq(
    AttributeReference("function_desc", StringType, nullable = false)())
}

/**
 * Returned for the "SHOW FUNCTIONS" command, which will list all of the
 * registered function list.
 */
private[sql] case class ShowFunctions(
    db: Option[String], pattern: Option[String]) extends LogicalPlan with Command {
  override def children: Seq[LogicalPlan] = Seq.empty
  override val output: Seq[Attribute] = Seq(
    AttributeReference("function", StringType, nullable = false)())
}
