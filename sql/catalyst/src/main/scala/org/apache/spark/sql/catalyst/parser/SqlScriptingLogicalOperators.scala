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

import scala.collection.mutable

import org.apache.spark.sql.catalyst.parser.HandlerType.HandlerType
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
 * @param handlers Collection of handlers defined in the compound body.
 * @param conditions Map of Condition Name - Sql State values declared in the compound body.
 */
case class CompoundBody(
    collection: Seq[CompoundPlanStatement],
    label: Option[String] = None,
    handlers: Seq[ErrorHandler] = Seq.empty,
    conditions: mutable.HashMap[String, String] = mutable.HashMap()) extends CompoundPlanStatement

/**
 * Logical operator for an error condition.
 * @param conditionName Name of the error condition.
 * @param value SQLSTATE or Error Code.
 */
case class ErrorCondition(
    conditionName: String,
    value: String) extends CompoundPlanStatement

object HandlerType extends Enumeration {
    type HandlerType = Value
    val EXIT, CONTINUE = Value
}

/**
 * Logical operator for an error condition.
 * @param conditions Name of the error condition variable for which the handler is built.
 * @param body CompoundBody of the handler.
 * @param handlerType Type of the handler (CONTINUE or EXIT).
 */
case class ErrorHandler(
    conditions: Seq[String],
    body: CompoundBody,
    handlerType: HandlerType) extends CompoundPlanStatement

/**
 * Logical operator for IF ELSE statement.
 * @param conditions Collection of conditions. First condition corresponds to IF clause,
 *                   while others (if any) correspond to following ELSE IF clauses.
 * @param conditionalBodies Collection of bodies that have a corresponding condition,
 *                          in IF or ELSE IF branches.
 * @param elseBody Body that is executed if none of the conditions are met,
 *                          i.e. ELSE branch.
 */
case class IfElseStatement(
    conditions: Seq[SingleStatement],
    conditionalBodies: Seq[CompoundBody],
    elseBody: Option[CompoundBody]) extends CompoundPlanStatement {
  assert(conditions.length == conditionalBodies.length)
}

/**
 * Logical operator for while statement.
 * @param condition Any expression evaluating to a Boolean.
 *                 Body is executed as long as the condition evaluates to true
 * @param body Compound body is a collection of statements that are executed if condition is true.
 * @param label An optional label for the loop which is unique amongst all labels for statements
 *              within which the LOOP statement is contained.
 *              If an end label is specified it must match the beginning label.
 *              The label can be used to LEAVE or ITERATE the loop.
 */
case class WhileStatement(
    condition: SingleStatement,
    body: CompoundBody,
    label: Option[String]) extends CompoundPlanStatement
