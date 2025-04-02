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

import java.util.Locale

import scala.collection.mutable

import org.apache.spark.sql.catalyst.expressions.{Attribute, Expression}
import org.apache.spark.sql.catalyst.plans.logical.ExceptionHandlerType.ExceptionHandlerType
import org.apache.spark.sql.catalyst.trees.{CurrentOrigin, Origin}
import org.apache.spark.sql.errors.SqlScriptingErrors

/**
 * Trait for all SQL Scripting logical operators that are product of parsing phase.
 * These operators will be used by the SQL Scripting interpreter to generate execution nodes.
 */
sealed trait CompoundPlanStatement extends LogicalPlan

/**
 * Logical operator representing result of parsing a single SQL statement
 *   that is supposed to be executed against Spark.
 * @param parsedPlan Result of SQL statement parsing.
 */
case class SingleStatement(parsedPlan: LogicalPlan)
  extends CompoundPlanStatement {

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

  override def output: Seq[Attribute] = parsedPlan.output

  override def children: Seq[LogicalPlan] = parsedPlan.children

  override protected def withNewChildrenInternal(
      newChildren: IndexedSeq[LogicalPlan]): LogicalPlan =
    SingleStatement(parsedPlan.withNewChildren(newChildren))
}

/**
 * Logical operator for a compound body. Contains all statements within the compound body.
 * @param collection Collection of statements within the compound body.
 * @param label Label set to CompoundBody by user or UUID otherwise.
 *              It can be None in case when CompoundBody is not part of BeginEndCompoundBlock
 *              for example when CompoundBody is inside loop or conditional block.
 * @param isScope Flag indicating if the CompoundBody is a labeled scope.
 *                Scopes are used for grouping local variables and exception handlers.
 * @param handlers Collection of error handlers that are defined within the compound body.
 * @param conditions Collection of error conditions that are defined within the compound body.
 */
case class CompoundBody(
    collection: Seq[CompoundPlanStatement],
    label: Option[String],
    isScope: Boolean,
    handlers: Seq[ExceptionHandler] = Seq.empty,
    conditions: mutable.Map[String, String] = mutable.HashMap())
  extends Command with CompoundPlanStatement {

  override def children: Seq[LogicalPlan] = collection

  override protected def withNewChildrenInternal(
      newChildren: IndexedSeq[LogicalPlan]): LogicalPlan = {
    CompoundBody(newChildren.map(_.asInstanceOf[CompoundPlanStatement]), label, isScope)
  }
}

/**
 * Logical operator for IF ELSE statement.
 * @param conditions Collection of conditions. First condition corresponds to IF clause,
 *                   while others (if any) correspond to following ELSEIF clauses.
 * @param conditionalBodies Collection of bodies that have a corresponding condition,
 *                          in IF or ELSEIF branches.
 * @param elseBody Body that is executed if none of the conditions are met,
 *                          i.e. ELSE branch.
 */
case class IfElseStatement(
    conditions: Seq[SingleStatement],
    conditionalBodies: Seq[CompoundBody],
    elseBody: Option[CompoundBody]) extends CompoundPlanStatement {
  assert(conditions.length == conditionalBodies.length)

  override def output: Seq[Attribute] = Seq.empty

  override def children: Seq[LogicalPlan] = Seq.concat(conditions, conditionalBodies, elseBody)

  override protected def withNewChildrenInternal(
      newChildren: IndexedSeq[LogicalPlan]): LogicalPlan = {
    val conditions = newChildren
      .filter(_.isInstanceOf[SingleStatement])
      .map(_.asInstanceOf[SingleStatement])
    var conditionalBodies = newChildren
      .filter(_.isInstanceOf[CompoundBody])
      .map(_.asInstanceOf[CompoundBody])
    var elseBody: Option[CompoundBody] = None

    assert(conditions.length == conditionalBodies.length ||
      conditions.length + 1 == conditionalBodies.length)

    if (conditions.length < conditionalBodies.length) {
      conditionalBodies = conditionalBodies.dropRight(1)
      elseBody = Some(conditionalBodies.last)
    }
    IfElseStatement(conditions, conditionalBodies, elseBody)
  }
}

/**
 * Logical operator for while statement.
 * @param condition Any expression evaluating to a Boolean.
 *                 Body is executed as long as the condition evaluates to true
 * @param body Compound body is a collection of statements that are executed if condition is true.
 * @param label An optional label for the loop which is unique amongst all labels for statements
 *              within which the WHILE statement is contained.
 *              If an end label is specified it must match the beginning label.
 *              The label can be used to LEAVE or ITERATE the loop.
 */
case class WhileStatement(
    condition: SingleStatement,
    body: CompoundBody,
    label: Option[String]) extends CompoundPlanStatement {

  override def output: Seq[Attribute] = Seq.empty

  override def children: Seq[LogicalPlan] = Seq(condition, body)

  override protected def withNewChildrenInternal(
      newChildren: IndexedSeq[LogicalPlan]): LogicalPlan = {
    assert(newChildren.length == 2)
    WhileStatement(
      newChildren(0).asInstanceOf[SingleStatement],
      newChildren(1).asInstanceOf[CompoundBody],
      label)
  }
}

/**
 * Logical operator for REPEAT statement.
 * @param condition Any expression evaluating to a Boolean.
 *                 Body is executed as long as the condition evaluates to false
 * @param body Compound body is a collection of statements that are executed once no matter what,
 *             and then as long as condition is false.
 * @param label An optional label for the loop which is unique amongst all labels for statements
 *              within which the REPEAT statement is contained.
 *              If an end label is specified it must match the beginning label.
 *              The label can be used to LEAVE or ITERATE the loop.
 */
case class RepeatStatement(
    condition: SingleStatement,
    body: CompoundBody,
    label: Option[String]) extends CompoundPlanStatement {

  override def output: Seq[Attribute] = Seq.empty

  override def children: Seq[LogicalPlan] = Seq(condition, body)

  override protected def withNewChildrenInternal(
      newChildren: IndexedSeq[LogicalPlan]): LogicalPlan = {
    assert(newChildren.length == 2)
    RepeatStatement(
      newChildren(0).asInstanceOf[SingleStatement],
      newChildren(1).asInstanceOf[CompoundBody],
      label)
  }
}

/**
 * Logical operator for LEAVE statement.
 * The statement can be used both for compounds or any kind of loops.
 * When used, the corresponding body/loop execution is skipped and the execution continues
 *   with the next statement after the body/loop.
 * @param label Label of the compound or loop to leave.
 */
case class LeaveStatement(label: String) extends CompoundPlanStatement {
  override def output: Seq[Attribute] = Seq.empty

  override def children: Seq[LogicalPlan] = Seq.empty

  override protected def withNewChildrenInternal(
      newChildren: IndexedSeq[LogicalPlan]): LogicalPlan = LeaveStatement(label)
}

/**
 * Logical operator for ITERATE statement.
 * The statement can be used only for loops.
 * When used, the rest of the loop is skipped and the loop execution continues
 *   with the next iteration.
 * @param label Label of the loop to iterate.
 */
case class IterateStatement(label: String) extends CompoundPlanStatement {
  override def output: Seq[Attribute] = Seq.empty

  override def children: Seq[LogicalPlan] = Seq.empty

  override protected def withNewChildrenInternal(
      newChildren: IndexedSeq[LogicalPlan]): LogicalPlan = IterateStatement(label)
}

/**
 * Logical operator for CASE statement, SEARCHED variant.<br>
 * Example:
 * {{{
 *   CASE
 *     WHEN x = 1 THEN
 *       SELECT 1;
 *     WHEN x = 2 THEN
 *       SELECT 2;
 *     ELSE
 *       SELECT 3;
 *   END CASE;
 * }}}
 * @param conditions Collection of conditions which correspond to WHEN clauses.
 * @param conditionalBodies Collection of bodies that have a corresponding condition,
 *                          in WHEN branches.
 * @param elseBody Body that is executed if none of the conditions are met, i.e. ELSE branch.
 */
case class SearchedCaseStatement(
    conditions: Seq[SingleStatement],
    conditionalBodies: Seq[CompoundBody],
    elseBody: Option[CompoundBody]) extends CompoundPlanStatement {
  assert(conditions.length == conditionalBodies.length)

  override def output: Seq[Attribute] = Seq.empty

  override def children: Seq[LogicalPlan] = Seq.concat(conditions, conditionalBodies, elseBody)

  override protected def withNewChildrenInternal(
      newChildren: IndexedSeq[LogicalPlan]): LogicalPlan = {
    val conditions = newChildren
      .filter(_.isInstanceOf[SingleStatement])
      .map(_.asInstanceOf[SingleStatement])
    var conditionalBodies = newChildren
      .filter(_.isInstanceOf[CompoundBody])
      .map(_.asInstanceOf[CompoundBody])
    var elseBody: Option[CompoundBody] = None

    assert(conditions.length == conditionalBodies.length ||
      conditions.length + 1 == conditionalBodies.length)

    if (conditions.length < conditionalBodies.length) {
      conditionalBodies = conditionalBodies.dropRight(1)
      elseBody = Some(conditionalBodies.last)
    }
    SearchedCaseStatement(conditions, conditionalBodies, elseBody)
  }
}

/**
 * Logical operator for CASE statement, SIMPLE variant.<br>
 * Example:
 * {{{
 *   CASE x
 *     WHEN 1 THEN
 *       SELECT 1;
 *     WHEN 2 THEN
 *       SELECT 2;
 *     ELSE
 *       SELECT 3;
 *   END CASE;
 * }}}
 * @param caseVariableExpression Expression with which all conditionExpressions will be compared to.
 * @param conditionExpressions Collection of expressions which correspond to WHEN clauses.
 * @param conditionalBodies Collection of bodies that have a corresponding condition,
 *                          in WHEN branches.
 * @param elseBody Body that is executed if none of the conditions are met, i.e. ELSE branch.
 */
case class SimpleCaseStatement(
    caseVariableExpression: Expression,
    conditionExpressions: Seq[Expression],
    conditionalBodies: Seq[CompoundBody],
    elseBody: Option[CompoundBody]) extends CompoundPlanStatement {
  assert(conditionExpressions.length == conditionalBodies.length)

  override def output: Seq[Attribute] = Seq.empty

  override def children: Seq[LogicalPlan] = conditionalBodies

  override protected def withNewChildrenInternal(
    newChildren: IndexedSeq[LogicalPlan]): LogicalPlan = {
    val conditionalBodies = newChildren.map(_.asInstanceOf[CompoundBody])
    SimpleCaseStatement(caseVariableExpression, conditionExpressions, conditionalBodies, elseBody)
  }
}

/**
 * Logical operator for LOOP statement.
 * @param body Compound body is a collection of statements that are executed until the
 *             LOOP statement is terminated by using the LEAVE statement.
 * @param label An optional label for the loop which is unique amongst all labels for statements
 *              within which the LOOP statement is contained.
 *              If an end label is specified it must match the beginning label.
 *              The label can be used to LEAVE or ITERATE the loop.
 */
case class LoopStatement(
    body: CompoundBody,
    label: Option[String]) extends CompoundPlanStatement {

  override def output: Seq[Attribute] = Seq.empty

  override def children: Seq[LogicalPlan] = Seq(body)

  override protected def withNewChildrenInternal(
      newChildren: IndexedSeq[LogicalPlan]): LogicalPlan = {
    assert(newChildren.length == 1)
    LoopStatement(newChildren(0).asInstanceOf[CompoundBody], label)
  }
}

/**
 * Logical operator for FOR statement.
 * @param query Query which is executed once, then it's result set is iterated on, row by row.
 * @param variableName Name of variable which is used to access the current row during iteration.
 * @param body Compound body is a collection of statements that are executed for each row in
 *             the result set of the query.
 * @param label An optional label for the loop which is unique amongst all labels for statements
 *              within which the FOR statement is contained.
 *              If an end label is specified it must match the beginning label.
 *              The label can be used to LEAVE or ITERATE the loop.
 */
case class ForStatement(
    query: SingleStatement,
    variableName: Option[String],
    body: CompoundBody,
    label: Option[String]) extends CompoundPlanStatement {

  override def output: Seq[Attribute] = Seq.empty

  override def children: Seq[LogicalPlan] = Seq(query, body)

  override protected def withNewChildrenInternal(
      newChildren: IndexedSeq[LogicalPlan]): LogicalPlan = newChildren match {
    case IndexedSeq(query: SingleStatement, body: CompoundBody) =>
      ForStatement(query, variableName, body, label)
  }
}

/**
 * Logical operator for an error condition.
 * @param conditionName Name of the error condition.
 * @param sqlState SQLSTATE or Error Code.
 */
case class ErrorCondition(
    conditionName: String,
    sqlState: String) extends CompoundPlanStatement {
  override def output: Seq[Attribute] = Seq.empty

  override def children: Seq[LogicalPlan] = Seq.empty

  override protected def withNewChildrenInternal(
      newChildren: IndexedSeq[LogicalPlan]): LogicalPlan = this.copy()
}

object ExceptionHandlerType extends Enumeration {
  type ExceptionHandlerType = Value
  val EXIT, CONTINUE = Value
}

/**
 * Class holding information about what triggers the handler.
 * @param sqlStates Set of sqlStates that will trigger handler.
 * @param conditions  Set of error condition names that will trigger handler.
 * @param sqlException Flag indicating if the handler is triggered by SQLEXCEPTION.
 * @param notFound Flag indicating if the handler is triggered by NOT FOUND.
 */
class ExceptionHandlerTriggers(
    val sqlStates: mutable.Set[String] = mutable.Set.empty,
    val conditions: mutable.Set[String] = mutable.Set.empty,
    var sqlException: Boolean = false,
    var notFound: Boolean = false) {

  def addUniqueSqlException(): Unit = {
    if (sqlException) {
      throw SqlScriptingErrors
        .duplicateConditionInHandlerDeclaration(CurrentOrigin.get, "SQLEXCEPTION")
    }
    sqlException = true
  }

  def addUniqueNotFound(): Unit = {
    if (notFound) {
      throw SqlScriptingErrors
        .duplicateConditionInHandlerDeclaration(CurrentOrigin.get, "NOT FOUND")
    }
    notFound = true
  }

  def addUniqueCondition(value: String): Unit = {
    val uppercaseValue = value.toUpperCase(Locale.ROOT)
    if (conditions.contains(uppercaseValue)) {
      throw SqlScriptingErrors
        .duplicateConditionInHandlerDeclaration(CurrentOrigin.get, uppercaseValue)
    }
    conditions += uppercaseValue
  }

  def addUniqueSqlState(value: String): Unit = {
    val uppercaseValue = value.toUpperCase(Locale.ROOT)
    if (sqlStates.contains(uppercaseValue)) {
      throw SqlScriptingErrors
        .duplicateSqlStateInHandlerDeclaration(CurrentOrigin.get, uppercaseValue)
    }
    sqlStates += uppercaseValue
  }
}

/**
 * Logical operator for an error handler.
 * @param exceptionHandlerTriggers Collection of different handler triggers:
 *        sqlStates -> set of sqlStates that will trigger handler
 *        conditions -> set of conditions that will trigger handler
 *        sqlException -> if handler is triggered by SQLEXCEPTION
 *        notFound -> if handler is triggered by NotFound
 * @param body CompoundBody of the handler.
 * @param handlerType Type of the handler (CONTINUE or EXIT).
 */
case class ExceptionHandler(
    exceptionHandlerTriggers: ExceptionHandlerTriggers,
    body: CompoundBody,
    handlerType: ExceptionHandlerType) extends CompoundPlanStatement {
  override def output: Seq[Attribute] = Seq.empty

  override def children: Seq[LogicalPlan] = Seq(body)

  override protected def withNewChildrenInternal(
      newChildren: IndexedSeq[LogicalPlan]): LogicalPlan = {
    assert(newChildren.length == 1)
    ExceptionHandler(
      exceptionHandlerTriggers,
      newChildren(0).asInstanceOf[CompoundBody],
      handlerType)
  }
}
