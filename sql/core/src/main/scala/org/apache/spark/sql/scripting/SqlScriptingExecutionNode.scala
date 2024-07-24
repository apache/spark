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

package org.apache.spark.sql.scripting

import org.apache.spark.SparkException
import org.apache.spark.internal.Logging
import org.apache.spark.sql.{Dataset, SparkSession}
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan
import org.apache.spark.sql.catalyst.trees.{Origin, WithOrigin}
import org.apache.spark.sql.errors.SqlScriptingErrors
import org.apache.spark.sql.types.BooleanType

/**
 * Trait for all SQL scripting execution nodes used during interpretation phase.
 */
sealed trait CompoundStatementExec extends Logging {

  /**
   * Whether the statement originates from the SQL script or is created during the interpretation.
   * Example: DropVariable statements are automatically created at the end of each compound.
   */
  val isInternal: Boolean = false

  /**
   * Reset execution of the current node.
   */
  def reset(): Unit
}

/**
 * Leaf node in the execution tree.
 */
trait LeafStatementExec extends CompoundStatementExec

/**
 * Non-leaf node in the execution tree. It is an iterator over executable child nodes.
 */
trait NonLeafStatementExec extends CompoundStatementExec {

  /**
   * Construct the iterator to traverse the tree rooted at this node in an in-order traversal.
   * @return
   *   Tree iterator.
   */
  def getTreeIterator: Iterator[CompoundStatementExec]

  /**
   * Evaluate the boolean condition represented by the statement.
   * @param session SparkSession that SQL script is executed within.
   * @param statement Statement representing the boolean condition to evaluate.
   * @return
   *    The value (`true` or `false`) of condition evaluation;
   *    or throw the error during the evaluation (eg: returning multiple rows of data
   *    or non-boolean statement).
   */
  protected def evaluateBooleanCondition(
      session: SparkSession,
      statement: LeafStatementExec): Boolean = statement match {
    case statement: SingleStatementExec =>
      assert(!statement.isExecuted)
      statement.isExecuted = true

      // DataFrame evaluates to True if it is single row, single column
      //  of boolean type with value True.
      val df = Dataset.ofRows(session, statement.parsedPlan)
      df.schema.fields match {
        case Array(field) if field.dataType == BooleanType =>
          df.limit(2).collect() match {
            case Array(row) => row.getBoolean(0)
            case _ =>
              throw SparkException.internalError(
                s"Boolean statement ${statement.getText} is invalid. It returns more than one row.")
          }
        case _ =>
          throw SqlScriptingErrors.invalidBooleanStatement(statement.origin, statement.getText)
      }
    case _ =>
      throw SparkException.internalError("Boolean condition must be SingleStatementExec")
  }
}

/**
 * Executable node for SingleStatement.
 * @param parsedPlan
 *   Logical plan of the parsed statement.
 * @param origin
 *   Origin descriptor for the statement.
 * @param isInternal
 *   Whether the statement originates from the SQL script or it is created during the
 *   interpretation. Example: DropVariable statements are automatically created at the end of each
 *   compound.
 */
class SingleStatementExec(
    var parsedPlan: LogicalPlan,
    override val origin: Origin,
    override val isInternal: Boolean)
  extends LeafStatementExec with WithOrigin {

  /**
   * Whether this statement has been executed during the interpretation phase.
   * Example: Statements in conditions of If/Else, While, etc.
   */
  var isExecuted = false

  /**
   * Get the SQL query text corresponding to this statement.
   * @return
   *   SQL query text.
   */
  def getText: String = {
    assert(origin.sqlText.isDefined && origin.startIndex.isDefined && origin.stopIndex.isDefined)
    origin.sqlText.get.substring(origin.startIndex.get, origin.stopIndex.get + 1)
  }

  override def reset(): Unit = isExecuted = false
}

/**
 * Abstract class for all statements that contain nested statements.
 * Implements recursive iterator logic over all child execution nodes.
 * @param collection
 *   Collection of child execution nodes.
 */
abstract class CompoundNestedStatementIteratorExec(collection: Seq[CompoundStatementExec])
  extends NonLeafStatementExec {

  private var localIterator = collection.iterator
  private var curr = if (localIterator.hasNext) Some(localIterator.next()) else None

  private lazy val treeIterator: Iterator[CompoundStatementExec] =
    new Iterator[CompoundStatementExec] {
      override def hasNext: Boolean = {
        val childHasNext = curr match {
          case Some(body: NonLeafStatementExec) => body.getTreeIterator.hasNext
          case Some(_: LeafStatementExec) => true
          case None => false
          case _ => throw SparkException.internalError(
            "Unknown statement type encountered during SQL script interpretation.")
        }
        localIterator.hasNext || childHasNext
      }

      @scala.annotation.tailrec
      override def next(): CompoundStatementExec = {
        curr match {
          case None => throw SparkException.internalError(
            "No more elements to iterate through in the current SQL compound statement.")
          case Some(statement: LeafStatementExec) =>
            curr = if (localIterator.hasNext) Some(localIterator.next()) else None
            statement
          case Some(body: NonLeafStatementExec) =>
            if (body.getTreeIterator.hasNext) {
              body.getTreeIterator.next()
            } else {
              curr = if (localIterator.hasNext) Some(localIterator.next()) else None
              next()
            }
          case _ => throw SparkException.internalError(
            "Unknown statement type encountered during SQL script interpretation.")
        }
      }
    }

  override def getTreeIterator: Iterator[CompoundStatementExec] = treeIterator

  override def reset(): Unit = {
    collection.foreach(_.reset())
    localIterator = collection.iterator
    curr = if (localIterator.hasNext) Some(localIterator.next()) else None
  }
}

/**
 * Executable node for CompoundBody.
 * @param statements
 *   Executable nodes for nested statements within the CompoundBody.
 */
class CompoundBodyExec(statements: Seq[CompoundStatementExec])
  extends CompoundNestedStatementIteratorExec(statements)

/**
 * Executable node for IfElseStatement.
 * @param conditions Collection of executable conditions. First condition corresponds to IF clause,
 *                   while others (if any) correspond to following ELSE IF clauses.
 * @param conditionalBodies Collection of executable bodies that have a corresponding condition,
*                 in IF or ELSE IF branches.
 * @param elseBody Body that is executed if none of the conditions are met,
 *                          i.e. ELSE branch.
 * @param session Spark session that SQL script is executed within.
 */
class IfElseStatementExec(
    conditions: Seq[SingleStatementExec],
    conditionalBodies: Seq[CompoundBodyExec],
    elseBody: Option[CompoundBodyExec],
    session: SparkSession) extends NonLeafStatementExec {
  private object IfElseState extends Enumeration {
    val Condition, Body = Value
  }

  private var state = IfElseState.Condition
  private var curr: Option[CompoundStatementExec] = Some(conditions.head)

  private var clauseIdx: Int = 0
  private val conditionsCount = conditions.length
  assert(conditionsCount == conditionalBodies.length)

  private lazy val treeIterator: Iterator[CompoundStatementExec] =
    new Iterator[CompoundStatementExec] {
      override def hasNext: Boolean = curr.nonEmpty

      override def next(): CompoundStatementExec = state match {
        case IfElseState.Condition =>
          assert(curr.get.isInstanceOf[SingleStatementExec])
          val condition = curr.get.asInstanceOf[SingleStatementExec]
          if (evaluateBooleanCondition(session, condition)) {
            state = IfElseState.Body
            curr = Some(conditionalBodies(clauseIdx))
          } else {
            clauseIdx += 1
            if (clauseIdx < conditionsCount) {
              // There are ELSE IF clauses remaining.
              state = IfElseState.Condition
              curr = Some(conditions(clauseIdx))
            } else if (elseBody.isDefined) {
              // ELSE clause exists.
              state = IfElseState.Body
              curr = Some(elseBody.get)
            } else {
              // No remaining clauses.
              curr = None
            }
          }
          condition
        case IfElseState.Body =>
          assert(curr.get.isInstanceOf[CompoundBodyExec])
          val currBody = curr.get.asInstanceOf[CompoundBodyExec]
          val retStmt = currBody.getTreeIterator.next()
          if (!currBody.getTreeIterator.hasNext) {
            curr = None
          }
          retStmt
      }
    }

  override def getTreeIterator: Iterator[CompoundStatementExec] = treeIterator

  override def reset(): Unit = {
    state = IfElseState.Condition
    curr = Some(conditions.head)
    clauseIdx = 0
    conditions.foreach(c => c.reset())
    conditionalBodies.foreach(b => b.reset())
    elseBody.foreach(b => b.reset())
  }
}

/**
 * Executable node for WhileStatement.
 * @param condition Executable node for the condition.
 * @param body Executable node for the body.
 * @param booleanEvaluator Evaluator for Boolean condition.
 */
class WhileStatementExec(
                          condition: SingleStatementExec,
                          body: CompoundBodyExec,
                          booleanEvaluator: StatementBooleanEvaluator) extends NonLeafStatementExec {

  private object WhileState extends Enumeration {
    val Condition, Body = Value
  }

  private var state = WhileState.Condition
  private var curr: Option[CompoundStatementExec] = Some(condition)

  private lazy val treeIterator: Iterator[CompoundStatementExec] =
    new Iterator[CompoundStatementExec] {
      override def hasNext: Boolean = curr.nonEmpty

      override def next(): CompoundStatementExec = state match {
          case WhileState.Condition =>
            assert(curr.get.isInstanceOf[SingleStatementExec])
            val condition = curr.get.asInstanceOf[SingleStatementExec]
            if (booleanEvaluator.eval(condition)) {
              state = WhileState.Body
              curr = Some(body)
              body.reset()
            } else {
              curr = None
            }
            condition
          case WhileState.Body =>
            val retStmt = body.getTreeIterator.next()
            if (!body.getTreeIterator.hasNext) {
              state = WhileState.Condition
              curr = Some(condition)
              condition.reset()
            }
            retStmt
        }
    }

  override def getTreeIterator: Iterator[CompoundStatementExec] = treeIterator

  override def reset(): Unit = {
    state = WhileState.Condition
    curr = Some(condition)
    condition.reset()
    body.reset()
  }
}
