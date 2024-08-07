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
            case Array(row) =>
              if (row.isNullAt(0)) {
                throw SqlScriptingErrors.booleanStatementWithEmptyRow(
                  statement.origin, statement.getText)
              }
              row.getBoolean(0)
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
 * @param label
 *   Label set by user or None otherwise.
 */
abstract class CompoundNestedStatementIteratorExec(
    collection: Seq[CompoundStatementExec],
    label: Option[String] = None)
  extends NonLeafStatementExec {

  private var localIterator = collection.iterator
  private var curr = if (localIterator.hasNext) Some(localIterator.next()) else None

  /** Used to stop the iteration in cases when LEAVE statement is encountered. */
  private var stopIteration = false

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
        !stopIteration && (localIterator.hasNext || childHasNext)
      }

      @scala.annotation.tailrec
      override def next(): CompoundStatementExec = {
        curr match {
          case None => throw SparkException.internalError(
            "No more elements to iterate through in the current SQL compound statement.")
          case Some(leaveStatement: LeaveStatementExec) =>
            handleLeaveStatement(leaveStatement)
            curr = None
            leaveStatement
          case Some(iterateStatement: IterateStatementExec) =>
            handleIterateStatement(iterateStatement)
            curr = None
            iterateStatement
          case Some(statement: LeafStatementExec) =>
            curr = if (localIterator.hasNext) Some(localIterator.next()) else None
            statement
          case Some(body: NonLeafStatementExec) =>
            if (body.getTreeIterator.hasNext) {
              body.getTreeIterator.next() match {
                case leaveStatement: LeaveStatementExec =>
                  handleLeaveStatement(leaveStatement)
                  leaveStatement
                case iterateStatement: IterateStatementExec =>
                  handleIterateStatement(iterateStatement)
                  iterateStatement
                case other => other
              }
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
    stopIteration = false
  }

  /** Actions to do when LEAVE statement is encountered, to stop the execution of this compound. */
  private def handleLeaveStatement(leaveStatement: LeaveStatementExec): Unit = {
    if (!leaveStatement.hasBeenMatched) {
      // Stop the iteration.
      stopIteration = true

      // TODO: Variable cleanup (once we add SQL script execution logic).
      // TODO: Add interpreter tests as well.

      // Check if label has been matched.
      leaveStatement.hasBeenMatched = label.isDefined && label.get.equals(leaveStatement.label)
    }
  }

  /**
   * Actions to do when ITERATE statement is encountered, to stop the execution of this compound.
   */
  private def handleIterateStatement(iterateStatement: IterateStatementExec): Unit = {
    if (!iterateStatement.hasBeenMatched) {
      // Stop the iteration.
      stopIteration = true

      // TODO: Variable cleanup (once we add SQL script execution logic).
      // TODO: Add interpreter tests as well.

      // No need to check if label has been matched, since ITERATE statement is already
      //   not allowed in CompoundBody.
    }
  }
}

/**
 * Executable node for CompoundBody.
 * @param statements
 *   Executable nodes for nested statements within the CompoundBody.
 * @param label
 *   Label set by user to CompoundBody or None otherwise.
 */
class CompoundBodyExec(statements: Seq[CompoundStatementExec], label: Option[String] = None)
  extends CompoundNestedStatementIteratorExec(statements, label)

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
 * @param label Label set to WhileStatement by user or None otherwise.
 * @param session Spark session that SQL script is executed within.
 */
class WhileStatementExec(
    condition: SingleStatementExec,
    body: CompoundBodyExec,
    label: Option[String],
    session: SparkSession) extends NonLeafStatementExec {

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
            val condition = curr.get.asInstanceOf[SingleStatementExec]
            if (evaluateBooleanCondition(session, condition)) {
              state = WhileState.Body
              curr = Some(body)
              body.reset()
            } else {
              curr = None
            }
            condition
          case WhileState.Body =>
            val retStmt = body.getTreeIterator.next()

            // Handle LEAVE or ITERATE statement if it has been encountered.
            retStmt match {
              case leaveStatementExec: LeaveStatementExec if !leaveStatementExec.hasBeenMatched =>
                if (label.contains(leaveStatementExec.label)) {
                  leaveStatementExec.hasBeenMatched = true
                }
                curr = None
                return retStmt
              case iterStatementExec: IterateStatementExec if !iterStatementExec.hasBeenMatched =>
                if (label.contains(iterStatementExec.label)) {
                  iterStatementExec.hasBeenMatched = true
                }
                state = WhileState.Condition
                curr = Some(condition)
                condition.reset()
                return retStmt
              case _ =>
            }

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

class SearchedCaseStatementExec(
    conditions: Seq[SingleStatementExec],
    conditionalBodies: Seq[CompoundBodyExec],
    elseBody: Option[CompoundBodyExec],
    session: SparkSession) extends NonLeafStatementExec {
  private object CaseState extends Enumeration {
    val Condition, Body = Value
  }

  private var state = CaseState.Condition
  private var curr: Option[CompoundStatementExec] = Some(conditions.head)

  private var clauseIdx: Int = 0
  private val conditionsCount = conditions.length
  assert(conditionsCount == conditionalBodies.length)

  private lazy val treeIterator: Iterator[CompoundStatementExec] =
    new Iterator[CompoundStatementExec] {
      override def hasNext: Boolean = curr.nonEmpty

      override def next(): CompoundStatementExec = state match {
        case CaseState.Condition =>
          assert(curr.get.isInstanceOf[SingleStatementExec])
          val condition = curr.get.asInstanceOf[SingleStatementExec]
          if (evaluateBooleanCondition(session, condition)) {
            state = CaseState.Body
            curr = Some(conditionalBodies(clauseIdx))
          } else {
            clauseIdx += 1
            if (clauseIdx < conditionsCount) {
              // There are ELSE IF clauses remaining.
              state = CaseState.Condition
              curr = Some(conditions(clauseIdx))
            } else if (elseBody.isDefined) {
              // ELSE clause exists.
              state = CaseState.Body
              curr = Some(elseBody.get)
            } else {
              // No remaining clauses.
              curr = None
            }
          }
          condition
        case CaseState.Body =>
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
    state = CaseState.Condition
    curr = Some(conditions.head)
    clauseIdx = 0
    conditions.foreach(c => c.reset())
    conditionalBodies.foreach(b => b.reset())
    elseBody.foreach(b => b.reset())
  }
}

/**
 * Executable node for RepeatStatement.
 * @param condition Executable node for the condition - evaluates to a row with a single boolean
 *                  expression, otherwise throws an exception
 * @param body Executable node for the body.
 * @param label Label set to RepeatStatement by user, None if not set
 * @param session Spark session that SQL script is executed within.
 */
class RepeatStatementExec(
    condition: SingleStatementExec,
    body: CompoundBodyExec,
    label: Option[String],
    session: SparkSession) extends NonLeafStatementExec {

  private object RepeatState extends Enumeration {
    val Condition, Body = Value
  }

  private var state = RepeatState.Body
  private var curr: Option[CompoundStatementExec] = Some(body)

  private lazy val treeIterator: Iterator[CompoundStatementExec] =
    new Iterator[CompoundStatementExec] {
      override def hasNext: Boolean = curr.nonEmpty

      override def next(): CompoundStatementExec = state match {
        case RepeatState.Condition =>
          val condition = curr.get.asInstanceOf[SingleStatementExec]
          if (!evaluateBooleanCondition(session, condition)) {
            state = RepeatState.Body
            curr = Some(body)
            body.reset()
          } else {
            curr = None
          }
          condition
        case RepeatState.Body =>
          val retStmt = body.getTreeIterator.next()

          retStmt match {
            case leaveStatementExec: LeaveStatementExec if !leaveStatementExec.hasBeenMatched =>
              if (label.contains(leaveStatementExec.label)) {
                leaveStatementExec.hasBeenMatched = true
              }
              curr = None
              return retStmt
            case iterStatementExec: IterateStatementExec if !iterStatementExec.hasBeenMatched =>
              if (label.contains(iterStatementExec.label)) {
                iterStatementExec.hasBeenMatched = true
              }
              state = RepeatState.Condition
              curr = Some(condition)
              condition.reset()
              return retStmt
            case _ =>
          }

          if (!body.getTreeIterator.hasNext) {
            state = RepeatState.Condition
            curr = Some(condition)
            condition.reset()
          }
          retStmt
      }
    }

  override def getTreeIterator: Iterator[CompoundStatementExec] = treeIterator

  override def reset(): Unit = {
    state = RepeatState.Body
    curr = Some(body)
    body.reset()
    condition.reset()
  }
}

/**
 * Executable node for LeaveStatement.
 * @param label Label of the compound or loop to leave.
 */
class LeaveStatementExec(val label: String) extends LeafStatementExec {
  /**
   * Label specified in the LEAVE statement might not belong to the immediate surrounding compound,
   *   but to the any surrounding compound.
   * Iteration logic is recursive, i.e. when iterating through the compound, if another
   *   compound is encountered, next() will be called to iterate its body. The same logic
   *   is applied to any other compound down the traversal tree.
   * In such cases, when LEAVE statement is encountered (as the leaf of the traversal tree),
   *   it will be propagated upwards and the logic will try to match it to the labels of
   *   surrounding compounds.
   * Once the match is found, this flag is set to true to indicate that search should be stopped.
   */
  var hasBeenMatched: Boolean = false
  override def reset(): Unit = hasBeenMatched = false
}

/**
 * Executable node for ITERATE statement.
 * @param label Label of the loop to iterate.
 */
class IterateStatementExec(val label: String) extends LeafStatementExec {
  /**
   * Label specified in the ITERATE statement might not belong to the immediate compound,
   * but to the any surrounding compound.
   * Iteration logic is recursive, i.e. when iterating through the compound, if another
   * compound is encountered, next() will be called to iterate its body. The same logic
   * is applied to any other compound down the tree.
   * In such cases, when ITERATE statement is encountered (as the leaf of the traversal tree),
   * it will be propagated upwards and the logic will try to match it to the labels of
   * surrounding compounds.
   * Once the match is found, this flag is set to true to indicate that search should be stopped.
   */
  var hasBeenMatched: Boolean = false
  override def reset(): Unit = hasBeenMatched = false
}
