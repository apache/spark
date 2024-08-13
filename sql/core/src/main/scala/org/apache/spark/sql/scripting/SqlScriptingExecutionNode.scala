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

import scala.collection.mutable
import org.apache.spark.{SparkException, SparkThrowable}
import org.apache.spark.internal.Logging
import org.apache.spark.sql.catalyst.parser.SingleStatement
import org.apache.spark.sql.{Dataset, Row, SparkSession}
import org.apache.spark.sql.catalyst.plans.logical.{DropVariable, LogicalPlan}
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
trait LeafStatementExec extends CompoundStatementExec {

  /** Whether an error was raised during the execution of this statement. */
  var raisedError: Boolean = false

  /** Error state of the statement. */
  var errorState: Option[String] = None

  /** Throwable to rethrow after the statement execution if the error is not handled. */
  var rethrow: Option[Throwable] = None

  /**
   * Execute the statement.
   * @param session Spark session.
   */
  def execute(session: SparkSession): Unit

  override def reset(): Unit = {
    raisedError = false
    errorState = None
    rethrow = None
  }
}

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
 * @param shouldCollectResult
 *   Whether we should collect result after statement execution. Example: results from conditions
 *   in if-else or loops should not be collected.
 */
class SingleStatementExec(
    var parsedPlan: LogicalPlan,
    override val origin: Origin,
    override val isInternal: Boolean = false,
    val shouldCollectResult: Boolean = false)
  extends LeafStatementExec with WithOrigin {

  /** Data returned after execution. */
  var result: Option[Array[Row]] = None

  /**
   * Get the SQL query text corresponding to this statement.
   * @return
   *   SQL query text.
   */
  def getText: String = {
    assert(origin.sqlText.isDefined && origin.startIndex.isDefined && origin.stopIndex.isDefined)
    origin.sqlText.get.substring(origin.startIndex.get, origin.stopIndex.get + 1)
  }

  override def reset(): Unit = {
    super.reset()
    result = None
  }

  override def execute(session: SparkSession): Unit = {
    try {
      val rows = Some(Dataset.ofRows(session, parsedPlan).collect())
      if (shouldCollectResult) {
        result = rows
      }
    } catch {
      case e: SparkThrowable =>
        raisedError = true
        errorState = Some(e.getSqlState)
        e match {
          case throwable: Throwable =>
            rethrow = Some(throwable)
          case _ =>
        }
      case throwable: Throwable =>
        raisedError = true
        errorState = Some("SQLEXCEPTION")
        rethrow = Some(throwable)
    }
  }
}

/**
 * Executable node for CompoundBody.
 * @param statements
 *   Executable nodes for nested statements within the CompoundBody.
 * @param session
 *   Spark session.
 */
class CompoundBodyExec(
    statements: Seq[CompoundStatementExec],
    session: SparkSession,
    label: Option[String] = None,
    conditionHandlerMap: mutable.HashMap[String, ErrorHandlerExec] = mutable.HashMap())
  extends NonLeafStatementExec {

  /**
   * Get handler to handle error given by condition.
   * @param condition SqlState of the error raised during statement execution.
   * @return Corresponding error handler executable node.
   */
  private def getHandler(condition: String): Option[ErrorHandlerExec] = {
    conditionHandlerMap.get(condition)
      .orElse{
        conditionHandlerMap.get("NOT FOUND") match {
          // If NOT FOUND handler is defined, use it only for errors with class '02'.
          case Some(handler) if condition.startsWith("02") => Some(handler)
          case _ => None
      }}
      .orElse{conditionHandlerMap.get("SQLEXCEPTION")}
  }

  /**
   * Handle error raised during the execution of the statement.
   * @param statement statement that possibly raised the error
   * @return pass through the statement
   */
  private def handleError(statement: LeafStatementExec): LeafStatementExec = {
    if (statement.raisedError) {
      getHandler(statement.errorState.get).foreach { handler =>
        statement.reset() // Clear all flags and result
        handler.reset()
        returnHere = curr
        curr = Some(handler.body)
      }
    }
    statement
  }

  /**
   * Drop variables declared in this CompoundBody.
   */
  private def cleanup(): Unit = {
    // Filter out internal DropVariable statements and execute them.
    statements.filter(
        dropVar => dropVar.isInstanceOf[SingleStatementExec]
        && dropVar.asInstanceOf[SingleStatementExec].parsedPlan.isInstanceOf[DropVariable]
        && dropVar.isInternal)
      .foreach(_.asInstanceOf[SingleStatementExec].execute(session))
  }

  /**
   * Check if the leave statement was used, if it is not used stop iterating surrounding
   * [[CompoundBodyExec]] and move iterator forward. If the label of the block matches the label of
   * the leave statement, mark the leave statement as used.
   * @param leave leave  statement
   * @return pass through the leave statement
   */
  private def handleLeave(leave: LeaveStatementExec): LeaveStatementExec = {
    if (!leave.used) {
      // Hard stop the iteration of the current begin/end block.
      stopIteration = true
      // Cleanup variables declared in the current block.
      cleanup()
      // If label of the block matches the label of the leave statement,
      // mark the leave statement as used. label can be None in case of a
      // CompoundBody inside loop or if/else structure. In such cases,
      // loop will have its own label to be matched by leave statement.
      if (label.isDefined) {
        leave.used = label.get.equals(leave.label)
      } else {
        leave.used = false
      }
    }
    curr = if (localIterator.hasNext) Some(localIterator.next()) else None
    leave
  }

  private var localIterator: Iterator[CompoundStatementExec] = statements.iterator
  private var curr: Option[CompoundStatementExec] =
    if (localIterator.hasNext) Some(localIterator.next()) else None

  // Flag to stop the iteration of the current begin/end block.
  // It is set to true when non-consumed leave statement is encountered.
  private var stopIteration: Boolean = false

  // Statement to return to after handling the error with continue handler.
  private var returnHere: Option[CompoundStatementExec] = None

  def getTreeIterator: Iterator[CompoundStatementExec] = treeIterator

  override def reset(): Unit = {
    statements.foreach(_.reset())
    localIterator = statements.iterator
    curr = if (localIterator.hasNext) Some(localIterator.next()) else None
  }

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
        (localIterator.hasNext || childHasNext || returnHere.isDefined) && !stopIteration
      }

      @scala.annotation.tailrec
      override def next(): CompoundStatementExec = {
        curr match {
          case None => throw SparkException.internalError(
            "No more elements to iterate through in the current SQL compound statement.")
          case Some(leave: LeaveStatementExec) =>
            handleLeave(leave)
          case Some(statement: LeafStatementExec) =>
            statement.execute(session)  // Execute the leaf statement
            curr = if (localIterator.hasNext) Some(localIterator.next()) else None
            handleError(statement)  // Handle error if raised
          case Some(body: NonLeafStatementExec) =>
            if (body.getTreeIterator.hasNext) {
              val statement = body.getTreeIterator.next() // Get next statement from the child node
              statement match {
                case leave: LeaveStatementExec =>
                  handleLeave(leave)
                case leafStatement: LeafStatementExec =>
                  // This check is done to handle error in surrounding begin/end block
                  // if it was not handled in the nested block.
                  handleError(leafStatement)
                case nonLeafStatement: NonLeafStatementExec => nonLeafStatement
              }
            } else {
              if (returnHere.isDefined) {
                curr = returnHere
                returnHere = None
              } else {
                curr = if (localIterator.hasNext) Some(localIterator.next()) else None
              }
              next()
            }
          case _ => throw SparkException.internalError(
            "Unknown statement type encountered during SQL script interpretation.")
        }
      }
    }
}

class ErrorHandlerExec(val body: CompoundBodyExec) extends NonLeafStatementExec {

  override def getTreeIterator: Iterator[CompoundStatementExec] = body.getTreeIterator

  override def reset(): Unit = body.reset()
}

/**
 * Executable node for Leave statement.
 * @param label
 *   Label of the [[CompoundBodyExec]] that should be exited.
 */
class LeaveStatementExec(val label: String) extends LeafStatementExec {

  var used: Boolean = false

  override def execute(session: SparkSession): Unit = ()

  override def reset(): Unit = used = false
}

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
 * @param session Spark session that SQL script is executed within.
 */
class WhileStatementExec(
    condition: SingleStatementExec,
    body: CompoundBodyExec,
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
            assert(curr.get.isInstanceOf[SingleStatementExec])
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
