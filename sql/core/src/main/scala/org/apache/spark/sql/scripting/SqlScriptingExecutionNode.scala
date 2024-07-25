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
import org.apache.spark.sql.{Dataset, Row, SparkSession}
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan
import org.apache.spark.sql.catalyst.trees.{Origin, WithOrigin}

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

  /**
  * Execute the statement.
  * @param session Spark session.
  */
  def execute(session: SparkSession): Unit

  /**
   * Whether an error was raised during the execution of this statement.
   */
  var raisedError: Boolean = false

  /**
   * Error state of the statement.
   */
  var errorState: Option[String] = None

  /**
   * Error raised during statement execution.
   */
  var error: Option[SparkThrowable] = None

  /**
   * Throwable to rethrow after the statement execution if the error is not handled.
   */
  var rethrow: Option[Throwable] = None
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

  /**
   * Data returned after execution.
   */
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
    raisedError = false
    errorState = None
    error = None
    result = None // Should we do this?
  }

  def execute(session: SparkSession): Unit = {
    try {
      val rows = Some(Dataset.ofRows(session, parsedPlan).collect())
      if (shouldCollectResult) {
        result = rows
      }
    } catch {
      case e: SparkThrowable =>
        raisedError = true
        errorState = Some(e.getSqlState)
        error = Some(e)
        e match {
          case throwable: Throwable =>
            rethrow = Some(throwable)
          case _ =>
        }
      case throwable: Throwable =>
        raisedError = true
        errorState = Some("UNKNOWN")
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
      label: Option[String],
      statements: Seq[CompoundStatementExec],
      conditionHandlerMap: mutable.HashMap[String, ErrorHandlerExec] = mutable.HashMap(),
      session: SparkSession)
  extends NonLeafStatementExec {

  private def getHandler(condition: String): Option[ErrorHandlerExec] = {
    var ret = conditionHandlerMap.get(condition)
    if (ret.isEmpty) {
      ret = conditionHandlerMap.get("UNKNOWN")
    }
    ret
  }

  /**
   * Handle error raised during the execution of the statement.
   * @param statement statement that possibly raised the error
   * @return pass through the statement
   */
  private def handleError(statement: LeafStatementExec): CompoundStatementExec = {
    if (statement.raisedError) {
      getHandler(statement.errorState.get).foreach { handler =>
        statement.reset() // Clear all flags and result
        handler.reset()
        curr = Some(handler.getHandlerBody)
        return handler.getHandlerBody
      }
    }
    statement
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
      // Hard stop the iteration of the current begin/end block
      stopIteration = true
      // If label of the block matches the label of the leave statement,
      // mark the leave statement as used
      if (label.getOrElse("").equals(leave.getLabel)) {
        leave.used = true
      }
    }
    curr = if (localIterator.hasNext) Some(localIterator.next()) else None
    leave
  }

  private var localIterator: Iterator[CompoundStatementExec] = statements.iterator
  private var curr: Option[CompoundStatementExec] =
    if (localIterator.hasNext) Some(localIterator.next()) else None
  private var stopIteration: Boolean = false  // hard stop iteration flag

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
        (localIterator.hasNext || childHasNext) && !stopIteration
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
            if (!statement.raisedError) {
              curr = if (localIterator.hasNext) Some(localIterator.next()) else None
            }
            handleError(statement)  // Handle error if raised
          case Some(body: NonLeafStatementExec) =>
            if (body.getTreeIterator.hasNext) {
              val statement = body.getTreeIterator.next() // Get next statement from the child node
              statement match {
                case leave: LeaveStatementExec =>
                  handleLeave(leave)
                case leafStatement: LeafStatementExec =>
                  // This check is done to handler error in surrounding begin/end block
                  // if it was not handled in the nested block
                  handleError(leafStatement)  // Handle error if raised
                case nonLeafStatement: NonLeafStatementExec => nonLeafStatement
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
}

class ErrorHandlerExec(body: CompoundBodyExec) extends NonLeafStatementExec {

  override def getTreeIterator: Iterator[CompoundStatementExec] = body.getTreeIterator

  def getHandlerBody: CompoundBodyExec = body

  override def reset(): Unit = body.reset()
}

/**
 * Executable node for Leave statement.
 * @param label
 *   Label of the [[CompoundBodyExec]] that should be exited.
 */
class LeaveStatementExec(val label: String) extends LeafStatementExec {

  var used: Boolean = false

  def getLabel: String = label

  override def execute(session: SparkSession): Unit = ()

  override def reset(): Unit = used = false
}
