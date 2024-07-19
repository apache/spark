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
 */
class SingleStatementExec(
    var parsedPlan: LogicalPlan,
    override val origin: Origin,
    override val isInternal: Boolean,
    val collectResult: Boolean = true)
  extends LeafStatementExec with WithOrigin {

  /**
   * Whether this statement has been executed during the interpretation phase.
   * Example: Statements in conditions of If/Else, While, etc.
   */
  var isExecuted = false

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

  override def reset(): Unit = isExecuted = false

  def execute(session: SparkSession): Unit = {
    isExecuted = true
    val rows = Some(Dataset.ofRows(session, parsedPlan).collect())
    if (collectResult) {
      result = rows
    }
  }
}

/**
 * Abstract class for all statements that contain nested statements.
 * Implements recursive iterator logic over all child execution nodes.
 * @param collection
 *   Collection of child execution nodes.
 */
abstract class CompoundNestedStatementIteratorExec(collection: Seq[CompoundStatementExec])
  extends NonLeafStatementExec {

  protected var localIterator: Iterator[CompoundStatementExec] = collection.iterator
  protected var curr: Option[CompoundStatementExec] =
    if (localIterator.hasNext) Some(localIterator.next()) else None

  protected lazy val treeIterator: Iterator[CompoundStatementExec] = null

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
 * @param session
 *   Spark session.
 */
class CompoundBodyExec(
      statements: Seq[CompoundStatementExec],
      session: SparkSession)
  extends CompoundNestedStatementIteratorExec(statements) {

  override protected lazy val treeIterator: Iterator[CompoundStatementExec] =
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
          case Some(statement: SingleStatementExec) =>
            curr = if (localIterator.hasNext) Some(localIterator.next()) else None
            if (!statement.isExecuted) {
              statement.execute(session)  // Execute the leaf statement
            }
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
}
