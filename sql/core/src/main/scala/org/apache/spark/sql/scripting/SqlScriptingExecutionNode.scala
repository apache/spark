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

import org.apache.spark.internal.Logging
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan
import org.apache.spark.sql.catalyst.trees.{Origin, WithOrigin}

/**
 * Trait for all SQL scripting execution nodes used during interpretation phase.
 */
sealed trait CompoundStatementExec extends Logging {
  /**
   * Whether the statement originates from the script or it is created during the interpretation.
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
 * Non-leaf node in the execution tree.
 * It is an iterator over executable child nodes.
 */
trait NonLeafStatementExec extends CompoundStatementExec with Iterator[CompoundStatementExec]

/**
 * Executable node for SingleStatement.
 * @param parsedPlan Logical plan of the parsed statement.
 * @param origin Origin descriptor for the statement.
 * @param isInternal Whether the statement originates from the script
 *                   or it is created during the interpretation.
 */
class SingleStatementExec(
    var parsedPlan: LogicalPlan,
    override val origin: Origin,
    override val isInternal : Boolean)
  extends LeafStatementExec
  with WithOrigin {

  /**
   * Whether this statement had to be executed during the interpretation phase.
   * Example: Statements in conditions of If/Else, While, etc.
   */
  var consumed = false

  /** @inheritdoc */
  override def reset(): Unit = consumed = false

  /** Get the SQL query text corresponding to this statement. */
  def getText(sqlScriptText: String): String = {
    if (origin.startIndex.isEmpty || origin.stopIndex.isEmpty) {
      return null
    }
    sqlScriptText.substring(origin.startIndex.get, origin.stopIndex.get + 1)
  }
}

/**
 * Abstract class for all statements that contain nested statements.
 * Implements recursive iterator logic over all child execution nodes.
 * @param collection Collection of child execution nodes.
 */
abstract class CompoundNestedStatementIteratorExec(collection: Seq[CompoundStatementExec])
  extends NonLeafStatementExec {

  var localIterator = collection.iterator
  var curr = if (localIterator.hasNext) Some(localIterator.next()) else None

  override def hasNext: Boolean = {
    val childHasNext = curr match {
      case Some(body: NonLeafStatementExec) => body.hasNext
      case Some(_: LeafStatementExec) => true
      case None => false
      case _ => throw new IllegalStateException("Unknown statement type")
    }
    localIterator.hasNext || childHasNext
  }

  override def next(): CompoundStatementExec = {
    curr match {
      case None => throw new IllegalStateException("No more elements")
      case Some(statement: LeafStatementExec) =>
        if (localIterator.hasNext) curr = Some(localIterator.next())
        else curr = None
        statement
      case Some(body: NonLeafStatementExec) =>
        if (body.hasNext) {
          body.next()
        } else {
          curr = if (localIterator.hasNext) Some(localIterator.next()) else None
          next()
        }
      case _ => throw new IllegalStateException("Unknown statement type")
    }
  }

  override def reset(): Unit = {
    collection.foreach(_.reset())
    localIterator = collection.iterator
    curr = if (localIterator.hasNext) Some(localIterator.next()) else None
  }
}

/**
 * Executable node for CompoundBody.
 * @param statements Executable nodes for nested statements within the CompoundBody.
 */
class CompoundBodyExec(statements: Seq[CompoundStatementExec])
  extends CompoundNestedStatementIteratorExec(statements)
