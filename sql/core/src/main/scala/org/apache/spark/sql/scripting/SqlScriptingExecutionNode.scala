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
import org.apache.spark.sql.{Dataset, SparkSession}
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan
import org.apache.spark.sql.types.BooleanType

sealed trait CompoundStatementExec extends Logging {
  val isInternal: Boolean = false
  def reset(): Unit
}

trait LeafStatementExec extends CompoundStatementExec

trait NonLeafStatementExec extends CompoundStatementExec with Iterator[CompoundStatementExec]

class SparkStatementWithPlanExec(
    var parsedPlan: LogicalPlan,
    val sourceStart: Int,
    val sourceEnd: Int,
    override val isInternal : Boolean)
  extends LeafStatementExec {

  var consumed = false
  override def reset(): Unit = consumed = false
  def getText(batch: String): String = batch.substring(sourceStart, sourceEnd)
}

class CompoundNestedStatementIteratorExec(collection: List[CompoundStatementExec])
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

class CompoundBodyExec(statements: List[CompoundStatementExec], label: String = "")
  extends CompoundNestedStatementIteratorExec(statements)

// Evaluators
trait StatementBooleanEvaluator {
  def eval(statement: LeafStatementExec): Boolean
}

case class DataFrameEvaluator(session: SparkSession) extends StatementBooleanEvaluator {
  override def eval(statement: LeafStatementExec): Boolean = statement match {
    case sparkStatement: SparkStatementWithPlanExec =>
      assert(!sparkStatement.consumed)
      sparkStatement.consumed = true
      val df = Dataset.ofRows(session, sparkStatement.parsedPlan)

      // Check schema first - if it does not match, we return false for sure.
      if (df.schema.fields.length > 1 || df.schema.fields(0).dataType != BooleanType) {
        return false
      }

      val res = df.limit(2).collect()
      // More than one rows returned - return false.
      if (res.length > 1) {
        return false
      }

      // Return boolean value from single row - single column result.
      res(0).getBoolean(0)
    case _ => false
  }
}