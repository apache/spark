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

package org.apache.spark.sql.catalyst.analysis

import scala.jdk.CollectionConverters.IteratorHasAsScala

import org.apache.spark.SparkException
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.{Expression, GenericInternalRow}
import org.apache.spark.sql.catalyst.plans.logical.{Call, LocalRelation, LogicalPlan, MultiResult}
import org.apache.spark.sql.catalyst.rules.Rule
import org.apache.spark.sql.catalyst.types.DataTypeUtils
import org.apache.spark.sql.connector.catalog.procedures.BoundProcedure
import org.apache.spark.sql.connector.read.{LocalScan, Scan}
import org.apache.spark.util.ArrayImplicits._

class InvokeProcedures(session: SparkSession) extends Rule[LogicalPlan] {

  override def apply(plan: LogicalPlan): LogicalPlan = plan resolveOperators {
    case c: Call if c.resolved && c.bound && c.execute && c.checkArgTypes().isSuccess =>
      session.sessionState.optimizer.execute(c) match {
        case Call(ResolvedProcedure(_, _, procedure: BoundProcedure), args, _) =>
          invoke(procedure, args)
        case _ =>
          throw SparkException.internalError("Unexpected plan for optimized CALL statement")
      }
  }

  private def invoke(procedure: BoundProcedure, args: Seq[Expression]): LogicalPlan = {
    val input = toInternalRow(args)
    val scanIterator = procedure.call(input)
    val relations = scanIterator.asScala.map(toRelation).toSeq
    relations match {
      case Nil => LocalRelation(Nil)
      case Seq(relation) => relation
      case _ => MultiResult(relations)
    }
  }

  private def toRelation(scan: Scan): LogicalPlan = scan match {
    case s: LocalScan =>
      val attrs = DataTypeUtils.toAttributes(s.readSchema)
      val data = s.rows.toImmutableArraySeq
      LocalRelation(attrs, data)
    case _ =>
      throw SparkException.internalError(
        s"Only local scans are temporarily supported as procedure output: ${scan.getClass.getName}")
  }

  private def toInternalRow(args: Seq[Expression]): InternalRow = {
    require(args.forall(_.foldable), "args must be foldable")
    val values = args.map(_.eval()).toArray
    new GenericInternalRow(values)
  }
}
