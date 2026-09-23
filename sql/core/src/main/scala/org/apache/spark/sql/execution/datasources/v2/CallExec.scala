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

package org.apache.spark.sql.execution.datasources.v2

import scala.jdk.CollectionConverters.IteratorHasAsScala

import org.apache.spark.SparkException
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.{Attribute, Expression, GenericInternalRow}
import org.apache.spark.sql.catalyst.plans.logical.LocalRelation
import org.apache.spark.sql.catalyst.types.DataTypeUtils
import org.apache.spark.sql.connector.catalog.procedures.BoundProcedure
import org.apache.spark.sql.connector.read.{LocalScan, Scan}
import org.apache.spark.util.LazyTry
import org.apache.spark.util.ArrayImplicits._

/**
 * Physical plan node for the CALL statement.
 *
 * The bound procedure is invoked when this node is executed (at the command-execution phase),
 * not during analysis. A procedure's result schema is only known once it has been invoked --
 * [[BoundProcedure.call]] returns the result sets as [[Scan]]s whose schema is read afterwards --
 * so the procedure is invoked exactly once and both [[output]] and the result rows are derived
 * from the resulting scans.
 */
case class CallExec(
    procedure: BoundProcedure,
    args: Seq[Expression]) extends LeafV2CommandExec {

  // Invoke the procedure lazily and at most once, memoized for both `output` and `run`. `LazyTry`
  // caches a failed invocation too, so a procedure that throws is not retried (a plain `lazy val`
  // re-runs its initializer after an exception).
  private val lazyRelations = LazyTry {
    procedure.call(toInternalRow(args)).asScala.map(toRelation).toSeq
  }

  private def relations: Seq[LocalRelation] = lazyRelations.get

  override def output: Seq[Attribute] = relations match {
    case Nil => Nil
    case Seq(relation) => relation.output
    case _ =>
      throw SparkException.internalError("Multi-result procedures are temporarily not supported")
  }

  override protected def run(): Seq[InternalRow] = relations match {
    case Nil => Seq.empty
    case Seq(relation) => relation.data
    case _ =>
      throw SparkException.internalError("Multi-result procedures are temporarily not supported")
  }

  private def toRelation(scan: Scan): LocalRelation = scan match {
    case s: LocalScan =>
      val attrs = DataTypeUtils.toAttributes(s.readSchema)
      LocalRelation(attrs, s.rows.toImmutableArraySeq)
    case _ =>
      throw SparkException.internalError(
        s"Only local scans are temporarily supported as procedure output: ${scan.getClass.getName}")
  }

  private def toInternalRow(args: Seq[Expression]): InternalRow = {
    require(args.forall(_.foldable), "args must be foldable")
    new GenericInternalRow(args.map(_.eval()).toArray)
  }
}
