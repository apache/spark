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

package org.apache.spark.sql.catalyst.expressions

import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.BindReferences.bindReferences
import org.apache.spark.sql.catalyst.expressions.aggregate.NoOp
import org.apache.spark.sql.catalyst.util.UnsafeRowUtils.avoidSetNullAt
import org.apache.spark.sql.internal.SQLConf


/**
 * A [[MutableProjection]] that is calculated by calling `eval` on each of the specified
 * expressions.
 *
 * @param expressions a sequence of expressions that determine the value of each column of the
 *                    output row.
 */
class InterpretedMutableProjection(expressions: Seq[Expression]) extends MutableProjection {
  def this(expressions: Seq[Expression], inputSchema: Seq[Attribute]) =
    this(bindReferences(expressions, inputSchema))

  private[this] val subExprEliminationEnabled = SQLConf.get.subexpressionEliminationEnabled
  private[this] val exprs = prepareExpressions(expressions, subExprEliminationEnabled)

  private[this] val buffer = new Array[Any](expressions.size)

  override def initialize(partitionIndex: Int): Unit = {
    exprs.foreach(_.foreach {
      case n: Nondeterministic => n.initialize(partitionIndex)
      case _ =>
    })
  }

  private[this] val validExprs = expressions.zipWithIndex.filter {
    case (NoOp, _) => false
    case _ => true
  }
  private[this] var mutableRow: InternalRow = new GenericInternalRow(expressions.size)
  def currentValue: InternalRow = mutableRow

  override def target(row: InternalRow): MutableProjection = {
    // If `mutableRow` is `UnsafeRow`, `MutableProjection` accepts mutable types only
    require(!row.isInstanceOf[UnsafeRow] ||
      validExprs.forall { case (e, _) => UnsafeRow.isMutable(e.dataType) },
      "MutableProjection cannot use UnsafeRow for output data types: " +
        validExprs.map(_._1.dataType).filterNot(UnsafeRow.isMutable)
          .map(_.catalogString).mkString(", "))
    mutableRow = row
    this
  }

  private[this] val fieldWriters: Array[Any => Unit] = validExprs.map { case (e, i) =>
    val writer = InternalRow.getWriter(i, e.dataType)
    if (!e.nullable || avoidSetNullAt(e.dataType)) {
      (v: Any) => writer(mutableRow, v)
    } else {
      (v: Any) => {
        if (v == null) {
          mutableRow.setNullAt(i)
        } else {
          writer(mutableRow, v)
        }
      }
    }
  }.toArray

  override def apply(input: InternalRow): InternalRow = {
    if (subExprEliminationEnabled) {
      runtime.setInput(input)
    }

    var i = 0
    while (i < validExprs.length) {
      val (_, ordinal) = validExprs(i)
      // Store the result into buffer first, to make the projection atomic (needed by aggregation)
      buffer(ordinal) = exprs(ordinal).eval(input)
      i += 1
    }
    i = 0
    while (i < validExprs.length) {
      val (_, ordinal) = validExprs(i)
      fieldWriters(i)(buffer(ordinal))
      i += 1
    }
    mutableRow
  }
}

/**
 * Helper functions for creating an [[InterpretedMutableProjection]].
 */
object InterpretedMutableProjection {

  /**
   * Returns a [[MutableProjection]] for given sequence of bound Expressions.
   */
  def createProjection(exprs: Seq[Expression]): MutableProjection = {
    new InterpretedMutableProjection(exprs)
  }
}
