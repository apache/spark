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
import org.apache.spark.sql.catalyst.expressions.codegen.{GenerateSafeProjection, GenerateUnsafeProjection}
import org.apache.spark.sql.types.{DataType, StructType}

/**
 * A [[Projection]] that is calculated by calling the `eval` of each of the specified expressions.
 * @param expressions a sequence of expressions that determine the value of each column of the
 *                    output row.
 */
class InterpretedProjection(expressions: Seq[Expression]) extends Projection {
  def this(expressions: Seq[Expression], inputSchema: Seq[Attribute]) =
    this(expressions.map(BindReferences.bindReference(_, inputSchema)))

  expressions.foreach(_.foreach {
    case n: Nondeterministic => n.setInitialValues()
    case _ =>
  })

  // null check is required for when Kryo invokes the no-arg constructor.
  protected val exprArray = if (expressions != null) expressions.toArray else null

  def apply(input: InternalRow): InternalRow = {
    val outputArray = new Array[Any](exprArray.length)
    var i = 0
    while (i < exprArray.length) {
      outputArray(i) = exprArray(i).eval(input)
      i += 1
    }
    new GenericInternalRow(outputArray)
  }

  override def toString(): String = s"Row => [${exprArray.mkString(",")}]"
}

/**
 * A [[MutableProjection]] that is calculated by calling `eval` on each of the specified
 * expressions.
 * @param expressions a sequence of expressions that determine the value of each column of the
 *                    output row.
 */
case class InterpretedMutableProjection(expressions: Seq[Expression]) extends MutableProjection {
  def this(expressions: Seq[Expression], inputSchema: Seq[Attribute]) =
    this(expressions.map(BindReferences.bindReference(_, inputSchema)))

  private[this] val buffer = new Array[Any](expressions.size)

  expressions.foreach(_.foreach {
    case n: Nondeterministic => n.setInitialValues()
    case _ =>
  })

  private[this] val exprArray = expressions.toArray
  private[this] var mutableRow: MutableRow = new GenericMutableRow(exprArray.length)
  def currentValue: InternalRow = mutableRow

  override def target(row: MutableRow): MutableProjection = {
    mutableRow = row
    this
  }

  override def apply(input: InternalRow): InternalRow = {
    var i = 0
    while (i < exprArray.length) {
      // Store the result into buffer first, to make the projection atomic (needed by aggregation)
      buffer(i) = exprArray(i).eval(input)
      i += 1
    }
    i = 0
    while (i < exprArray.length) {
      mutableRow(i) = buffer(i)
      i += 1
    }
    mutableRow
  }
}

/**
 * A projection that returns UnsafeRow.
 */
abstract class UnsafeProjection extends Projection {
  override def apply(row: InternalRow): UnsafeRow
}

object UnsafeProjection {

  /**
   * Returns an UnsafeProjection for given StructType.
   */
  def create(schema: StructType): UnsafeProjection = create(schema.fields.map(_.dataType))

  /**
   * Returns an UnsafeProjection for given Array of DataTypes.
   */
  def create(fields: Array[DataType]): UnsafeProjection = {
    create(fields.zipWithIndex.map(x => new BoundReference(x._2, x._1, true)))
  }

  /**
   * Returns an UnsafeProjection for given sequence of Expressions (bounded).
   */
  def create(exprs: Seq[Expression]): UnsafeProjection = {
    val unsafeExprs = exprs.map(_ transform {
      case CreateStruct(children) => CreateStructUnsafe(children)
      case CreateNamedStruct(children) => CreateNamedStructUnsafe(children)
    })
    GenerateUnsafeProjection.generate(unsafeExprs)
  }

  def create(expr: Expression): UnsafeProjection = create(Seq(expr))

  /**
   * Returns an UnsafeProjection for given sequence of Expressions, which will be bound to
   * `inputSchema`.
   */
  def create(exprs: Seq[Expression], inputSchema: Seq[Attribute]): UnsafeProjection = {
    create(exprs.map(BindReferences.bindReference(_, inputSchema)))
  }

  /**
   * Same as other create()'s but allowing enabling/disabling subexpression elimination.
   * TODO: refactor the plumbing and clean this up.
   */
  def create(
      exprs: Seq[Expression],
      inputSchema: Seq[Attribute],
      subexpressionEliminationEnabled: Boolean): UnsafeProjection = {
    val e = exprs.map(BindReferences.bindReference(_, inputSchema))
      .map(_ transform {
        case CreateStruct(children) => CreateStructUnsafe(children)
        case CreateNamedStruct(children) => CreateNamedStructUnsafe(children)
    })
    GenerateUnsafeProjection.generate(e, subexpressionEliminationEnabled)
  }
}

/**
 * A projection that could turn UnsafeRow into GenericInternalRow
 */
object FromUnsafeProjection {

  /**
   * Returns a Projection for given StructType.
   */
  def apply(schema: StructType): Projection = {
    apply(schema.fields.map(_.dataType))
  }

  /**
   * Returns an UnsafeProjection for given Array of DataTypes.
   */
  def apply(fields: Seq[DataType]): Projection = {
    create(fields.zipWithIndex.map(x => new BoundReference(x._2, x._1, true)))
  }

  /**
   * Returns a Projection for given sequence of Expressions (bounded).
   */
  private def create(exprs: Seq[Expression]): Projection = {
    GenerateSafeProjection.generate(exprs)
  }
}
