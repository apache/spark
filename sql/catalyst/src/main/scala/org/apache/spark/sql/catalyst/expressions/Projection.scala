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
import org.apache.spark.sql.catalyst.expressions.codegen.{GenerateMutableProjection, GenerateSafeProjection, GenerateUnsafeProjection}
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.types.{DataType, StructType}
import org.apache.spark.util.ArrayImplicits._

/**
 * A [[Projection]] that is calculated by calling the `eval` of each of the specified expressions.
 *
 * @param expressions a sequence of expressions that determine the value of each column of the
 *                    output row.
 */
class InterpretedProjection(expressions: Seq[Expression]) extends Projection {
  def this(expressions: Seq[Expression], inputSchema: Seq[Attribute]) =
    this(bindReferences(expressions, inputSchema))

  // null check is required for when Kryo invokes the no-arg constructor.
  protected val exprArray = if (expressions != null) {
    prepareExpressions(expressions, subExprEliminationEnabled = false).toArray
  } else {
    null
  }

  override def initialize(partitionIndex: Int): Unit = {
    initializeExprs(exprArray.toImmutableArraySeq, partitionIndex)
  }

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
 * Converts a [[InternalRow]] to another Row given a sequence of expression that define each
 * column of the new row. If the schema of the input row is specified, then the given expression
 * will be bound to that schema.
 *
 * In contrast to a normal projection, a MutableProjection reuses the same underlying row object
 * each time an input row is added.  This significantly reduces the cost of calculating the
 * projection, but means that it is not safe to hold on to a reference to a [[InternalRow]] after
 * `next()` has been called on the [[Iterator]] that produced it. Instead, the user must call
 * `InternalRow.copy()` and hold on to the returned [[InternalRow]] before calling `next()`.
 */
abstract class MutableProjection extends Projection {
  def currentValue: InternalRow

  /** Uses the given row to store the output of the projection. */
  def target(row: InternalRow): MutableProjection
}

/**
 * The factory object for `MutableProjection`.
 */
object MutableProjection
    extends CodeGeneratorWithInterpretedFallback[Seq[Expression], MutableProjection] {

  override protected def createCodeGeneratedObject(in: Seq[Expression]): MutableProjection = {
    GenerateMutableProjection.generate(in, SQLConf.get.subexpressionEliminationEnabled)
  }

  override protected def createInterpretedObject(in: Seq[Expression]): MutableProjection = {
    InterpretedMutableProjection.createProjection(in)
  }

  /**
   * Returns a MutableProjection for given sequence of bound Expressions.
   */
  def create(exprs: Seq[Expression]): MutableProjection = {
    createObject(exprs)
  }

  /**
   * Returns a MutableProjection for given sequence of Expressions, which will be bound to
   * `inputSchema`.
   */
  def create(exprs: Seq[Expression], inputSchema: Seq[Attribute]): MutableProjection = {
    create(bindReferences(exprs, inputSchema))
  }
}

/**
 * A projection that returns UnsafeRow.
 *
 * CAUTION: the returned projection object should *not* be assumed to be thread-safe.
 */
abstract class UnsafeProjection extends Projection {
  override def apply(row: InternalRow): UnsafeRow
}

/**
 * The factory object for `UnsafeProjection`.
 */
object UnsafeProjection
    extends CodeGeneratorWithInterpretedFallback[Seq[Expression], UnsafeProjection] {

  override protected def createCodeGeneratedObject(in: Seq[Expression]): UnsafeProjection = {
    GenerateUnsafeProjection.generate(in, SQLConf.get.subexpressionEliminationEnabled)
  }

  override protected def createInterpretedObject(in: Seq[Expression]): UnsafeProjection = {
    InterpretedUnsafeProjection.createProjection(in)
  }

  /**
   * Returns an UnsafeProjection for given StructType.
   *
   * CAUTION: the returned projection object is *not* thread-safe.
   */
  def create(schema: StructType): UnsafeProjection = create(schema.fields.map(_.dataType))

  /**
   * Returns an UnsafeProjection for given Array of DataTypes.
   *
   * CAUTION: the returned projection object is *not* thread-safe.
   */
  def create(fields: Array[DataType]): UnsafeProjection = {
    create(fields.zipWithIndex.map(x => BoundReference(x._2, x._1, true)).toImmutableArraySeq)
  }

  /**
   * Returns an UnsafeProjection for given sequence of bound Expressions.
   */
  def create(exprs: Seq[Expression]): UnsafeProjection = {
    createObject(exprs)
  }

  def create(expr: Expression): UnsafeProjection = create(Seq(expr))

  /**
   * Returns an UnsafeProjection for given sequence of Expressions, which will be bound to
   * `inputSchema`.
   */
  def create(exprs: Seq[Expression], inputSchema: Seq[Attribute]): UnsafeProjection = {
    create(bindReferences(exprs, inputSchema))
  }
}

/**
 * A projection that could turn UnsafeRow into GenericInternalRow
 */
object SafeProjection extends CodeGeneratorWithInterpretedFallback[Seq[Expression], Projection] {

  override protected def createCodeGeneratedObject(in: Seq[Expression]): Projection = {
    GenerateSafeProjection.generate(in)
  }

  override protected def createInterpretedObject(in: Seq[Expression]): Projection = {
    InterpretedSafeProjection.createProjection(in)
  }

  /**
   * Returns a SafeProjection for given StructType.
   */
  def create(schema: StructType): Projection = create(schema.fields.map(_.dataType))

  /**
   * Returns a SafeProjection for given Array of DataTypes.
   */
  def create(fields: Array[DataType]): Projection = {
    createObject(fields.zipWithIndex.map(x => BoundReference(x._2, x._1, true)).toImmutableArraySeq)
  }

  /**
   * Returns a SafeProjection for given sequence of Expressions (bounded).
   */
  def create(exprs: Seq[Expression]): Projection = {
    createObject(exprs)
  }

  /**
   * Returns a SafeProjection for given sequence of Expressions, which will be bound to
   * `inputSchema`.
   */
  def create(exprs: Seq[Expression], inputSchema: Seq[Attribute]): Projection = {
    create(bindReferences(exprs, inputSchema))
  }
}
