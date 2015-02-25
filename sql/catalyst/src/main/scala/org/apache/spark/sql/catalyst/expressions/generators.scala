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

import scala.collection.Map

import org.apache.spark.sql.catalyst.trees
import org.apache.spark.sql.types._

/**
 * An expression that produces zero or more rows given a single input row.
 *
 * Generators produce multiple output rows instead of a single value like other expressions,
 * and thus they must have a schema to associate with the rows that are output.
 *
 * However, unlike row producing relational operators, which are either leaves or determine their
 * output schema functionally from their input, generators can contain other expressions that
 * might result in their modification by rules.  This structure means that they might be copied
 * multiple times after first determining their output schema. If a new output schema is created for
 * each copy references up the tree might be rendered invalid. As a result generators must
 * instead define a function `makeOutput` which is called only once when the schema is first
 * requested.  The attributes produced by this function will be automatically copied anytime rules
 * result in changes to the Generator or its children.
 */
abstract class Generator extends Expression {
  self: Product =>

  override type EvaluatedType = TraversableOnce[Row]

  override lazy val dataType =
    ArrayType(StructType(output.map(a => StructField(a.name, a.dataType, a.nullable, a.metadata))))

  override def nullable = false

  /**
   * Should be overridden by specific generators.  Called only once for each instance to ensure
   * that rule application does not change the output schema of a generator.
   */
  protected def makeOutput(): Seq[Attribute]

  private var _output: Seq[Attribute] = null

  def output: Seq[Attribute] = {
    if (_output == null) {
      _output = makeOutput()
    }
    _output
  }

  /** Should be implemented by child classes to perform specific Generators. */
  override def eval(input: Row): TraversableOnce[Row]

  /** Overridden `makeCopy` also copies the attributes that are produced by this generator. */
  override def makeCopy(newArgs: Array[AnyRef]): this.type = {
    val copy = super.makeCopy(newArgs)
    copy._output = _output
    copy
  }
}

/**
 * A generator that produces its output using the provided lambda function.
 */
case class UserDefinedGenerator(
    schema: Seq[Attribute],
    function: Row => TraversableOnce[Row],
    children: Seq[Expression])
  extends Generator{

  override protected def makeOutput(): Seq[Attribute] = schema

  override def eval(input: Row): TraversableOnce[Row] = {
    val inputRow = new InterpretedProjection(children)
    function(inputRow(input))
  }

  override def toString = s"UserDefinedGenerator(${children.mkString(",")})"
}

/**
 * Given an input array produces a sequence of rows for each value in the array.
 */
case class Explode(attributeNames: Seq[String], child: Expression)
  extends Generator with trees.UnaryNode[Expression] {

  override lazy val resolved =
    child.resolved &&
    (child.dataType.isInstanceOf[ArrayType] || child.dataType.isInstanceOf[MapType])

  private lazy val elementTypes = child.dataType match {
    case ArrayType(et, containsNull) => (et, containsNull) :: Nil
    case MapType(kt, vt, valueContainsNull) => (kt, false) :: (vt, valueContainsNull) :: Nil
  }

  // TODO: Move this pattern into Generator.
  protected def makeOutput() =
    if (attributeNames.size == elementTypes.size) {
      attributeNames.zip(elementTypes).map {
        case (n, (t, nullable)) => AttributeReference(n, t, nullable)()
      }
    } else {
      elementTypes.zipWithIndex.map {
        case ((t, nullable), i) => AttributeReference(s"c_$i", t, nullable)()
      }
    }

  override def eval(input: Row): TraversableOnce[Row] = {
    child.dataType match {
      case ArrayType(_, _) =>
        val inputArray = child.eval(input).asInstanceOf[Seq[Any]]
        if (inputArray == null) Nil else inputArray.map(v => new GenericRow(Array(v)))
      case MapType(_, _, _) =>
        val inputMap = child.eval(input).asInstanceOf[Map[Any,Any]]
        if (inputMap == null) Nil else inputMap.map { case (k,v) => new GenericRow(Array(k,v)) }
    }
  }

  override def toString() = s"explode($child)"
}
