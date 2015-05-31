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

import org.apache.spark.sql.types._


/**
 * Returns an Array containing the evaluation of all children expressions.
 */
case class CreateArray(children: Seq[Expression]) extends Expression {
  override type EvaluatedType = Any

  override def foldable: Boolean = children.forall(_.foldable)

  lazy val childTypes = children.map(_.dataType).distinct

  override lazy val resolved =
    childrenResolved && childTypes.size <= 1

  override def dataType: DataType = {
    assert(resolved, s"Invalid dataType of mixed ArrayType ${childTypes.mkString(",")}")
    ArrayType(
      childTypes.headOption.getOrElse(NullType),
      containsNull = children.exists(_.nullable))
  }

  override def nullable: Boolean = false

  override def eval(input: Row): Any = {
    children.map(_.eval(input))
  }

  override def toString: String = s"Array(${children.mkString(",")})"
}

/**
 * Returns a Row containing the evaluation of all children expressions.
 * TODO: [[CreateStruct]] does not support codegen.
 */
case class CreateStruct(children: Seq[NamedExpression]) extends Expression {
  override type EvaluatedType = Row

  override def foldable: Boolean = children.forall(_.foldable)

  override lazy val resolved: Boolean = childrenResolved

  override lazy val dataType: StructType = {
    assert(resolved,
      s"CreateStruct contains unresolvable children: ${children.filterNot(_.resolved)}.")
    val fields = children.map { child =>
      StructField(child.name, child.dataType, child.nullable, child.metadata)
    }
    StructType(fields)
  }

  override def nullable: Boolean = false

  override def eval(input: Row): EvaluatedType = {
    Row(children.map(_.eval(input)): _*)
  }
}
