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

import scala.collection.mutable

import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.analysis.TypeCheckResult
import org.apache.spark.sql.catalyst.expressions.codegen.{GeneratedExpressionCode, CodeGenContext}
import org.apache.spark.sql.catalyst.util.TypeUtils
import org.apache.spark.sql.types._

/**
 * Returns an Array containing the evaluation of all children expressions.
 */
case class CreateArray(children: Seq[Expression]) extends Expression {

  override def foldable: Boolean = children.forall(_.foldable)

  override def checkInputDataTypes(): TypeCheckResult =
    TypeUtils.checkForSameTypeInputExpr(children.map(_.dataType), "function array")

  override def dataType: DataType = {
    ArrayType(
      children.headOption.map(_.dataType).getOrElse(NullType),
      containsNull = children.exists(_.nullable))
  }

  override def nullable: Boolean = false

  override def eval(input: InternalRow): Any = {
    children.map(_.eval(input))
  }

  override def genCode(ctx: CodeGenContext, ev: GeneratedExpressionCode): String = {
    val arraySeqClass = classOf[mutable.ArraySeq[Any]].getName
    s"""
      boolean ${ev.isNull} = false;
      $arraySeqClass<Object> ${ev.primitive} = new $arraySeqClass<Object>(${children.size});
    """ +
      children.zipWithIndex.map { case (e, i) =>
        val eval = e.gen(ctx)
        eval.code + s"""
          if (${eval.isNull}) {
            ${ev.primitive}.update($i, null);
          } else {
            ${ev.primitive}.update($i, ${eval.primitive});
          }
         """
      }.mkString("\n")
  }

  override def prettyName: String = "array"
}

/**
 * Returns a Row containing the evaluation of all children expressions.
 */
case class CreateStruct(children: Seq[Expression]) extends Expression {

  override def foldable: Boolean = children.forall(_.foldable)

  override lazy val resolved: Boolean = childrenResolved

  override lazy val dataType: StructType = {
    val fields = children.zipWithIndex.map { case (child, idx) =>
      child match {
        case ne: NamedExpression =>
          StructField(ne.name, ne.dataType, ne.nullable, ne.metadata)
        case _ =>
          StructField(s"col${idx + 1}", child.dataType, child.nullable, Metadata.empty)
      }
    }
    StructType(fields)
  }

  override def nullable: Boolean = false

  override def eval(input: InternalRow): Any = {
    InternalRow(children.map(_.eval(input)): _*)
  }

  override def genCode(ctx: CodeGenContext, ev: GeneratedExpressionCode): String = {
    val rowClass = classOf[GenericMutableRow].getName
    s"""
      boolean ${ev.isNull} = false;
      final $rowClass ${ev.primitive} = new $rowClass(${children.size});
    """ +
      children.zipWithIndex.map { case (e, i) =>
        val eval = e.gen(ctx)
        eval.code + s"""
        if (${eval.isNull}) {
          ${ev.primitive}.update($i, null);
        } else {
          ${ev.primitive}.update($i, ${eval.primitive});
        }
       """
      }.mkString("\n")
  }

  override def prettyName: String = "struct"
}

/**
 * Creates a struct with the given field names and values
 *
 * @param children Seq(name1, val1, name2, val2, ...)
 */
case class CreateNamedStruct(children: Seq[Expression]) extends Expression {

  private lazy val (nameExprs, valExprs) =
    children.grouped(2).map { case Seq(name, value) => (name, value) }.toList.unzip

  private lazy val names = nameExprs.map(_.eval(EmptyRow).toString)

  override lazy val dataType: StructType = {
    val fields = names.zip(valExprs).map { case (name, valExpr) =>
      StructField(name, valExpr.dataType, valExpr.nullable, Metadata.empty)
    }
    StructType(fields)
  }

  override def foldable: Boolean = valExprs.forall(_.foldable)

  override def nullable: Boolean = false

  override def checkInputDataTypes(): TypeCheckResult = {
    if (children.size % 2 != 0) {
      TypeCheckResult.TypeCheckFailure(s"$prettyName expects an even number of arguments.")
    } else {
      val invalidNames =
        nameExprs.filterNot(e => e.foldable && e.dataType == StringType && !nullable)
      if (invalidNames.nonEmpty) {
        TypeCheckResult.TypeCheckFailure(
          s"Odd position only allow foldable and not-null StringType expressions, got :" +
            s" ${invalidNames.mkString(",")}")
      } else {
        TypeCheckResult.TypeCheckSuccess
      }
    }
  }

  override def eval(input: InternalRow): Any = {
    InternalRow(valExprs.map(_.eval(input)): _*)
  }

  override def genCode(ctx: CodeGenContext, ev: GeneratedExpressionCode): String = {
    val rowClass = classOf[GenericMutableRow].getName
    s"""
      boolean ${ev.isNull} = false;
      final $rowClass ${ev.primitive} = new $rowClass(${valExprs.size});
    """ +
      valExprs.zipWithIndex.map { case (e, i) =>
        val eval = e.gen(ctx)
        eval.code + s"""
        if (${eval.isNull}) {
          ${ev.primitive}.update($i, null);
        } else {
          ${ev.primitive}.update($i, ${eval.primitive});
        }
       """
      }.mkString("\n")
  }

  override def prettyName: String = "named_struct"
}
