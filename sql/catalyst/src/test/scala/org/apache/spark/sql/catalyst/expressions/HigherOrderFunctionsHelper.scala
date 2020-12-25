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

import org.apache.spark.SparkFunSuite
import org.apache.spark.sql.types._

trait HigherOrderFunctionsHelper {
  self: SparkFunSuite =>

  protected def createLambda(
    dt: DataType,
    nullable: Boolean,
    f: Expression => Expression): Expression = {
    val lv = NamedLambdaVariable("arg", dt, nullable)
    val function = f(lv)
    LambdaFunction(function, Seq(lv))
  }

  protected def createLambda(
    dt1: DataType,
    nullable1: Boolean,
    dt2: DataType,
    nullable2: Boolean,
    f: (Expression, Expression) => Expression): Expression = {
    val lv1 = NamedLambdaVariable("arg1", dt1, nullable1)
    val lv2 = NamedLambdaVariable("arg2", dt2, nullable2)
    val function = f(lv1, lv2)
    LambdaFunction(function, Seq(lv1, lv2))
  }

  protected def createLambda(
    dt1: DataType,
    nullable1: Boolean,
    dt2: DataType,
    nullable2: Boolean,
    dt3: DataType,
    nullable3: Boolean,
    f: (Expression, Expression, Expression) => Expression): Expression = {
    val lv1 = NamedLambdaVariable("arg1", dt1, nullable1)
    val lv2 = NamedLambdaVariable("arg2", dt2, nullable2)
    val lv3 = NamedLambdaVariable("arg3", dt3, nullable3)
    val function = f(lv1, lv2, lv3)
    LambdaFunction(function, Seq(lv1, lv2, lv3))
  }

  protected def validateBinding(
    e: Expression,
    argInfo: Seq[(DataType, Boolean)]): LambdaFunction = e match {
    case f: LambdaFunction =>
      assert(f.arguments.size === argInfo.size)
      f.arguments.zip(argInfo).foreach {
        case (arg, (dataType, nullable)) =>
          assert(arg.dataType === dataType)
          assert(arg.nullable === nullable)
      }
      f
  }

  protected def transform(expr: Expression, f: Expression => Expression): Expression = {
    val ArrayType(et, cn) = expr.dataType
    ArrayTransform(expr, createLambda(et, cn, f)).bind(validateBinding)
  }

  protected def transform(
    expr: Expression,
    f: (Expression, Expression) => Expression): Expression = {

    val ArrayType(et, cn) = expr.dataType
    ArrayTransform(expr, createLambda(et, cn, IntegerType, false, f)).bind(validateBinding)
  }

  protected def arraySort(expr: Expression): Expression = {
    arraySort(expr, ArraySort.comparator)
  }

  protected def arraySort(
    expr: Expression,
    f: (Expression, Expression) => Expression): Expression = {

    val ArrayType(et, cn) = expr.dataType
    ArraySort(expr, createLambda(et, cn, et, cn, f)).bind(validateBinding)
  }

  protected def filter(expr: Expression, f: Expression => Expression): Expression = {
    val ArrayType(et, cn) = expr.dataType
    ArrayFilter(expr, createLambda(et, cn, f)).bind(validateBinding)
  }

  protected def filter(expr: Expression, f: (Expression, Expression) => Expression): Expression = {
    val ArrayType(et, cn) = expr.dataType
    ArrayFilter(expr, createLambda(et, cn, IntegerType, false, f)).bind(validateBinding)
  }

  protected def mapFilter(
    expr: Expression,
    f: (Expression, Expression) => Expression): Expression = {

    val MapType(kt, vt, vcn) = expr.dataType
    MapFilter(expr, createLambda(kt, false, vt, vcn, f)).bind(validateBinding)
  }


  protected def transformKeys(
    expr: Expression,
    f: (Expression, Expression) => Expression): Expression = {

    val MapType(kt, vt, vcn) = expr.dataType
    TransformKeys(expr, createLambda(kt, false, vt, vcn, f)).bind(validateBinding)
  }

  protected def aggregate(
    expr: Expression,
    zero: Expression,
    merge: (Expression, Expression) => Expression,
    finish: Expression => Expression): Expression = {
    val ArrayType(et, cn) = expr.dataType
    val zeroType = zero.dataType
    ArrayAggregate(
      expr,
      zero,
      createLambda(zeroType, true, et, cn, merge),
      createLambda(zeroType, true, finish))
      .bind(validateBinding)
  }

  protected def aggregate(
    expr: Expression,
    zero: Expression,
    merge: (Expression, Expression) => Expression): Expression = {
    aggregate(expr, zero, merge, identity)
  }

  protected def transformValues(
    expr: Expression,
    f: (Expression, Expression) => Expression): Expression = {

    val MapType(kt, vt, vcn) = expr.dataType
    TransformValues(expr, createLambda(kt, false, vt, vcn, f)).bind(validateBinding)
  }

  protected def forall(expr: Expression, f: Expression => Expression): Expression = {
    val ArrayType(et, cn) = expr.dataType
    ArrayForAll(expr, createLambda(et, cn, f)).bind(validateBinding)
  }

  protected def exists(expr: Expression, f: Expression => Expression): Expression = {
    val ArrayType(et, cn) = expr.dataType
    ArrayExists(expr, createLambda(et, cn, f)).bind(validateBinding)
  }

}
