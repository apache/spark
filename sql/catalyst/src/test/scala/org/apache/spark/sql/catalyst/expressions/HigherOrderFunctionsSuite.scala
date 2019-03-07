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
import org.apache.spark.sql.catalyst.analysis.TypeCheckResult
import org.apache.spark.sql.catalyst.util.ArrayBasedMapData
import org.apache.spark.sql.types._

class HigherOrderFunctionsSuite extends SparkFunSuite with ExpressionEvalHelper {
  import org.apache.spark.sql.catalyst.dsl.expressions._

  private def createLambda(
      dt: DataType,
      nullable: Boolean,
      f: Expression => Expression): Expression = {
    val lv = NamedLambdaVariable("arg", dt, nullable)
    val function = f(lv)
    LambdaFunction(function, Seq(lv))
  }

  private def createLambda(
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

  private def createLambda(
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

  private def validateBinding(
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

  def transform(expr: Expression, f: Expression => Expression): Expression = {
    val ArrayType(et, cn) = expr.dataType
    ArrayTransform(expr, createLambda(et, cn, f)).bind(validateBinding)
  }

  def transform(expr: Expression, f: (Expression, Expression) => Expression): Expression = {
    val ArrayType(et, cn) = expr.dataType
    ArrayTransform(expr, createLambda(et, cn, IntegerType, false, f)).bind(validateBinding)
  }

  def filter(expr: Expression, f: Expression => Expression): Expression = {
    val ArrayType(et, cn) = expr.dataType
    ArrayFilter(expr, createLambda(et, cn, f)).bind(validateBinding)
  }

  def transformKeys(expr: Expression, f: (Expression, Expression) => Expression): Expression = {
    val MapType(kt, vt, vcn) = expr.dataType
    TransformKeys(expr, createLambda(kt, false, vt, vcn, f)).bind(validateBinding)
  }

  def aggregate(
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

  def aggregate(
      expr: Expression,
      zero: Expression,
      merge: (Expression, Expression) => Expression): Expression = {
    aggregate(expr, zero, merge, identity)
  }

  def transformValues(expr: Expression, f: (Expression, Expression) => Expression): Expression = {
    val MapType(kt, vt, vcn) = expr.dataType
    TransformValues(expr, createLambda(kt, false, vt, vcn, f)).bind(validateBinding)
  }

  test("ArrayTransform") {
    val ai0 = Literal.create(Seq(1, 2, 3), ArrayType(IntegerType, containsNull = false))
    val ai1 = Literal.create(Seq[Integer](1, null, 3), ArrayType(IntegerType, containsNull = true))
    val ain = Literal.create(null, ArrayType(IntegerType, containsNull = false))

    val plusOne: Expression => Expression = x => x + 1
    val plusIndex: (Expression, Expression) => Expression = (x, i) => x + i

    checkEvaluation(transform(ai0, plusOne), Seq(2, 3, 4))
    checkEvaluation(transform(ai0, plusIndex), Seq(1, 3, 5))
    checkEvaluation(transform(transform(ai0, plusIndex), plusOne), Seq(2, 4, 6))
    checkEvaluation(transform(ai1, plusOne), Seq(2, null, 4))
    checkEvaluation(transform(ai1, plusIndex), Seq(1, null, 5))
    checkEvaluation(transform(transform(ai1, plusIndex), plusOne), Seq(2, null, 6))
    checkEvaluation(transform(ain, plusOne), null)

    val as0 = Literal.create(Seq("a", "b", "c"), ArrayType(StringType, containsNull = false))
    val as1 = Literal.create(Seq("a", null, "c"), ArrayType(StringType, containsNull = true))
    val asn = Literal.create(null, ArrayType(StringType, containsNull = false))

    val repeatTwice: Expression => Expression = x => Concat(Seq(x, x))
    val repeatIndexTimes: (Expression, Expression) => Expression = (x, i) => StringRepeat(x, i)

    checkEvaluation(transform(as0, repeatTwice), Seq("aa", "bb", "cc"))
    checkEvaluation(transform(as0, repeatIndexTimes), Seq("", "b", "cc"))
    checkEvaluation(transform(transform(as0, repeatIndexTimes), repeatTwice),
      Seq("", "bb", "cccc"))
    checkEvaluation(transform(as1, repeatTwice), Seq("aa", null, "cc"))
    checkEvaluation(transform(as1, repeatIndexTimes), Seq("", null, "cc"))
    checkEvaluation(transform(transform(as1, repeatIndexTimes), repeatTwice),
      Seq("", null, "cccc"))
    checkEvaluation(transform(asn, repeatTwice), null)

    val aai = Literal.create(Seq(Seq(1, 2, 3), null, Seq(4, 5)),
      ArrayType(ArrayType(IntegerType, containsNull = false), containsNull = true))
    checkEvaluation(transform(aai, array => Cast(transform(array, plusOne), StringType)),
      Seq("[2, 3, 4]", null, "[5, 6]"))
    checkEvaluation(transform(aai, array => Cast(transform(array, plusIndex), StringType)),
      Seq("[1, 3, 5]", null, "[4, 6]"))
  }

  test("MapFilter") {
    def mapFilter(expr: Expression, f: (Expression, Expression) => Expression): Expression = {
      val MapType(kt, vt, vcn) = expr.dataType
      MapFilter(expr, createLambda(kt, false, vt, vcn, f)).bind(validateBinding)
    }
    val mii0 = Literal.create(Map(1 -> 0, 2 -> 10, 3 -> -1),
      MapType(IntegerType, IntegerType, valueContainsNull = false))
    val mii1 = Literal.create(Map(1 -> null, 2 -> 10, 3 -> null),
      MapType(IntegerType, IntegerType, valueContainsNull = true))
    val miin = Literal.create(null, MapType(IntegerType, IntegerType, valueContainsNull = false))

    val kGreaterThanV: (Expression, Expression) => Expression = (k, v) => k > v

    checkEvaluation(mapFilter(mii0, kGreaterThanV), Map(1 -> 0, 3 -> -1))
    checkEvaluation(mapFilter(mii1, kGreaterThanV), Map())
    checkEvaluation(mapFilter(miin, kGreaterThanV), null)

    val valueIsNull: (Expression, Expression) => Expression = (_, v) => v.isNull

    checkEvaluation(mapFilter(mii0, valueIsNull), Map())
    checkEvaluation(mapFilter(mii1, valueIsNull), Map(1 -> null, 3 -> null))
    checkEvaluation(mapFilter(miin, valueIsNull), null)

    val msi0 = Literal.create(Map("abcdf" -> 5, "abc" -> 10, "" -> 0),
      MapType(StringType, IntegerType, valueContainsNull = false))
    val msi1 = Literal.create(Map("abcdf" -> 5, "abc" -> 10, "" -> null),
      MapType(StringType, IntegerType, valueContainsNull = true))
    val msin = Literal.create(null, MapType(StringType, IntegerType, valueContainsNull = false))

    val isLengthOfKey: (Expression, Expression) => Expression = (k, v) => Length(k) === v

    checkEvaluation(mapFilter(msi0, isLengthOfKey), Map("abcdf" -> 5, "" -> 0))
    checkEvaluation(mapFilter(msi1, isLengthOfKey), Map("abcdf" -> 5))
    checkEvaluation(mapFilter(msin, isLengthOfKey), null)

    val mia0 = Literal.create(Map(1 -> Seq(0, 1, 2), 2 -> Seq(10), -3 -> Seq(-1, 0, -2, 3)),
      MapType(IntegerType, ArrayType(IntegerType), valueContainsNull = false))
    val mia1 = Literal.create(Map(1 -> Seq(0, 1, 2), 2 -> null, -3 -> Seq(-1, 0, -2, 3)),
      MapType(IntegerType, ArrayType(IntegerType), valueContainsNull = true))
    val mian = Literal.create(
      null, MapType(IntegerType, ArrayType(IntegerType), valueContainsNull = false))

    val customFunc: (Expression, Expression) => Expression = (k, v) => Size(v) + k > 3

    checkEvaluation(mapFilter(mia0, customFunc), Map(1 -> Seq(0, 1, 2)))
    checkEvaluation(mapFilter(mia1, customFunc), Map(1 -> Seq(0, 1, 2)))
    checkEvaluation(mapFilter(mian, customFunc), null)
  }

  test("ArrayFilter") {
    val ai0 = Literal.create(Seq(1, 2, 3), ArrayType(IntegerType, containsNull = false))
    val ai1 = Literal.create(Seq[Integer](1, null, 3), ArrayType(IntegerType, containsNull = true))
    val ain = Literal.create(null, ArrayType(IntegerType, containsNull = false))

    val isEven: Expression => Expression = x => x % 2 === 0
    val isNullOrOdd: Expression => Expression = x => x.isNull || x % 2 === 1

    checkEvaluation(filter(ai0, isEven), Seq(2))
    checkEvaluation(filter(ai0, isNullOrOdd), Seq(1, 3))
    checkEvaluation(filter(ai1, isEven), Seq.empty)
    checkEvaluation(filter(ai1, isNullOrOdd), Seq(1, null, 3))
    checkEvaluation(filter(ain, isEven), null)
    checkEvaluation(filter(ain, isNullOrOdd), null)

    val as0 =
      Literal.create(Seq("a0", "b1", "a2", "c3"), ArrayType(StringType, containsNull = false))
    val as1 = Literal.create(Seq("a", null, "c"), ArrayType(StringType, containsNull = true))
    val asn = Literal.create(null, ArrayType(StringType, containsNull = false))

    val startsWithA: Expression => Expression = x => x.startsWith("a")

    checkEvaluation(filter(as0, startsWithA), Seq("a0", "a2"))
    checkEvaluation(filter(as1, startsWithA), Seq("a"))
    checkEvaluation(filter(asn, startsWithA), null)

    val aai = Literal.create(Seq(Seq(1, 2, 3), null, Seq(4, 5)),
      ArrayType(ArrayType(IntegerType, containsNull = false), containsNull = true))
    checkEvaluation(transform(aai, ix => filter(ix, isNullOrOdd)),
      Seq(Seq(1, 3), null, Seq(5)))
  }

  test("ArrayExists") {
    def exists(expr: Expression, f: Expression => Expression): Expression = {
      val ArrayType(et, cn) = expr.dataType
      ArrayExists(expr, createLambda(et, cn, f)).bind(validateBinding)
    }

    val ai0 = Literal.create(Seq(1, 2, 3), ArrayType(IntegerType, containsNull = false))
    val ai1 = Literal.create(Seq[Integer](1, null, 3), ArrayType(IntegerType, containsNull = true))
    val ain = Literal.create(null, ArrayType(IntegerType, containsNull = false))

    val isEven: Expression => Expression = x => x % 2 === 0
    val isNullOrOdd: Expression => Expression = x => x.isNull || x % 2 === 1

    checkEvaluation(exists(ai0, isEven), true)
    checkEvaluation(exists(ai0, isNullOrOdd), true)
    checkEvaluation(exists(ai1, isEven), false)
    checkEvaluation(exists(ai1, isNullOrOdd), true)
    checkEvaluation(exists(ain, isEven), null)
    checkEvaluation(exists(ain, isNullOrOdd), null)

    val as0 =
      Literal.create(Seq("a0", "b1", "a2", "c3"), ArrayType(StringType, containsNull = false))
    val as1 = Literal.create(Seq(null, "b", "c"), ArrayType(StringType, containsNull = true))
    val asn = Literal.create(null, ArrayType(StringType, containsNull = false))

    val startsWithA: Expression => Expression = x => x.startsWith("a")

    checkEvaluation(exists(as0, startsWithA), true)
    checkEvaluation(exists(as1, startsWithA), false)
    checkEvaluation(exists(asn, startsWithA), null)

    val aai = Literal.create(Seq(Seq(1, 2, 3), null, Seq(4, 5)),
      ArrayType(ArrayType(IntegerType, containsNull = false), containsNull = true))
    checkEvaluation(transform(aai, ix => exists(ix, isNullOrOdd)),
      Seq(true, null, true))
  }

  test("ArrayAggregate") {
    val ai0 = Literal.create(Seq(1, 2, 3), ArrayType(IntegerType, containsNull = false))
    val ai1 = Literal.create(Seq[Integer](1, null, 3), ArrayType(IntegerType, containsNull = true))
    val ai2 = Literal.create(Seq.empty[Int], ArrayType(IntegerType, containsNull = false))
    val ain = Literal.create(null, ArrayType(IntegerType, containsNull = false))

    checkEvaluation(aggregate(ai0, 0, (acc, elem) => acc + elem, acc => acc * 10), 60)
    checkEvaluation(aggregate(ai1, 0, (acc, elem) => acc + coalesce(elem, 0), acc => acc * 10), 40)
    checkEvaluation(aggregate(ai2, 0, (acc, elem) => acc + elem, acc => acc * 10), 0)
    checkEvaluation(aggregate(ain, 0, (acc, elem) => acc + elem, acc => acc * 10), null)

    val as0 = Literal.create(Seq("a", "b", "c"), ArrayType(StringType, containsNull = false))
    val as1 = Literal.create(Seq("a", null, "c"), ArrayType(StringType, containsNull = true))
    val as2 = Literal.create(Seq.empty[String], ArrayType(StringType, containsNull = false))
    val asn = Literal.create(null, ArrayType(StringType, containsNull = false))

    checkEvaluation(aggregate(as0, "", (acc, elem) => Concat(Seq(acc, elem))), "abc")
    checkEvaluation(aggregate(as1, "", (acc, elem) => Concat(Seq(acc, coalesce(elem, "x")))), "axc")
    checkEvaluation(aggregate(as2, "", (acc, elem) => Concat(Seq(acc, elem))), "")
    checkEvaluation(aggregate(asn, "", (acc, elem) => Concat(Seq(acc, elem))), null)

    val aai = Literal.create(Seq[Seq[Integer]](Seq(1, 2, 3), null, Seq(4, 5)),
      ArrayType(ArrayType(IntegerType, containsNull = false), containsNull = true))
    checkEvaluation(
      aggregate(aai, 0,
        (acc, array) => coalesce(aggregate(array, acc, (acc, elem) => acc + elem), acc)),
      15)
  }

  test("TransformKeys") {
    val ai0 = Literal.create(
      create_map(1 -> 1, 2 -> 2, 3 -> 3, 4 -> 4),
      MapType(IntegerType, IntegerType, valueContainsNull = false))
    val ai1 = Literal.create(
      Map.empty[Int, Int],
      MapType(IntegerType, IntegerType, valueContainsNull = true))
    val ai2 = Literal.create(
      create_map(1 -> 1, 2 -> null, 3 -> 3),
      MapType(IntegerType, IntegerType, valueContainsNull = true))
    val ai3 = Literal.create(null, MapType(IntegerType, IntegerType, valueContainsNull = false))

    val plusOne: (Expression, Expression) => Expression = (k, v) => k + 1
    val plusValue: (Expression, Expression) => Expression = (k, v) => k + v
    val modKey: (Expression, Expression) => Expression = (k, v) => k % 3

    checkEvaluation(transformKeys(ai0, plusOne), create_map(2 -> 1, 3 -> 2, 4 -> 3, 5 -> 4))
    checkEvaluation(transformKeys(ai0, plusValue), create_map(2 -> 1, 4 -> 2, 6 -> 3, 8 -> 4))
    checkEvaluation(
      transformKeys(transformKeys(ai0, plusOne), plusValue),
      create_map(3 -> 1, 5 -> 2, 7 -> 3, 9 -> 4))
    // Duplicated map keys will be removed w.r.t. the last wins policy.
    checkEvaluation(transformKeys(ai0, modKey), create_map(1 -> 4, 2 -> 2, 0 -> 3))
    checkEvaluation(transformKeys(ai1, plusOne), Map.empty[Int, Int])
    checkEvaluation(transformKeys(ai1, plusOne), Map.empty[Int, Int])
    checkEvaluation(
      transformKeys(transformKeys(ai1, plusOne), plusValue), Map.empty[Int, Int])
    checkEvaluation(transformKeys(ai2, plusOne), create_map(2 -> 1, 3 -> null, 4 -> 3))
    checkEvaluation(
      transformKeys(transformKeys(ai2, plusOne), plusOne), create_map(3 -> 1, 4 -> null, 5 -> 3))
    checkEvaluation(transformKeys(ai3, plusOne), null)

    val as0 = Literal.create(
      create_map("a" -> "xy", "bb" -> "yz", "ccc" -> "zx"),
      MapType(StringType, StringType, valueContainsNull = false))
    val as1 = Literal.create(
      create_map("a" -> "xy", "bb" -> "yz", "ccc" -> null),
      MapType(StringType, StringType, valueContainsNull = true))
    val as2 = Literal.create(null,
      MapType(StringType, StringType, valueContainsNull = false))
    val as3 = Literal.create(Map.empty[StringType, StringType],
      MapType(StringType, StringType, valueContainsNull = true))

    val concatValue: (Expression, Expression) => Expression = (k, v) => Concat(Seq(k, v))
    val convertKeyToKeyLength: (Expression, Expression) => Expression =
      (k, v) => Length(k) + 1

    checkEvaluation(
      transformKeys(as0, concatValue), create_map("axy" -> "xy", "bbyz" -> "yz", "ccczx" -> "zx"))
    checkEvaluation(
      transformKeys(transformKeys(as0, concatValue), concatValue),
      create_map("axyxy" -> "xy", "bbyzyz" -> "yz", "ccczxzx" -> "zx"))
    checkEvaluation(transformKeys(as3, concatValue), Map.empty[String, String])
    checkEvaluation(
      transformKeys(transformKeys(as3, concatValue), convertKeyToKeyLength),
      Map.empty[Int, String])
    checkEvaluation(transformKeys(as0, convertKeyToKeyLength),
      create_map(2 -> "xy", 3 -> "yz", 4 -> "zx"))
    checkEvaluation(transformKeys(as1, convertKeyToKeyLength),
      create_map(2 -> "xy", 3 -> "yz", 4 -> null))
    checkEvaluation(transformKeys(as2, convertKeyToKeyLength), null)
    checkEvaluation(transformKeys(as3, convertKeyToKeyLength), Map.empty[Int, String])

    val ax0 = Literal.create(
      create_map(1 -> "x", 2 -> "y", 3 -> "z"),
      MapType(IntegerType, StringType, valueContainsNull = false))

    checkEvaluation(transformKeys(ax0, plusOne), create_map(2 -> "x", 3 -> "y", 4 -> "z"))

    // map key can't be map
    val makeMap: (Expression, Expression) => Expression = (k, v) => CreateMap(Seq(k, v))
    val map = transformKeys(ai0, makeMap)
    map.checkInputDataTypes() match {
      case TypeCheckResult.TypeCheckSuccess => fail("should not allow map as map key")
      case TypeCheckResult.TypeCheckFailure(msg) =>
        assert(msg.contains("The key of map cannot be/contain map"))
    }
  }

  test("TransformValues") {
    val ai0 = Literal.create(
      Map(1 -> 1, 2 -> 2, 3 -> 3),
      MapType(IntegerType, IntegerType, valueContainsNull = false))
    val ai1 = Literal.create(
      Map(1 -> 1, 2 -> null, 3 -> 3),
      MapType(IntegerType, IntegerType, valueContainsNull = true))
    val ai2 = Literal.create(
      Map.empty[Int, Int],
      MapType(IntegerType, IntegerType, valueContainsNull = true))
    val ai3 = Literal.create(null, MapType(IntegerType, IntegerType, valueContainsNull = false))

    val plusOne: (Expression, Expression) => Expression = (k, v) => v + 1
    val valueUpdate: (Expression, Expression) => Expression = (k, v) => k * k

    checkEvaluation(transformValues(ai0, plusOne), Map(1 -> 2, 2 -> 3, 3 -> 4))
    checkEvaluation(transformValues(ai0, valueUpdate), Map(1 -> 1, 2 -> 4, 3 -> 9))
    checkEvaluation(
      transformValues(transformValues(ai0, plusOne), valueUpdate), Map(1 -> 1, 2 -> 4, 3 -> 9))
    checkEvaluation(transformValues(ai1, plusOne), Map(1 -> 2, 2 -> null, 3 -> 4))
    checkEvaluation(transformValues(ai1, valueUpdate), Map(1 -> 1, 2 -> 4, 3 -> 9))
    checkEvaluation(
      transformValues(transformValues(ai1, plusOne), valueUpdate), Map(1 -> 1, 2 -> 4, 3 -> 9))
    checkEvaluation(transformValues(ai2, plusOne), Map.empty[Int, Int])
    checkEvaluation(transformValues(ai3, plusOne), null)

    val as0 = Literal.create(
      Map("a" -> "xy", "bb" -> "yz", "ccc" -> "zx"),
      MapType(StringType, StringType, valueContainsNull = false))
    val as1 = Literal.create(
      Map("a" -> "xy", "bb" -> null, "ccc" -> "zx"),
      MapType(StringType, StringType, valueContainsNull = true))
    val as2 = Literal.create(Map.empty[StringType, StringType],
      MapType(StringType, StringType, valueContainsNull = true))
    val as3 = Literal.create(null, MapType(StringType, StringType, valueContainsNull = true))

    val concatValue: (Expression, Expression) => Expression = (k, v) => Concat(Seq(k, v))
    val valueTypeUpdate: (Expression, Expression) => Expression =
      (k, v) => Length(v) + 1

    checkEvaluation(
      transformValues(as0, concatValue), Map("a" -> "axy", "bb" -> "bbyz", "ccc" -> "ccczx"))
    checkEvaluation(transformValues(as0, valueTypeUpdate),
      Map("a" -> 3, "bb" -> 3, "ccc" -> 3))
    checkEvaluation(
      transformValues(transformValues(as0, concatValue), concatValue),
      Map("a" -> "aaxy", "bb" -> "bbbbyz", "ccc" -> "cccccczx"))
    checkEvaluation(transformValues(as1, concatValue),
      Map("a" -> "axy", "bb" -> null, "ccc" -> "ccczx"))
    checkEvaluation(transformValues(as1, valueTypeUpdate),
      Map("a" -> 3, "bb" -> null, "ccc" -> 3))
    checkEvaluation(
      transformValues(transformValues(as1, concatValue), concatValue),
      Map("a" -> "aaxy", "bb" -> null, "ccc" -> "cccccczx"))
    checkEvaluation(transformValues(as2, concatValue), Map.empty[String, String])
    checkEvaluation(transformValues(as2, valueTypeUpdate), Map.empty[String, Int])
    checkEvaluation(
      transformValues(transformValues(as2, concatValue), valueTypeUpdate),
      Map.empty[String, Int])
    checkEvaluation(transformValues(as3, concatValue), null)

    val ax0 = Literal.create(
      Map(1 -> "x", 2 -> "y", 3 -> "z"),
      MapType(IntegerType, StringType, valueContainsNull = false))

    checkEvaluation(transformValues(ax0, valueUpdate), Map(1 -> 1, 2 -> 4, 3 -> 9))
  }

  test("MapZipWith") {
    def map_zip_with(
        left: Expression,
        right: Expression,
        f: (Expression, Expression, Expression) => Expression): Expression = {
      val MapType(kt, vt1, _) = left.dataType
      val MapType(_, vt2, _) = right.dataType
      MapZipWith(left, right, createLambda(kt, false, vt1, true, vt2, true, f))
        .bind(validateBinding)
    }

    val mii0 = Literal.create(create_map(1 -> 10, 2 -> 20, 3 -> 30),
      MapType(IntegerType, IntegerType, valueContainsNull = false))
    val mii1 = Literal.create(create_map(1 -> -1, 2 -> -2, 4 -> -4),
      MapType(IntegerType, IntegerType, valueContainsNull = false))
    val mii2 = Literal.create(create_map(1 -> null, 2 -> -2, 3 -> null),
      MapType(IntegerType, IntegerType, valueContainsNull = true))
    val mii3 = Literal.create(Map(), MapType(IntegerType, IntegerType, valueContainsNull = false))
    val miin = Literal.create(null, MapType(IntegerType, IntegerType, valueContainsNull = false))

    val multiplyKeyWithValues: (Expression, Expression, Expression) => Expression = {
      (k, v1, v2) => k * v1 * v2
    }

    checkEvaluation(
      map_zip_with(mii0, mii1, multiplyKeyWithValues),
      Map(1 -> -10, 2 -> -80, 3 -> null, 4 -> null))
    checkEvaluation(
      map_zip_with(mii0, mii2, multiplyKeyWithValues),
      Map(1 -> null, 2 -> -80, 3 -> null))
    checkEvaluation(
      map_zip_with(mii0, mii3, multiplyKeyWithValues),
      Map(1 -> null, 2 -> null, 3 -> null))
    checkEvaluation(
      map_zip_with(mii0, miin, multiplyKeyWithValues),
      null)
    assert(map_zip_with(mii0, mii1, multiplyKeyWithValues).dataType ===
      MapType(IntegerType, IntegerType, valueContainsNull = true))

    val mss0 = Literal.create(Map("a" -> "x", "b" -> "y", "d" -> "z"),
      MapType(StringType, StringType, valueContainsNull = false))
    val mss1 = Literal.create(Map("d" -> "b", "b" -> "d"),
      MapType(StringType, StringType, valueContainsNull = false))
    val mss2 = Literal.create(Map("c" -> null, "b" -> "t", "a" -> null),
      MapType(StringType, StringType, valueContainsNull = true))
    val mss3 = Literal.create(Map(), MapType(StringType, StringType, valueContainsNull = false))
    val mssn = Literal.create(null, MapType(StringType, StringType, valueContainsNull = false))

    val concat: (Expression, Expression, Expression) => Expression = {
      (k, v1, v2) => Concat(Seq(k, v1, v2))
    }

    checkEvaluation(
      map_zip_with(mss0, mss1, concat),
      Map("a" -> null, "b" -> "byd", "d" -> "dzb"))
    checkEvaluation(
      map_zip_with(mss1, mss2, concat),
      Map("d" -> null, "b" -> "bdt", "c" -> null, "a" -> null))
    checkEvaluation(
      map_zip_with(mss0, mss3, concat),
      Map("a" -> null, "b" -> null, "d" -> null))
    checkEvaluation(
      map_zip_with(mss0, mssn, concat),
      null)
    assert(map_zip_with(mss0, mss1, concat).dataType ===
      MapType(StringType, StringType, valueContainsNull = true))

    def b(data: Byte*): Array[Byte] = Array[Byte](data: _*)

    val mbb0 = Literal.create(Map(b(1, 2) -> b(4), b(2, 1) -> b(5), b(1, 3) -> b(8)),
      MapType(BinaryType, BinaryType, valueContainsNull = false))
    val mbb1 = Literal.create(Map(b(2, 1) -> b(7), b(1, 2) -> b(3), b(1, 1) -> b(6)),
      MapType(BinaryType, BinaryType, valueContainsNull = false))
    val mbb2 = Literal.create(Map(b(1, 3) -> null, b(1, 2) -> b(2), b(2, 1) -> null),
      MapType(BinaryType, BinaryType, valueContainsNull = true))
    val mbb3 = Literal.create(Map(), MapType(BinaryType, BinaryType, valueContainsNull = false))
    val mbbn = Literal.create(null, MapType(BinaryType, BinaryType, valueContainsNull = false))

    checkEvaluation(
      map_zip_with(mbb0, mbb1, concat),
      Map(b(1, 2) -> b(1, 2, 4, 3), b(2, 1) -> b(2, 1, 5, 7), b(1, 3) -> null, b(1, 1) -> null))
    checkEvaluation(
      map_zip_with(mbb1, mbb2, concat),
      Map(b(2, 1) -> null, b(1, 2) -> b(1, 2, 3, 2), b(1, 1) -> null, b(1, 3) -> null))
    checkEvaluation(
      map_zip_with(mbb0, mbb3, concat),
      Map(b(1, 2) -> null, b(2, 1) -> null, b(1, 3) -> null))
    checkEvaluation(
      map_zip_with(mbb0, mbbn, concat),
      null)
  }

  test("ZipWith") {
    def zip_with(
        left: Expression,
        right: Expression,
        f: (Expression, Expression) => Expression): Expression = {
      val ArrayType(leftT, _) = left.dataType
      val ArrayType(rightT, _) = right.dataType
      ZipWith(left, right, createLambda(leftT, true, rightT, true, f)).bind(validateBinding)
    }

    val ai0 = Literal.create(Seq(1, 2, 3), ArrayType(IntegerType, containsNull = false))
    val ai1 = Literal.create(Seq(1, 2, 3, 4), ArrayType(IntegerType, containsNull = false))
    val ai2 = Literal.create(Seq[Integer](1, null, 3), ArrayType(IntegerType, containsNull = true))
    val ai3 = Literal.create(Seq[Integer](1, null), ArrayType(IntegerType, containsNull = true))
    val ain = Literal.create(null, ArrayType(IntegerType, containsNull = false))

    val add: (Expression, Expression) => Expression = (x, y) => x + y
    val plusOne: Expression => Expression = x => x + 1

    checkEvaluation(zip_with(ai0, ai1, add), Seq(2, 4, 6, null))
    checkEvaluation(zip_with(ai3, ai2, add), Seq(2, null, null))
    checkEvaluation(zip_with(ai2, ai3, add), Seq(2, null, null))
    checkEvaluation(zip_with(ain, ain, add), null)
    checkEvaluation(zip_with(ai1, ain, add), null)
    checkEvaluation(zip_with(ain, ai1, add), null)

    val as0 = Literal.create(Seq("a", "b", "c"), ArrayType(StringType, containsNull = false))
    val as1 = Literal.create(Seq("a", null, "c"), ArrayType(StringType, containsNull = true))
    val as2 = Literal.create(Seq("a"), ArrayType(StringType, containsNull = true))
    val asn = Literal.create(null, ArrayType(StringType, containsNull = false))

    val concat: (Expression, Expression) => Expression = (x, y) => Concat(Seq(x, y))

    checkEvaluation(zip_with(as0, as1, concat), Seq("aa", null, "cc"))
    checkEvaluation(zip_with(as0, as2, concat), Seq("aa", null, null))

    val aai1 = Literal.create(Seq(Seq(1, 2, 3), null, Seq(4, 5)),
      ArrayType(ArrayType(IntegerType, containsNull = false), containsNull = true))
    val aai2 = Literal.create(Seq(Seq(1, 2, 3)),
      ArrayType(ArrayType(IntegerType, containsNull = false), containsNull = true))
    checkEvaluation(
      zip_with(aai1, aai2, (a1, a2) =>
        Cast(zip_with(transform(a1, plusOne), transform(a2, plusOne), add), StringType)),
      Seq("[4, 6, 8]", null, null))
    checkEvaluation(zip_with(aai1, aai1, (a1, a2) => Cast(transform(a1, plusOne), StringType)),
      Seq("[2, 3, 4]", null, "[5, 6]"))
  }
}
