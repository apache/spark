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
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.util.DateTimeTestUtils.UTC_OPT
import org.apache.spark.sql.types._
import org.apache.spark.unsafe.types.UTF8String

// A test suite to check analysis behaviors of `TryCast`.
class TryCastSuite extends SparkFunSuite {

  private def cast(v: Any, targetType: DataType, timeZoneId: Option[String] = None): TryCast = {
    v match {
      case lit: Expression => TryCast(lit, targetType, timeZoneId)
      case _ => TryCast(Literal(v), targetType, timeZoneId)
    }
  }

  test("print string") {
    assert(TryCast(Literal("1"), IntegerType).toString == "try_cast(1 as int)")
    assert(TryCast(Literal("1"), IntegerType).sql == "TRY_CAST('1' AS INT)")
  }

  test("nullability") {
    assert(cast("abcdef", StringType).nullable)
    assert(cast("abcdef", BinaryType).nullable)
  }

  test("only require timezone for datetime types") {
    assert(cast("abc", IntegerType).resolved)
    assert(!cast("abc", TimestampType).resolved)
    assert(cast("abc", TimestampType, UTC_OPT).resolved)
  }

  test("element type nullability") {
    val array = Literal.create(Seq("123", "true"),
      ArrayType(StringType, containsNull = false))
    // array element can be null after try_cast which violates the target type.
    val c1 = cast(array, ArrayType(BooleanType, containsNull = false))
    assert(!c1.resolved)

    val map = Literal.create(Map("a" -> "123", "b" -> "true"),
      MapType(StringType, StringType, valueContainsNull = false))
    // key can be null after try_cast which violates the map key requirement.
    val c2 = cast(map, MapType(IntegerType, StringType, valueContainsNull = true))
    assert(!c2.resolved)
    // map value can be null after try_cast which violates the target type.
    val c3 = cast(map, MapType(StringType, IntegerType, valueContainsNull = false))
    assert(!c3.resolved)

    val struct = Literal.create(
      InternalRow(
        UTF8String.fromString("123"),
        UTF8String.fromString("true")),
      new StructType()
        .add("a", StringType, nullable = true)
        .add("b", StringType, nullable = true))
    // struct field `b` can be null after try_cast which violates the target type.
    val c4 = cast(struct, new StructType()
      .add("a", BooleanType, nullable = true)
      .add("b", BooleanType, nullable = false))
    assert(!c4.resolved)
  }
}
