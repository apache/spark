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

package org.apache.spark.sql

import org.apache.spark.sql.functions.{col, typedUdf, udf}
import org.apache.spark.sql.test.SharedSQLContext
import org.apache.spark.sql.test.SQLTestData._

case class Blah(a: Int, b: String)

class TypedUDFSuite extends QueryTest with SharedSQLContext {
  import testImplicits._

  test("typedUdf") {
    import java.lang.{ Integer => JInt }

    // this seems to use TypedScalaUDF.eval, not sure why
    val df1 = Seq((1, "a", 1: JInt), (2, "b", 2: JInt), (3, "c", null)).toDF("x", "y", "z").select(
      typedUdf{ () => false }.apply() as "f0",
      typedUdf{ x: Int => x * 2 }.apply('x) as "f1",
      typedUdf{ y: String => y + "!" }.apply('y) as "f2",
      typedUdf{ blah: Blah => blah.copy(a = blah.a + 1) }.apply('x, 'y) as "f3",
      typedUdf{ (x: Int, y: String) => (x + 1, y) }.apply('x, 'y) as "f4",
      typedUdf{ z: Int => z + 1 }.apply('z) as "f5",
      typedUdf{ z: Option[Int] => z.map(_ + 1) }.apply('z) as "f6",
      typedUdf[JInt, JInt]{ z => if (z != null) z + 1 else null }.apply('z) as "f7",
      typedUdf{ x: Int => x + 1 }.apply(col("x") * 2) as "f8"
    )
    // df1.printSchema
    // df1.explain
    // df1.show
    checkAnswer(df1, Seq(
      Row(false, 2, "a!", Row(2, "a"), Row(2, "a"), 2, 2, 2, 3),
      Row(false, 4, "b!", Row(3, "b"), Row(3, "b"), 3, 3, 3, 5),
      Row(false, 6, "c!", Row(4, "c"), Row(4, "c"), 1, null, null, 7)
    ))

    // this seems to use TypedScalaUDF.doGenCode, not sure why
    val df2 = testData
      .filter("key < 4")
      .withColumn("z", udf{ (x: JInt) => if (x < 3) x else null }.apply('value) as 'z)
      .select(
        typedUdf{ () => false }.apply() as "f0",
        typedUdf{ x: Int => x * 2 }.apply('key) as "f1",
        typedUdf{ y: String => y + "!" }.apply('value) as "f2",
        typedUdf{ blah: Blah => blah.copy(a = blah.a + 1) }.apply('key, 'value) as "f3",
        typedUdf{ (x: Int, y: String) => (x + 1, y) }.apply('key, 'value) as "f4",
        typedUdf{ z: Int => z + 1 }.apply('z) as "f5",
        typedUdf{ z: Option[Int] => z.map(_ + 1) }.apply('z) as "f6",
        typedUdf[JInt, JInt]{ z => if (z != null) z + 1 else null }.apply('z) as "f7",
        typedUdf{ x: Int => x + 1 }.apply(col("key") * 2) as "f8"
      )
    // df2.printSchema
    // df2.explain
    // df2.show
    checkAnswer(df2, Seq(
      Row(false, 2, "1!", Row(2, "1"), Row(2, "1"), 2, 2, 2, 3),
      Row(false, 4, "2!", Row(3, "2"), Row(3, "2"), 3, 3, 3, 5),
      Row(false, 6, "3!", Row(4, "3"), Row(4, "3"), 1, null, null, 7)
    ))
  }

}
