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

import org.apache.spark.{SPARK_REVISION, SPARK_VERSION_SHORT}
import org.apache.spark.sql.catalyst.parser.ParseException
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.test.SharedSparkSession
import org.apache.spark.sql.types.BinaryType

class MiscFunctionsSuite extends QueryTest with SharedSparkSession {
  import testImplicits._

  test("reflect and java_method") {
    val df = Seq((1, "one")).toDF("a", "b")
    val className = ReflectClass.getClass.getName.stripSuffix("$")
    checkAnswer(
      df.selectExpr(
        s"reflect('$className', 'method1', a, b)",
        s"java_method('$className', 'method1', a, b)"),
      Row("m1one", "m1one"))
  }

  test("version") {
    val df = sql("SELECT version()")
    checkAnswer(
      df,
      Row(SPARK_VERSION_SHORT + " " + SPARK_REVISION))
    assert(df.schema.fieldNames === Seq("version()"))
  }

  test("SPARK-21957: get current_user in normal spark apps") {
    val user = spark.sparkContext.sparkUser
    withSQLConf(SQLConf.ANSI_ENABLED.key -> "false") {
      val df = sql("select current_user(), current_user, user, user()")
      checkAnswer(df, Row(user, user, user, user))
    }
    withSQLConf(SQLConf.ANSI_ENABLED.key -> "true",
      SQLConf.ENFORCE_RESERVED_KEYWORDS.key -> "true") {
      Seq("user", "current_user").foreach { func =>
        checkAnswer(sql(s"select $func"), Row(user))
        checkError(
          exception = intercept[ParseException](sql(s"select $func()")),
          errorClass = "PARSE_SYNTAX_ERROR",
          parameters = Map("error" -> s"'$func'", "hint" -> ""))
      }
    }
  }

  test("SPARK-37591: AES functions - GCM mode") {
    Seq(
      ("abcdefghijklmnop", ""),
      ("abcdefghijklmnop", "abcdefghijklmnop"),
      ("abcdefghijklmnop12345678", "Spark"),
      ("abcdefghijklmnop12345678ABCDEFGH", "GCM mode")
    ).foreach { case (key, input) =>
      val df = Seq((key, input)).toDF("key", "input")
      val encrypted = df.selectExpr("aes_encrypt(input, key, 'GCM', 'NONE') AS enc", "input", "key")
      assert(encrypted.schema("enc").dataType === BinaryType)
      assert(encrypted.filter($"enc" === $"input").isEmpty)
      val result = encrypted.selectExpr(
        "CAST(aes_decrypt(enc, key, 'GCM', 'NONE') AS STRING) AS res", "input")
      assert(!result.filter($"res" === $"input").isEmpty &&
        result.filter($"res" =!= $"input").isEmpty)
    }
  }
}

object ReflectClass {
  def method1(v1: Int, v2: String): String = "m" + v1 + v2
}
