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
import org.apache.spark.sql.catalyst.expressions.Hex
import org.apache.spark.sql.catalyst.parser.ParseException
import org.apache.spark.sql.functions._
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.test.SharedSparkSession
import org.apache.spark.sql.types.{BinaryType, StringType}

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

    checkAnswer(df.selectExpr("version()"), df.select(version()))
  }

  test("SPARK-21957, SPARK-44860: get current_user, session_user in normal spark apps") {
    val user = spark.sparkContext.sparkUser
    withSQLConf(SQLConf.ANSI_ENABLED.key -> "false") {
      val df =
        sql("select current_user(), current_user, user, user(), session_user(), session_user")
      checkAnswer(df, Row(user, user, user, user, user, user))
    }
    withSQLConf(SQLConf.ANSI_ENABLED.key -> "true",
      SQLConf.ENFORCE_RESERVED_KEYWORDS.key -> "true") {
      Seq("user", "current_user", "session_user").foreach { func =>
        checkAnswer(sql(s"select $func"), Row(user))
        checkError(
          exception = intercept[ParseException](sql(s"select $func()")),
          errorClass = "PARSE_SYNTAX_ERROR",
          parameters = Map("error" -> s"'$func'", "hint" -> ""))
      }
    }
  }

  test("SPARK-37591, SPARK-43038: AES functions - GCM/CBC mode") {
    Seq(
      "GCM" -> "NONE",
      "CBC" -> "PKCS").foreach { case (mode, padding) =>
      Seq(
        ("abcdefghijklmnop", ""),
        ("abcdefghijklmnop", "abcdefghijklmnop"),
        ("abcdefghijklmnop12345678", "Spark"),
        ("abcdefghijklmnop12345678ABCDEFGH", "GCM mode")
      ).foreach { case (key, input) =>
        val df = Seq((key, input)).toDF("key", "input")
        val encrypted = df.selectExpr(
          s"aes_encrypt(input, key, '$mode', '$padding') AS enc", "input", "key")
        assert(encrypted.schema("enc").dataType === BinaryType)
        assert(encrypted.filter($"enc" === $"input").isEmpty)
        val result = encrypted.selectExpr(
          s"CAST(aes_decrypt(enc, key, '$mode', '$padding') AS STRING) AS res", "input")
        assert(!result.filter($"res" === $"input").isEmpty &&
          result.filter($"res" =!= $"input").isEmpty)
      }
    }
  }

  test("uuid") {
    val df = Seq((1, 2)).toDF("a", "b")
    assert(df.selectExpr("uuid()").collect() != null)
    assert(df.select(uuid()).collect() != null)
  }

  test("aes_encrypt") {
    val iv = Hex.unhex("000000000000000000000000".getBytes())
    val df = Seq(("Spark", "abcdefghijklmnop12345678ABCDEFGH",
      "GCM", "DEFAULT", iv, "This is an AAD mixed into the input")).
      toDF("input", "key", "mode", "padding", "iv", "aad")

    checkAnswer(
      df.selectExpr("aes_encrypt(input, key, mode, padding, iv, aad)"),
      df.select(aes_encrypt(col("input"), col("key"), col("mode"),
        col("padding"), col("iv"), col("aad"))))

    checkAnswer(
      df.selectExpr("aes_encrypt(input, key, mode, padding, iv)"),
      df.select(aes_encrypt(col("input"), col("key"), col("mode"),
        col("padding"), col("iv"))))

    val df1 = Seq(("Spark SQL", "1234567890abcdef", "ECB", "PKCS")).
      toDF("input", "key", "mode", "padding")

    checkAnswer(
      df1.selectExpr("base64(aes_encrypt(input, key, mode, padding))"),
      df1.select(base64(aes_encrypt(col("input"), col("key"), col("mode"), col("padding")))))

    val df2 = Seq(("Spark SQL", "0000111122223333", "ECB")).toDF("input", "key", "mode")

    checkAnswer(
      df2.selectExpr("hex(aes_encrypt(input, key, mode))"),
      df2.select(hex(aes_encrypt(col("input"), col("key"), col("mode")))))

    val df3 = Seq(("Spark", "abcdefghijklmnop")).toDF("input", "key")
    checkAnswer(
      df3.selectExpr("cast(aes_decrypt(unbase64(base64(" +
        "aes_encrypt(input, key))), key) AS STRING)"),
      Seq(Row("Spark")))
    checkAnswer(
      df3.select(aes_decrypt(unbase64(base64(
        aes_encrypt(col("input"), col("key")))), col("key")).cast(StringType)),
      Seq(Row("Spark")))
  }

  test("aes_decrypt") {
    val df = Seq(("AAAAAAAAAAAAAAAAQiYi+sTLm7KD9UcZ2nlRdYDe/PX4",
      "abcdefghijklmnop12345678ABCDEFGH", "GCM", "DEFAULT", "This is an AAD mixed into the input"
    )).toDF("input", "key", "mode", "padding", "aad")

    checkAnswer(
      df.selectExpr("aes_decrypt(unbase64(input), key, mode, padding, aad)"),
      df.select(aes_decrypt(unbase64(col("input")), col("key"),
        col("mode"), col("padding"), col("aad"))))

    val df1 = Seq(("AAAAAAAAAAAAAAAAAAAAAPSd4mWyMZ5mhvjiAPQJnfg=",
      "abcdefghijklmnop12345678ABCDEFGH", "CBC", "DEFAULT"
    )).toDF("input", "key", "mode", "padding")

    checkAnswer(
      df1.selectExpr("aes_decrypt(unbase64(input), key, mode, padding)"),
      df1.select(aes_decrypt(unbase64(col("input")), col("key"),
        col("mode"), col("padding"))))

     checkAnswer(
      df1.selectExpr("aes_decrypt(unbase64(input), key, mode)"),
      df1.select(aes_decrypt(unbase64(col("input")), col("key"), col("mode"))))

    val df2 = Seq(("83F16B2AA704794132802D248E6BFD4E380078182D1544813898AC97E709B28A94",
      "0000111122223333")).toDF("input", "key")
     checkAnswer(
      df2.selectExpr("aes_decrypt(unhex(input), key)"),
      df2.select(aes_decrypt(unhex(col("input")), col("key"))))
  }

  test("try_aes_decrypt") {
    val df = Seq(("AAAAAAAAAAAAAAAAQiYi+sTLm7KD9UcZ2nlRdYDe/PX4",
      "abcdefghijklmnop12345678ABCDEFGH", "GCM", "DEFAULT", "This is an AAD mixed into the input"
    )).toDF("input", "key", "mode", "padding", "aad")

    checkAnswer(
      df.selectExpr("try_aes_decrypt(unbase64(input), key, mode, padding, aad)"),
      df.select(try_aes_decrypt(unbase64(col("input")), col("key"),
        col("mode"), col("padding"), col("aad"))))

    val df1 = Seq(("AAAAAAAAAAAAAAAAAAAAAPSd4mWyMZ5mhvjiAPQJnfg=",
      "abcdefghijklmnop12345678ABCDEFGH", "CBC", "DEFAULT"
    )).toDF("input", "key", "mode", "padding")

    checkAnswer(
      df1.selectExpr("try_aes_decrypt(unbase64(input), key, mode, padding)"),
      df1.select(try_aes_decrypt(unbase64(col("input")), col("key"),
        col("mode"), col("padding"))))

     checkAnswer(
      df1.selectExpr("try_aes_decrypt(unbase64(input), key, mode)"),
      df1.select(try_aes_decrypt(unbase64(col("input")), col("key"), col("mode"))))

    val df2 = Seq(("83F16B2AA704794132802D248E6BFD4E380078182D1544813898AC97E709B28A94",
      "0000111122223333")).toDF("input", "key")
     checkAnswer(
      df2.selectExpr("try_aes_decrypt(unhex(input), key)"),
      df2.select(try_aes_decrypt(unhex(col("input")), col("key"))))
  }

  test("sha") {
    val df = Seq("Spark").toDF("a")
    checkAnswer(df.selectExpr("sha(a)"), df.select(sha(col("a"))))
  }

  test("input_file_block_length") {
    val tableName = "t1"
    withTable(tableName) {
      sql(s"CREATE TABLE $tableName(a String) USING parquet")
      sql(s"insert into $tableName values('a')")
      val df = spark.table(tableName)
      checkAnswer(
        df.selectExpr("input_file_block_length()"),
        df.select(input_file_block_length())
      )
    }
  }

  test("input_file_block_start") {
    val tableName = "t1"
    withTable(tableName) {
      sql(s"CREATE TABLE $tableName(a String) USING parquet")
      sql(s"insert into $tableName values('a')")
      val df = spark.table(tableName)
      checkAnswer(
        df.selectExpr("input_file_block_start()"),
        df.select(input_file_block_start())
      )
    }
  }

  test("reflect") {
    val df = Seq("a5cf6c42-0c85-418f-af6c-3e4e5b1328f2").toDF("a")
    checkAnswer(df.selectExpr("reflect('java.util.UUID', 'fromString', a)"),
      Seq(Row("a5cf6c42-0c85-418f-af6c-3e4e5b1328f2")))
    checkAnswer(df.select(reflect(lit("java.util.UUID"), lit("fromString"), col("a"))),
      Seq(Row("a5cf6c42-0c85-418f-af6c-3e4e5b1328f2")))

    checkError(
      exception = intercept[AnalysisException] {
        df.selectExpr("reflect(cast(null as string), 'fromString', a)")
      },
      errorClass = "DATATYPE_MISMATCH.UNEXPECTED_NULL",
      parameters = Map(
        "exprName" -> "`class`",
        "sqlExpr" -> "\"reflect(CAST(NULL AS STRING), fromString, a)\""),
      context = ExpectedContext("", "", 0, 45, "reflect(cast(null as string), 'fromString', a)"))
    checkError(
      exception = intercept[AnalysisException] {
        df.selectExpr("reflect('java.util.UUID', cast(null as string), a)")
      },
      errorClass = "DATATYPE_MISMATCH.UNEXPECTED_NULL",
      parameters = Map(
        "exprName" -> "`method`",
        "sqlExpr" -> "\"reflect(java.util.UUID, CAST(NULL AS STRING), a)\""),
      context = ExpectedContext("", "", 0, 49,
        "reflect('java.util.UUID', cast(null as string), a)"))
  }

  test("java_method") {
    val df = Seq("a5cf6c42-0c85-418f-af6c-3e4e5b1328f2").toDF("a")
    checkAnswer(df.selectExpr("java_method('java.util.UUID', 'fromString', a)"),
      Seq(Row("a5cf6c42-0c85-418f-af6c-3e4e5b1328f2")))
    checkAnswer(df.select(java_method(lit("java.util.UUID"), lit("fromString"), col("a"))),
      Seq(Row("a5cf6c42-0c85-418f-af6c-3e4e5b1328f2")))
  }

  test("typeof") {
    val df = Seq(1).toDF("a")
    checkAnswer(df.selectExpr("typeof(a)"), Seq(Row("int")))
    checkAnswer(df.select(typeof(col("a"))), Seq(Row("int")))
  }

  test("stack") {
    val df = Seq((1, 2, 3)).toDF("a", "b", "c")
    checkAnswer(df.selectExpr("stack(2, a, b, c)"),
      Seq(Row(1, 2), Row(3, null)))
    checkAnswer(df.select(stack(lit(2), col("a"), col("b"), col("c"))),
      Seq(Row(1, 2), Row(3, null)))
  }

  test("random") {
     val df = Seq((1, 2)).toDF("a", "b")
    assert(df.selectExpr("random()").collect() != null)
    assert(df.select(random()).collect() != null)

    assert(df.selectExpr("random(1)").collect() != null)
    assert(df.select(random(lit(1))).collect() != null)
  }
}

object ReflectClass {
  def method1(v1: Int, v2: String): String = "m" + v1 + v2
}
