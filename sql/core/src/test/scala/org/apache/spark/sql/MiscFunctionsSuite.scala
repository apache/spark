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
import org.apache.spark.sql.catalyst.expressions.{AesEncrypt, Hex}
import org.apache.spark.sql.catalyst.optimizer.ReplaceExpressions
import org.apache.spark.sql.catalyst.parser.ParseException
import org.apache.spark.sql.functions._
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.test.SharedSparkSession
import org.apache.spark.sql.types.{ArrayType, BinaryType, DoubleType, FloatType, StringType}

class MiscFunctionsSuite extends SharedSparkSession {
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
          condition = "PARSE_SYNTAX_ERROR",
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

  // Constant wrapper IVs must remain valid wherever deterministic AES was accepted.
  test("SPARK-59768: fixed AES wrapper arguments work in joins and column defaults") {
    for {
      (mode, length) <- Seq("GCM" -> 12, "CBC" -> 16)
      iv <- Seq(
        s"to_binary('${"00" * length}', 'hex')",
        s"encode('${"0" * length}', 'utf-8')")
    } {
      val encrypt = s"aes_encrypt('Spark', '0000111122223333', '$mode', 'DEFAULT', $iv)"
      checkAnswer(sql(s"""SELECT l.v FROM VALUES ('Spark') l(v)
                         |JOIN VALUES ('Spark') r(v)
                         |ON aes_encrypt(l.v, '0000111122223333', '$mode', 'DEFAULT', $iv) =
                         |   aes_encrypt(r.v, '0000111122223333', '$mode', 'DEFAULT', $iv)
                         |""".stripMargin), Row("Spark"))
      withTable("aes_defaults") {
        sql(s"CREATE TABLE aes_defaults(id INT, enc BINARY DEFAULT $encrypt) USING parquet")
        sql("INSERT INTO aes_defaults(id) VALUES (1)")
        sql("INSERT INTO aes_defaults VALUES (2, DEFAULT)")
        checkAnswer(sql(s"SELECT enc = $encrypt FROM aes_defaults"), Seq(Row(true), Row(true)))
      }
    }
  }

  // A session variable supplies one fixed IV throughout analysis and execution.
  test("SPARK-59768: session variables supply fixed AES IVs") {
    Seq("GCM" -> 12, "CBC" -> 16).foreach { case (mode, length) =>
      sql(s"DECLARE VARIABLE aes_iv BINARY DEFAULT unhex('${"00" * length}')")
      try {
        val encrypt = s"aes_encrypt('Spark', '0000111122223333', '$mode', 'DEFAULT', aes_iv)"
        val analyzed = sql(s"SELECT $encrypt").queryExecution.analyzed
        val aes = analyzed.expressions.flatMap(_.collect { case a: AesEncrypt => a }).head
        assert(aes.deterministic)
        val replacement = ReplaceExpressions.replace(aes)
        assert(replacement.deterministic && replacement.foldable)
        checkAnswer(sql(s"""SELECT l.v FROM VALUES ('Spark') l(v)
                           |JOIN VALUES ('Spark') r(v)
                           |ON aes_encrypt(l.v, '0000111122223333', '$mode', 'DEFAULT', aes_iv) =
                           |   aes_encrypt(r.v, '0000111122223333', '$mode', 'DEFAULT', aes_iv)
                           |""".stripMargin), Row("Spark"))
      } finally {
        sql("DROP TEMPORARY VARIABLE aes_iv")
      }
    }
  }

  // Invalid arguments in skipped branches must never reach AES evaluation.
  test("SPARK-59768: AES guards still skip invalid keys and IVs after extraction") {
    val expressions = Seq(
      "CASE WHEN octet_length(k) = 16 THEN aes_encrypt(v, k) END",
      "if(octet_length(k) = 16, aes_encrypt(v, k), NULL)",
      "coalesce(if(octet_length(k) <> 16, X'01', NULL), aes_encrypt(v, k))",
      "octet_length(k) = 16 AND aes_encrypt(v, k) IS NOT NULL",
      "octet_length(k) <> 16 OR aes_encrypt(v, k) IS NOT NULL",
      "CASE WHEN octet_length(iv) = 12 " +
        "THEN aes_encrypt(v, '0000111122223333', 'GCM', 'DEFAULT', iv) END")
    val input = "VALUES ('Spark', '0000111122223333', '000000000000'), " +
      "('SQL', 'short', 'short') AS t(v, k, iv)"
    expressions.foreach { expression =>
      val ordered = sql(s"SELECT v FROM $input ORDER BY $expression").collect().toSeq
      assert(ordered.sortBy(_.getString(0)) == Seq(Row("SQL"), Row("Spark")))
      checkAnswer(sql(s"SELECT v FROM $input DISTRIBUTE BY $expression"),
        Seq(Row("Spark"), Row("SQL")))
      checkAnswer(sql(s"SELECT sum(n) FROM (" +
        s"SELECT count(*) AS n FROM $input GROUP BY $expression)"), Row(2L))
    }
  }

  // Extracting TryEval with AES preserves both interpreted and generated exception handling.
  test("SPARK-59768: try functions catch AES errors after extraction") {
    Seq("true", "false").foreach { codegen =>
      withSQLConf(SQLConf.WHOLESTAGE_CODEGEN_ENABLED.key -> codegen) {
        Seq(
          "try_aes_decrypt(aes_encrypt(v, k), k)",
          "try_to_binary(hex(aes_encrypt(v, k)), 'hex')"
        ).foreach { expression =>
          val input = "VALUES ('Spark', '0000111122223333'), ('SQL', 'short') AS t(v, k)"
          val ordered = sql(s"SELECT v FROM $input ORDER BY $expression").collect().toSeq
          assert(ordered.sortBy(_.getString(0)) == Seq(Row("SQL"), Row("Spark")))
          checkAnswer(sql(s"SELECT v FROM $input DISTRIBUTE BY $expression"),
            Seq(Row("SQL"), Row("Spark")))
          checkAnswer(sql(s"SELECT sum(n) FROM (" +
            s"SELECT count(*) AS n FROM $input GROUP BY $expression)"), Row(2L))
        }
      }
    }
  }

  // Predicate subqueries must move with wrappers and guards accepted by GROUP BY.
  test("SPARK-59768: AES grouping retains predicate subqueries and conditional guards") {
    val input = "VALUES ('Spark', '0000111122223333'), ('SQL', 'short') AS t(v, k)"
    val predicates = Seq(
      "k IN (SELECT a FROM VALUES ('0000111122223333') AS s(a))",
      "EXISTS (SELECT 1 WHERE t.k = '0000111122223333')")
    for {
      singlePass <- Seq("true", "false")
      predicate <- predicates
    } {
      withSQLConf(SQLConf.ANALYZER_SINGLE_PASS_RESOLVER_ENABLED.key -> singlePass) {
        Seq(
          s"CASE WHEN $predicate THEN aes_encrypt(v, k) END",
          s"if($predicate, aes_encrypt(v, k), NULL)",
          s"coalesce(if(NOT ($predicate), X'01', NULL), aes_encrypt(v, k))",
          s"$predicate AND aes_encrypt(v, k) IS NOT NULL",
          s"NOT ($predicate) OR aes_encrypt(v, k) IS NOT NULL",
          s"aes_encrypt(CAST($predicate AS STRING), '0000111122223333')"
        ).foreach { expression =>
          // The single-pass resolver does not support the harness's RDD deserialization plan.
          QueryTest.checkAnswer(
            sql(s"SELECT sum(n) FROM (" +
              s"SELECT count(*) AS n FROM $input GROUP BY $expression)"),
            Seq(Row(2L)),
            checkToRDD = false)
        }
      }
    }
    withUserDefinedFunction("nd_predicate" -> true) {
      spark.udf.register("nd_predicate", udf((value: Boolean) => value).asNondeterministic())
      predicates.foreach { predicate =>
        checkAnswer(sql(s"SELECT sum(n) FROM (" +
          s"SELECT count(*) AS n FROM $input GROUP BY nd_predicate($predicate))"), Row(2L))
      }
    }
    withSQLConf(
      SQLConf.ALLOW_SUBQUERY_EXPRESSIONS_IN_LAMBDAS_AND_HIGHER_ORDER_FUNCTIONS.key -> "true") {
      val expression = "transform(array(CAST(k IN (SELECT a FROM VALUES " +
        "('0000111122223333') AS s(a)) AS STRING)), " +
        "x -> aes_encrypt(x, '0000111122223333'))"
      checkAnswer(sql(s"SELECT sum(n) FROM (" +
        s"SELECT count(*) AS n FROM $input GROUP BY $expression)"), Row(2L))
    }
  }

  // Operator extraction must keep each AES call inside the lambda that binds its input.
  test("SPARK-59768: lambda-bound AES evaluates inside the higher-order function") {
    val expression = "transform(array(v), x -> aes_encrypt(x, '0000111122223333'))"
    val input = "VALUES ('Spark'), ('SQL') AS t(v)"
    val ordered = sql(s"SELECT v FROM $input ORDER BY $expression").collect().toSeq
    assert(ordered.sortBy(_.getString(0)) == Seq(Row("SQL"), Row("Spark")))
    checkAnswer(sql(s"SELECT v FROM $input DISTRIBUTE BY $expression"),
      Seq(Row("Spark"), Row("SQL")))
    checkAnswer(sql(s"SELECT sum(n) FROM (" +
      s"SELECT count(*) AS n FROM $input GROUP BY $expression)"), Row(2L))
  }

  // Constant folding must not reuse one randomly generated ciphertext for every row.
  test("SPARK-59768: constant AES inputs generate a fresh IV for every row") {
    val encrypted = spark.range(32).selectExpr("aes_encrypt('Spark', '0000111122223333')")
    assert(encrypted.distinct().count() == 32)
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
      condition = "DATATYPE_MISMATCH.UNEXPECTED_NULL",
      parameters = Map(
        "exprName" -> "`class`",
        "sqlExpr" -> "\"reflect(CAST(NULL AS STRING), fromString, a)\""),
      context = ExpectedContext("", "", 0, 45, "reflect(cast(null as string), 'fromString', a)"))
    checkError(
      exception = intercept[AnalysisException] {
        df.selectExpr("reflect('java.util.UUID', cast(null as string), a)")
      },
      condition = "DATATYPE_MISMATCH.UNEXPECTED_NULL",
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

  test("vector functions") {
    import org.apache.spark.sql.types.{StructField, StructType}
    val schema = StructType(Seq(
      StructField("a", ArrayType(FloatType)),
      StructField("b", ArrayType(FloatType))))
    val df = spark.createDataFrame(
      java.util.Arrays.asList(Row(Array(1.0f, 2.0f, 3.0f), Array(4.0f, 5.0f, 6.0f))), schema)

    // Similarity/distance
    val cos = df.select(vector_cosine_similarity($"a", $"b")).first().getFloat(0)
    assert(math.abs(cos - 0.9746319f) < 0.0001f)
    val dot = df.select(vector_inner_product($"a", $"b")).first().getFloat(0)
    assert(math.abs(dot - 32.0f) < 0.01f)
    val l2 = df.select(vector_l2_distance($"a", $"b")).first().getFloat(0)
    assert(math.abs(l2 - 5.196152f) < 0.001f)

    // Norm/normalize
    val schema2 = StructType(Seq(StructField("v", ArrayType(FloatType))))
    val df2 = spark.createDataFrame(
      java.util.Arrays.asList(Row(Array(3.0f, 4.0f))), schema2)
    val norm = df2.select(vector_norm($"v", lit(2.0f))).first().getFloat(0)
    assert(math.abs(norm - 5.0f) < 0.01f)

    // Aggregate
    val df3 = spark.createDataFrame(
      java.util.Arrays.asList(Row(Array(1.0f, 2.0f)), Row(Array(3.0f, 4.0f))), schema2)
    val avg = df3.select(vector_avg($"v")).first().getSeq[Float](0)
    assert(math.abs(avg(0) - 2.0f) < 0.01f)
    assert(math.abs(avg(1) - 3.0f) < 0.01f)
    val sum = df3.select(vector_sum($"v")).first().getSeq[Float](0)
    assert(math.abs(sum(0) - 4.0f) < 0.01f)
    assert(math.abs(sum(1) - 6.0f) < 0.01f)
  }

  test("SPARK-60245: vector_sum preserves double precision in DataFrames") {
    val df = Seq(Seq(16777216.0d, 1.0d), Seq(1.0d, 1e-8d)).toDF("v").repartition(2)
    val result = df.select(vector_sum($"v"))
    assertResult(ArrayType(DoubleType, containsNull = false))(result.schema.head.dataType)
    assertResult(Seq(16777217.0d, 1.00000001d))(result.head().getSeq[Double](0))
  }

  gridTest("SPARK-60245: observe vector aggregates")(Seq(
      ("float", "vector_sum"),
      ("double", "vector_sum"),
      ("float", "vector_avg"))) { case (elementType, functionName) =>
    val df = spark.range(0, 8, 1, 4).select(
      array($"id".cast(elementType), lit(1).cast(elementType)).as("v"))
    val aggregate = if (functionName == "vector_sum") vector_sum($"v") else vector_avg($"v")
    val expected = if (functionName == "vector_sum") Seq(28.0, 8.0) else Seq(3.5, 1.0)
    val observation = Observation(s"${functionName}_$elementType")

    assertResult(8)(df.observe(observation, aggregate.as("value")).collect().length)
    assertResult(Map("value" -> expected))(observation.get)
  }
}

object ReflectClass {
  def method1(v1: Int, v2: String): String = "m" + v1 + v2
}
