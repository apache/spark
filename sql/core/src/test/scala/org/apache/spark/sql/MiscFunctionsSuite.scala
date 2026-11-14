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
import org.apache.spark.sql.catalyst.ExtendedAnalysisException
import org.apache.spark.sql.catalyst.expressions.{AesEncrypt, Alias, ArrayTransform, Hex}
import org.apache.spark.sql.catalyst.parser.ParseException
import org.apache.spark.sql.catalyst.plans.logical.Project
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

  test("SPARK-60934: null and foldable aes_encrypt stay one value") {
    // A null IV never reaches the cipher. A foldable non-empty IV, or a foldable
    // mode that does not draw, is one ciphertext. A foldable empty IV still draws,
    // and so does CBC or GCM with the default empty IV, so a join must reject it.
    checkAnswer(
      sql("SELECT hex(aes_encrypt('abc', '1234567890123456', 'GCM', 'NONE', " +
        "CAST(NULL AS BINARY)))"),
      Row(null))
    checkAnswer(
      sql("SELECT count(*) FROM VALUES (1) AS t(id) JOIN VALUES (1) AS u(id) " +
        "ON aes_encrypt('abc', '1234567890123456', 'GCM', 'NONE', " +
        "CAST(NULL AS BINARY)) = CAST(NULL AS BINARY)"),
      Row(0))
    checkAnswer(
      sql("WITH c AS (SELECT aes_encrypt('abc', '1234567890123456') AS enc) " +
        "SELECT a.enc = b.enc FROM c a JOIN c b"),
      Row(true))
    checkAnswer(
      sql("SELECT aes_encrypt('abc', '1234567890123456') = " +
        "aes_encrypt('abc', '1234567890123456')"),
      Row(false))
    checkAnswer(
      sql("SELECT count(*) FROM VALUES (1) AS t(id) JOIN VALUES (1) AS u(id) " +
        "ON aes_encrypt('abc', '1234567890123456', 'GCM', 'NONE', " +
        "unhex('000000000000000000000000')) = CAST(NULL AS BINARY)"),
      Row(0))
    checkAnswer(
      sql("SELECT count(*) FROM VALUES (1) AS t(id) JOIN VALUES (1) AS u(id) " +
        "ON aes_encrypt('abc', '1234567890123456', upper('ecb')) = " +
        "CAST(NULL AS BINARY)"),
      Row(0))
    val emptyIv = intercept[ExtendedAnalysisException] {
      sql("SELECT count(*) FROM VALUES (1) AS t(id) JOIN VALUES (1) AS u(id) " +
        "ON aes_encrypt('abc', '1234567890123456', 'GCM', 'NONE', X'') = X'00'").collect()
    }
    assert(emptyIv.getCondition === "INVALID_NON_DETERMINISTIC_EXPRESSIONS")
  }

  test("SPARK-60934: cast of a binary mode literal stays one ciphertext") {
    // CAST(X'47434D' AS STRING) is GCM, so the CTE must compare one value.
    // CAST(X'454342' AS STRING) is ECB and stays deterministic.
    checkAnswer(
      sql("WITH c AS (SELECT aes_encrypt('abc', '1234567890123456', " +
        "CAST(X'47434D' AS STRING)) AS enc) " +
        "SELECT a.enc = b.enc FROM c a JOIN c b"),
      Row(true))
    checkAnswer(
      sql("WITH c AS (SELECT aes_encrypt('abc', '1234567890123456', " +
        "CAST(X'454342' AS STRING)) AS enc) " +
        "SELECT a.enc = b.enc FROM c a JOIN c b"),
      Row(true))
  }

  test("SPARK-60934: to_binary and encode IVs are accepted in a join") {
    // These builders are unfoldable until ReplaceExpressions. A non-empty
    // constant IV is still one ciphertext, so the join condition is legal.
    checkAnswer(
      sql("SELECT count(*) FROM VALUES (1) AS t(id) JOIN VALUES (1) AS u(id) " +
        "ON aes_encrypt('abc', '1234567890123456', 'GCM', 'NONE', " +
        "to_binary('000000000000000000000000', 'hex')) = X'00'"),
      Row(0))
    checkAnswer(
      sql("SELECT count(*) FROM VALUES (1) AS t(id) JOIN VALUES (1) AS u(id) " +
        "ON aes_encrypt('abc', '1234567890123456', 'CBC', 'DEFAULT', " +
        "encode('0123456789abcdef', 'utf-8')) = X'00'"),
      Row(0))
    checkAnswer(
      sql("SELECT count(*) FROM VALUES (1) AS t(id) JOIN VALUES (1) AS u(id) " +
        "ON aes_encrypt('abc', '1234567890123456', 'GCM', 'NONE', " +
        "to_binary('0123456789ab', 'utf-8')) = X'00'"),
      Row(0))
  }

  test("SPARK-60934: generated-IV aes_encrypt can group and sort") {
    // The call draws an IV without extending Nondeterministic. GROUP BY has to
    // evaluate it once per row, and ORDER BY has to keep every input row.
    val grouped = sql(
      "SELECT aes_encrypt(cast(id AS string), '1234567890123456') = " +
      "aes_encrypt(cast(id AS string), '1234567890123456'), count(*) " +
      "FROM VALUES (1), (1) AS t(id) " +
      "GROUP BY aes_encrypt(cast(id AS string), '1234567890123456')")
    assert(grouped.queryExecution.analyzed.toString.contains("_nondeterministic"))
    checkAnswer(grouped, Seq(Row(true, 1), Row(true, 1)))

    val ordered = sql(
      "SELECT id FROM VALUES (1), (2) AS t(id) " +
      "ORDER BY aes_encrypt(cast(id AS string), '1234567890123456')")
    assert(ordered.queryExecution.analyzed.toString.contains("_nondeterministic"))
    assert(ordered.collect().map(_.getInt(0)).toSet === Set(1, 2))

    val ecb = sql(
      "SELECT count(*) FROM VALUES (1), (1) AS t(id) " +
      "GROUP BY aes_encrypt(cast(id AS string), '1234567890123456', 'ECB')")
    assert(!ecb.queryExecution.analyzed.toString.contains("_nondeterministic"))
    checkAnswer(ecb, Row(2))

    val explicitIv = sql(
      "SELECT count(*) FROM VALUES (1), (1) AS t(id) " +
      "GROUP BY aes_encrypt(cast(id AS string), '1234567890123456', 'GCM', 'NONE', " +
      "unhex('000000000000000000000000'))")
    assert(!explicitIv.queryExecution.analyzed.toString.contains("_nondeterministic"))
    checkAnswer(explicitIv, Row(2))

    val join = intercept[ExtendedAnalysisException] {
      sql("SELECT count(*) FROM VALUES (1) AS t(id) JOIN VALUES (1) AS u(id) " +
        "ON aes_encrypt('abc', '1234567890123456') = X'00'").collect()
    }
    assert(join.getCondition === "INVALID_NON_DETERMINISTIC_EXPRESSIONS")
  }

  test("SPARK-60934: aes_encrypt that reads a lambda variable stays bound") {
    // x is bound only inside the lambda. The whole transform is one value per
    // input row. A call that ignores the lambda variable is still pulled out.
    def nondeterministicAliases(df: DataFrame): Seq[Alias] = {
      df.queryExecution.analyzed.collect {
        case p: Project => p.projectList.collect {
          case a: Alias if a.name.contains("_nondeterministic") => a
        }
      }.flatten.toSeq
    }

    val grouped = sql(
      "SELECT count(*) FROM VALUES (array('a', 'b')), (array('c')) AS t(arr) " +
      "GROUP BY transform(arr, x -> aes_encrypt(x, '0000111122223333'))")
    assert(nondeterministicAliases(grouped).exists(_.child.isInstanceOf[ArrayTransform]))
    checkAnswer(grouped, Seq(Row(1), Row(1)))

    val ordered = sql(
      "SELECT arr FROM VALUES (array('a', 'b')), (array('c')) AS t(arr) " +
      "ORDER BY transform(arr, x -> aes_encrypt(x, '0000111122223333'))")
    assert(nondeterministicAliases(ordered).exists(_.child.isInstanceOf[ArrayTransform]))
    assert(ordered.collect().map(_.getSeq[String](0).toSeq).toSet ===
      Set(Seq("a", "b"), Seq("c")))

    val distributed = sql(
      "SELECT arr FROM VALUES (array('a', 'b')), (array('c')) AS t(arr) " +
      "DISTRIBUTE BY transform(arr, x -> aes_encrypt(x, '0000111122223333'))")
    assert(nondeterministicAliases(distributed).exists(_.child.isInstanceOf[ArrayTransform]))
    assert(distributed.collect().map(_.getSeq[String](0).toSeq).toSet ===
      Set(Seq("a", "b"), Seq("c")))

    val ignoresLambda = sql(
      "SELECT count(*) FROM VALUES (array('a', 'b')), (array('c')) AS t(arr) " +
      "GROUP BY transform(arr, x -> aes_encrypt('a', '0000111122223333'))")
    val pulled = nondeterministicAliases(ignoresLambda)
    assert(pulled.nonEmpty && pulled.forall(_.child.isInstanceOf[AesEncrypt]))
    checkAnswer(ignoresLambda, Seq(Row(1), Row(1)))
  }

  test("SPARK-60934: a throwing mode or IV does not fail analysis") {
    // CheckAnalysis reads the flag before constant folding. A foldable child
    // that throws in a branch no row takes stays unknown, including a view.
    withSQLConf(SQLConf.ANSI_ENABLED.key -> "true") {
      checkAnswer(
        sql("SELECT CASE WHEN id < 0 THEN aes_encrypt('a', '1234567890123456', " +
          "CAST(1/0 AS STRING)) END FROM range(3)"),
        Seq(Row(null), Row(null), Row(null)))
      checkAnswer(
        sql("SELECT CASE WHEN id < 0 THEN aes_encrypt('a', '1234567890123456', " +
          "'GCM', 'NONE', unhex(CAST(1/0 AS STRING))) END FROM range(3)"),
        Seq(Row(null), Row(null), Row(null)))
      withTempView("throwing_aes_mode") {
        sql("CREATE TEMP VIEW throwing_aes_mode AS SELECT CASE WHEN id < 0 THEN " +
          "aes_encrypt('a', '1234567890123456', CAST(1/0 AS STRING)) END AS enc " +
          "FROM range(3)")
        checkAnswer(sql("SELECT enc FROM throwing_aes_mode"),
          Seq(Row(null), Row(null), Row(null)))
      }
      checkAnswer(
        sql("SELECT count(*) FROM VALUES (1) AS t(id) JOIN VALUES (1) AS u(id) " +
          "ON aes_encrypt('abc', '1234567890123456', 'ECB') = " +
          "aes_encrypt('abc', '1234567890123456', 'ECB')"),
        Row(1))
    }
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
