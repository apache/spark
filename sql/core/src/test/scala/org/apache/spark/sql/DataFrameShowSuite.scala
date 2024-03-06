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

import java.io.ByteArrayOutputStream
import java.nio.charset.StandardCharsets
import java.sql.{Date, Timestamp}

import org.apache.spark.sql.functions.rand
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.test.SharedSparkSession

class DataFrameShowSuite extends QueryTest with SharedSparkSession {
  import testImplicits._

  ignore("show") {
    // This test case is intended ignored, but to make sure it compiles correctly
    testData.select($"*").show()
    testData.select($"*").show(1000)
  }


  test("showString: truncate = [0, 20]") {
    val longString = Array.fill(21)("1").mkString
    val df = sparkContext.parallelize(Seq("1", longString)).toDF()
    val expectedAnswerForFalse = """+---------------------+
                                   ||value                |
                                   |+---------------------+
                                   ||1                    |
                                   ||111111111111111111111|
                                   |+---------------------+
                                   |""".stripMargin
    assert(df.showString(10, truncate = 0) === expectedAnswerForFalse)
    val expectedAnswerForTrue = """+--------------------+
                                  ||               value|
                                  |+--------------------+
                                  ||                   1|
                                  ||11111111111111111...|
                                  |+--------------------+
                                  |""".stripMargin
    assert(df.showString(10, truncate = 20) === expectedAnswerForTrue)
  }

  test("showString: truncate = [0, 20], vertical = true") {
    val longString = Array.fill(21)("1").mkString
    val df = sparkContext.parallelize(Seq("1", longString)).toDF()
    val expectedAnswerForFalse = "-RECORD 0----------------------\n" +
                                 " value | 1                     \n" +
                                 "-RECORD 1----------------------\n" +
                                 " value | 111111111111111111111 \n"
    assert(df.showString(10, truncate = 0, vertical = true) === expectedAnswerForFalse)
    val expectedAnswerForTrue = "-RECORD 0---------------------\n" +
                                " value | 1                    \n" +
                                "-RECORD 1---------------------\n" +
                                " value | 11111111111111111... \n"
    assert(df.showString(10, truncate = 20, vertical = true) === expectedAnswerForTrue)
  }

  test("showString: truncate = [3, 17]") {
    val longString = Array.fill(21)("1").mkString
    val df = sparkContext.parallelize(Seq("1", longString)).toDF()
    val expectedAnswerForFalse = """+-----+
                                   ||value|
                                   |+-----+
                                   ||    1|
                                   ||  111|
                                   |+-----+
                                   |""".stripMargin
    assert(df.showString(10, truncate = 3) === expectedAnswerForFalse)
    val expectedAnswerForTrue = """+-----------------+
                                  ||            value|
                                  |+-----------------+
                                  ||                1|
                                  ||11111111111111...|
                                  |+-----------------+
                                  |""".stripMargin
    assert(df.showString(10, truncate = 17) === expectedAnswerForTrue)
  }

  test("showString: truncate = [3, 17], vertical = true") {
    val longString = Array.fill(21)("1").mkString
    val df = sparkContext.parallelize(Seq("1", longString)).toDF()
    val expectedAnswerForFalse = "-RECORD 0----\n" +
                                 " value | 1   \n" +
                                 "-RECORD 1----\n" +
                                 " value | 111 \n"
    assert(df.showString(10, truncate = 3, vertical = true) === expectedAnswerForFalse)
    val expectedAnswerForTrue = "-RECORD 0------------------\n" +
                                " value | 1                 \n" +
                                "-RECORD 1------------------\n" +
                                " value | 11111111111111... \n"
    assert(df.showString(10, truncate = 17, vertical = true) === expectedAnswerForTrue)
  }

  test("showString(negative)") {
    val expectedAnswer = """+---+-----+
                           ||key|value|
                           |+---+-----+
                           |+---+-----+
                           |only showing top 0 rows
                           |""".stripMargin
    assert(testData.select($"*").showString(-1) === expectedAnswer)
  }

  test("showString(negative), vertical = true") {
    val expectedAnswer = "(0 rows)\n"
    assert(testData.select($"*").showString(-1, vertical = true) === expectedAnswer)
  }

  test("showString(0)") {
    val expectedAnswer = """+---+-----+
                           ||key|value|
                           |+---+-----+
                           |+---+-----+
                           |only showing top 0 rows
                           |""".stripMargin
    assert(testData.select($"*").showString(0) === expectedAnswer)
  }

  test("showString(Int.MaxValue)") {
    val df = Seq((1, 2), (3, 4)).toDF("a", "b")
    val expectedAnswer = """+---+---+
                           ||  a|  b|
                           |+---+---+
                           ||  1|  2|
                           ||  3|  4|
                           |+---+---+
                           |""".stripMargin
    assert(df.showString(Int.MaxValue) === expectedAnswer)
  }

  test("showString(0), vertical = true") {
    val expectedAnswer = "(0 rows)\n"
    assert(testData.select($"*").showString(0, vertical = true) === expectedAnswer)
  }

  test("showString: array") {
    val df = Seq(
      (Array(1, 2, 3), Array(1, 2, 3)),
      (Array(2, 3, 4), Array(2, 3, 4))
    ).toDF()
    val expectedAnswer = """+---------+---------+
                           ||       _1|       _2|
                           |+---------+---------+
                           ||[1, 2, 3]|[1, 2, 3]|
                           ||[2, 3, 4]|[2, 3, 4]|
                           |+---------+---------+
                           |""".stripMargin
    assert(df.showString(10) === expectedAnswer)
  }

  test("showString: array, vertical = true") {
    val df = Seq(
      (Array(1, 2, 3), Array(1, 2, 3)),
      (Array(2, 3, 4), Array(2, 3, 4))
    ).toDF()
    val expectedAnswer = "-RECORD 0--------\n" +
                         " _1  | [1, 2, 3] \n" +
                         " _2  | [1, 2, 3] \n" +
                         "-RECORD 1--------\n" +
                         " _1  | [2, 3, 4] \n" +
                         " _2  | [2, 3, 4] \n"
    assert(df.showString(10, vertical = true) === expectedAnswer)
  }

  test("showString: binary") {
    val df = Seq(
      ("12".getBytes(StandardCharsets.UTF_8), "ABC.".getBytes(StandardCharsets.UTF_8)),
      ("34".getBytes(StandardCharsets.UTF_8), "12346".getBytes(StandardCharsets.UTF_8))
    ).toDF()
    val expectedAnswer = """+-------+----------------+
                           ||     _1|              _2|
                           |+-------+----------------+
                           ||[31 32]|   [41 42 43 2E]|
                           ||[33 34]|[31 32 33 34 36]|
                           |+-------+----------------+
                           |""".stripMargin
    assert(df.showString(10) === expectedAnswer)
  }

  test("showString: binary, vertical = true") {
    val df = Seq(
      ("12".getBytes(StandardCharsets.UTF_8), "ABC.".getBytes(StandardCharsets.UTF_8)),
      ("34".getBytes(StandardCharsets.UTF_8), "12346".getBytes(StandardCharsets.UTF_8))
    ).toDF()
    val expectedAnswer = "-RECORD 0---------------\n" +
                         " _1  | [31 32]          \n" +
                         " _2  | [41 42 43 2E]    \n" +
                         "-RECORD 1---------------\n" +
                         " _1  | [33 34]          \n" +
                         " _2  | [31 32 33 34 36] \n"
    assert(df.showString(10, vertical = true) === expectedAnswer)
  }

  test("showString: minimum column width") {
    val df = Seq(
      (1, 1),
      (2, 2)
    ).toDF()
    val expectedAnswer = """+---+---+
                           || _1| _2|
                           |+---+---+
                           ||  1|  1|
                           ||  2|  2|
                           |+---+---+
                           |""".stripMargin
    assert(df.showString(10) === expectedAnswer)
  }

  test("showString: minimum column width, vertical = true") {
    val df = Seq(
      (1, 1),
      (2, 2)
    ).toDF()
    val expectedAnswer = "-RECORD 0--\n" +
                         " _1  | 1   \n" +
                         " _2  | 1   \n" +
                         "-RECORD 1--\n" +
                         " _1  | 2   \n" +
                         " _2  | 2   \n"
    assert(df.showString(10, vertical = true) === expectedAnswer)
  }

  test("SPARK-33690: showString: escape meta-characters") {
    val df1 = spark.sql("SELECT 'aaa\nbbb\tccc\rddd\feee\bfff\u000Bggg\u0007hhh'")
    assert(df1.showString(1, truncate = 0) ===
      """+--------------------------------------+
        ||aaa\nbbb\tccc\rddd\feee\bfff\vggg\ahhh|
        |+--------------------------------------+
        ||aaa\nbbb\tccc\rddd\feee\bfff\vggg\ahhh|
        |+--------------------------------------+
        |""".stripMargin)

    val df2 = spark.sql("SELECT array('aaa\nbbb\tccc\rddd\feee\bfff\u000Bggg\u0007hhh')")
    assert(df2.showString(1, truncate = 0) ===
      """+---------------------------------------------+
        ||array(aaa\nbbb\tccc\rddd\feee\bfff\vggg\ahhh)|
        |+---------------------------------------------+
        ||[aaa\nbbb\tccc\rddd\feee\bfff\vggg\ahhh]     |
        |+---------------------------------------------+
        |""".stripMargin)

    val df3 =
      spark.sql("SELECT map('aaa\nbbb\tccc', 'aaa\nbbb\tccc\rddd\feee\bfff\u000Bggg\u0007hhh')")
    assert(df3.showString(1, truncate = 0) ===
      """+----------------------------------------------------------+
        ||map(aaa\nbbb\tccc, aaa\nbbb\tccc\rddd\feee\bfff\vggg\ahhh)|
        |+----------------------------------------------------------+
        ||{aaa\nbbb\tccc -> aaa\nbbb\tccc\rddd\feee\bfff\vggg\ahhh} |
        |+----------------------------------------------------------+
        |""".stripMargin)

    val df4 =
      spark.sql("SELECT named_struct('v', 'aaa\nbbb\tccc\rddd\feee\bfff\u000Bggg\u0007hhh')")
    assert(df4.showString(1, truncate = 0) ===
      """+-------------------------------------------------------+
        ||named_struct(v, aaa\nbbb\tccc\rddd\feee\bfff\vggg\ahhh)|
        |+-------------------------------------------------------+
        ||{aaa\nbbb\tccc\rddd\feee\bfff\vggg\ahhh}               |
        |+-------------------------------------------------------+
        |""".stripMargin)
  }

  test("SPARK-7319 showString") {
    val expectedAnswer = """+---+-----+
                           ||key|value|
                           |+---+-----+
                           ||  1|    1|
                           |+---+-----+
                           |only showing top 1 row
                           |""".stripMargin
    assert(testData.select($"*").showString(1) === expectedAnswer)
  }

  test("SPARK-7319 showString, vertical = true") {
    val expectedAnswer = "-RECORD 0----\n" +
                         " key   | 1   \n" +
                         " value | 1   \n" +
                         "only showing top 1 row\n"
    assert(testData.select($"*").showString(1, vertical = true) === expectedAnswer)
  }

  test("SPARK-23023 Cast rows to strings in showString") {
    val df1 = Seq(Seq(1, 2, 3, 4)).toDF("a")
    assert(df1.showString(10) ===
      s"""+------------+
         ||           a|
         |+------------+
         ||[1, 2, 3, 4]|
         |+------------+
         |""".stripMargin)
    val df2 = Seq(Map(1 -> "a", 2 -> "b")).toDF("a")
    assert(df2.showString(10) ===
      s"""+----------------+
         ||               a|
         |+----------------+
         ||{1 -> a, 2 -> b}|
         |+----------------+
         |""".stripMargin)
    val df3 = Seq(((1, "a"), 0), ((2, "b"), 0)).toDF("a", "b")
    assert(df3.showString(10) ===
      s"""+------+---+
         ||     a|  b|
         |+------+---+
         ||{1, a}|  0|
         ||{2, b}|  0|
         |+------+---+
         |""".stripMargin)
  }

  test("SPARK-7327 show with empty dataFrame") {
    val expectedAnswer = """+---+-----+
                           ||key|value|
                           |+---+-----+
                           |+---+-----+
                           |""".stripMargin
    assert(testData.select($"*").filter($"key" < 0).showString(1) === expectedAnswer)
  }

  test("SPARK-7327 show with empty dataFrame, vertical = true") {
    assert(testData.select($"*").filter($"key" < 0).showString(1, vertical = true) === "(0 rows)\n")
  }

  test("SPARK-18350 show with session local timezone") {
    val d = Date.valueOf("2016-12-01")
    val ts = Timestamp.valueOf("2016-12-01 00:00:00")
    val df = Seq((d, ts)).toDF("d", "ts")
    val expectedAnswer = """+----------+-------------------+
                           ||d         |ts                 |
                           |+----------+-------------------+
                           ||2016-12-01|2016-12-01 00:00:00|
                           |+----------+-------------------+
                           |""".stripMargin
    assert(df.showString(1, truncate = 0) === expectedAnswer)

    withSQLConf(SQLConf.SESSION_LOCAL_TIMEZONE.key -> "UTC") {

      val expectedAnswer = """+----------+-------------------+
                             ||d         |ts                 |
                             |+----------+-------------------+
                             ||2016-12-01|2016-12-01 08:00:00|
                             |+----------+-------------------+
                             |""".stripMargin
      assert(df.showString(1, truncate = 0) === expectedAnswer)
    }
  }

  test("SPARK-18350 show with session local timezone, vertical = true") {
    val d = Date.valueOf("2016-12-01")
    val ts = Timestamp.valueOf("2016-12-01 00:00:00")
    val df = Seq((d, ts)).toDF("d", "ts")
    val expectedAnswer = "-RECORD 0------------------\n" +
                         " d   | 2016-12-01          \n" +
                         " ts  | 2016-12-01 00:00:00 \n"
    assert(df.showString(1, truncate = 0, vertical = true) === expectedAnswer)

    withSQLConf(SQLConf.SESSION_LOCAL_TIMEZONE.key -> "UTC") {

      val expectedAnswer = "-RECORD 0------------------\n" +
                           " d   | 2016-12-01          \n" +
                           " ts  | 2016-12-01 08:00:00 \n"
      assert(df.showString(1, truncate = 0, vertical = true) === expectedAnswer)
    }
  }

  test("SPARK-8608: call `show` on local DataFrame with random columns should return same value") {
    val df = testData.select(rand(33))
    assert(df.showString(5) == df.showString(5))

    // We will reuse the same Expression object for LocalRelation.
    val df1 = (1 to 10).map(Tuple1.apply).toDF().select(rand(33))
    assert(df1.showString(5) == df1.showString(5))
  }

  test("dataframe toString") {
    assert(testData.toString === "[key: int, value: string]")
    assert(testData("key").toString === "key")
    assert($"test".toString === "test")
  }

  test("SPARK-12398 truncated toString") {
    val df1 = Seq((1L, "row1")).toDF("id", "name")
    assert(df1.toString() === "[id: bigint, name: string]")

    val df2 = Seq((1L, "c2", false)).toDF("c1", "c2", "c3")
    assert(df2.toString === "[c1: bigint, c2: string ... 1 more field]")

    val df3 = Seq((1L, "c2", false, 10)).toDF("c1", "c2", "c3", "c4")
    assert(df3.toString === "[c1: bigint, c2: string ... 2 more fields]")

    val df4 = Seq((1L, Tuple2(1L, "val"))).toDF("c1", "c2")
    assert(df4.toString === "[c1: bigint, c2: struct<_1: bigint, _2: string>]")

    val df5 = Seq((1L, Tuple2(1L, "val"), 20.0)).toDF("c1", "c2", "c3")
    assert(df5.toString === "[c1: bigint, c2: struct<_1: bigint, _2: string> ... 1 more field]")

    val df6 = Seq((1L, Tuple2(1L, "val"), 20.0, 1)).toDF("c1", "c2", "c3", "c4")
    assert(df6.toString === "[c1: bigint, c2: struct<_1: bigint, _2: string> ... 2 more fields]")

    val df7 = Seq((1L, Tuple3(1L, "val", 2), 20.0, 1)).toDF("c1", "c2", "c3", "c4")
    assert(
      df7.toString ===
        "[c1: bigint, c2: struct<_1: bigint, _2: string ... 1 more field> ... 2 more fields]")

    val df8 = Seq((1L, Tuple7(1L, "val", 2, 3, 4, 5, 6), 20.0, 1)).toDF("c1", "c2", "c3", "c4")
    assert(
      df8.toString ===
        "[c1: bigint, c2: struct<_1: bigint, _2: string ... 5 more fields> ... 2 more fields]")

    val df9 =
      Seq((1L, Tuple4(1L, Tuple4(1L, 2L, 3L, 4L), 2L, 3L), 20.0, 1)).toDF("c1", "c2", "c3", "c4")
    assert(
      df9.toString ===
        "[c1: bigint, c2: struct<_1: bigint," +
          " _2: struct<_1: bigint," +
          " _2: bigint ... 2 more fields> ... 2 more fields> ... 2 more fields]")

  }

  test("SPARK-34308: printSchema: escape meta-characters") {
    val captured = new ByteArrayOutputStream()

    val df1 = spark.sql("SELECT 'aaa\nbbb\tccc\rddd\feee\bfff\u000Bggg\u0007hhh'")
    Console.withOut(captured) {
      df1.printSchema()
    }
    assert(captured.toString ===
      """root
        | |-- aaa\nbbb\tccc\rddd\feee\bfff\vggg\ahhh: string (nullable = false)
        |
        |""".stripMargin)
    captured.reset()

    val df2 = spark.sql("SELECT array('aaa\nbbb\tccc\rddd\feee\bfff\u000Bggg\u0007hhh')")
    Console.withOut(captured) {
      df2.printSchema()
    }
    assert(captured.toString ===
      """root
        | |-- array(aaa\nbbb\tccc\rddd\feee\bfff\vggg\ahhh): array (nullable = false)
        | |    |-- element: string (containsNull = false)
        |
        |""".stripMargin)
    captured.reset()

    val df3 =
      spark.sql("SELECT map('aaa\nbbb\tccc', 'aaa\nbbb\tccc\rddd\feee\bfff\u000Bggg\u0007hhh')")
    Console.withOut(captured) {
      df3.printSchema()
    }
    assert(captured.toString ===
      """root
        | |-- map(aaa\nbbb\tccc, aaa\nbbb\tccc\rddd\feee\bfff\vggg\ahhh): map (nullable = false)
        | |    |-- key: string
        | |    |-- value: string (valueContainsNull = false)
        |
        |""".stripMargin)
    captured.reset()

    val df4 =
      spark.sql("SELECT named_struct('v', 'aaa\nbbb\tccc\rddd\feee\bfff\u000Bggg\u0007hhh')")
    Console.withOut(captured) {
      df4.printSchema()
    }
    assert(captured.toString ===
      """root
        | |-- named_struct(v, aaa\nbbb\tccc\rddd\feee\bfff\vggg\ahhh): struct (nullable = false)
        | |    |-- v: string (nullable = false)
        |
        |""".stripMargin)
  }
}
