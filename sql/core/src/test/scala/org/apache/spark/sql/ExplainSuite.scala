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

import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.test.SharedSQLContext
import org.apache.spark.sql.types.StructType

class ExplainSuite extends QueryTest with SharedSQLContext {
  import testImplicits._

  /**
   * Get the explain from a DataFrame and run the specified action on it.
   */
  private def withNormalizedExplain(df: DataFrame, extended: Boolean)(f: String => Unit) = {
    val output = new java.io.ByteArrayOutputStream()
    Console.withOut(output) {
      df.explain(extended = extended)
    }
    val normalizedOutput = output.toString.replaceAll("#\\d+", "#x")
    f(normalizedOutput)
  }

  /**
   * Runs the plan and makes sure the plans contains all of the keywords.
   */
  private def checkKeywordsExistsInExplain(df: DataFrame, keywords: String*): Unit = {
    withNormalizedExplain(df, extended = true) { normalizedOutput =>
      for (key <- keywords) {
        assert(normalizedOutput.contains(key))
      }
    }
  }

  test("SPARK-23034 show rdd names in RDD scan nodes (Dataset)") {
    val rddWithName = spark.sparkContext.parallelize(Row(1, "abc") :: Nil).setName("testRdd")
    val df = spark.createDataFrame(rddWithName, StructType.fromDDL("c0 int, c1 string"))
    checkKeywordsExistsInExplain(df, keywords = "Scan ExistingRDD testRdd")
  }

  test("SPARK-23034 show rdd names in RDD scan nodes (DataFrame)") {
    val rddWithName = spark.sparkContext.parallelize(ExplainSingleData(1) :: Nil).setName("testRdd")
    val df = spark.createDataFrame(rddWithName)
    checkKeywordsExistsInExplain(df, keywords = "Scan testRdd")
  }

  test("SPARK-24850 InMemoryRelation string representation does not include cached plan") {
    val df = Seq(1).toDF("a").cache()
    checkKeywordsExistsInExplain(df,
      keywords = "InMemoryRelation", "StorageLevel(disk, memory, deserialized, 1 replicas)")
  }

  test("optimized plan should show the rewritten aggregate expression") {
    withTempView("test_agg") {
      sql(
        """
          |CREATE TEMPORARY VIEW test_agg AS SELECT * FROM VALUES
          |  (1, true), (1, false),
          |  (2, true),
          |  (3, false), (3, null),
          |  (4, null), (4, null),
          |  (5, null), (5, true), (5, false) AS test_agg(k, v)
        """.stripMargin)

      // simple explain of queries having every/some/any aggregates. Optimized
      // plan should show the rewritten aggregate expression.
      val df = sql("SELECT k, every(v), some(v), any(v) FROM test_agg GROUP BY k")
      checkKeywordsExistsInExplain(df,
        "Aggregate [k#x], [k#x, min(v#x) AS every(v)#x, max(v#x) AS some(v)#x, " +
          "max(v#x) AS any(v)#x]")
    }
  }

  test("explain inline tables cross-joins") {
    val df = sql(
      """
        |SELECT * FROM VALUES ('one', 1), ('three', null)
        |  CROSS JOIN VALUES ('one', 1), ('three', null)
      """.stripMargin)
    checkKeywordsExistsInExplain(df,
      "Join Cross",
      ":- LocalRelation [col1#x, col2#x]",
      "+- LocalRelation [col1#x, col2#x]")
  }

  test("explain table valued functions") {
    checkKeywordsExistsInExplain(sql("select * from RaNgE(2)"), "Range (0, 2, step=1, splits=None)")
    checkKeywordsExistsInExplain(sql("SELECT * FROM range(3) CROSS JOIN range(3)"),
      "Join Cross",
      ":- Range (0, 3, step=1, splits=None)",
      "+- Range (0, 3, step=1, splits=None)")
  }

  test("explain string functions") {
    // Check if catalyst combine nested `Concat`s
    val df1 = sql(
      """
        |SELECT (col1 || col2 || col3 || col4) col
        |  FROM (SELECT id col1, id col2, id col3, id col4 FROM range(10))
      """.stripMargin)
    checkKeywordsExistsInExplain(df1,
      "Project [concat(cast(id#xL as string), cast(id#xL as string), cast(id#xL as string)" +
        ", cast(id#xL as string)) AS col#x]")

    // Check if catalyst combine nested `Concat`s if concatBinaryAsString=false
    withSQLConf(SQLConf.CONCAT_BINARY_AS_STRING.key -> "false") {
      val df2 = sql(
        """
          |SELECT ((col1 || col2) || (col3 || col4)) col
          |FROM (
          |  SELECT
          |    string(id) col1,
          |    string(id + 1) col2,
          |    encode(string(id + 2), 'utf-8') col3,
          |    encode(string(id + 3), 'utf-8') col4
          |  FROM range(10)
          |)
        """.stripMargin)
      checkKeywordsExistsInExplain(df2,
        "Project [concat(cast(id#xL as string), cast((id#xL + 1) as string), " +
          "cast(encode(cast((id#xL + 2) as string), utf-8) as string), " +
          "cast(encode(cast((id#xL + 3) as string), utf-8) as string)) AS col#x]")

      val df3 = sql(
        """
          |SELECT (col1 || (col3 || col4)) col
          |FROM (
          |  SELECT
          |    string(id) col1,
          |    encode(string(id + 2), 'utf-8') col3,
          |    encode(string(id + 3), 'utf-8') col4
          |  FROM range(10)
          |)
        """.stripMargin)
      checkKeywordsExistsInExplain(df3,
        "Project [concat(cast(id#xL as string), " +
          "cast(encode(cast((id#xL + 2) as string), utf-8) as string), " +
          "cast(encode(cast((id#xL + 3) as string), utf-8) as string)) AS col#x]")
    }
  }

  test("check operator precedence") {
    // We follow Oracle operator precedence in the table below that lists the levels
    // of precedence among SQL operators from high to low:
    // ---------------------------------------------------------------------------------------
    // Operator                                          Operation
    // ---------------------------------------------------------------------------------------
    // +, -                                              identity, negation
    // *, /                                              multiplication, division
    // +, -, ||                                          addition, subtraction, concatenation
    // =, !=, <, >, <=, >=, IS NULL, LIKE, BETWEEN, IN   comparison
    // NOT                                               exponentiation, logical negation
    // AND                                               conjunction
    // OR                                                disjunction
    // ---------------------------------------------------------------------------------------
    checkKeywordsExistsInExplain(sql("select 'a' || 1 + 2"),
      "Project [null AS (CAST(concat(a, CAST(1 AS STRING)) AS DOUBLE) + CAST(2 AS DOUBLE))#x]")
    checkKeywordsExistsInExplain(sql("select 1 - 2 || 'b'"),
      "Project [-1b AS concat(CAST((1 - 2) AS STRING), b)#x]")
    checkKeywordsExistsInExplain(sql("select 2 * 4  + 3 || 'b'"),
      "Project [11b AS concat(CAST(((2 * 4) + 3) AS STRING), b)#x]")
    checkKeywordsExistsInExplain(sql("select 3 + 1 || 'a' || 4 / 2"),
      "Project [4a2.0 AS concat(concat(CAST((3 + 1) AS STRING), a), " +
        "CAST((CAST(4 AS DOUBLE) / CAST(2 AS DOUBLE)) AS STRING))#x]")
    checkKeywordsExistsInExplain(sql("select 1 == 1 OR 'a' || 'b' ==  'ab'"),
      "Project [true AS ((1 = 1) OR (concat(a, b) = ab))#x]")
    checkKeywordsExistsInExplain(sql("select 'a' || 'c' == 'ac' AND 2 == 3"),
      "Project [false AS ((concat(a, c) = ac) AND (2 = 3))#x]")
  }

  test("explain for these functions; use range to avoid constant folding") {
    val df = sql("select ifnull(id, 'x'), nullif(id, 'x'), nvl(id, 'x'), nvl2(id, 'x', 'y') " +
      "from range(2)")
    checkKeywordsExistsInExplain(df,
      "Project [coalesce(cast(id#xL as string), x) AS ifnull(`id`, 'x')#x, " +
        "id#xL AS nullif(`id`, 'x')#xL, coalesce(cast(id#xL as string), x) AS nvl(`id`, 'x')#x, " +
        "x AS nvl2(`id`, 'x', 'y')#x]")
  }

  test("SPARK-26659: explain of DataWritingCommandExec should not contain duplicate cmd.nodeName") {
    withTable("temptable") {
      val df = sql("create table temptable using parquet as select * from range(2)")
      withNormalizedExplain(df, extended = false) { normalizedOutput =>
        assert("Create\\w*?TableAsSelectCommand".r.findAllMatchIn(normalizedOutput).length == 1)
      }
    }
  }
}

case class ExplainSingleData(id: Int)
