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

import org.apache.spark.sql.catalyst.util.CollatorFactory
import org.apache.spark.sql.test.SharedSparkSession
import org.apache.spark.sql.types.StringType

class CollationSuite extends QueryTest
  with SharedSparkSession {
  test("collate keyword") {
    checkAnswer(sql("select collate('aaa', 'ucs_basic')"), Row("aaa"))
    // This is backward compatibility check - Even string with explicit ucs_basic collation
    // will still be of type StringType object.
    assert(sql("select collate('aaa', 'ucs_basic')").schema(0).dataType == StringType)

    // check collation of string literal.
    checkAnswer(sql("select collation('aaa')"), Row("UCS_BASIC"))

    // ucs basic
    checkAnswer(
      sql(s"select collate('aaa', 'ucs_basic') = collate('aaa', 'ucs_basic')"),
      Row(true))
    checkAnswer(
      sql(s"select collate('aaa', 'ucs_basic') = collate('AAA', 'ucs_basic')"),
      Row(false))
    checkAnswer(
      sql(s"select collate('aaa', 'ucs_basic') = collate('BBB', 'ucs_basic')"),
      Row(false))

    // ucs basic case insensitive.
    checkAnswer(
      sql(s"select collate('aaa', 'ucs_basic_lcase') = collate('AAA', 'ucs_basic_lcase')"),
      Row(true))
    checkAnswer(
      sql(s"select collate('aaa', 'ucs_basic_lcase') = collate('BBB', 'ucs_basic_lcase')"),
      Row(false))
    checkAnswer(
      sql(s"select collation(collate('aaa', 'ucs_basic_lcase'))"),
      Row("UCS_BASIC_LCASE"))
  }

  test("collate keyword with locale") {
    // Collated row is a simple string.
    checkAnswer(sql("select collate('aaa', 'SR_CI_AI')"), Row("aaa"))
    assert(sql("select collate('aaa', 'SR_CI_AI')").schema(0).dataType ==
      StringType(CollatorFactory.getInstance().collationNameToId("SR_CI_AI")))

    // Serbian case + accent insensitive ordering
    var collationName = "SR_CI_AI"
    checkAnswer(sql(s"select collation(collate('aaa', '$collationName'))"), Row(collationName))
    checkAnswer(
      sql(s"select collate('aaa', '$collationName') = collate('AAA', '$collationName')"), Row(true))
    checkAnswer(
      sql(s"select collate('aaa', '$collationName') = collate('aaa', '$collationName')"), Row(true))
    checkAnswer(sql(s"select collate('aaa', '$collationName') = collate('zzz', '$collationName')"),
      Row(false))
    checkAnswer(
      sql(s"select collate('љзшђ', '$collationName') = collate('ЉЗШЂ', '$collationName')"),
      Row(true))
    checkAnswer(sql(s"select collate('ććć', '$collationName') = collate('ĆĆĆ', '$collationName')"),
      Row(true))
    checkAnswer(sql(s"select collate('ččč', '$collationName') = collate('ĆĆĆ', '$collationName')"),
      Row(true))

    // Serbian case insensitive ordering but accent sensitive
    collationName = "SR_CI_AS"
    checkAnswer(sql(s"select collation(collate('aaa', '$collationName'))"), Row(collationName))
    checkAnswer(
      sql(s"select collate('aaa', '$collationName') = collate('AAA', '$collationName')"),
      Row(true))
    checkAnswer(
      sql(s"select collate('aaa', '$collationName') = collate('aaa', '$collationName')"), Row(true))
    checkAnswer(sql(s"select collate('aaa', '$collationName') = collate('zzz', '$collationName')"),
      Row(false))
    checkAnswer(
      sql(s"select collate('љзшђ', '$collationName') = collate('ЉЗШЂ', '$collationName')"),
      Row(true))
    checkAnswer(sql(s"select collate('ććć', '$collationName') = collate('ĆĆĆ', '$collationName')"),
      Row(true))
    checkAnswer(sql(s"select collate('ččč', '$collationName') = collate('ĆĆĆ', '$collationName')"),
      Row(false))
    checkAnswer(sql(s"select collate('ččč', '$collationName') = collate('ććć', '$collationName')"),
      Row(false))
    checkAnswer(sql(s"select collate('ččč', '$collationName') = collate('ččč', '$collationName')"),
      Row(true))

    // Serbian case and accent sensitive ordering.
    collationName = "SR_CS_AS"
    checkAnswer(sql(s"select collation(collate('aaa', '$collationName'))"), Row(collationName))
    checkAnswer(
      sql(s"select collate('aaa', '$collationName') = collate('AAA', '$collationName')"),
      Row(false))
    checkAnswer(
      sql(s"select collate('aaa', '$collationName') = collate('aaa', '$collationName')"), Row(true))
    checkAnswer(sql(s"select collate('aaa', '$collationName') = collate('zzz', '$collationName')"),
      Row(false))
    checkAnswer(
      sql(s"select collate('љзшђ', '$collationName') = collate('ЉЗШЂ', '$collationName')"),
      Row(false))
    checkAnswer(sql(s"select collate('ććć', '$collationName') = collate('ĆĆĆ', '$collationName')"),
      Row(false))
    checkAnswer(sql(s"select collate('ččč', '$collationName') = collate('ĆĆĆ', '$collationName')"),
      Row(false))
    checkAnswer(sql(s"select collate('ččč', '$collationName') = collate('ććć', '$collationName')"),
      Row(false))
    checkAnswer(sql(s"select collate('ččč', '$collationName') = collate('ččč', '$collationName')"),
      Row(true))
  }

  test("order by") {
    // No collations
    checkAnswer(sql("""
      SELECT fruit FROM
      VALUES ('äpfel'), ('Äpfel'), ('Apfel'), ('apfel'), ('Banane'), ('banane')
       as data(fruit)
      ORDER BY fruit
      """), Seq(
      Row("Apfel"), Row("Banane"), Row("apfel"), Row("banane"), Row("Äpfel"), Row("äpfel")))

    // Case/accent-sensitive german
    checkAnswer(sql("""
      SELECT fruit FROM
      VALUES ('äpfel'), ('Äpfel'), ('Apfel'), ('apfel'), ('Banane'), ('banane')
       as data(fruit)
      ORDER BY collate(fruit, 'DE_CS_AS')
      """),
      Seq(
        Row("apfel"), Row("Apfel"),
        Row("äpfel"), Row("Äpfel"),
        Row("banane"), Row("Banane")))
  }

  test("agg simple") {
    checkAnswer(sql("""
      WITH t AS (
        SELECT collate(col1, 'SR_CI_AI') as c
        FROM
        VALUES ('aaa'), ('bbb'), ('AAA'), ('BBB')
      )
      SELECT COUNT(*), c FROM t GROUP BY c
      """), Seq(Row(2, "aaa"), Row(2, "bbb")))
    // TODO: Order here is not deterministic + group election is not deterministic.
  }

  test("collation and group by") {
    // accents. All of these will end up in the same group.
    checkAnswer(sql(
      """
      with t as (
        SELECT collate(c, 'SR_CI_AI') as c
        FROM VALUES
          ('ććć'), ('ccc'), ('ččč'), ('ČČČ')
         as data(c)
       )
       select count(*), c from t  group by c
      """), Seq(Row(4, "ććć")))

    // Now look at the secondary characteristics. Look at accents but ignore case.
    checkAnswer(sql(
      """
      with t as (
        SELECT collate(c, 'SR_CI_AS') as c
        FROM VALUES
          ('ććć'), ('ccc'), ('ččč'), ('ČČČ')
         as data(c)
       )
       select count(*), c from t group by c
      """), Seq(Row(1, "ccc"), Row(1, "ććć"), Row(2, "ččč")))

    // Now tertiary characteristics. Look at accents and case.
    // Every string will end up in it's own group.
    checkAnswer(sql(
      """
      with t as (
        SELECT collate(c, 'SR_CS_AS') as c
        FROM VALUES
          ('ććć'), ('ccc'), ('ččč'), ('ČČČ')
         as data(c)
       )
       select count(*), c from t  group by c
      """), Seq(Row(1, "ccc"), Row(1, "ććć"), Row(1, "ččč"), Row(1, "ČČČ")))
  }

  test("views should propagate collation") {
    sql(
      """
        SELECT collate(c, 'SR_CI_AI') as c
        FROM VALUES
          ('ććć'), ('ccc'), ('ččč'), ('ČČČ')
         as data(c)
      """).createOrReplaceTempView("V")

    checkAnswer(sql("SELECT DISTINCT collation(c) FROM V"), Row("SR_CI_AI"))
  }

  test("in operator") {
    sql(
      """
        SELECT * FROM VALUES ('ććć'), ('ccc') as data(c)
      """).createOrReplaceTempView("V")

    // Case and accent insensitive
    checkAnswer(sql("SELECT collate('CCC', 'SR_CI_AI') IN " +
      "(SELECT collate(c, 'SR_CI_AI') FROM V)"), Row(true))
    checkAnswer(sql("SELECT collate('ččč', 'SR_CI_AI') IN " +
      "(SELECT collate(c, 'SR_CI_AI') FROM V)"), Row(true))
    checkAnswer(sql("SELECT collate('xxx', 'SR_CI_AI') IN " +
      "(SELECT collate(c, 'SR_CI_AI') FROM V)"), Row(false))

    // Case insensitive but accent sensitive
    checkAnswer(sql("SELECT collate('CCC', 'SR_CI_AS') IN " +
      "(SELECT collate(c, 'SR_CI_AS') FROM V)"), Row(true))
    checkAnswer(sql("SELECT collate('ččč', 'SR_CI_AS') IN " +
      "(SELECT collate(c, 'SR_CI_AS') FROM V)"), Row(false))

    // Case and accent sensitive
    checkAnswer(sql("SELECT collate('CCC', 'SR_CS_AS') IN " +
      "(SELECT collate(c, 'SR_CS_AS') FROM V)"), Row(false))
    checkAnswer(sql("SELECT collate('ččč', 'SR_CS_AS') IN " +
      "(SELECT collate(c, 'SR_CS_AS') FROM V)"), Row(false))
    checkAnswer(sql("SELECT collate('ccc', 'SR_CS_AS') IN " +
      "(SELECT collate(c, 'SR_CS_AS') FROM V)"), Row(true))
  }

  test("join operator") {
    // Ignore accents and casing.
    sql(
      """
        SELECT collate(c, 'SR_CI_AI') as c FROM VALUES
        ('ććć'), ('ĆĆĆ'), ('ččč'), ('ČČČ')
        as data(c)
      """).createOrReplaceTempView("V1")
    sql(
      """
        SELECT collate(c, 'SR_CI_AI') as c FROM VALUES ('ccc'), ('CCC') as data(c)
      """).createOrReplaceTempView("V2")

    // Everyone is going to pair with everyone.
    checkAnswer(sql("SELECT * FROM V1 JOIN V2 ON V1.c = V2.c"),
      Seq(
        Row("ććć", "ccc"),
        Row("ĆĆĆ", "ccc"),
        Row("ččč", "ccc"),
        Row("ČČČ", "ccc"),
        Row("ććć", "CCC"),
        Row("ĆĆĆ", "CCC"),
        Row("ččč", "CCC"),
        Row("ČČČ", "CCC")))

    // Ignore casing but not accents
    sql(
      """
        SELECT collate(c, 'SR_CI_AS') as c FROM VALUES
        ('ććć'), ('ĆĆĆ'), ('ččč'), ('ČČČ')
        as data(c)
      """).createOrReplaceTempView("V1")
    sql(
      """
        SELECT collate(c, 'SR_CI_AS') as c FROM VALUES ('ććć'), ('CCC') as data(c)
      """).createOrReplaceTempView("V2")

    checkAnswer(sql("SELECT * FROM V1 JOIN V2 ON V1.c = V2.c"),
      Seq(
        Row("ććć", "ććć"),
        Row("ĆĆĆ", "ććć")))

    // Respect casing and accents
    sql(
      """
        SELECT collate(c, 'SR_CS_AS') as c FROM VALUES
        ('ććć'), ('ĆĆĆ'), ('ččč'), ('ČČČ')
        as data(c)
      """).createOrReplaceTempView("V1")
    sql(
      """
        SELECT collate(c, 'SR_CS_AS') as c FROM VALUES ('ććć'), ('CCC') as data(c)
      """).createOrReplaceTempView("V2")

    checkAnswer(sql("SELECT * FROM V1 JOIN V2 ON V1.c = V2.c"),
      Seq(
        Row("ććć", "ććć")))
  }

  test("create table support") {
    // TODO: Filter pushdown and partitioning are todos.
    val tableName = "parquet_dummy_t"
    withTable(tableName) {
      sql(s"CREATE TABLE IF NOT EXISTS $tableName (c1 STRING COLLATE 'SR_CI_AI') USING PARQUET")
      sql(s"INSERT INTO $tableName VALUES ('aaa')")
      sql(s"INSERT INTO $tableName VALUES ('AAA')")
      checkAnswer(sql(s"SELECT DISTINCT collation(c1) FROM $tableName"), Seq(Row("SR_CI_AI")))
      checkAnswer(sql(s"SELECT COUNT(DISTINCT c1) FROM $tableName"), Seq(Row(1)))
    }
  }

  test("disable filter pushdown") {
    val tableName = "parquet_dummy_t2"
    val collation = "'sr_ci_ai'"
    withTable(tableName) {
      spark.sql(
        s"""
           | CREATE TABLE $tableName(c1 STRING COLLATE $collation) USING PARQUET
           |""".stripMargin)
      spark.sql(s"INSERT INTO $tableName VALUES ('aaa')")
      spark.sql(s"INSERT INTO $tableName VALUES ('AAA')")

      val filters = Seq(
        (">=", Seq(Row("aaa"), Row("AAA"))),
        ("<=", Seq(Row("aaa"), Row("AAA"))),
        (">", Seq()),
        ("<", Seq()),
        ("!=", Seq())
      )

      filters.foreach { filter =>
        val df = sql(s"SELECT * FROM $tableName WHERE c1 ${filter._1} collate('aaa', $collation)")
        assert(df.queryExecution.toString().contains("PushedFilters: []"))
        checkAnswer(df, filter._2)
      }
    }
  }
}