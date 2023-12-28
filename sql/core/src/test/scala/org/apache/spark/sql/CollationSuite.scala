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

import org.apache.spark.sql.execution.adaptive.AdaptiveSparkPlanHelper
import org.apache.spark.sql.test.SharedSparkSession
import org.apache.spark.sql.types.CollatedStringType

class CollationSuite extends QueryTest
  with SharedSparkSession
  with AdaptiveSparkPlanHelper {

  // Collation pre-read - https://docs.oracle.com/javase/8/docs/api/java/text/Collator.html
  // You can set a Collator's strength property to determine the level of difference considered
  // significant in comparisons.
  // Four strengths are provided:
  // PRIMARY, SECONDARY, TERTIARY, and IDENTICAL.
  // The exact assignment of strengths to language features is locale dependant.
  // For example, in Czech, "e" and "f" are considered primary differences,
  // while "e" and "ě" are secondary differences,
  // "e" and "E" are tertiary differences and "e" and "e" are identical.

  test("collate keyword") {
    // Collated row is a simple string.
    checkAnswer(sql("select collate('aaa', 'sr-primary')"), Row("aaa"))
    assert(sql("select collate('aaa', 'sr-primary')").schema(0).dataType ==
      CollatedStringType("sr-primary"))

    // Serbian case + accent insensitive ordering
    var collationName = "sr-primary"
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
    collationName = "sr-secondary"
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
    collationName = "sr-tertiary"
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
      ORDER BY collate(fruit, 'de-tertiary')
      """),
      Seq(
        Row("apfel"), Row("Apfel"),
        Row("äpfel"), Row("Äpfel"),
        Row("banane"), Row("Banane")))
  }

  test("agg simple") {
    assert(sql("""
      WITH t AS (
        SELECT collate(col1, 'sr-pr') as c
        FROM
        VALUES ('a'), ('A')
      )
      SELECT count(DISTINCT c) FROM t
      """).collect()(0).get(0) == 1)
  }

  test("collation and group by") {
    val res = sql(
      """
      with t as (
        SELECT collate(c, 'sr-pr') as c
        FROM VALUES
          ('aaa'), ('bbb'), ('AAA'), ('BBB')
         as data(c)
       )
       select count(*), c from t  group by c
      """).collect().map(r => (r.getLong(0), r.getString(1)))
    assert(res === Array((2, "aaa"), (2, "bbb")))

    // accents. All of these will end up in the same group.
    val res2 = sql(
      """
      with t as (
        SELECT collate(c, 'sr-pr') as c
        FROM VALUES
          ('ććć'), ('ccc'), ('ččč'), ('ČČČ')
         as data(c)
       )
       select count(*), c from t  group by c
      """).collect().map(r => (r.getLong(0), r.getString(1)))
    assert(res2 === Array((4, "ććć")))

    // Now look at the secondary characteristics. Look at accents but ignore case.
    val res3 = sql(
      """
      with t as (
        SELECT collate(c, 'sr-se') as c
        FROM VALUES
          ('ććć'), ('ccc'), ('ččč'), ('ČČČ')
         as data(c)
       )
       select count(*), c from t  group by c
      """).collect().map(r => (r.getLong(0), r.getString(1)))
    assert(res3 === Array((1, "ccc"), (1, "ććć"), (2, "ččč")))

    // Now teriary characteristics. Look at accents and case.
    val res4 = sql(
      """
      with t as (
        SELECT collate(c, 'sr-tr') as c
        FROM VALUES
          ('ććć'), ('ccc'), ('ččč'), ('ČČČ')
         as data(c)
       )
       select count(*), c from t  group by c
      """).collect().map(r => (r.getLong(0), r.getString(1)))
    assert(res4 === Array((1, "ccc"), (1, "ććć"), (1, "ččč"), (1, "ČČČ")))
  }

  test("views should propagate collation") {
    sql(
      """
        SELECT collate(c, 'sr-pr') as c
        FROM VALUES
          ('ććć'), ('ccc'), ('ččč'), ('ČČČ')
         as data(c)
      """).createOrReplaceTempView("V")

    assert(sql("SELECT collation(c) FROM V").collect().head.getString(0) == "sr-pr")
  }

  test("in operator") {
    sql(
      """
        SELECT collate(c, 'sr-pr') as c
        FROM VALUES
          ('ććć'), ('ccc'), ('ččč'), ('ČČČ')
         as data(c)
      """).createOrReplaceTempView("V")

    assert(sql("SELECT collate('CCC', 'sr-pr') IN (SELECT c FROM V)").collect().head.getBoolean(0))
  }

  test("join operator") {
    // TODO: This still doesn't work.
    // In physical plan we get BroadcastHashJoin which still isn't collation aware.
    sql(
      """
        SELECT collate(c, 'sr-pr') as c
        FROM VALUES
          ('ććć'), ('ccc'), ('ččč'), ('ČČČ')
         as data(c)
      """).createOrReplaceTempView("V")

    sql("SELECT * FROM V a JOIN V b ON a.c = b.c").explain(true)
  }
}