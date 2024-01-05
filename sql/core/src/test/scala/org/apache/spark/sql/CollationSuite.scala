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
    checkAnswer(sql("""
      WITH t AS (
        SELECT collate(col1, 'sr-primary') as c
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
        SELECT collate(c, 'sr-primary') as c
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
        SELECT collate(c, 'sr-secondary') as c
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
        SELECT collate(c, 'sr-tertiary') as c
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
        SELECT collate(c, 'sr-primary') as c
        FROM VALUES
          ('ććć'), ('ccc'), ('ččč'), ('ČČČ')
         as data(c)
      """).createOrReplaceTempView("V")

    checkAnswer(sql("SELECT DISTINCT collation(c) FROM V"), Row("sr-primary"))
  }

  test("in operator") {
    sql(
      """
        SELECT * FROM VALUES ('ććć'), ('ccc') as data(c)
      """).createOrReplaceTempView("V")

    // Primary
    checkAnswer(sql("SELECT collate('CCC', 'sr-primary') IN " +
      "(SELECT collate(c, 'sr-primary') FROM V)"), Row(true))
    checkAnswer(sql("SELECT collate('ččč', 'sr-primary') IN " +
      "(SELECT collate(c, 'sr-primary') FROM V)"), Row(true))
    checkAnswer(sql("SELECT collate('xxx', 'sr-primary') IN " +
      "(SELECT collate(c, 'sr-primary') FROM V)"), Row(false))

    // Secondary
    checkAnswer(sql("SELECT collate('CCC', 'sr-secondary') IN " +
      "(SELECT collate(c, 'sr-secondary') FROM V)"), Row(true))
    checkAnswer(sql("SELECT collate('ččč', 'sr-secondary') IN " +
      "(SELECT collate(c, 'sr-secondary') FROM V)"), Row(false))

    // Tertiary
    checkAnswer(sql("SELECT collate('CCC', 'sr-tertiary') IN " +
      "(SELECT collate(c, 'sr-tertiary') FROM V)"), Row(false))
    checkAnswer(sql("SELECT collate('ččč', 'sr-tertiary') IN " +
      "(SELECT collate(c, 'sr-tertiary') FROM V)"), Row(false))
    checkAnswer(sql("SELECT collate('ccc', 'sr-tertiary') IN " +
      "(SELECT collate(c, 'sr-tertiary') FROM V)"), Row(true))
  }

  test("join operator") {
    // Ignore accents and casing.
    sql(
      """
        SELECT collate(c, 'sr-primary') as c FROM VALUES
        ('ććć'), ('ĆĆĆ'), ('ččč'), ('ČČČ')
        as data(c)
      """).createOrReplaceTempView("V1")
    sql(
      """
        SELECT collate(c, 'sr-primary') as c FROM VALUES ('ccc'), ('CCC') as data(c)
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
        SELECT collate(c, 'sr-secondary') as c FROM VALUES
        ('ććć'), ('ĆĆĆ'), ('ččč'), ('ČČČ')
        as data(c)
      """).createOrReplaceTempView("V1")
    sql(
      """
        SELECT collate(c, 'sr-secondary') as c FROM VALUES ('ććć'), ('CCC') as data(c)
      """).createOrReplaceTempView("V2")

    checkAnswer(sql("SELECT * FROM V1 JOIN V2 ON V1.c = V2.c"),
      Seq(
        Row("ććć", "ććć"),
        Row("ĆĆĆ", "ććć")))

    // Respect casing and accents
    sql(
      """
        SELECT collate(c, 'sr-tertiary') as c FROM VALUES
        ('ććć'), ('ĆĆĆ'), ('ččč'), ('ČČČ')
        as data(c)
      """).createOrReplaceTempView("V1")
    sql(
      """
        SELECT collate(c, 'sr-tertiary') as c FROM VALUES ('ććć'), ('CCC') as data(c)
      """).createOrReplaceTempView("V2")

    checkAnswer(sql("SELECT * FROM V1 JOIN V2 ON V1.c = V2.c"),
      Seq(
        Row("ććć", "ććć")))
  }
}