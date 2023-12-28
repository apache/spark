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

class CollationSuite extends QueryTest
  with SharedSparkSession
  with AdaptiveSparkPlanHelper {

  test("collate keyword") {
    // Serbian case insensitive ordering
    assert(sql("select collate('aaa', 'sr-pr')").collect()(0).getString(0) == "aaa")
    assert(sql("select collation(collate('aaa', 'sr-pr'))").collect()(0).getString(0) == "sr-pr")
    assert(sql("select collate('aaa', 'sr-pr') = collate('AAA', 'sr-pr')")
      .collect()(0).getBoolean(0))
  }

  test("collation comparison literals") {
    // Collation pre-read - https://docs.oracle.com/javase/8/docs/api/java/text/Collator.html
    // You can set a Collator's strength property to determine the level of difference considered
    // significant in comparisons.
    // Four strengths are provided:
    // PRIMARY, SECONDARY, TERTIARY, and IDENTICAL.
    // The exact assignment of strengths to language features is locale dependant.
    // For example, in Czech, "e" and "f" are considered primary differences,
    // while "e" and "ě" are secondary differences,
    // "e" and "E" are tertiary differences and "e" and "e" are identical.

    // In spark implementation chose between PRIMARY, SECONDARY and TERTIARY through following map:
    // case "pr" => Collator.PRIMARY
    // case "se" => Collator.SECONDARY
    // case "tr" => Collator.TERTIARY
    // case "identical" => Collator.IDENTICAL


    // Serbian case insensitive ordering
    assert(sql("select collate('aaa', 'sr-pr') = collate('AAA', 'sr-pr')")
      .collect().head.getBoolean(0))
    assert(sql("select collate('aaa', 'sr-pr') = collate('aaa', 'sr-pr')")
      .collect().head.getBoolean(0))
    assert(sql("select collate('aaa', 'sr-pr') = collate('AaA', 'sr-pr')")
      .collect().head.getBoolean(0))
    assert(!sql("select collate('aaa', 'sr-pr') = collate('zzz', 'sr-pr')")
      .collect().head.getBoolean(0))

    assert(sql("select collate('љзшђ', 'sr-pr') = collate('ЉЗШЂ', 'sr-pr')")
      .collect().head.getBoolean(0))

    // switching to case sensitive Serbian.
    assert(!sql("select collate('aaa', 'sr-tr') = collate('AAA', 'sr-tr')")
      .collect().head.getBoolean(0))
    assert(sql("select collate('aaa', 'sr-tr') = collate('aaa', 'sr-tr')")
      .collect().head.getBoolean(0))
    assert(!sql("select collate('aaa', 'sr-tr') = collate('AaA', 'sr-tr')")
      .collect().head.getBoolean(0))
    assert(!sql("select collate('aaa', 'sr-tr') = collate('zzz', 'sr-tr')")
      .collect().head.getBoolean(0))
    assert(!sql("select collate('љзшђ', 'sr-tr') = collate('ЉЗШЂ', 'sr-tr')")
      .collect().head.getBoolean(0))
    assert(sql("select collate('ЉЗШЂ', 'sr-tr') = collate('ЉЗШЂ', 'sr-tr')")
      .collect().head.getBoolean(0))
  }

  test("collation comparison rows") {
    // Case-insensitive
    val ret = sql("""
      SELECT collate(name, 'sr-pr') FROM
      VALUES('Павле'), ('Зоја'), ('Ивона'), ('Александар'),
      ('ПАВЛЕ'), ('ЗОЈА'), ('ИВОНА'), ('АЛЕКСАНДАР')
       as data(name)
      ORDER BY collate(name, 'sr-pr')
      """).collect().map(r => r.getString(0))

    assert(ret === Array(
      "Александар", "АЛЕКСАНДАР", "Зоја", "ЗОЈА", "Ивона", "ИВОНА", "Павле", "ПАВЛЕ"))
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