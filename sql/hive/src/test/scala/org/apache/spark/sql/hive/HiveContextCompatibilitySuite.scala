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

package org.apache.spark.sql.hive

import org.apache.spark.{SparkConf, SparkContext, SparkFunSuite}

class HiveContextCompatibilitySuite extends SparkFunSuite {

  override protected val enableAutoThreadAudit = false
  private var sc: SparkContext = null
  private var hc: HiveContext = null

  override def beforeAll(): Unit = {
    super.beforeAll()
    sc = SparkContext.getOrCreate(new SparkConf().setMaster("local").setAppName("test"))
    HiveUtils.newTemporaryConfiguration(useInMemoryDerby = true).foreach { case (k, v) =>
      sc.hadoopConfiguration.set(k, v)
    }
    hc = new HiveContext(sc)
  }

  override def afterEach(): Unit = {
    try {
      hc.sharedState.cacheManager.clearCache()
      hc.sessionState.catalog.reset()
    } finally {
      super.afterEach()
    }
  }

  override def afterAll(): Unit = {
    try {
      sc = null
      hc = null
    } finally {
      super.afterAll()
    }
  }

  test("basic operations") {
    val _hc = hc
    import _hc.sparkSession.implicits._
    val df1 = (1 to 20).map { i => (i, i) }.toDF("a", "x")
    val df2 = (1 to 100).map { i => (i, i % 10, i % 2 == 0) }.toDF("a", "b", "c")
      .select($"a", $"b")
      .filter($"a" > 10 && $"b" > 6 && $"c")
    val df3 = df1.join(df2, "a")
    val res = df3.collect()
    val expected = Seq((18, 18, 8)).toDF("a", "x", "b").collect()
    assert(res.toSeq == expected.toSeq)
    df3.createOrReplaceTempView("mai_table")
    val df4 = hc.table("mai_table")
    val res2 = df4.collect()
    assert(res2.toSeq == expected.toSeq)
  }

  test("basic DDLs") {
    val _hc = hc
    import _hc.sparkSession.implicits._
    val databases = hc.sql("SHOW DATABASES").collect().map(_.getString(0))
    assert(databases.toSeq == Seq("default"))
    hc.sql("CREATE DATABASE mee_db")
    hc.sql("USE mee_db")
    val databases2 = hc.sql("SHOW DATABASES").collect().map(_.getString(0))
    assert(databases2.toSet == Set("default", "mee_db"))
    val df = (1 to 10).map { i => ("bob" + i.toString, i) }.toDF("name", "age")
    df.createOrReplaceTempView("mee_table")
    hc.sql("CREATE TABLE moo_table (name string, age int)")
    hc.sql("INSERT INTO moo_table SELECT * FROM mee_table")
    assert(
      hc.sql("SELECT * FROM moo_table order by name").collect().toSeq ==
      df.collect().toSeq.sortBy(_.getString(0)))
    val tables = hc.sql("SHOW TABLES IN mee_db").select("tableName").collect().map(_.getString(0))
    assert(tables.toSet == Set("moo_table", "mee_table"))
    hc.sql("DROP TABLE moo_table")
    hc.sql("DROP TABLE mee_table")
    val tables2 = hc.sql("SHOW TABLES IN mee_db").select("tableName").collect().map(_.getString(0))
    assert(tables2.isEmpty)
    hc.sql("USE default")
    hc.sql("DROP DATABASE mee_db CASCADE")
    val databases3 = hc.sql("SHOW DATABASES").collect().map(_.getString(0))
    assert(databases3.toSeq == Seq("default"))
  }

}
