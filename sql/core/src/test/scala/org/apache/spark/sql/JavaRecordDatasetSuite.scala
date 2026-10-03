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

import java.util.Locale

import _root_.test.org.apache.spark.sql.JavaRecordEncoderTestData._

import org.apache.spark.SparkRuntimeException
import org.apache.spark.sql.test.SharedSparkSession

class JavaRecordDatasetSuite extends QueryTest with SharedSparkSession {

  private implicit val personEncoder: Encoder[Person] = Encoders.record(classOf[Person])
  private implicit val simpleEncoder: Encoder[SimpleRecord] =
    Encoders.record(classOf[SimpleRecord])

  private val people = Seq(
    new Person("Bob", 30, new Address("1 Main St", "Springfield")),
    new Person("Ann", 25, null))

  test("typed operations on a record Dataset") {
    val ds = spark.createDataset(people)
    checkDataset(ds, people: _*)
    checkDataset(
      ds.map(p => new Person(p.name().toUpperCase(Locale.ROOT), p.age() + 1, p.address())),
      new Person("BOB", 31, people.head.address()),
      new Person("ANN", 26, null))
    checkAnswer(
      ds.select("name", "address.city"),
      Row("Bob", "Springfield") :: Row("Ann", null) :: Nil)
  }

  test("records as grouping keys and inside tuples") {
    import testImplicits._
    val ds = spark.createDataset(people ++ people)
    checkDatasetUnorderly(
      ds.groupByKey(p => new Address(p.name(), "x"))(Encoders.record(classOf[Address]))
        .mapGroups((key, rows) => (key.street(), rows.size)),
      ("Ann", 2), ("Bob", 2))
    val distinct = spark.createDataset(people)
    val joined = distinct.as("l").joinWith(distinct.as("r"), $"l.name" === $"r.name")
    assert(joined.collect().toSet === people.map(p => (p, p)).toSet)
  }

  test("convert a DataFrame to a record Dataset by column name") {
    val df = spark.sql(
      "SELECT 'x' AS extra, named_struct('city', 'C', 'street', 'S') AS address, " +
        "30 AS age, 'Bob' AS name")
    checkDataset(df.as[Person], new Person("Bob", 30, new Address("S", "C")))
  }

  test("null value for a primitive or @Nonnull record component") {
    val primitive = spark.sql("SELECT 'Bob' AS name, CAST(NULL AS INT) AS age, NULL AS address")
    val nonnull = spark.sql("SELECT CAST(NULL AS STRING) AS name, 1 AS count, 'n' AS note")
    Seq(
      () => primitive.as[Person].collect(),
      () => nonnull.as(Encoders.record(classOf[NonnullRecord])).collect()).foreach { collect =>
      val e = intercept[SparkRuntimeException](collect())
      assert(e.getCondition === "NOT_NULL_ASSERT_VIOLATION")
    }
  }

  test("query a record Dataset with SQL") {
    withTempView("people") {
      spark.createDataset(people).createOrReplaceTempView("people")
      checkAnswer(
        spark.sql("SELECT name, address.street FROM people WHERE age >= 30"),
        Row("Bob", "1 Main St"))
    }
  }

  test("createDataFrame with a record class") {
    val records = Seq(new SimpleRecord(1, "a", 1.0), new SimpleRecord(2, "b", 2.0))
    val expected = Seq(Row(1, "a", 1.0), Row(2, "b", 2.0))
    val fromList =
      spark.createDataFrame(java.util.Arrays.asList(records: _*), classOf[SimpleRecord])
    assert(fromList.columns.toSeq === Seq("id", "name", "value"))
    checkAnswer(fromList, expected)
    checkAnswer(
      spark.createDataFrame(spark.sparkContext.parallelize(records), classOf[SimpleRecord]),
      expected)
  }
}
