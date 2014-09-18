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

import org.scalatest.FunSuite

import org.apache.spark.sql.test.TestSQLContext

import scala.math.BigDecimal
import scala.language.reflectiveCalls

import java.sql.Timestamp

case class Person(name: String, age: Int)

case class Car(owner: Person, model: String)

case class Garage(cars: Seq[Car])

case class DataInt(arr: Seq[Int])
case class DataDouble(arr: Seq[Double])
case class DataFloat(arr: Seq[Float])
case class DataString(arr: Seq[String])
case class DataByte(arr: Seq[Byte])
case class DataLong(arr: Seq[Long])
case class DataShort(arr: Seq[Short])
case class DataArrayShort(arr: Seq[Seq[Short]])
case class DataBigDecimal(arr: Seq[BigDecimal])
case class DataTimestamp(arr: Seq[Timestamp])

class TypedSqlSuite extends FunSuite {
  import TestSQLContext._

  val people = sparkContext.parallelize(
    Person("Michael", 30) ::
    Person("Bob", 40) :: Nil)

  val cars = sparkContext.parallelize(
    Car(Person("Michael", 30), "GrandAm") :: Nil)

  val garage = sparkContext.parallelize(
    Seq(Car(Person("Michael", 30), "GrandAm"), Car(Person("Mary", 52), "Buick")))

  test("typed query") {
    val results = sql"SELECT name FROM $people WHERE age = 30"
    assert(results.first().name == "Michael")
  }

  test("typed query with array") {
    val results = sql"SELECT owner FROM $garage"
    assert(results.first().owner == "Michael")
  }

  test("int results") {
    val results = sql"SELECT * FROM $people WHERE age = 30"
    assert(results.first().name == "Michael")
    assert(results.first().age == 30)
  }

  ignore("nested results") {
    val results = sql"SELECT * FROM $cars"
    assert(results.first().owner.name == "Michael")
  }

  test("join query") {
    val results = sql"""SELECT a.name FROM $people a JOIN $people b ON a.age = b.age ORDER BY name"""

    assert(results.first().name == "Bob")
  }

  test("lambda udf") {
    def addOne = (_: Int) + 1
    val result = sql"SELECT $addOne(1) as two, $addOne(2) as three".first
    assert(result.two === 2)
    assert(result.three === 3)
  }

  test("with quotes") {
    assert(sql"SELECT 'test' as str".first.str == "test")
  }

  ignore("function udf") {
  // This does not even get to the macro code.
  //  def addOne(i: Int) = i + 1
  //  assert(sql"SELECT $addOne(1) as two".first.two === 2)
  }


  // tests for different configurations of arrays, primitive and nested
  val sqlContext = new org.apache.spark.sql.SQLContext(sparkContext)

  test("array int results") {
    val data = sparkContext.parallelize(1 to 10).map(x => DataInt(Seq(1, 2, 3)))
    val ai = sql"SELECT arr FROM $data"
    assert(ai.take(1).head.arr === Seq(1, 2, 3))
  }

  test("array double results") {
    val data = sparkContext.parallelize(1 to 10).map(x => DataDouble(Seq(1.0, 2.0, 3.0)))
    val ad = sql"SELECT arr FROM $data"
    assert(ad.take(1).head.arr === Seq(1.0, 2.0, 3.0))
  }

  test("array float results") {
    val data = sparkContext.parallelize(1 to 10).map(x => DataFloat(Seq(1F, 2F, 3F)))
    val af = sql"SELECT arr FROM $data"
    assert(af.take(1).head.arr === Seq(1F, 2F, 3F))
  }

  test("array string results") {
    val data = sparkContext.parallelize(1 to 10).map(x => DataString(Seq("hey","yes","no")))
    val as = sql"SELECT arr FROM $data"
    assert(as.take(1).head.arr === Seq("hey","yes","no"))
  }

  test("array byte results") {
    val data = sparkContext.parallelize(1 to 10).map(x => DataByte(Seq(1.toByte, 2.toByte, 3.toByte)))
    val ab = sql"SELECT arr FROM $data"
    assert(ab.take(1).head.arr === Seq(1.toByte, 2.toByte, 3.toByte))
  }

  test("array long results") {
    val data = sparkContext.parallelize(1 to 10).map(x => DataLong(Seq(1L, 2L, 3L)))
    val al = sql"SELECT arr FROM $data"
    assert(al.take(1).head.arr === Seq(1L, 2L, 3L))
  }

  test("array short results") {
    val data = sparkContext.parallelize(1 to 10).map(x => DataShort(Seq(1.toShort, 2.toShort, 3.toShort)))
    val ash = sql"SELECT arr FROM $data"
    assert(ash.take(1).head.arr === Seq(1.toShort, 2.toShort, 3.toShort))
  }

  test("array of array of short results") {
    val data = sparkContext.parallelize(1 to 10).map(x => DataArrayShort(Seq(Seq(1.toShort, 2.toShort, 3.toShort))))
    val aash = sql"SELECT arr FROM $data"
    assert(aash.take(1).head.arr === Seq(Seq(1.toShort, 2.toShort, 3.toShort)))
  }

  test("array bigdecimal results") {
    val data = sparkContext.parallelize(1 to 10).map(x => DataBigDecimal(Seq(new java.math.BigDecimal(1), new java.math.BigDecimal(2), new java.math.BigDecimal(3))))
    val abd = sql"SELECT arr FROM $data"
    assert(abd.take(1).head.arr === Seq(BigDecimal(1), BigDecimal(2), BigDecimal(3)))
  }

  test("array timestamp results") {
    val data = sparkContext.parallelize(1 to 10).map(x => DataTimestamp(Seq(new Timestamp(1L), new Timestamp(2L), new Timestamp(3L))))
    val ats = sql"SELECT arr FROM $data"
    assert(ats.take(1).head.arr === Seq(new Timestamp(1L), new Timestamp(2L), new Timestamp(3L)))
  }
}
