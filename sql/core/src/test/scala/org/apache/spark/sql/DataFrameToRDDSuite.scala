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

import org.apache.spark.sql.test.SharedSQLContext

// For SPARK-7160: toTypedRDD[T].
class DataFrameToRDDSuite extends QueryTest with SharedSQLContext {
  import testImplicits._

  test("toTypedRDD[T] works with simple case class") {
    import org.scalatest.Matchers._
    val oldRDD = sqlContext.sparkContext.parallelize(
      Seq(A("apple", 1), A("banana", 2), A("cherry", 3)))
    val df = oldRDD.toDF()
    val newRDD = df.toTypedRDD[A]()
    newRDD.collect() should contain theSameElementsAs oldRDD.collect()
  }

  test("toTypedRDD[T] works with case class dependent on another case class") {
    import org.scalatest.Matchers._
    val oldRDD = sqlContext.sparkContext.parallelize(
      Seq(B(A("apple", 1), 1.0), B(A("banana", 2), 2.0), B(A("cherry", 3), 3.0)))
    val df = oldRDD.toDF()
    val newRDD = df.toTypedRDD[B]()
    newRDD.collect() should contain theSameElementsAs oldRDD.collect()
  }

  test("toTypedRDD[T] works with case class having a Seq") {
    import org.scalatest.Matchers._
    val oldRDD = sqlContext.sparkContext.parallelize(
      Seq(
        C("fruits", Seq(A("apple", 1), A("banana", 2), A("cherry", 3))),
        C("vegetables", Seq(A("eggplant", 4), A("spinach", 5), A("zucchini", 6)))))
    val df = oldRDD.toDF()
    val newRDD = df.toTypedRDD[C]()
    newRDD.collect() should contain theSameElementsAs oldRDD.collect()
  }

  test("toTypedRDD[T] works with case class having a Map") {
    import org.scalatest.Matchers._
    val oldRDD = sqlContext.sparkContext.parallelize(
      Seq(
        D("fruits", Map(1 -> A("apple", 1), 2 -> A("banana", 2), 3 -> A("cherry", 3))),
        D("vegetables", Map(4 -> A("eggplant", 4), 5 -> A("spinach", 5), 6 -> A("zucchini", 6)))))
    val df = oldRDD.toDF()
    val newRDD = df.toTypedRDD[D]()
    newRDD.collect() should contain theSameElementsAs oldRDD.collect()
  }

  test("toTypedRDD[T] works with case class having an Option") {
    import org.scalatest.Matchers._
    val oldRDD = sqlContext.sparkContext.parallelize(
      Seq(E(Some(A("apple", 1))), E(Some(A("banana", 2))), E(Some(A("cherry", 3))), E(None)))
    val df = oldRDD.toDF()
    val newRDD = df.toTypedRDD[E]()
    newRDD.collect() should contain theSameElementsAs oldRDD.collect()
  }

  test("toTypedRDD[T] works with case class having a (Scala) BigDecimal") {
    import org.scalatest.Matchers._
    val oldRDD = sqlContext.sparkContext.parallelize(
      Seq(F(BigDecimal(1.0)), F(BigDecimal(2.0)), F(BigDecimal(3.0))))
    val df = oldRDD.toDF()
    val newRDD = df.toTypedRDD[F]()
    newRDD.collect() should contain theSameElementsAs oldRDD.collect()
  }

  test("toTypedRDD[T] fails with an incompatible case class") {
    intercept[IllegalArgumentException] {
      val oldRDD = sqlContext.sparkContext.parallelize(Seq(A("apple", 1)))
      val df = oldRDD.toDF()
      val newRDD = df.toTypedRDD[B]()
      newRDD.collect()
    }
  }

  test("toTypedRDD[T] can be used to reload an RDD saved to Parquet") {
    import java.io.File
    import org.apache.spark.util.Utils
    import org.scalatest.Matchers._

    val tempDir = Utils.createTempDir()
    val filePath = new File(tempDir, "testParquet").getCanonicalPath

    val rdd0 =
      sqlContext.sparkContext.parallelize(Seq(A("apple", 1), A("banana", 2), A("cherry", 3)))
    val df0 = rdd0.toDF()
    df0.write.format("parquet").save(filePath)

    val df1 = sqlContext.read.format("parquet").load(filePath)
    val rdd1 = df1.toTypedRDD[A]()
    rdd1.collect() should contain theSameElementsAs rdd0.collect()
  }
}

case class A(x: String, y: Int)
case class B(a: A, z: Double)
case class C(x: String, a: Seq[A])
case class D(x: String, a: Map[Int, A])
case class E(o: Option[A])
case class F(bd: BigDecimal)
