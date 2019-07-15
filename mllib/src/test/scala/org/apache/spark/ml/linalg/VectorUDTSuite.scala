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

package org.apache.spark.ml.linalg

import org.apache.spark.ml.feature.LabeledPoint
import org.apache.spark.sql.{QueryTest, Row, SparkSession}
import org.apache.spark.sql.catalyst.{FunctionIdentifier, JavaTypeInference}
import org.apache.spark.sql.hive.test.TestHive
import org.apache.spark.sql.types._

class VectorUDTSuite extends QueryTest {

  test("preloaded VectorUDT") {
    val dv1 = Vectors.dense(Array.empty[Double])
    val dv2 = Vectors.dense(1.0, 2.0)
    val sv1 = Vectors.sparse(2, Array.empty, Array.empty)
    val sv2 = Vectors.sparse(2, Array(1), Array(2.0))

    for (v <- Seq(dv1, dv2, sv1, sv2)) {
      val udt = UDTRegistration.getUDTFor(v.getClass.getName).get.getConstructor().newInstance()
        .asInstanceOf[VectorUDT]
      assert(v === udt.deserialize(udt.serialize(v)))
      assert(udt.typeName == "vector")
      assert(udt.simpleString == "vector")
    }
  }

  test("JavaTypeInference with VectorUDT") {
    val (dataType, _) = JavaTypeInference.inferDataType(classOf[LabeledPoint])
    assert(dataType.asInstanceOf[StructType].fields.map(_.dataType)
      === Seq(new VectorUDT, DoubleType))
  }

  test("SPARK-28158 Hive UDFs supports UDT type") {
    val functionName = "Logistic_Regression"
    val sql = spark.sql _
    try {
      val df = spark.read.format("libsvm").options(Map("vectorType" -> "dense"))
        .load(TestHive.getHiveFile("test-data/libsvm/sample_libsvm_data.txt").getPath)
      df.createOrReplaceTempView("src")

      // `Logistic_Regression` accepts features (with Vector type), and returns the
      // prediction value. To simplify the UDF implementation, the `Logistic_Regression`
      // will return 0.95d directly.
      sql(
        s"""
           |CREATE FUNCTION Logistic_Regression
           |AS 'org.apache.spark.sql.hive.LogisticRegressionUDF'
           |USING JAR '${TestHive.getHiveFile("TestLogRegUDF.jar").toURI}'
        """.stripMargin)

      checkAnswer(
        sql("SELECT Logistic_Regression(features) FROM src"),
        Row(0.95) :: Nil)
    } catch {
      case cause: Throwable => throw cause
    } finally {
      // If the test failed part way, we don't want to mask the failure by failing to remove
      // temp tables that never got created.
      spark.sql(s"DROP FUNCTION IF EXISTS $functionName")
      assert(
        !spark.sessionState.catalog.functionExists(FunctionIdentifier(functionName)),
        s"Function $functionName should have been dropped. But, it still exists.")
    }
  }

  override protected val spark: SparkSession = TestHive.sparkSession
}
