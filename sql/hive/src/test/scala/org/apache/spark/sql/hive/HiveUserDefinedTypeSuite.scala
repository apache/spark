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

import scala.jdk.CollectionConverters._

import org.apache.hadoop.hive.ql.udf.generic.GenericUDF
import org.apache.hadoop.hive.serde2.objectinspector.{ObjectInspector, StandardListObjectInspector}
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory

import org.apache.spark.sql.{QueryTest, Row}
import org.apache.spark.sql.catalyst.FunctionIdentifier
import org.apache.spark.sql.hive.test.TestHiveSingleton
import org.apache.spark.sql.test.{ExamplePoint, ExamplePointUDT}
import org.apache.spark.sql.types.StructType

class HiveUserDefinedTypeSuite extends QueryTest with TestHiveSingleton {
  private val functionClass = classOf[org.apache.spark.sql.hive.TestUDF].getCanonicalName

  test("Support UDT in Hive UDF") {
    val functionName = "get_point_x"
    try {
      val schema = new StructType().add("point", new ExamplePointUDT, nullable = false)
      val input = Row.fromSeq(Seq(new ExamplePoint(3.141592d, -3.141592d)))
      val df = spark.createDataFrame(Array(input).toList.asJava, schema)
      df.createOrReplaceTempView("src")
      spark.sql(s"CREATE FUNCTION $functionName AS '$functionClass'")

      checkAnswer(
        spark.sql(s"SELECT $functionName(point) FROM src"),
        Row(input.getAs[ExamplePoint](0).x))
    } finally {
      // If the test failed part way, we don't want to mask the failure by failing to remove
      // temp tables that never got created.
      spark.sql(s"DROP FUNCTION IF EXISTS $functionName")
      assert(
        !spark.sessionState.catalog.functionExists(FunctionIdentifier(functionName)),
        s"Function $functionName should have been dropped. But, it still exists.")
    }
  }
}

class TestUDF extends GenericUDF {
  private var data: StandardListObjectInspector = _

  override def getDisplayString(children: Array[String]): String = "get_point_x"

  override def initialize(arguments: Array[ObjectInspector]): ObjectInspector = {
    data = arguments(0).asInstanceOf[StandardListObjectInspector]
    PrimitiveObjectInspectorFactory.javaDoubleObjectInspector
  }

  override def evaluate(arguments: Array[GenericUDF.DeferredObject]): AnyRef = {
    val point = data.getList(arguments(0).get())
    java.lang.Double.valueOf(point.get(0).asInstanceOf[Double])
  }
}
