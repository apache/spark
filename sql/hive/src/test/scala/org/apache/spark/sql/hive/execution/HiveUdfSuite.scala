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

package org.apache.spark.sql.hive.execution

import java.io.{DataInput, DataOutput}
import java.util
import java.util.Properties

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.hive.ql.udf.generic.GenericUDF
import org.apache.hadoop.hive.ql.udf.generic.GenericUDF.DeferredObject
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory
import org.apache.hadoop.hive.serde2.objectinspector.{ObjectInspector, ObjectInspectorFactory}
import org.apache.hadoop.hive.serde2.{AbstractSerDe, SerDeStats}
import org.apache.hadoop.io.Writable
import org.apache.spark.sql.{QueryTest, Row}
import org.apache.spark.sql.hive.test.TestHive

import org.apache.spark.util.Utils

import scala.collection.JavaConversions._

case class Fields(f1: Int, f2: Int, f3: Int, f4: Int, f5: Int)

// Case classes for the custom UDF's.
case class IntegerCaseClass(i: Int)
case class ListListIntCaseClass(lli: Seq[(Int, Int, Int)])
case class StringCaseClass(s: String)
case class ListStringCaseClass(l: Seq[String])

/**
 * A test suite for Hive custom UDFs.
 */
class HiveUdfSuite extends QueryTest {
  import TestHive._

  test("spark sql udf test that returns a struct") {
    udf.register("getStruct", (_: Int) => Fields(1, 2, 3, 4, 5))
    assert(sql(
      """
        |SELECT getStruct(1).f1,
        |       getStruct(1).f2,
        |       getStruct(1).f3,
        |       getStruct(1).f4,
        |       getStruct(1).f5 FROM src LIMIT 1
      """.stripMargin).head() === Row(1, 2, 3, 4, 5))
  }
  
  test("SPARK-4785 When called with arguments referring column fields, PMOD throws NPE") {
    checkAnswer(
      sql("SELECT PMOD(CAST(key as INT), 10) FROM src LIMIT 1"),
      Row(8)
    )
  }

  test("hive struct udf") {
    sql(
      """
      |CREATE EXTERNAL TABLE hiveUdfTestTable (
      |   pair STRUCT<id: INT, value: INT>
      |)
      |PARTITIONED BY (partition STRING)
      |ROW FORMAT SERDE '%s'
      |STORED AS SEQUENCEFILE
    """.
        stripMargin.format(classOf[PairSerDe].getName))

    val location = Utils.getSparkClassLoader.getResource("data/files/testUdf").getFile
    sql(s"""
      ALTER TABLE hiveUdfTestTable
      ADD IF NOT EXISTS PARTITION(partition='testUdf')
      LOCATION '$location'""")

    sql(s"CREATE TEMPORARY FUNCTION testUdf AS '${classOf[PairUdf].getName}'")
    sql("SELECT testUdf(pair) FROM hiveUdfTestTable")
    sql("DROP TEMPORARY FUNCTION IF EXISTS testUdf")
  }

  test("SPARK-2693 udaf aggregates test") {
    checkAnswer(sql("SELECT percentile(key, 1) FROM src LIMIT 1"),
      sql("SELECT max(key) FROM src").collect().toSeq)
      
    checkAnswer(sql("SELECT percentile(key, array(1, 1)) FROM src LIMIT 1"),
      sql("SELECT array(max(key), max(key)) FROM src").collect().toSeq)
  }

  test("Generic UDAF aggregates") {
    checkAnswer(sql("SELECT ceiling(percentile_approx(key, 0.99999)) FROM src LIMIT 1"),
      sql("SELECT max(key) FROM src LIMIT 1").collect().toSeq)
      
    checkAnswer(sql("SELECT percentile_approx(100.0, array(0.9, 0.9)) FROM src LIMIT 1"),
      sql("SELECT array(100, 100) FROM src LIMIT 1").collect().toSeq)
   }
  
  test("UDFIntegerToString") {
    val testData = TestHive.sparkContext.parallelize(
      IntegerCaseClass(1) :: IntegerCaseClass(2) :: Nil)
    testData.registerTempTable("integerTable")

    sql(s"CREATE TEMPORARY FUNCTION testUDFIntegerToString AS '${classOf[UDFIntegerToString].getName}'")
    checkAnswer(
      sql("SELECT testUDFIntegerToString(i) FROM integerTable"), //.collect(),
      Seq(Row("1"), Row("2")))
    sql("DROP TEMPORARY FUNCTION IF EXISTS testUDFIntegerToString")

    TestHive.reset()
  }

  test("UDFListListInt") {
    val testData = TestHive.sparkContext.parallelize(
      ListListIntCaseClass(Nil) ::
      ListListIntCaseClass(Seq((1, 2, 3))) ::
      ListListIntCaseClass(Seq((4, 5, 6), (7, 8, 9))) :: Nil)
    testData.registerTempTable("listListIntTable")

    sql(s"CREATE TEMPORARY FUNCTION testUDFListListInt AS '${classOf[UDFListListInt].getName}'")
    checkAnswer(
      sql("SELECT testUDFListListInt(lli) FROM listListIntTable"), //.collect(),
      Seq(Row(0), Row(2), Row(13)))
    sql("DROP TEMPORARY FUNCTION IF EXISTS testUDFListListInt")

    TestHive.reset()
  }

  test("UDFListString") {
    val testData = TestHive.sparkContext.parallelize(
      ListStringCaseClass(Seq("a", "b", "c")) ::
      ListStringCaseClass(Seq("d", "e")) :: Nil)
    testData.registerTempTable("listStringTable")

    sql(s"CREATE TEMPORARY FUNCTION testUDFListString AS '${classOf[UDFListString].getName}'")
    checkAnswer(
      sql("SELECT testUDFListString(l) FROM listStringTable"), //.collect(),
      Seq(Row("a,b,c"), Row("d,e")))
    sql("DROP TEMPORARY FUNCTION IF EXISTS testUDFListString")

    TestHive.reset()
  }

  test("UDFStringString") {
    val testData = TestHive.sparkContext.parallelize(
      StringCaseClass("world") :: StringCaseClass("goodbye") :: Nil)
    testData.registerTempTable("stringTable")

    sql(s"CREATE TEMPORARY FUNCTION testStringStringUdf AS '${classOf[UDFStringString].getName}'")
    checkAnswer(
      sql("SELECT testStringStringUdf(\"hello\", s) FROM stringTable"), //.collect(),
      Seq(Row("hello world"), Row("hello goodbye")))
    sql("DROP TEMPORARY FUNCTION IF EXISTS testStringStringUdf")

    TestHive.reset()
  }

  test("UDFTwoListList") {
    val testData = TestHive.sparkContext.parallelize(
      ListListIntCaseClass(Nil) ::
      ListListIntCaseClass(Seq((1, 2, 3))) ::
      ListListIntCaseClass(Seq((4, 5, 6), (7, 8, 9))) ::
      Nil)
    testData.registerTempTable("TwoListTable")

    sql(s"CREATE TEMPORARY FUNCTION testUDFTwoListList AS '${classOf[UDFTwoListList].getName}'")
    checkAnswer(
      sql("SELECT testUDFTwoListList(lli, lli) FROM TwoListTable"), //.collect(),
      Seq(Row("0, 0"), Row("2, 2"), Row("13, 13")))
    sql("DROP TEMPORARY FUNCTION IF EXISTS testUDFTwoListList")

    TestHive.reset()
  }
}

class TestPair(x: Int, y: Int) extends Writable with Serializable {
  def this() = this(0, 0)
  var entry: (Int, Int) = (x, y)

  override def write(output: DataOutput): Unit = {
    output.writeInt(entry._1)
    output.writeInt(entry._2)
  }

  override def readFields(input: DataInput): Unit = {
    val x = input.readInt()
    val y = input.readInt()
    entry = (x, y)
  }
}

class PairSerDe extends AbstractSerDe {
  override def initialize(p1: Configuration, p2: Properties): Unit = {}

  override def getObjectInspector: ObjectInspector = {
    ObjectInspectorFactory
      .getStandardStructObjectInspector(
        Seq("pair"),
        Seq(ObjectInspectorFactory.getStandardStructObjectInspector(
          Seq("id", "value"),
          Seq(PrimitiveObjectInspectorFactory.javaIntObjectInspector,
              PrimitiveObjectInspectorFactory.javaIntObjectInspector))
    ))
  }

  override def getSerializedClass: Class[_ <: Writable] = classOf[TestPair]

  override def getSerDeStats: SerDeStats = null

  override def serialize(p1: scala.Any, p2: ObjectInspector): Writable = null

  override def deserialize(value: Writable): AnyRef = {
    val pair = value.asInstanceOf[TestPair]

    val row = new util.ArrayList[util.ArrayList[AnyRef]]
    row.add(new util.ArrayList[AnyRef](2))
    row(0).add(Integer.valueOf(pair.entry._1))
    row(0).add(Integer.valueOf(pair.entry._2))

    row
  }
}

class PairUdf extends GenericUDF {
  override def initialize(p1: Array[ObjectInspector]): ObjectInspector =
    ObjectInspectorFactory.getStandardStructObjectInspector(
      Seq("id", "value"),
      Seq(PrimitiveObjectInspectorFactory.javaIntObjectInspector, PrimitiveObjectInspectorFactory.javaIntObjectInspector)
  )

  override def evaluate(args: Array[DeferredObject]): AnyRef = {
    println("Type = %s".format(args(0).getClass.getName))
    Integer.valueOf(args(0).get.asInstanceOf[TestPair].entry._2)
  }

  override def getDisplayString(p1: Array[String]): String = ""
}
