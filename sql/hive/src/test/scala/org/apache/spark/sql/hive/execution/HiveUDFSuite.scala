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

import scala.collection.JavaConversions._

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.hive.ql.udf.generic.{GenericUDAFAverage, GenericUDF}
import org.apache.hadoop.hive.ql.udf.generic.GenericUDF.DeferredObject
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory
import org.apache.hadoop.hive.serde2.objectinspector.{ObjectInspector, ObjectInspectorFactory}
import org.apache.hadoop.hive.serde2.{AbstractSerDe, SerDeStats}
import org.apache.hadoop.io.Writable

import org.apache.spark.sql.{AnalysisException, QueryTest, Row, SQLConf}
import org.apache.spark.sql.hive.test.HiveTestUtils
import org.apache.spark.util.Utils

case class Fields(f1: Int, f2: Int, f3: Int, f4: Int, f5: Int)

// Case classes for the custom UDF's.
case class IntegerCaseClass(i: Int)
case class ListListIntCaseClass(lli: Seq[(Int, Int, Int)])
case class StringCaseClass(s: String)
case class ListStringCaseClass(l: Seq[String])

/**
 * A test suite for Hive custom UDFs.
 */
class HiveUDFSuite extends QueryTest with HiveTestUtils {
  import testImplicits._

  test("spark sql udf test that returns a struct") {
    ctx.udf.register("getStruct", (_: Int) => Fields(1, 2, 3, 4, 5))
    assert(ctx.sql(
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
      ctx.sql("SELECT PMOD(CAST(key as INT), 10) FROM src LIMIT 1"),
      Row(8)
    )
  }

  test("hive struct udf") {
    ctx.sql(
      """
      |CREATE EXTERNAL TABLE hiveUDFTestTable (
      |   pair STRUCT<id: INT, value: INT>
      |)
      |PARTITIONED BY (partition STRING)
      |ROW FORMAT SERDE '%s'
      |STORED AS SEQUENCEFILE
    """.
        stripMargin.format(classOf[PairSerDe].getName))

    val location = Utils.getSparkClassLoader.getResource("data/files/testUDF").getFile
    ctx.sql(s"""
      ALTER TABLE hiveUDFTestTable
      ADD IF NOT EXISTS PARTITION(partition='testUDF')
      LOCATION '$location'""")

    ctx.sql(s"CREATE TEMPORARY FUNCTION testUDF AS '${classOf[PairUDF].getName}'")
    ctx.sql("SELECT testUDF(pair) FROM hiveUDFTestTable")
    ctx.sql("DROP TEMPORARY FUNCTION IF EXISTS testUDF")
  }

  test("Max/Min on named_struct") {
    def testOrderInStruct(): Unit = {
      checkAnswer(ctx.sql(
        """
          |SELECT max(named_struct(
          |           "key", key,
          |           "value", value)).value FROM src
        """.stripMargin), Seq(Row("val_498")))
      checkAnswer(ctx.sql(
        """
          |SELECT min(named_struct(
          |           "key", key,
          |           "value", value)).value FROM src
        """.stripMargin), Seq(Row("val_0")))

      // nested struct cases
      checkAnswer(ctx.sql(
        """
          |SELECT max(named_struct(
          |           "key", named_struct(
                              "key", key,
                              "value", value),
          |           "value", value)).value FROM src
        """.stripMargin), Seq(Row("val_498")))
      checkAnswer(ctx.sql(
        """
          |SELECT min(named_struct(
          |           "key", named_struct(
                             "key", key,
                             "value", value),
          |           "value", value)).value FROM src
        """.stripMargin), Seq(Row("val_0")))
    }
    val codegenDefault = ctx.getConf(SQLConf.CODEGEN_ENABLED)
    ctx.setConf(SQLConf.CODEGEN_ENABLED, true)
    testOrderInStruct()
    ctx.setConf(SQLConf.CODEGEN_ENABLED, false)
    testOrderInStruct()
    ctx.setConf(SQLConf.CODEGEN_ENABLED, codegenDefault)
  }

  test("SPARK-6409 UDAFAverage test") {
    ctx.sql(s"CREATE TEMPORARY FUNCTION test_avg AS '${classOf[GenericUDAFAverage].getName}'")
    checkAnswer(
      ctx.sql("SELECT test_avg(1), test_avg(substr(value,5)) FROM src"),
      Seq(Row(1.0, 260.182)))
    ctx.sql("DROP TEMPORARY FUNCTION IF EXISTS test_avg")
    ctx.reset()
  }

  test("SPARK-2693 udaf aggregates test") {
    checkAnswer(ctx.sql("SELECT percentile(key, 1) FROM src LIMIT 1"),
      ctx.sql("SELECT max(key) FROM src").collect().toSeq)

    checkAnswer(ctx.sql("SELECT percentile(key, array(1, 1)) FROM src LIMIT 1"),
      ctx.sql("SELECT array(max(key), max(key)) FROM src").collect().toSeq)
  }

  test("Generic UDAF aggregates") {
    checkAnswer(ctx.sql("SELECT ceiling(percentile_approx(key, 0.99999)) FROM src LIMIT 1"),
      ctx.sql("SELECT max(key) FROM src LIMIT 1").collect().toSeq)

    checkAnswer(ctx.sql("SELECT percentile_approx(100.0, array(0.9, 0.9)) FROM src LIMIT 1"),
      ctx.sql("SELECT array(100, 100) FROM src LIMIT 1").collect().toSeq)
   }

  test("UDFIntegerToString") {
    val testData = ctx.sparkContext.parallelize(
      IntegerCaseClass(1) :: IntegerCaseClass(2) :: Nil).toDF()
    testData.registerTempTable("integerTable")

    val udfName = classOf[UDFIntegerToString].getName
    ctx.sql(s"CREATE TEMPORARY FUNCTION testUDFIntegerToString AS '$udfName'")
    checkAnswer(
      ctx.sql("SELECT testUDFIntegerToString(i) FROM integerTable"),
      Seq(Row("1"), Row("2")))
    ctx.sql("DROP TEMPORARY FUNCTION IF EXISTS testUDFIntegerToString")

    ctx.reset()
  }

  test("UDFToListString") {
    val testData = ctx.sparkContext.parallelize(StringCaseClass("") :: Nil).toDF()
    testData.registerTempTable("inputTable")

    ctx.sql(
      s"CREATE TEMPORARY FUNCTION testUDFToListString AS '${classOf[UDFToListString].getName}'")
    val errMsg = intercept[AnalysisException] {
      ctx.sql("SELECT testUDFToListString(s) FROM inputTable")
    }
    assert(errMsg.getMessage contains "List type in java is unsupported because " +
      "JVM type erasure makes spark fail to catch a component type in List<>;")

    ctx.sql("DROP TEMPORARY FUNCTION IF EXISTS testUDFToListString")
    ctx.reset()
  }

  test("UDFToListInt") {
    val testData = ctx.sparkContext.parallelize(StringCaseClass("") :: Nil).toDF()
    testData.registerTempTable("inputTable")

    ctx.sql(s"CREATE TEMPORARY FUNCTION testUDFToListInt AS '${classOf[UDFToListInt].getName}'")
    val errMsg = intercept[AnalysisException] {
      ctx.sql("SELECT testUDFToListInt(s) FROM inputTable")
    }
    assert(errMsg.getMessage contains "List type in java is unsupported because " +
      "JVM type erasure makes spark fail to catch a component type in List<>;")

    ctx.sql("DROP TEMPORARY FUNCTION IF EXISTS testUDFToListInt")
    ctx.reset()
  }

  test("UDFToStringIntMap") {
    val testData = ctx.sparkContext.parallelize(StringCaseClass("") :: Nil).toDF()
    testData.registerTempTable("inputTable")

    ctx.sql(s"CREATE TEMPORARY FUNCTION testUDFToStringIntMap " +
      s"AS '${classOf[UDFToStringIntMap].getName}'")
    val errMsg = intercept[AnalysisException] {
      ctx.sql("SELECT testUDFToStringIntMap(s) FROM inputTable")
    }
    assert(errMsg.getMessage contains "Map type in java is unsupported because " +
      "JVM type erasure makes spark fail to catch key and value types in Map<>;")

    ctx.sql("DROP TEMPORARY FUNCTION IF EXISTS testUDFToStringIntMap")
    ctx.reset()
  }

  test("UDFToIntIntMap") {
    val testData = ctx.sparkContext.parallelize(StringCaseClass("") :: Nil).toDF()
    testData.registerTempTable("inputTable")

    ctx.sql(s"CREATE TEMPORARY FUNCTION testUDFToIntIntMap " +
      s"AS '${classOf[UDFToIntIntMap].getName}'")
    val errMsg = intercept[AnalysisException] {
      ctx.sql("SELECT testUDFToIntIntMap(s) FROM inputTable")
    }
    assert(errMsg.getMessage contains "Map type in java is unsupported because " +
      "JVM type erasure makes spark fail to catch key and value types in Map<>;")

    ctx.sql("DROP TEMPORARY FUNCTION IF EXISTS testUDFToIntIntMap")
    ctx.reset()
  }

  test("UDFListListInt") {
    val testData = ctx.sparkContext.parallelize(
      ListListIntCaseClass(Nil) ::
      ListListIntCaseClass(Seq((1, 2, 3))) ::
      ListListIntCaseClass(Seq((4, 5, 6), (7, 8, 9))) :: Nil).toDF()
    testData.registerTempTable("listListIntTable")

    ctx.sql(s"CREATE TEMPORARY FUNCTION testUDFListListInt AS '${classOf[UDFListListInt].getName}'")
    checkAnswer(
      ctx.sql("SELECT testUDFListListInt(lli) FROM listListIntTable"),
      Seq(Row(0), Row(2), Row(13)))
    ctx.sql("DROP TEMPORARY FUNCTION IF EXISTS testUDFListListInt")

    ctx.reset()
  }

  test("UDFListString") {
    val testData = ctx.sparkContext.parallelize(
      ListStringCaseClass(Seq("a", "b", "c")) ::
      ListStringCaseClass(Seq("d", "e")) :: Nil).toDF()
    testData.registerTempTable("listStringTable")

    ctx.sql(s"CREATE TEMPORARY FUNCTION testUDFListString AS '${classOf[UDFListString].getName}'")
    checkAnswer(
      ctx.sql("SELECT testUDFListString(l) FROM listStringTable"),
      Seq(Row("a,b,c"), Row("d,e")))
    ctx.sql("DROP TEMPORARY FUNCTION IF EXISTS testUDFListString")

    ctx.reset()
  }

  test("UDFStringString") {
    val testData = ctx.sparkContext.parallelize(
      StringCaseClass("world") :: StringCaseClass("goodbye") :: Nil).toDF()
    testData.registerTempTable("stringTable")

    ctx.sql(
      s"CREATE TEMPORARY FUNCTION testStringStringUDF AS '${classOf[UDFStringString].getName}'")
    checkAnswer(
      ctx.sql("SELECT testStringStringUDF(\"hello\", s) FROM stringTable"),
      Seq(Row("hello world"), Row("hello goodbye")))
    ctx.sql("DROP TEMPORARY FUNCTION IF EXISTS testStringStringUDF")

    ctx.reset()
  }

  test("UDFTwoListList") {
    val testData = ctx.sparkContext.parallelize(
      ListListIntCaseClass(Nil) ::
      ListListIntCaseClass(Seq((1, 2, 3))) ::
      ListListIntCaseClass(Seq((4, 5, 6), (7, 8, 9))) ::
      Nil).toDF()
    testData.registerTempTable("TwoListTable")

    ctx.sql(s"CREATE TEMPORARY FUNCTION testUDFTwoListList AS '${classOf[UDFTwoListList].getName}'")
    checkAnswer(
      ctx.sql("SELECT testUDFTwoListList(lli, lli) FROM TwoListTable"),
      Seq(Row("0, 0"), Row("2, 2"), Row("13, 13")))
    ctx.sql("DROP TEMPORARY FUNCTION IF EXISTS testUDFTwoListList")

    ctx.reset()
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

class PairUDF extends GenericUDF {
  override def initialize(p1: Array[ObjectInspector]): ObjectInspector =
    ObjectInspectorFactory.getStandardStructObjectInspector(
      Seq("id", "value"),
      Seq(PrimitiveObjectInspectorFactory.javaIntObjectInspector,
        PrimitiveObjectInspectorFactory.javaIntObjectInspector)
  )

  override def evaluate(args: Array[DeferredObject]): AnyRef = {
    Integer.valueOf(args(0).get.asInstanceOf[TestPair].entry._2)
  }

  override def getDisplayString(p1: Array[String]): String = ""
}
