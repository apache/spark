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

import java.io.{DataOutput, DataInput}
import java.util
import java.util.Properties

import org.apache.spark.util.Utils

import scala.collection.JavaConversions._

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.hive.serde2.{SerDeStats, AbstractSerDe}
import org.apache.hadoop.io.Writable
import org.apache.hadoop.hive.serde2.objectinspector.{ObjectInspectorFactory, ObjectInspector}

import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory
import org.apache.hadoop.hive.ql.udf.generic.GenericUDF
import org.apache.hadoop.hive.ql.udf.generic.GenericUDF.DeferredObject

import org.apache.spark.sql.Row
import org.apache.spark.sql.hive.test.TestHive
import org.apache.spark.sql.hive.test.TestHive._

case class Fields(f1: Int, f2: Int, f3: Int, f4: Int, f5: Int)

/**
 * A test suite for Hive custom UDFs.
 */
class HiveUdfSuite extends HiveComparisonTest {

  test("spark sql udf test that returns a struct") {
    registerFunction("getStruct", (_: Int) => Fields(1, 2, 3, 4, 5))
    assert(sql(
      """
        |SELECT getStruct(1).f1,
        |       getStruct(1).f2,
        |       getStruct(1).f3,
        |       getStruct(1).f4,
        |       getStruct(1).f5 FROM src LIMIT 1
      """.stripMargin).first() === Row(1, 2, 3, 4, 5))
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
    assert(sql("SELECT percentile(key,1) FROM src").first === sql("SELECT max(key) FROM src").first)
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
