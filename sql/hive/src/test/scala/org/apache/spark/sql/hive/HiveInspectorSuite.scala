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

import java.util
import java.util.{Locale, TimeZone}

import org.apache.hadoop.hive.ql.udf.UDAFPercentile
import org.apache.hadoop.hive.serde2.io.DoubleWritable
import org.apache.hadoop.hive.serde2.objectinspector.{ObjectInspector, ObjectInspectorFactory, StructObjectInspector}
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorFactory.ObjectInspectorOptions
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory
import org.apache.hadoop.io.LongWritable
import org.scalatest.FunSuite

import org.apache.spark.sql.catalyst.expressions.{Literal, Row}
import org.apache.spark.sql.types._

class HiveInspectorSuite extends FunSuite with HiveInspectors {
  test("Test wrap SettableStructObjectInspector") {
    val udaf = new UDAFPercentile.PercentileLongEvaluator()
    udaf.init()

    udaf.iterate(new LongWritable(1), 0.1)
    udaf.iterate(new LongWritable(1), 0.1)

    val state = udaf.terminatePartial()

    val soi = ObjectInspectorFactory.getReflectionObjectInspector(
      classOf[UDAFPercentile.State],
      ObjectInspectorOptions.JAVA).asInstanceOf[StructObjectInspector]

    val a = unwrap(state, soi).asInstanceOf[Row]
    val b = wrap(a, soi).asInstanceOf[UDAFPercentile.State]

    val sfCounts = soi.getStructFieldRef("counts")
    val sfPercentiles = soi.getStructFieldRef("percentiles")

    assert(2 === soi.getStructFieldData(b, sfCounts)
      .asInstanceOf[util.Map[LongWritable, LongWritable]]
      .get(new LongWritable(1L))
      .get())
    assert(0.1 === soi.getStructFieldData(b, sfPercentiles)
      .asInstanceOf[util.ArrayList[DoubleWritable]]
      .get(0)
      .get())
  }

  // Timezone is fixed to America/Los_Angeles for those timezone sensitive tests (timestamp_*)
  TimeZone.setDefault(TimeZone.getTimeZone("America/Los_Angeles"))
  // Add Locale setting
  Locale.setDefault(Locale.US)

  val data =
    Literal(true) ::
    Literal(0.asInstanceOf[Byte]) ::
    Literal(0.asInstanceOf[Short]) ::
    Literal(0) ::
    Literal(0.asInstanceOf[Long]) ::
    Literal(0.asInstanceOf[Float]) ::
    Literal(0.asInstanceOf[Double]) ::
    Literal("0") ::
    Literal(java.sql.Date.valueOf("2014-09-23")) ::
    Literal(Decimal(BigDecimal(123.123))) ::
    Literal(new java.sql.Timestamp(123123)) ::
    Literal(Array[Byte](1,2,3)) ::
    Literal(Seq[Int](1,2,3), ArrayType(IntegerType)) ::
    Literal(Map[Int, Int](1->2, 2->1), MapType(IntegerType, IntegerType)) ::
    Literal(Row(1,2.0d,3.0f),
      StructType(StructField("c1", IntegerType) ::
      StructField("c2", DoubleType) ::
      StructField("c3", FloatType) :: Nil)) ::
    Nil

  val row = data.map(_.eval(null))
  val dataTypes = data.map(_.dataType)

  def toWritableInspector(dataType: DataType): ObjectInspector = dataType match {
    case ArrayType(tpe, _) =>
      ObjectInspectorFactory.getStandardListObjectInspector(toWritableInspector(tpe))
    case MapType(keyType, valueType, _) =>
      ObjectInspectorFactory.getStandardMapObjectInspector(
        toWritableInspector(keyType), toWritableInspector(valueType))
    case StringType => PrimitiveObjectInspectorFactory.writableStringObjectInspector
    case IntegerType => PrimitiveObjectInspectorFactory.writableIntObjectInspector
    case DoubleType => PrimitiveObjectInspectorFactory.writableDoubleObjectInspector
    case BooleanType => PrimitiveObjectInspectorFactory.writableBooleanObjectInspector
    case LongType => PrimitiveObjectInspectorFactory.writableLongObjectInspector
    case FloatType => PrimitiveObjectInspectorFactory.writableFloatObjectInspector
    case ShortType => PrimitiveObjectInspectorFactory.writableShortObjectInspector
    case ByteType => PrimitiveObjectInspectorFactory.writableByteObjectInspector
    case NullType => PrimitiveObjectInspectorFactory.writableVoidObjectInspector
    case BinaryType => PrimitiveObjectInspectorFactory.writableBinaryObjectInspector
    case DateType => PrimitiveObjectInspectorFactory.writableDateObjectInspector
    case TimestampType => PrimitiveObjectInspectorFactory.writableTimestampObjectInspector
    case DecimalType() => PrimitiveObjectInspectorFactory.writableHiveDecimalObjectInspector
    case StructType(fields) =>
      ObjectInspectorFactory.getStandardStructObjectInspector(
        java.util.Arrays.asList(fields.map(f => f.name) :_*),
        java.util.Arrays.asList(fields.map(f => toWritableInspector(f.dataType)) :_*))
  }

  def checkDataType(dt1: Seq[DataType], dt2: Seq[DataType]): Unit = {
    dt1.zip(dt2).map {
      case (dd1, dd2) =>
        assert(dd1.getClass === dd2.getClass)  // DecimalType doesn't has the default precision info
    }
  }

  def checkValues(row1: Seq[Any], row2: Seq[Any]): Unit = {
    row1.zip(row2).map {
      case (r1, r2) => checkValue(r1, r2)
    }
  }

  def checkValues(row1: Seq[Any], row2: Row): Unit = {
    row1.zip(row2.toSeq).map {
      case (r1, r2) => checkValue(r1, r2)
    }
  }

  def checkValue(v1: Any, v2: Any): Unit = {
    (v1, v2) match {
      case (r1: Decimal, r2: Decimal) =>
        // Ignore the Decimal precision
        assert(r1.compare(r2) === 0)
      case (r1: Array[Byte], r2: Array[Byte])
        if r1 != null && r2 != null && r1.length == r2.length =>
        r1.zip(r2).map { case (b1, b2) => assert(b1 === b2) }
      case (r1, r2) => assert(r1 === r2)
    }
  }

  test("oi => datatype => oi") {
    val ois = dataTypes.map(toInspector)

    checkDataType(ois.map(inspectorToDataType), dataTypes)
    checkDataType(dataTypes.map(toWritableInspector).map(inspectorToDataType), dataTypes)
  }

  test("wrap / unwrap null, constant null and writables") {
    val writableOIs = dataTypes.map(toWritableInspector)
    val nullRow = data.map(d => null)

    checkValues(nullRow, nullRow.zip(writableOIs).map {
      case (d, oi) => unwrap(wrap(d, oi), oi)
    })

    // struct couldn't be constant, sweep it out
    val constantExprs = data.filter(!_.dataType.isInstanceOf[StructType])
    val constantData = constantExprs.map(_.eval())
    val constantNullData = constantData.map(_ => null)
    val constantWritableOIs = constantExprs.map(e => toWritableInspector(e.dataType))
    val constantNullWritableOIs = constantExprs.map(e => toInspector(Literal(null, e.dataType)))

    checkValues(constantData, constantData.zip(constantWritableOIs).map {
      case (d, oi) => unwrap(wrap(d, oi), oi)
    })

    checkValues(constantNullData, constantData.zip(constantNullWritableOIs).map {
      case (d, oi) => unwrap(wrap(d, oi), oi)
    })

    checkValues(constantNullData, constantNullData.zip(constantWritableOIs).map {
      case (d, oi) => unwrap(wrap(d, oi), oi)
    })
  }

  test("wrap / unwrap primitive writable object inspector") {
    val writableOIs = dataTypes.map(toWritableInspector)

    checkValues(row, row.zip(writableOIs).map {
      case (data, oi) => unwrap(wrap(data, oi), oi)
    })
  }

  test("wrap / unwrap primitive java object inspector") {
    val ois = dataTypes.map(toInspector)

    checkValues(row, row.zip(ois).map {
      case (data, oi) => unwrap(wrap(data, oi), oi)
    })
  }

  test("wrap / unwrap Struct Type") {
    val dt = StructType(dataTypes.zipWithIndex.map {
      case (t, idx) => StructField(s"c_$idx", t)
    })

    checkValues(row, unwrap(wrap(Row.fromSeq(row), toInspector(dt)), toInspector(dt)).asInstanceOf[Row])
    checkValue(null, unwrap(wrap(null, toInspector(dt)), toInspector(dt)))
  }

  test("wrap / unwrap Array Type") {
    val dt = ArrayType(dataTypes(0))

    val d = row(0) :: row(0) :: Nil
    checkValue(d, unwrap(wrap(d, toInspector(dt)), toInspector(dt)))
    checkValue(null, unwrap(wrap(null, toInspector(dt)), toInspector(dt)))
    checkValue(d, unwrap(wrap(d, toInspector(Literal(d, dt))), toInspector(Literal(d, dt))))
    checkValue(d, unwrap(wrap(null, toInspector(Literal(d, dt))), toInspector(Literal(d, dt))))
  }

  test("wrap / unwrap Map Type") {
    val dt = MapType(dataTypes(0), dataTypes(1))

    val d = Map(row(0) -> row(1))
    checkValue(d, unwrap(wrap(d, toInspector(dt)), toInspector(dt)))
    checkValue(null, unwrap(wrap(null, toInspector(dt)), toInspector(dt)))
    checkValue(d, unwrap(wrap(d, toInspector(Literal(d, dt))), toInspector(Literal(d, dt))))
    checkValue(d, unwrap(wrap(null, toInspector(Literal(d, dt))), toInspector(Literal(d, dt))))
  }
}
