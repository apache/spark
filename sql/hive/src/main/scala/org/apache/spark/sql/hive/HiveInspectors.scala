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

import org.apache.hadoop.hive.common.`type`.HiveDecimal
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfoFactory
import org.apache.hadoop.hive.serde2.objectinspector._
import org.apache.hadoop.hive.serde2.objectinspector.PrimitiveObjectInspector._
import org.apache.hadoop.hive.serde2.objectinspector.primitive._
import org.apache.hadoop.hive.serde2.{io => hiveIo}
import org.apache.hadoop.{io => hadoopIo}

import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.catalyst.types
import org.apache.spark.sql.catalyst.types._

/* Implicit conversions */
import scala.collection.JavaConversions._

private[hive] trait HiveInspectors {

  def javaClassToDataType(clz: Class[_]): DataType = clz match {
    // writable
    case c: Class[_] if c == classOf[hadoopIo.DoubleWritable] => DoubleType
    case c: Class[_] if c == classOf[hiveIo.DoubleWritable] => DoubleType
    case c: Class[_] if c == classOf[hiveIo.HiveDecimalWritable] => DecimalType
    case c: Class[_] if c == classOf[hiveIo.ByteWritable] => ByteType
    case c: Class[_] if c == classOf[hiveIo.ShortWritable] => ShortType
    case c: Class[_] if c == classOf[hiveIo.DateWritable] => DateType
    case c: Class[_] if c == classOf[hiveIo.TimestampWritable] => TimestampType
    case c: Class[_] if c == classOf[hadoopIo.Text] => StringType
    case c: Class[_] if c == classOf[hadoopIo.IntWritable] => IntegerType
    case c: Class[_] if c == classOf[hadoopIo.LongWritable] => LongType
    case c: Class[_] if c == classOf[hadoopIo.FloatWritable] => FloatType
    case c: Class[_] if c == classOf[hadoopIo.BooleanWritable] => BooleanType
    case c: Class[_] if c == classOf[hadoopIo.BytesWritable] => BinaryType

    // java class
    case c: Class[_] if c == classOf[java.lang.String] => StringType
    case c: Class[_] if c == classOf[java.sql.Date] => DateType
    case c: Class[_] if c == classOf[java.sql.Timestamp] => TimestampType
    case c: Class[_] if c == classOf[HiveDecimal] => DecimalType
    case c: Class[_] if c == classOf[java.math.BigDecimal] => DecimalType
    case c: Class[_] if c == classOf[Array[Byte]] => BinaryType
    case c: Class[_] if c == classOf[java.lang.Short] => ShortType
    case c: Class[_] if c == classOf[java.lang.Integer] => IntegerType
    case c: Class[_] if c == classOf[java.lang.Long] => LongType
    case c: Class[_] if c == classOf[java.lang.Double] => DoubleType
    case c: Class[_] if c == classOf[java.lang.Byte] => ByteType
    case c: Class[_] if c == classOf[java.lang.Float] => FloatType
    case c: Class[_] if c == classOf[java.lang.Boolean] => BooleanType

    // primitive type
    case c: Class[_] if c == java.lang.Short.TYPE => ShortType
    case c: Class[_] if c == java.lang.Integer.TYPE => IntegerType
    case c: Class[_] if c == java.lang.Long.TYPE => LongType
    case c: Class[_] if c == java.lang.Double.TYPE => DoubleType
    case c: Class[_] if c == java.lang.Byte.TYPE => ByteType
    case c: Class[_] if c == java.lang.Float.TYPE => FloatType
    case c: Class[_] if c == java.lang.Boolean.TYPE => BooleanType

    case c: Class[_] if c.isArray => ArrayType(javaClassToDataType(c.getComponentType))

    // Hive seems to return this for struct types?
    case c: Class[_] if c == classOf[java.lang.Object] => NullType
  }

  /** Converts hive types to native catalyst types. */
  def unwrap(a: Any): Any = a match {
    case null => null
    case i: hadoopIo.IntWritable => i.get
    case t: hadoopIo.Text => t.toString
    case l: hadoopIo.LongWritable => l.get
    case d: hadoopIo.DoubleWritable => d.get
    case d: hiveIo.DoubleWritable => d.get
    case s: hiveIo.ShortWritable => s.get
    case b: hadoopIo.BooleanWritable => b.get
    case b: hiveIo.ByteWritable => b.get
    case b: hadoopIo.FloatWritable => b.get
    case b: hadoopIo.BytesWritable => {
      val bytes = new Array[Byte](b.getLength)
      System.arraycopy(b.getBytes(), 0, bytes, 0, b.getLength)
      bytes
    }
    case d: hiveIo.DateWritable => d.get
    case t: hiveIo.TimestampWritable => t.getTimestamp
    case b: hiveIo.HiveDecimalWritable => BigDecimal(b.getHiveDecimal().bigDecimalValue())
    case map: java.util.Map[_,_] => map.map { case (k, v) => (unwrap(k), unwrap(v)) }.toMap
    // StandardStructObjectInspector expects the data as either Object Array or java.util.List
    case array: Array[_] => Row(array.map(unwrap): _*)
    case array: java.util.List[_] => Row(array.toArray.map(unwrap): _*)
    // TODO how about the ListObjectInspector
    // case list: java.util.List[_] => list.map(unwrap)
    case p: java.lang.Short => p
    case p: java.lang.Long => p
    case p: java.lang.Float => p
    case p: java.lang.Integer => p
    case p: java.lang.Double => p
    case p: java.lang.Byte => p
    case p: java.lang.Boolean => p
    case str: String => str
    case p: java.math.BigDecimal => p
    case p: Array[Byte] => p
    case p: java.sql.Date => p
    case p: java.sql.Timestamp => p
  }

  def unwrapData(data: Any, oi: ObjectInspector): Any = oi match {
    case hvoi: HiveVarcharObjectInspector =>
      if (data == null) null else hvoi.getPrimitiveJavaObject(data).getValue
    case hdoi: HiveDecimalObjectInspector =>
      if (data == null) null else BigDecimal(hdoi.getPrimitiveJavaObject(data).bigDecimalValue())
    case pi: PrimitiveObjectInspector => pi.getPrimitiveJavaObject(data)
    case li: ListObjectInspector =>
      Option(li.getList(data))
        .map(_.map(unwrapData(_, li.getListElementObjectInspector)).toSeq)
        .orNull
    case mi: MapObjectInspector =>
      Option(mi.getMap(data)).map(
        _.map {
          case (k,v) =>
            (unwrapData(k, mi.getMapKeyObjectInspector),
              unwrapData(v, mi.getMapValueObjectInspector))
        }.toMap).orNull
    case si: StructObjectInspector =>
      val allRefs = si.getAllStructFieldRefs
      new GenericRow(
        allRefs.map(r =>
          unwrapData(si.getStructFieldData(data,r), r.getFieldObjectInspector)).toArray)
  }

  /**
   * Converts native catalyst types to the types expected by Hive
   * @param a the value to be wrapped
   * @param oi the destination ObjectInspector, which supposed to be the ObjectInspector enumerated
   *           in functions:
   *           def toInspector(dataType: DataType)
   *           def toInspector(expr: Expression)
   */
  def wrap(a: Any, oi: ObjectInspector): AnyRef = if (a == null) {
    null
  } else {
    oi match {
      case x: ConstantObjectInspector => x.getWritableConstantValue
      case x: PrimitiveObjectInspector => a match {
        case s: String => s: java.lang.String
        case i: Int => i: java.lang.Integer
        case b: Boolean => b: java.lang.Boolean
        case f: Float => f: java.lang.Float
        case d: Double => d: java.lang.Double
        case l: Long => l: java.lang.Long
        case l: Short => l: java.lang.Short
        case l: Byte => l: java.lang.Byte
        case b: BigDecimal => HiveShim.createDecimal(b.underlying())
        case b: Array[Byte] => b
        case d: java.sql.Date => d 
        case t: java.sql.Timestamp => t
      }
      case x: StructObjectInspector => {
        val fieldRefs = x.getAllStructFieldRefs
        val row = a.asInstanceOf[Seq[_]]
        val result = new java.util.ArrayList[AnyRef](fieldRefs.length)
        var i = 0
        while(i < fieldRefs.length) {
          result.add(wrap(row(i), fieldRefs.get(i).getFieldObjectInspector))
          i += 1
        }

        result
      }
      case x: ListObjectInspector =>
        seqAsJavaList(a.asInstanceOf[Seq[_]].map(v => wrap(v, x.getListElementObjectInspector)))
      case x: MapObjectInspector =>
        // Some UDFs seem to assume we pass in a HashMap.
        val hashMap = new java.util.HashMap[AnyRef, AnyRef]()
        hashMap.putAll(a.asInstanceOf[Map[_, _]].map {
          case (k, v) =>
            wrap(k, x.getMapKeyObjectInspector) -> wrap(v, x.getMapValueObjectInspector)
        })

        hashMap
    }
  }

  def wrap(row: Seq[Any], inspectors: Seq[ObjectInspector], cache: Array[AnyRef])
  : Array[AnyRef] = {
    var i = 0
    while(i < inspectors.length) {
      cache(i) = wrap(row(i), inspectors(i))
      i += 1
    }
    cache
  }

  def toInspector(dataType: DataType): ObjectInspector = dataType match {
    case ArrayType(tpe, _) =>
      ObjectInspectorFactory.getStandardListObjectInspector(toInspector(tpe))
    case MapType(keyType, valueType, _) =>
      ObjectInspectorFactory.getStandardMapObjectInspector(
        toInspector(keyType), toInspector(valueType))
    case StringType => PrimitiveObjectInspectorFactory.javaStringObjectInspector
    case IntegerType => PrimitiveObjectInspectorFactory.javaIntObjectInspector
    case DoubleType => PrimitiveObjectInspectorFactory.javaDoubleObjectInspector
    case BooleanType => PrimitiveObjectInspectorFactory.javaBooleanObjectInspector
    case LongType => PrimitiveObjectInspectorFactory.javaLongObjectInspector
    case FloatType => PrimitiveObjectInspectorFactory.javaFloatObjectInspector
    case ShortType => PrimitiveObjectInspectorFactory.javaShortObjectInspector
    case ByteType => PrimitiveObjectInspectorFactory.javaByteObjectInspector
    case NullType => PrimitiveObjectInspectorFactory.javaVoidObjectInspector
    case BinaryType => PrimitiveObjectInspectorFactory.javaByteArrayObjectInspector
    case DateType => PrimitiveObjectInspectorFactory.javaDateObjectInspector
    case TimestampType => PrimitiveObjectInspectorFactory.javaTimestampObjectInspector
    case DecimalType => PrimitiveObjectInspectorFactory.javaHiveDecimalObjectInspector
    case StructType(fields) =>
      ObjectInspectorFactory.getStandardStructObjectInspector(
        fields.map(f => f.name), fields.map(f => toInspector(f.dataType)))
  }

  def toInspector(expr: Expression): ObjectInspector = expr match {
    case Literal(value: String, StringType) =>
      PrimitiveObjectInspectorFactory.getPrimitiveWritableConstantObjectInspector(
        TypeInfoFactory.stringTypeInfo, new hadoopIo.Text(value))
    case Literal(value: Int, IntegerType) =>
      PrimitiveObjectInspectorFactory.getPrimitiveWritableConstantObjectInspector(
        TypeInfoFactory.intTypeInfo, new hadoopIo.IntWritable(value))
    case Literal(value: Double, DoubleType) =>
      PrimitiveObjectInspectorFactory.getPrimitiveWritableConstantObjectInspector(
        TypeInfoFactory.doubleTypeInfo, new hiveIo.DoubleWritable(value))
    case Literal(value: Boolean, BooleanType) =>
      PrimitiveObjectInspectorFactory.getPrimitiveWritableConstantObjectInspector(
        TypeInfoFactory.booleanTypeInfo, new hadoopIo.BooleanWritable(value))
    case Literal(value: Long, LongType) =>
      PrimitiveObjectInspectorFactory.getPrimitiveWritableConstantObjectInspector(
        TypeInfoFactory.longTypeInfo, new hadoopIo.LongWritable(value))
    case Literal(value: Float, FloatType) =>
      PrimitiveObjectInspectorFactory.getPrimitiveWritableConstantObjectInspector(
        TypeInfoFactory.floatTypeInfo, new hadoopIo.FloatWritable(value))
    case Literal(value: Short, ShortType) =>
      PrimitiveObjectInspectorFactory.getPrimitiveWritableConstantObjectInspector(
        TypeInfoFactory.shortTypeInfo, new hiveIo.ShortWritable(value))
    case Literal(value: Byte, ByteType) =>
      PrimitiveObjectInspectorFactory.getPrimitiveWritableConstantObjectInspector(
        TypeInfoFactory.byteTypeInfo, new hiveIo.ByteWritable(value))
    case Literal(value: Array[Byte], BinaryType) =>
      PrimitiveObjectInspectorFactory.getPrimitiveWritableConstantObjectInspector(
        TypeInfoFactory.binaryTypeInfo, new hadoopIo.BytesWritable(value))
    case Literal(value: java.sql.Date, DateType) =>
      PrimitiveObjectInspectorFactory.getPrimitiveWritableConstantObjectInspector(
        TypeInfoFactory.dateTypeInfo, new hiveIo.DateWritable(value))
    case Literal(value: java.sql.Timestamp, TimestampType) =>
      PrimitiveObjectInspectorFactory.getPrimitiveWritableConstantObjectInspector(
        TypeInfoFactory.timestampTypeInfo, new hiveIo.TimestampWritable(value))
    case Literal(value: BigDecimal, DecimalType) =>
      PrimitiveObjectInspectorFactory.getPrimitiveWritableConstantObjectInspector(
        TypeInfoFactory.decimalTypeInfo,
        new hiveIo.HiveDecimalWritable(HiveShim.createDecimal(value.underlying())))
    case Literal(_, NullType) =>
      PrimitiveObjectInspectorFactory.getPrimitiveWritableConstantObjectInspector(
        TypeInfoFactory.voidTypeInfo, null)
    case Literal(_, _) => sys.error("Hive doesn't support the constant complicated type.")
    case _ => toInspector(expr.dataType)
  }

  def inspectorToDataType(inspector: ObjectInspector): DataType = inspector match {
    case s: StructObjectInspector =>
      StructType(s.getAllStructFieldRefs.map(f => {
        types.StructField(
          f.getFieldName, inspectorToDataType(f.getFieldObjectInspector), nullable = true)
      }))
    case l: ListObjectInspector => ArrayType(inspectorToDataType(l.getListElementObjectInspector))
    case m: MapObjectInspector =>
      MapType(
        inspectorToDataType(m.getMapKeyObjectInspector),
        inspectorToDataType(m.getMapValueObjectInspector))
    case _: WritableStringObjectInspector => StringType
    case _: JavaStringObjectInspector => StringType
    case _: WritableIntObjectInspector => IntegerType
    case _: JavaIntObjectInspector => IntegerType
    case _: WritableDoubleObjectInspector => DoubleType
    case _: JavaDoubleObjectInspector => DoubleType
    case _: WritableBooleanObjectInspector => BooleanType
    case _: JavaBooleanObjectInspector => BooleanType
    case _: WritableLongObjectInspector => LongType
    case _: JavaLongObjectInspector => LongType
    case _: WritableShortObjectInspector => ShortType
    case _: JavaShortObjectInspector => ShortType
    case _: WritableByteObjectInspector => ByteType
    case _: JavaByteObjectInspector => ByteType
    case _: WritableFloatObjectInspector => FloatType
    case _: JavaFloatObjectInspector => FloatType
    case _: WritableBinaryObjectInspector => BinaryType
    case _: JavaBinaryObjectInspector => BinaryType
    case _: WritableHiveDecimalObjectInspector => DecimalType
    case _: JavaHiveDecimalObjectInspector => DecimalType
    case _: WritableDateObjectInspector => DateType
    case _: JavaDateObjectInspector => DateType
    case _: WritableTimestampObjectInspector => TimestampType
    case _: JavaTimestampObjectInspector => TimestampType
    case _: WritableVoidObjectInspector => NullType
    case _: JavaVoidObjectInspector => NullType
  }

  implicit class typeInfoConversions(dt: DataType) {
    import org.apache.hadoop.hive.serde2.typeinfo._
    import TypeInfoFactory._

    def toTypeInfo: TypeInfo = dt match {
      case ArrayType(elemType, _) =>
        getListTypeInfo(elemType.toTypeInfo)
      case StructType(fields) =>
        getStructTypeInfo(fields.map(_.name), fields.map(_.dataType.toTypeInfo))
      case MapType(keyType, valueType, _) =>
        getMapTypeInfo(keyType.toTypeInfo, valueType.toTypeInfo)
      case BinaryType => binaryTypeInfo
      case BooleanType => booleanTypeInfo
      case ByteType => byteTypeInfo
      case DoubleType => doubleTypeInfo
      case FloatType => floatTypeInfo
      case IntegerType => intTypeInfo
      case LongType => longTypeInfo
      case ShortType => shortTypeInfo
      case StringType => stringTypeInfo
      case DecimalType => decimalTypeInfo
      case DateType => dateTypeInfo
      case TimestampType => timestampTypeInfo
      case NullType => voidTypeInfo
    }
  }
}
