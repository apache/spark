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

import org.apache.hadoop.hive.common.`type`.{HiveDecimal, HiveVarchar}
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfoFactory
import org.apache.hadoop.hive.serde2.objectinspector._
import org.apache.hadoop.hive.serde2.objectinspector.PrimitiveObjectInspector._
import org.apache.hadoop.hive.serde2.objectinspector.primitive._
import org.apache.hadoop.hive.serde2.{io => hiveIo}
import org.apache.hadoop.{io => hadoopIo}

import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.catalyst.types
import org.apache.spark.sql.catalyst.types._
import org.apache.spark.sql.catalyst.types.decimal.Decimal

/* Implicit conversions */
import scala.collection.JavaConversions._

private[hive] trait HiveInspectors {

  def javaClassToDataType(clz: Class[_]): DataType = clz match {
    // writable
    case c: Class[_] if c == classOf[hadoopIo.DoubleWritable] => DoubleType
    case c: Class[_] if c == classOf[hiveIo.DoubleWritable] => DoubleType
    case c: Class[_] if c == classOf[hiveIo.HiveDecimalWritable] => DecimalType.Unlimited
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
    case c: Class[_] if c == classOf[HiveDecimal] => DecimalType.Unlimited
    case c: Class[_] if c == classOf[java.math.BigDecimal] => DecimalType.Unlimited
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

  /**
   * Converts hive types to native catalyst types.
   * @param data the data in Hive type
   * @param oi   the ObjectInspector associated with the Hive Type
   * @return     convert the data into catalyst type
   * TODO return the function of (data => Any) instead for performance consideration
   */
  def unwrap(data: Any, oi: ObjectInspector): Any = oi match {
    case _ if data == null => null
    case poi: VoidObjectInspector => null
    case poi: WritableConstantHiveVarcharObjectInspector =>
      poi.getWritableConstantValue.getHiveVarchar.getValue
    case poi: WritableConstantHiveDecimalObjectInspector =>
      HiveShim.toCatalystDecimal(
        PrimitiveObjectInspectorFactory.javaHiveDecimalObjectInspector,
        poi.getWritableConstantValue.getHiveDecimal)
    case poi: WritableConstantTimestampObjectInspector =>
      poi.getWritableConstantValue.getTimestamp.clone()
    case poi: WritableConstantIntObjectInspector => 
      poi.getWritableConstantValue.get()
    case poi: WritableConstantDoubleObjectInspector => 
      poi.getWritableConstantValue.get()
    case poi: WritableConstantBooleanObjectInspector =>
      poi.getWritableConstantValue.get()
    case poi: WritableConstantLongObjectInspector =>
      poi.getWritableConstantValue.get()
    case poi: WritableConstantFloatObjectInspector =>
      poi.getWritableConstantValue.get()
    case poi: WritableConstantShortObjectInspector =>
      poi.getWritableConstantValue.get()
    case poi: WritableConstantByteObjectInspector =>
      poi.getWritableConstantValue.get()
    case poi: WritableConstantBinaryObjectInspector =>
      val writable = poi.getWritableConstantValue
      val temp = new Array[Byte](writable.getLength)
      System.arraycopy(writable.getBytes, 0, temp, 0, temp.length)
      temp
    case poi: WritableConstantDateObjectInspector => poi.getWritableConstantValue.get()
    case hvoi: HiveVarcharObjectInspector => hvoi.getPrimitiveJavaObject(data).getValue
    case hdoi: HiveDecimalObjectInspector => HiveShim.toCatalystDecimal(hdoi, data)
    // org.apache.hadoop.hive.serde2.io.TimestampWritable.set will reset current time object
    // if next timestamp is null, so Timestamp object is cloned
    case ti: TimestampObjectInspector => ti.getPrimitiveJavaObject(data).clone()
    case pi: PrimitiveObjectInspector => pi.getPrimitiveJavaObject(data)
    case li: ListObjectInspector =>
      Option(li.getList(data))
        .map(_.map(unwrap(_, li.getListElementObjectInspector)).toSeq)
        .orNull
    case mi: MapObjectInspector =>
      Option(mi.getMap(data)).map(
        _.map {
          case (k,v) =>
            (unwrap(k, mi.getMapKeyObjectInspector),
              unwrap(v, mi.getMapValueObjectInspector))
        }.toMap).orNull
    case si: StructObjectInspector =>
      val allRefs = si.getAllStructFieldRefs
      new GenericRow(
        allRefs.map(r =>
          unwrap(si.getStructFieldData(data,r), r.getFieldObjectInspector)).toArray)
  }


  /**
   * Wraps with Hive types based on object inspector.
   * TODO: Consolidate all hive OI/data interface code.
   */
  protected def wrapperFor(oi: ObjectInspector): Any => Any = oi match {
    case _: JavaHiveVarcharObjectInspector =>
      (o: Any) => new HiveVarchar(o.asInstanceOf[String], o.asInstanceOf[String].size)

    case _: JavaHiveDecimalObjectInspector =>
      (o: Any) => HiveShim.createDecimal(o.asInstanceOf[Decimal].toBigDecimal.underlying())

    case soi: StandardStructObjectInspector =>
      val wrappers = soi.getAllStructFieldRefs.map(ref => wrapperFor(ref.getFieldObjectInspector))
      (o: Any) => {
        val struct = soi.create()
        (soi.getAllStructFieldRefs, wrappers, o.asInstanceOf[Row]).zipped.foreach {
          (field, wrapper, data) => soi.setStructFieldData(struct, field, wrapper(data))
        }
        struct
      }

    case loi: ListObjectInspector =>
      val wrapper = wrapperFor(loi.getListElementObjectInspector)
      (o: Any) => seqAsJavaList(o.asInstanceOf[Seq[_]].map(wrapper))

    case moi: MapObjectInspector =>
      // The Predef.Map is scala.collection.immutable.Map.
      // Since the map values can be mutable, we explicitly import scala.collection.Map at here.
      import scala.collection.Map

      val keyWrapper = wrapperFor(moi.getMapKeyObjectInspector)
      val valueWrapper = wrapperFor(moi.getMapValueObjectInspector)
      (o: Any) => mapAsJavaMap(o.asInstanceOf[Map[_, _]].map { case (key, value) =>
        keyWrapper(key) -> valueWrapper(value)
      })

    case _ =>
      identity[Any]
  }

  /**
   * Converts native catalyst types to the types expected by Hive
   * @param a the value to be wrapped
   * @param oi This ObjectInspector associated with the value returned by this function, and
   *           the ObjectInspector should also be consistent with those returned from
   *           toInspector: DataType => ObjectInspector and
   *           toInspector: Expression => ObjectInspector
   */
  def wrap(a: Any, oi: ObjectInspector): AnyRef = if (a == null) {
    null
  } else {
    oi match {
      case x: ConstantObjectInspector => x.getWritableConstantValue
      case x: PrimitiveObjectInspector => a match {
        // TODO what if x.preferWritable() == true? reuse the writable?
        case s: String => s: java.lang.String
        case i: Int => i: java.lang.Integer
        case b: Boolean => b: java.lang.Boolean
        case f: Float => f: java.lang.Float
        case d: Double => d: java.lang.Double
        case l: Long => l: java.lang.Long
        case l: Short => l: java.lang.Short
        case l: Byte => l: java.lang.Byte
        case b: BigDecimal => HiveShim.createDecimal(b.underlying())
        case d: Decimal => HiveShim.createDecimal(d.toBigDecimal.underlying())
        case b: Array[Byte] => b
        case d: java.sql.Date => d
        case t: java.sql.Timestamp => t
      }
      case x: StructObjectInspector =>
        val fieldRefs = x.getAllStructFieldRefs
        val row = a.asInstanceOf[Seq[_]]
        val result = new java.util.ArrayList[AnyRef](fieldRefs.length)
        var i = 0
        while (i < fieldRefs.length) {
          result.add(wrap(row(i), fieldRefs.get(i).getFieldObjectInspector))
          i += 1
        }

        result
      case x: ListObjectInspector =>
        val list = new java.util.ArrayList[Object]
        a.asInstanceOf[Seq[_]].foreach {
          v => list.add(wrap(v, x.getListElementObjectInspector))
        }
        list
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

  def wrap(
      row: Seq[Any],
      inspectors: Seq[ObjectInspector],
      cache: Array[AnyRef]): Array[AnyRef] = {
    var i = 0
    while (i < inspectors.length) {
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
    case DecimalType() => PrimitiveObjectInspectorFactory.javaHiveDecimalObjectInspector
    case StructType(fields) =>
      ObjectInspectorFactory.getStandardStructObjectInspector(
        fields.map(f => f.name), fields.map(f => toInspector(f.dataType)))
  }

  def toInspector(expr: Expression): ObjectInspector = expr match {
    case Literal(value, StringType) =>
      HiveShim.getStringWritableConstantObjectInspector(value)
    case Literal(value, IntegerType) =>
      HiveShim.getIntWritableConstantObjectInspector(value)
    case Literal(value, DoubleType) =>
      HiveShim.getDoubleWritableConstantObjectInspector(value)
    case Literal(value, BooleanType) =>
      HiveShim.getBooleanWritableConstantObjectInspector(value)
    case Literal(value, LongType) =>
      HiveShim.getLongWritableConstantObjectInspector(value)
    case Literal(value, FloatType) =>
      HiveShim.getFloatWritableConstantObjectInspector(value)
    case Literal(value, ShortType) =>
      HiveShim.getShortWritableConstantObjectInspector(value)
    case Literal(value, ByteType) =>
      HiveShim.getByteWritableConstantObjectInspector(value)
    case Literal(value, BinaryType) =>
      HiveShim.getBinaryWritableConstantObjectInspector(value)
    case Literal(value, DateType) =>
      HiveShim.getDateWritableConstantObjectInspector(value)
    case Literal(value, TimestampType) =>
      HiveShim.getTimestampWritableConstantObjectInspector(value)
    case Literal(value, DecimalType()) =>
      HiveShim.getDecimalWritableConstantObjectInspector(value)
    case Literal(_, NullType) =>
      HiveShim.getPrimitiveNullWritableConstantObjectInspector
    case Literal(value, ArrayType(dt, _)) =>
      val listObjectInspector = toInspector(dt)
      if (value == null) {
        ObjectInspectorFactory.getStandardConstantListObjectInspector(listObjectInspector, null)
      } else {
        val list = new java.util.ArrayList[Object]()
        value.asInstanceOf[Seq[_]].foreach(v => list.add(wrap(v, listObjectInspector)))
        ObjectInspectorFactory.getStandardConstantListObjectInspector(listObjectInspector, list)
      }
    case Literal(value, MapType(keyType, valueType, _)) =>
      val keyOI = toInspector(keyType)
      val valueOI = toInspector(valueType)
      if (value == null) {
        ObjectInspectorFactory.getStandardConstantMapObjectInspector(keyOI, valueOI, null)
      } else {
        val map = new java.util.HashMap[Object, Object]()
        value.asInstanceOf[Map[_, _]].foreach (entry => {
          map.put(wrap(entry._1, keyOI), wrap(entry._2, valueOI))
        })
        ObjectInspectorFactory.getStandardConstantMapObjectInspector(keyOI, valueOI, map)
      }
    case Literal(_, dt) => sys.error(s"Hive doesn't support the constant type [$dt].")
    case _ if expr.foldable => toInspector(Literal(expr.eval(), expr.dataType))
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
    case w: WritableHiveDecimalObjectInspector => HiveShim.decimalTypeInfoToCatalyst(w)
    case j: JavaHiveDecimalObjectInspector => HiveShim.decimalTypeInfoToCatalyst(j)
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
      case d: DecimalType => HiveShim.decimalTypeInfo(d)
      case DateType => dateTypeInfo
      case TimestampType => timestampTypeInfo
      case NullType => voidTypeInfo
    }
  }
}
