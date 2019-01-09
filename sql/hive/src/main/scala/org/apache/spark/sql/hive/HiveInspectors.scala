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

import java.lang.reflect.{ParameterizedType, Type, WildcardType}

import scala.collection.JavaConverters._

import org.apache.hadoop.{io => hadoopIo}
import org.apache.hadoop.hive.common.`type`.{HiveChar, HiveDecimal, HiveVarchar}
import org.apache.hadoop.hive.serde2.{io => hiveIo}
import org.apache.hadoop.hive.serde2.objectinspector.{StructField => HiveStructField, _}
import org.apache.hadoop.hive.serde2.objectinspector.primitive._
import org.apache.hadoop.hive.serde2.typeinfo.{DecimalTypeInfo, TypeInfoFactory}

import org.apache.spark.sql.AnalysisException
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.catalyst.util._
import org.apache.spark.sql.types
import org.apache.spark.sql.types._
import org.apache.spark.unsafe.types.UTF8String

/**
 * 1. The Underlying data type in catalyst and in Hive
 * In catalyst:
 *  Primitive  =>
 *     UTF8String
 *     int / scala.Int
 *     boolean / scala.Boolean
 *     float / scala.Float
 *     double / scala.Double
 *     long / scala.Long
 *     short / scala.Short
 *     byte / scala.Byte
 *     [[org.apache.spark.sql.types.Decimal]]
 *     Array[Byte]
 *     java.sql.Date
 *     java.sql.Timestamp
 *  Complex Types =>
 *    Map: `MapData`
 *    List: `ArrayData`
 *    Struct: [[org.apache.spark.sql.catalyst.InternalRow]]
 *    Union: NOT SUPPORTED YET
 *  The Complex types plays as a container, which can hold arbitrary data types.
 *
 * In Hive, the native data types are various, in UDF/UDAF/UDTF, and associated with
 * Object Inspectors, in Hive expression evaluation framework, the underlying data are
 * Primitive Type
 *   Java Boxed Primitives:
 *       org.apache.hadoop.hive.common.type.HiveVarchar
 *       org.apache.hadoop.hive.common.type.HiveChar
 *       java.lang.String
 *       java.lang.Integer
 *       java.lang.Boolean
 *       java.lang.Float
 *       java.lang.Double
 *       java.lang.Long
 *       java.lang.Short
 *       java.lang.Byte
 *       org.apache.hadoop.hive.common.`type`.HiveDecimal
 *       byte[]
 *       java.sql.Date
 *       java.sql.Timestamp
 *   Writables:
 *       org.apache.hadoop.hive.serde2.io.HiveVarcharWritable
 *       org.apache.hadoop.hive.serde2.io.HiveCharWritable
 *       org.apache.hadoop.io.Text
 *       org.apache.hadoop.io.IntWritable
 *       org.apache.hadoop.hive.serde2.io.DoubleWritable
 *       org.apache.hadoop.io.BooleanWritable
 *       org.apache.hadoop.io.LongWritable
 *       org.apache.hadoop.io.FloatWritable
 *       org.apache.hadoop.hive.serde2.io.ShortWritable
 *       org.apache.hadoop.hive.serde2.io.ByteWritable
 *       org.apache.hadoop.io.BytesWritable
 *       org.apache.hadoop.hive.serde2.io.DateWritable
 *       org.apache.hadoop.hive.serde2.io.TimestampWritable
 *       org.apache.hadoop.hive.serde2.io.HiveDecimalWritable
 * Complex Type
 *   List: Object[] / java.util.List
 *   Map: java.util.Map
 *   Struct: Object[] / java.util.List / java POJO
 *   Union: class StandardUnion { byte tag; Object object }
 *
 * NOTICE: HiveVarchar/HiveChar is not supported by catalyst, it will be simply considered as
 *  String type.
 *
 *
 * 2. Hive ObjectInspector is a group of flexible APIs to inspect value in different data
 *  representation, and developers can extend those API as needed, so technically,
 *  object inspector supports arbitrary data type in java.
 *
 * Fortunately, only few built-in Hive Object Inspectors are used in generic udf/udaf/udtf
 * evaluation.
 * 1) Primitive Types (PrimitiveObjectInspector & its sub classes)
  {{{
   public interface PrimitiveObjectInspector {
     // Java Primitives (java.lang.Integer, java.lang.String etc.)
     Object getPrimitiveJavaObject(Object o);
     // Writables (hadoop.io.IntWritable, hadoop.io.Text etc.)
     Object getPrimitiveWritableObject(Object o);
     // ObjectInspector only inspect the `writable` always return true, we need to check it
     // before invoking the methods above.
     boolean preferWritable();
     ...
   }
  }}}

 * 2) Complex Types:
 *   ListObjectInspector: inspects java array or [[java.util.List]]
 *   MapObjectInspector: inspects [[java.util.Map]]
 *   Struct.StructObjectInspector: inspects java array, [[java.util.List]] and
 *                                 even a normal java object (POJO)
 *   UnionObjectInspector: (tag: Int, object data) (TODO: not supported by SparkSQL yet)
 *
 * 3) ConstantObjectInspector:
 * Constant object inspector can be either primitive type or Complex type, and it bundles a
 * constant value as its property, usually the value is created when the constant object inspector
 * constructed.
 * {{{
   public interface ConstantObjectInspector extends ObjectInspector {
      Object getWritableConstantValue();
      ...
    }
  }}}
 * Hive provides 3 built-in constant object inspectors:
 * Primitive Object Inspectors:
 *     WritableConstantStringObjectInspector
 *     WritableConstantHiveVarcharObjectInspector
 *     WritableConstantHiveCharObjectInspector
 *     WritableConstantHiveDecimalObjectInspector
 *     WritableConstantTimestampObjectInspector
 *     WritableConstantIntObjectInspector
 *     WritableConstantDoubleObjectInspector
 *     WritableConstantBooleanObjectInspector
 *     WritableConstantLongObjectInspector
 *     WritableConstantFloatObjectInspector
 *     WritableConstantShortObjectInspector
 *     WritableConstantByteObjectInspector
 *     WritableConstantBinaryObjectInspector
 *     WritableConstantDateObjectInspector
 * Map Object Inspector:
 *     StandardConstantMapObjectInspector
 * List Object Inspector:
 *     StandardConstantListObjectInspector]]
 * Struct Object Inspector: Hive doesn't provide the built-in constant object inspector for Struct
 * Union Object Inspector: Hive doesn't provide the built-in constant object inspector for Union
 *
 *
 * 3. This trait facilitates:
 *    Data Unwrapping: Hive Data => Catalyst Data (unwrap)
 *    Data Wrapping: Catalyst Data => Hive Data (wrap)
 *    Binding the Object Inspector for Catalyst Data (toInspector)
 *    Retrieving the Catalyst Data Type from Object Inspector (inspectorToDataType)
 *
 *
 * 4. Future Improvement (TODO)
 *   This implementation is quite ugly and inefficient:
 *     a. Pattern matching in runtime
 *     b. Small objects creation in catalyst data => writable
 *     c. Unnecessary unwrap / wrap for nested UDF invoking:
 *       e.g. date_add(printf("%s-%s-%s", a,b,c), 3)
 *       We don't need to unwrap the data for printf and wrap it again and passes in data_add
 */
private[hive] trait HiveInspectors {

  def javaTypeToDataType(clz: Type): DataType = clz match {
    // writable
    case c: Class[_] if c == classOf[hadoopIo.DoubleWritable] => DoubleType
    case c: Class[_] if c == classOf[hiveIo.DoubleWritable] => DoubleType
    case c: Class[_] if c == classOf[hiveIo.HiveDecimalWritable] => DecimalType.SYSTEM_DEFAULT
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
    case c: Class[_] if c == classOf[HiveDecimal] => DecimalType.SYSTEM_DEFAULT
    case c: Class[_] if c == classOf[java.math.BigDecimal] => DecimalType.SYSTEM_DEFAULT
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

    case c: Class[_] if c.isArray => ArrayType(javaTypeToDataType(c.getComponentType))

    // Hive seems to return this for struct types?
    case c: Class[_] if c == classOf[java.lang.Object] => NullType

    case p: ParameterizedType if isSubClassOf(p.getRawType, classOf[java.util.List[_]]) =>
      val Array(elementType) = p.getActualTypeArguments
      ArrayType(javaTypeToDataType(elementType))

    case p: ParameterizedType if isSubClassOf(p.getRawType, classOf[java.util.Map[_, _]]) =>
      val Array(keyType, valueType) = p.getActualTypeArguments
      MapType(javaTypeToDataType(keyType), javaTypeToDataType(valueType))

    // raw java list type unsupported
    case c: Class[_] if isSubClassOf(c, classOf[java.util.List[_]]) =>
      throw new AnalysisException(
        "Raw list type in java is unsupported because Spark cannot infer the element type.")

    // raw java map type unsupported
    case c: Class[_] if isSubClassOf(c, classOf[java.util.Map[_, _]]) =>
      throw new AnalysisException(
        "Raw map type in java is unsupported because Spark cannot infer key and value types.")

    case _: WildcardType =>
      throw new AnalysisException(
        "Collection types with wildcards (e.g. List<?> or Map<?, ?>) are unsupported because " +
          "Spark cannot infer the data type for these type parameters.")

    case c => throw new AnalysisException(s"Unsupported java type $c")
  }

  private def isSubClassOf(t: Type, parent: Class[_]): Boolean = t match {
    case cls: Class[_] => parent.isAssignableFrom(cls)
    case _ => false
  }

  private def withNullSafe(f: Any => Any): Any => Any = {
    input => if (input == null) null else f(input)
  }

  /**
   * Wraps with Hive types based on object inspector.
   */
  protected def wrapperFor(oi: ObjectInspector, dataType: DataType): Any => Any = oi match {
    case _ if dataType.isInstanceOf[UserDefinedType[_]] =>
      val sqlType = dataType.asInstanceOf[UserDefinedType[_]].sqlType
      wrapperFor(oi, sqlType)
    case x: ConstantObjectInspector =>
      (o: Any) =>
        x.getWritableConstantValue
    case x: PrimitiveObjectInspector => x match {
      // TODO we don't support the HiveVarcharObjectInspector yet.
      case _: StringObjectInspector if x.preferWritable() =>
        withNullSafe(o => getStringWritable(o))
      case _: StringObjectInspector =>
        withNullSafe(o => o.asInstanceOf[UTF8String].toString())
      case _: IntObjectInspector if x.preferWritable() =>
        withNullSafe(o => getIntWritable(o))
      case _: IntObjectInspector =>
        withNullSafe(o => o.asInstanceOf[java.lang.Integer])
      case _: BooleanObjectInspector if x.preferWritable() =>
        withNullSafe(o => getBooleanWritable(o))
      case _: BooleanObjectInspector =>
        withNullSafe(o => o.asInstanceOf[java.lang.Boolean])
      case _: FloatObjectInspector if x.preferWritable() =>
        withNullSafe(o => getFloatWritable(o))
      case _: FloatObjectInspector =>
        withNullSafe(o => o.asInstanceOf[java.lang.Float])
      case _: DoubleObjectInspector if x.preferWritable() =>
        withNullSafe(o => getDoubleWritable(o))
      case _: DoubleObjectInspector =>
        withNullSafe(o => o.asInstanceOf[java.lang.Double])
      case _: LongObjectInspector if x.preferWritable() =>
        withNullSafe(o => getLongWritable(o))
      case _: LongObjectInspector =>
        withNullSafe(o => o.asInstanceOf[java.lang.Long])
      case _: ShortObjectInspector if x.preferWritable() =>
        withNullSafe(o => getShortWritable(o))
      case _: ShortObjectInspector =>
        withNullSafe(o => o.asInstanceOf[java.lang.Short])
      case _: ByteObjectInspector if x.preferWritable() =>
        withNullSafe(o => getByteWritable(o))
      case _: ByteObjectInspector =>
        withNullSafe(o => o.asInstanceOf[java.lang.Byte])
      case _: JavaHiveVarcharObjectInspector =>
        withNullSafe { o =>
            val s = o.asInstanceOf[UTF8String].toString
            new HiveVarchar(s, s.length)
        }
      case _: JavaHiveCharObjectInspector =>
        withNullSafe { o =>
            val s = o.asInstanceOf[UTF8String].toString
            new HiveChar(s, s.length)
          }
      case _: JavaHiveDecimalObjectInspector =>
        withNullSafe(o =>
          HiveDecimal.create(o.asInstanceOf[Decimal].toJavaBigDecimal))
      case _: JavaDateObjectInspector =>
        withNullSafe(o =>
            DateTimeUtils.toJavaDate(o.asInstanceOf[Int]))
      case _: JavaTimestampObjectInspector =>
        withNullSafe(o =>
            DateTimeUtils.toJavaTimestamp(o.asInstanceOf[Long]))
      case _: HiveDecimalObjectInspector if x.preferWritable() =>
        withNullSafe(o => getDecimalWritable(o.asInstanceOf[Decimal]))
      case _: HiveDecimalObjectInspector =>
        withNullSafe(o =>
            HiveDecimal.create(o.asInstanceOf[Decimal].toJavaBigDecimal))
      case _: BinaryObjectInspector if x.preferWritable() =>
        withNullSafe(o => getBinaryWritable(o))
      case _: BinaryObjectInspector =>
        withNullSafe(o => o.asInstanceOf[Array[Byte]])
      case _: DateObjectInspector if x.preferWritable() =>
        withNullSafe(o => getDateWritable(o))
      case _: DateObjectInspector =>
        withNullSafe(o => DateTimeUtils.toJavaDate(o.asInstanceOf[Int]))
      case _: TimestampObjectInspector if x.preferWritable() =>
        withNullSafe(o => getTimestampWritable(o))
      case _: TimestampObjectInspector =>
        withNullSafe(o => DateTimeUtils.toJavaTimestamp(o.asInstanceOf[Long]))
      case _: VoidObjectInspector =>
        (_: Any) => null // always be null for void object inspector
    }

    case soi: StandardStructObjectInspector =>
      val schema = dataType.asInstanceOf[StructType]
      val wrappers = soi.getAllStructFieldRefs.asScala.zip(schema.fields).map {
        case (ref, field) => wrapperFor(ref.getFieldObjectInspector, field.dataType)
      }
      withNullSafe { o =>
        val struct = soi.create()
        val row = o.asInstanceOf[InternalRow]
        soi.getAllStructFieldRefs.asScala.zip(wrappers).zipWithIndex.foreach {
          case ((field, wrapper), i) =>
            soi.setStructFieldData(struct, field, wrapper(row.get(i, schema(i).dataType)))
        }
        struct
      }

    case ssoi: SettableStructObjectInspector =>
      val structType = dataType.asInstanceOf[StructType]
      val wrappers = ssoi.getAllStructFieldRefs.asScala.zip(structType).map {
        case (ref, tpe) => wrapperFor(ref.getFieldObjectInspector, tpe.dataType)
      }
      withNullSafe { o =>
        val row = o.asInstanceOf[InternalRow]
        // 1. create the pojo (most likely) object
        val result = ssoi.create()
        ssoi.getAllStructFieldRefs.asScala.zip(wrappers).zipWithIndex.foreach {
          case ((field, wrapper), i) =>
            val tpe = structType(i).dataType
            ssoi.setStructFieldData(
            result,
            field,
            wrapper(row.get(i, tpe)).asInstanceOf[AnyRef])
        }
        result
      }

    case soi: StructObjectInspector =>
      val structType = dataType.asInstanceOf[StructType]
      val wrappers = soi.getAllStructFieldRefs.asScala.zip(structType).map {
        case (ref, tpe) => wrapperFor(ref.getFieldObjectInspector, tpe.dataType)
      }
      withNullSafe { o =>
        val row = o.asInstanceOf[InternalRow]
        val result = new java.util.ArrayList[AnyRef](wrappers.size)
        soi.getAllStructFieldRefs.asScala.zip(wrappers).zipWithIndex.foreach {
          case ((field, wrapper), i) =>
          val tpe = structType(i).dataType
          result.add(wrapper(row.get(i, tpe)).asInstanceOf[AnyRef])
        }
        result
      }

    case loi: ListObjectInspector =>
      val elementType = dataType.asInstanceOf[ArrayType].elementType
      val wrapper = wrapperFor(loi.getListElementObjectInspector, elementType)
      withNullSafe { o =>
        val array = o.asInstanceOf[ArrayData]
        val values = new java.util.ArrayList[Any](array.numElements())
        array.foreach(elementType, (_, e) => values.add(wrapper(e)))
        values
      }

    case moi: MapObjectInspector =>
      val mt = dataType.asInstanceOf[MapType]
      val keyWrapper = wrapperFor(moi.getMapKeyObjectInspector, mt.keyType)
      val valueWrapper = wrapperFor(moi.getMapValueObjectInspector, mt.valueType)
      withNullSafe { o =>
          val map = o.asInstanceOf[MapData]
          val jmap = new java.util.HashMap[Any, Any](map.numElements())
          map.foreach(mt.keyType, mt.valueType, (k, v) =>
            jmap.put(keyWrapper(k), valueWrapper(v)))
          jmap
        }

    case _ =>
      identity[Any]
  }

  /**
   * Builds unwrappers ahead of time according to object inspector
   * types to avoid pattern matching and branching costs per row.
   *
   * Strictly follows the following order in unwrapping (constant OI has the higher priority):
   * Constant Null object inspector =>
   *   return null
   * Constant object inspector =>
   *   extract the value from constant object inspector
   * If object inspector prefers writable =>
   *   extract writable from `data` and then get the catalyst type from the writable
   * Extract the java object directly from the object inspector
   *
   * NOTICE: the complex data type requires recursive unwrapping.
   *
   * @param objectInspector the ObjectInspector used to create an unwrapper.
   * @return A function that unwraps data objects.
   *         Use the overloaded HiveStructField version for in-place updating of a MutableRow.
   */
  def unwrapperFor(objectInspector: ObjectInspector): Any => Any =
    objectInspector match {
      case coi: ConstantObjectInspector if coi.getWritableConstantValue == null =>
        _ => null
      case poi: WritableConstantStringObjectInspector =>
        val constant = UTF8String.fromString(poi.getWritableConstantValue.toString)
        _ => constant
      case poi: WritableConstantHiveVarcharObjectInspector =>
        val constant = UTF8String.fromString(poi.getWritableConstantValue.getHiveVarchar.getValue)
        _ => constant
      case poi: WritableConstantHiveCharObjectInspector =>
        val constant = UTF8String.fromString(poi.getWritableConstantValue.getHiveChar.getValue)
        _ => constant
      case poi: WritableConstantHiveDecimalObjectInspector =>
        val constant = HiveShim.toCatalystDecimal(
          PrimitiveObjectInspectorFactory.javaHiveDecimalObjectInspector,
          poi.getWritableConstantValue.getHiveDecimal)
        _ => constant
      case poi: WritableConstantTimestampObjectInspector =>
        val t = poi.getWritableConstantValue
        val constant = t.getSeconds * 1000000L + t.getNanos / 1000L
        _ => constant
      case poi: WritableConstantIntObjectInspector =>
        val constant = poi.getWritableConstantValue.get()
        _ => constant
      case poi: WritableConstantDoubleObjectInspector =>
        val constant = poi.getWritableConstantValue.get()
        _ => constant
      case poi: WritableConstantBooleanObjectInspector =>
        val constant = poi.getWritableConstantValue.get()
        _ => constant
      case poi: WritableConstantLongObjectInspector =>
        val constant = poi.getWritableConstantValue.get()
        _ => constant
      case poi: WritableConstantFloatObjectInspector =>
        val constant = poi.getWritableConstantValue.get()
        _ => constant
      case poi: WritableConstantShortObjectInspector =>
        val constant = poi.getWritableConstantValue.get()
        _ => constant
      case poi: WritableConstantByteObjectInspector =>
        val constant = poi.getWritableConstantValue.get()
        _ => constant
      case poi: WritableConstantBinaryObjectInspector =>
        val writable = poi.getWritableConstantValue
        val constant = new Array[Byte](writable.getLength)
        System.arraycopy(writable.getBytes, 0, constant, 0, constant.length)
        _ => constant
      case poi: WritableConstantDateObjectInspector =>
        val constant = DateTimeUtils.fromJavaDate(poi.getWritableConstantValue.get())
        _ => constant
      case mi: StandardConstantMapObjectInspector =>
        val keyUnwrapper = unwrapperFor(mi.getMapKeyObjectInspector)
        val valueUnwrapper = unwrapperFor(mi.getMapValueObjectInspector)
        val keyValues = mi.getWritableConstantValue
        val constant = ArrayBasedMapData(keyValues, keyUnwrapper, valueUnwrapper)
        _ => constant
      case li: StandardConstantListObjectInspector =>
        val unwrapper = unwrapperFor(li.getListElementObjectInspector)
        val values = li.getWritableConstantValue.asScala
          .map(unwrapper)
          .toArray
        val constant = new GenericArrayData(values)
        _ => constant
      case poi: VoidObjectInspector =>
        _ => null // always be null for void object inspector
      case pi: PrimitiveObjectInspector => pi match {
        // We think HiveVarchar/HiveChar is also a String
        case hvoi: HiveVarcharObjectInspector if hvoi.preferWritable() =>
          data: Any => {
            if (data != null) {
              UTF8String.fromString(hvoi.getPrimitiveWritableObject(data).getHiveVarchar.getValue)
            } else {
              null
            }
          }
        case hvoi: HiveVarcharObjectInspector =>
          data: Any => {
            if (data != null) {
              UTF8String.fromString(hvoi.getPrimitiveJavaObject(data).getValue)
            } else {
              null
            }
          }
        case hvoi: HiveCharObjectInspector if hvoi.preferWritable() =>
          data: Any => {
            if (data != null) {
              UTF8String.fromString(hvoi.getPrimitiveWritableObject(data).getHiveChar.getValue)
            } else {
              null
            }
          }
        case hvoi: HiveCharObjectInspector =>
          data: Any => {
            if (data != null) {
              UTF8String.fromString(hvoi.getPrimitiveJavaObject(data).getValue)
            } else {
              null
            }
          }
        case x: StringObjectInspector if x.preferWritable() =>
          data: Any => {
            if (data != null) {
              // Text is in UTF-8 already. No need to convert again via fromString. Copy bytes
              val wObj = x.getPrimitiveWritableObject(data)
              val result = wObj.copyBytes()
              UTF8String.fromBytes(result, 0, result.length)
            } else {
              null
            }
          }
        case x: StringObjectInspector =>
          data: Any => {
            if (data != null) {
              UTF8String.fromString(x.getPrimitiveJavaObject(data))
            } else {
              null
            }
          }
        case x: IntObjectInspector if x.preferWritable() =>
          data: Any => {
            if (data != null) x.get(data) else null
          }
        case x: BooleanObjectInspector if x.preferWritable() =>
          data: Any => {
            if (data != null) x.get(data) else null
          }
        case x: FloatObjectInspector if x.preferWritable() =>
          data: Any => {
            if (data != null) x.get(data) else null
          }
        case x: DoubleObjectInspector if x.preferWritable() =>
          data: Any => {
            if (data != null) x.get(data) else null
          }
        case x: LongObjectInspector if x.preferWritable() =>
          data: Any => {
            if (data != null) x.get(data) else null
          }
        case x: ShortObjectInspector if x.preferWritable() =>
          data: Any => {
            if (data != null) x.get(data) else null
          }
        case x: ByteObjectInspector if x.preferWritable() =>
          data: Any => {
            if (data != null) x.get(data) else null
          }
        case x: HiveDecimalObjectInspector =>
          data: Any => {
            if (data != null) {
              HiveShim.toCatalystDecimal(x, data)
            } else {
              null
            }
          }
        case x: BinaryObjectInspector if x.preferWritable() =>
          data: Any => {
            if (data != null) {
              // BytesWritable.copyBytes() only available since Hadoop2
              // In order to keep backward-compatible, we have to copy the
              // bytes with old apis
              val bw = x.getPrimitiveWritableObject(data)
              val result = new Array[Byte](bw.getLength())
              System.arraycopy(bw.getBytes(), 0, result, 0, bw.getLength())
              result
            } else {
              null
            }
          }
        case x: DateObjectInspector if x.preferWritable() =>
          data: Any => {
            if (data != null) {
              DateTimeUtils.fromJavaDate(x.getPrimitiveWritableObject(data).get())
            } else {
              null
            }
          }
        case x: DateObjectInspector =>
          data: Any => {
            if (data != null) {
              DateTimeUtils.fromJavaDate(x.getPrimitiveJavaObject(data))
            } else {
              null
            }
          }
        case x: TimestampObjectInspector if x.preferWritable() =>
          data: Any => {
            if (data != null) {
              val t = x.getPrimitiveWritableObject(data)
              t.getSeconds * 1000000L + t.getNanos / 1000L
            } else {
              null
            }
          }
        case ti: TimestampObjectInspector =>
          data: Any => {
            if (data != null) {
              DateTimeUtils.fromJavaTimestamp(ti.getPrimitiveJavaObject(data))
            } else {
              null
            }
          }
        case _ =>
          data: Any => {
            if (data != null) {
              pi.getPrimitiveJavaObject(data)
            } else {
              null
            }
          }
      }
      case li: ListObjectInspector =>
        val unwrapper = unwrapperFor(li.getListElementObjectInspector)
        data: Any => {
          if (data != null) {
            Option(li.getList(data))
              .map { l =>
                val values = l.asScala.map(unwrapper).toArray
                new GenericArrayData(values)
              }
              .orNull
          } else {
            null
          }
        }
      case mi: MapObjectInspector =>
        val keyUnwrapper = unwrapperFor(mi.getMapKeyObjectInspector)
        val valueUnwrapper = unwrapperFor(mi.getMapValueObjectInspector)
        data: Any => {
          if (data != null) {
            val map = mi.getMap(data)
            if (map == null) {
              null
            } else {
              ArrayBasedMapData(map, keyUnwrapper, valueUnwrapper)
            }
          } else {
            null
          }
        }
      // currently, hive doesn't provide the ConstantStructObjectInspector
      case si: StructObjectInspector =>
        val fields = si.getAllStructFieldRefs.asScala
        val unwrappers = fields.map { field =>
          val unwrapper = unwrapperFor(field.getFieldObjectInspector)
          data: Any => unwrapper(si.getStructFieldData(data, field))
        }
        data: Any => {
          if (data != null) {
            InternalRow.fromSeq(unwrappers.map(_(data)))
          } else {
            null
          }
        }
    }

  /**
   * Builds unwrappers ahead of time according to object inspector
   * types to avoid pattern matching and branching costs per row.
   *
   * @param field The HiveStructField to create an unwrapper for.
   * @return A function that performs in-place updating of a MutableRow.
   *         Use the overloaded ObjectInspector version for assignments.
   */
  def unwrapperFor(field: HiveStructField): (Any, InternalRow, Int) => Unit =
    field.getFieldObjectInspector match {
      case oi: BooleanObjectInspector =>
        (value: Any, row: InternalRow, ordinal: Int) => row.setBoolean(ordinal, oi.get(value))
      case oi: ByteObjectInspector =>
        (value: Any, row: InternalRow, ordinal: Int) => row.setByte(ordinal, oi.get(value))
      case oi: ShortObjectInspector =>
        (value: Any, row: InternalRow, ordinal: Int) => row.setShort(ordinal, oi.get(value))
      case oi: IntObjectInspector =>
        (value: Any, row: InternalRow, ordinal: Int) => row.setInt(ordinal, oi.get(value))
      case oi: LongObjectInspector =>
        (value: Any, row: InternalRow, ordinal: Int) => row.setLong(ordinal, oi.get(value))
      case oi: FloatObjectInspector =>
        (value: Any, row: InternalRow, ordinal: Int) => row.setFloat(ordinal, oi.get(value))
      case oi: DoubleObjectInspector =>
        (value: Any, row: InternalRow, ordinal: Int) => row.setDouble(ordinal, oi.get(value))
      case oi =>
        val unwrapper = unwrapperFor(oi)
        (value: Any, row: InternalRow, ordinal: Int) => row(ordinal) = unwrapper(value)
    }

  def wrap(a: Any, oi: ObjectInspector, dataType: DataType): AnyRef = {
    wrapperFor(oi, dataType)(a).asInstanceOf[AnyRef]
  }

  def wrap(
      row: InternalRow,
      wrappers: Array[(Any) => Any],
      cache: Array[AnyRef],
      dataTypes: Array[DataType]): Array[AnyRef] = {
    var i = 0
    val length = wrappers.length
    while (i < length) {
      cache(i) = wrappers(i)(row.get(i, dataTypes(i))).asInstanceOf[AnyRef]
      i += 1
    }
    cache
  }

  def wrap(
      row: Seq[Any],
      wrappers: Array[(Any) => Any],
      cache: Array[AnyRef],
      dataTypes: Array[DataType]): Array[AnyRef] = {
    var i = 0
    val length = wrappers.length
    while (i < length) {
      cache(i) = wrappers(i)(row(i)).asInstanceOf[AnyRef]
      i += 1
    }
    cache
  }

  /**
   * @param dataType Catalyst data type
   * @return Hive java object inspector (recursively), not the Writable ObjectInspector
   * We can easily map to the Hive built-in object inspector according to the data type.
   */
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
    // TODO decimal precision?
    case DecimalType() => PrimitiveObjectInspectorFactory.javaHiveDecimalObjectInspector
    case StructType(fields) =>
      ObjectInspectorFactory.getStandardStructObjectInspector(
        java.util.Arrays.asList(fields.map(f => f.name) : _*),
        java.util.Arrays.asList(fields.map(f => toInspector(f.dataType)) : _*))
  }

  /**
   * Map the catalyst expression to ObjectInspector, however,
   * if the expression is `Literal` or foldable, a constant writable object inspector returns;
   * Otherwise, we always get the object inspector according to its data type(in catalyst)
   * @param expr Catalyst expression to be mapped
   * @return Hive java objectinspector (recursively).
   */
  def toInspector(expr: Expression): ObjectInspector = expr match {
    case Literal(value, StringType) =>
      getStringWritableConstantObjectInspector(value)
    case Literal(value, IntegerType) =>
      getIntWritableConstantObjectInspector(value)
    case Literal(value, DoubleType) =>
      getDoubleWritableConstantObjectInspector(value)
    case Literal(value, BooleanType) =>
      getBooleanWritableConstantObjectInspector(value)
    case Literal(value, LongType) =>
      getLongWritableConstantObjectInspector(value)
    case Literal(value, FloatType) =>
      getFloatWritableConstantObjectInspector(value)
    case Literal(value, ShortType) =>
      getShortWritableConstantObjectInspector(value)
    case Literal(value, ByteType) =>
      getByteWritableConstantObjectInspector(value)
    case Literal(value, BinaryType) =>
      getBinaryWritableConstantObjectInspector(value)
    case Literal(value, DateType) =>
      getDateWritableConstantObjectInspector(value)
    case Literal(value, TimestampType) =>
      getTimestampWritableConstantObjectInspector(value)
    case Literal(value, DecimalType()) =>
      getDecimalWritableConstantObjectInspector(value)
    case Literal(_, NullType) =>
      getPrimitiveNullWritableConstantObjectInspector
    case Literal(value, ArrayType(dt, _)) =>
      val listObjectInspector = toInspector(dt)
      if (value == null) {
        ObjectInspectorFactory.getStandardConstantListObjectInspector(listObjectInspector, null)
      } else {
        val list = new java.util.ArrayList[Object]()
        value.asInstanceOf[ArrayData].foreach(dt, (_, e) =>
          list.add(wrap(e, listObjectInspector, dt)))
        ObjectInspectorFactory.getStandardConstantListObjectInspector(listObjectInspector, list)
      }
    case Literal(value, MapType(keyType, valueType, _)) =>
      val keyOI = toInspector(keyType)
      val valueOI = toInspector(valueType)
      if (value == null) {
        ObjectInspectorFactory.getStandardConstantMapObjectInspector(keyOI, valueOI, null)
      } else {
        val map = value.asInstanceOf[MapData]
        val jmap = new java.util.HashMap[Any, Any](map.numElements())

        map.foreach(keyType, valueType, (k, v) =>
          jmap.put(wrap(k, keyOI, keyType), wrap(v, valueOI, valueType)))

        ObjectInspectorFactory.getStandardConstantMapObjectInspector(keyOI, valueOI, jmap)
      }
    // We will enumerate all of the possible constant expressions, throw exception if we missed
    case Literal(_, dt) => sys.error(s"Hive doesn't support the constant type [$dt].")
    // ideally, we don't test the foldable here(but in optimizer), however, some of the
    // Hive UDF / UDAF requires its argument to be constant objectinspector, we do it eagerly.
    case _ if expr.foldable => toInspector(Literal.create(expr.eval(), expr.dataType))
    // For those non constant expression, map to object inspector according to its data type
    case _ => toInspector(expr.dataType)
  }

  def inspectorToDataType(inspector: ObjectInspector): DataType = inspector match {
    case s: StructObjectInspector =>
      StructType(s.getAllStructFieldRefs.asScala.map(f =>
        types.StructField(
          f.getFieldName, inspectorToDataType(f.getFieldObjectInspector), nullable = true)
      ))
    case l: ListObjectInspector => ArrayType(inspectorToDataType(l.getListElementObjectInspector))
    case m: MapObjectInspector =>
      MapType(
        inspectorToDataType(m.getMapKeyObjectInspector),
        inspectorToDataType(m.getMapValueObjectInspector))
    case _: WritableStringObjectInspector => StringType
    case _: JavaStringObjectInspector => StringType
    case _: WritableHiveVarcharObjectInspector => StringType
    case _: JavaHiveVarcharObjectInspector => StringType
    case _: WritableHiveCharObjectInspector => StringType
    case _: JavaHiveCharObjectInspector => StringType
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
    case w: WritableHiveDecimalObjectInspector => decimalTypeInfoToCatalyst(w)
    case j: JavaHiveDecimalObjectInspector => decimalTypeInfoToCatalyst(j)
    case _: WritableDateObjectInspector => DateType
    case _: JavaDateObjectInspector => DateType
    case _: WritableTimestampObjectInspector => TimestampType
    case _: JavaTimestampObjectInspector => TimestampType
    case _: WritableVoidObjectInspector => NullType
    case _: JavaVoidObjectInspector => NullType
  }

  private def decimalTypeInfoToCatalyst(inspector: PrimitiveObjectInspector): DecimalType = {
    val info = inspector.getTypeInfo.asInstanceOf[DecimalTypeInfo]
    DecimalType(info.precision(), info.scale())
  }

  private def getStringWritableConstantObjectInspector(value: Any): ObjectInspector =
    PrimitiveObjectInspectorFactory.getPrimitiveWritableConstantObjectInspector(
      TypeInfoFactory.stringTypeInfo, getStringWritable(value))

  private def getIntWritableConstantObjectInspector(value: Any): ObjectInspector =
    PrimitiveObjectInspectorFactory.getPrimitiveWritableConstantObjectInspector(
      TypeInfoFactory.intTypeInfo, getIntWritable(value))

  private def getDoubleWritableConstantObjectInspector(value: Any): ObjectInspector =
    PrimitiveObjectInspectorFactory.getPrimitiveWritableConstantObjectInspector(
      TypeInfoFactory.doubleTypeInfo, getDoubleWritable(value))

  private def getBooleanWritableConstantObjectInspector(value: Any): ObjectInspector =
    PrimitiveObjectInspectorFactory.getPrimitiveWritableConstantObjectInspector(
      TypeInfoFactory.booleanTypeInfo, getBooleanWritable(value))

  private def getLongWritableConstantObjectInspector(value: Any): ObjectInspector =
    PrimitiveObjectInspectorFactory.getPrimitiveWritableConstantObjectInspector(
      TypeInfoFactory.longTypeInfo, getLongWritable(value))

  private def getFloatWritableConstantObjectInspector(value: Any): ObjectInspector =
    PrimitiveObjectInspectorFactory.getPrimitiveWritableConstantObjectInspector(
      TypeInfoFactory.floatTypeInfo, getFloatWritable(value))

  private def getShortWritableConstantObjectInspector(value: Any): ObjectInspector =
    PrimitiveObjectInspectorFactory.getPrimitiveWritableConstantObjectInspector(
      TypeInfoFactory.shortTypeInfo, getShortWritable(value))

  private def getByteWritableConstantObjectInspector(value: Any): ObjectInspector =
    PrimitiveObjectInspectorFactory.getPrimitiveWritableConstantObjectInspector(
      TypeInfoFactory.byteTypeInfo, getByteWritable(value))

  private def getBinaryWritableConstantObjectInspector(value: Any): ObjectInspector =
    PrimitiveObjectInspectorFactory.getPrimitiveWritableConstantObjectInspector(
      TypeInfoFactory.binaryTypeInfo, getBinaryWritable(value))

  private def getDateWritableConstantObjectInspector(value: Any): ObjectInspector =
    PrimitiveObjectInspectorFactory.getPrimitiveWritableConstantObjectInspector(
      TypeInfoFactory.dateTypeInfo, getDateWritable(value))

  private def getTimestampWritableConstantObjectInspector(value: Any): ObjectInspector =
    PrimitiveObjectInspectorFactory.getPrimitiveWritableConstantObjectInspector(
      TypeInfoFactory.timestampTypeInfo, getTimestampWritable(value))

  private def getDecimalWritableConstantObjectInspector(value: Any): ObjectInspector =
    PrimitiveObjectInspectorFactory.getPrimitiveWritableConstantObjectInspector(
      TypeInfoFactory.decimalTypeInfo, getDecimalWritable(value))

  private def getPrimitiveNullWritableConstantObjectInspector: ObjectInspector =
    PrimitiveObjectInspectorFactory.getPrimitiveWritableConstantObjectInspector(
      TypeInfoFactory.voidTypeInfo, null)

  private def getStringWritable(value: Any): hadoopIo.Text =
    if (value == null) null else new hadoopIo.Text(value.asInstanceOf[UTF8String].getBytes)

  private def getIntWritable(value: Any): hadoopIo.IntWritable =
    if (value == null) null else new hadoopIo.IntWritable(value.asInstanceOf[Int])

  private def getDoubleWritable(value: Any): hiveIo.DoubleWritable =
    if (value == null) {
      null
    } else {
      new hiveIo.DoubleWritable(value.asInstanceOf[Double])
    }

  private def getBooleanWritable(value: Any): hadoopIo.BooleanWritable =
    if (value == null) {
      null
    } else {
      new hadoopIo.BooleanWritable(value.asInstanceOf[Boolean])
    }

  private def getLongWritable(value: Any): hadoopIo.LongWritable =
    if (value == null) null else new hadoopIo.LongWritable(value.asInstanceOf[Long])

  private def getFloatWritable(value: Any): hadoopIo.FloatWritable =
    if (value == null) {
      null
    } else {
      new hadoopIo.FloatWritable(value.asInstanceOf[Float])
    }

  private def getShortWritable(value: Any): hiveIo.ShortWritable =
    if (value == null) null else new hiveIo.ShortWritable(value.asInstanceOf[Short])

  private def getByteWritable(value: Any): hiveIo.ByteWritable =
    if (value == null) null else new hiveIo.ByteWritable(value.asInstanceOf[Byte])

  private def getBinaryWritable(value: Any): hadoopIo.BytesWritable =
    if (value == null) {
      null
    } else {
      new hadoopIo.BytesWritable(value.asInstanceOf[Array[Byte]])
    }

  private def getDateWritable(value: Any): hiveIo.DateWritable =
    if (value == null) null else new hiveIo.DateWritable(value.asInstanceOf[Int])

  private def getTimestampWritable(value: Any): hiveIo.TimestampWritable =
    if (value == null) {
      null
    } else {
      new hiveIo.TimestampWritable(DateTimeUtils.toJavaTimestamp(value.asInstanceOf[Long]))
    }

  private def getDecimalWritable(value: Any): hiveIo.HiveDecimalWritable =
    if (value == null) {
      null
    } else {
      // TODO precise, scale?
      new hiveIo.HiveDecimalWritable(
        HiveDecimal.create(value.asInstanceOf[Decimal].toJavaBigDecimal))
    }

  implicit class typeInfoConversions(dt: DataType) {
    import org.apache.hadoop.hive.serde2.typeinfo._
    import TypeInfoFactory._

    private def decimalTypeInfo(decimalType: DecimalType): TypeInfo = decimalType match {
      case DecimalType.Fixed(precision, scale) => new DecimalTypeInfo(precision, scale)
    }

    def toTypeInfo: TypeInfo = dt match {
      case ArrayType(elemType, _) =>
        getListTypeInfo(elemType.toTypeInfo)
      case StructType(fields) =>
        getStructTypeInfo(
          java.util.Arrays.asList(fields.map(_.name) : _*),
          java.util.Arrays.asList(fields.map(_.dataType.toTypeInfo) : _*))
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
      case d: DecimalType => decimalTypeInfo(d)
      case DateType => dateTypeInfo
      case TimestampType => timestampTypeInfo
      case NullType => voidTypeInfo
    }
  }
}
