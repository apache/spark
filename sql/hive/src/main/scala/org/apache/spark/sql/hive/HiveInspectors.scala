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
import org.apache.hadoop.hive.serde2.objectinspector._
import org.apache.hadoop.hive.serde2.objectinspector.primitive._
import org.apache.hadoop.hive.serde2.{io => hiveIo}
import org.apache.hadoop.{io => hadoopIo}

import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.types
import org.apache.spark.sql.types._

/* Implicit conversions */
import scala.collection.JavaConversions._

/**
 * 1. The Underlying data type in catalyst and in Hive
 * In catalyst:
 *  Primitive  =>
 *     java.lang.String
 *     int / scala.Int
 *     boolean / scala.Boolean
 *     float / scala.Float
 *     double / scala.Double
 *     long / scala.Long
 *     short / scala.Short
 *     byte / scala.Byte
 *     org.apache.spark.sql.types.Decimal
 *     Array[Byte]
 *     java.sql.Date
 *     java.sql.Timestamp
 *  Complex Types =>
 *    Map: scala.collection.immutable.Map
 *    List: scala.collection.immutable.Seq
 *    Struct:
 *           org.apache.spark.sql.catalyst.expression.Row
 *    Union: NOT SUPPORTED YET
 *  The Complex types plays as a container, which can hold arbitrary data types.
 *
 * In Hive, the native data types are various, in UDF/UDAF/UDTF, and associated with
 * Object Inspectors, in Hive expression evaluation framework, the underlying data are
 * Primitive Type
 *   Java Boxed Primitives:
 *       org.apache.hadoop.hive.common.type.HiveVarchar
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
 * NOTICE: HiveVarchar is not supported by catalyst, it will be simply considered as String type.
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
   *
   * Strictly follows the following order in unwrapping (constant OI has the higher priority):
   *  Constant Null object inspector =>
   *    return null
   *  Constant object inspector =>
   *    extract the value from constant object inspector
   *  Check whether the `data` is null =>
   *    return null if true
   *  If object inspector prefers writable =>
   *    extract writable from `data` and then get the catalyst type from the writable
   *  Extract the java object directly from the object inspector
   *
   *  NOTICE: the complex data type requires recursive unwrapping.
   */
  def unwrap(data: Any, oi: ObjectInspector): Any = oi match {
    case coi: ConstantObjectInspector if coi.getWritableConstantValue == null => null
    case poi: WritableConstantStringObjectInspector => poi.getWritableConstantValue.toString
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
    case mi: StandardConstantMapObjectInspector =>
      // take the value from the map inspector object, rather than the input data
      mi.getWritableConstantValue.map { case (k, v) =>
        (unwrap(k, mi.getMapKeyObjectInspector),
          unwrap(v, mi.getMapValueObjectInspector))
      }.toMap
    case li: StandardConstantListObjectInspector =>
      // take the value from the list inspector object, rather than the input data
      li.getWritableConstantValue.map(unwrap(_, li.getListElementObjectInspector)).toSeq
    // if the value is null, we don't care about the object inspector type
    case _ if data == null => null
    case poi: VoidObjectInspector => null // always be null for void object inspector
    case pi: PrimitiveObjectInspector => pi match {
      // We think HiveVarchar is also a String
      case hvoi: HiveVarcharObjectInspector if hvoi.preferWritable() =>
        hvoi.getPrimitiveWritableObject(data).getHiveVarchar.getValue
      case hvoi: HiveVarcharObjectInspector => hvoi.getPrimitiveJavaObject(data).getValue
      case x: StringObjectInspector if x.preferWritable() =>
        x.getPrimitiveWritableObject(data).toString
      case x: IntObjectInspector if x.preferWritable() => x.get(data)
      case x: BooleanObjectInspector if x.preferWritable() => x.get(data)
      case x: FloatObjectInspector if x.preferWritable() => x.get(data)
      case x: DoubleObjectInspector if x.preferWritable() => x.get(data)
      case x: LongObjectInspector if x.preferWritable() => x.get(data)
      case x: ShortObjectInspector if x.preferWritable() => x.get(data)
      case x: ByteObjectInspector if x.preferWritable() => x.get(data)
      case x: HiveDecimalObjectInspector => HiveShim.toCatalystDecimal(x, data)
      case x: BinaryObjectInspector if x.preferWritable() =>
        // BytesWritable.copyBytes() only available since Hadoop2
        // In order to keep backward-compatible, we have to copy the
        // bytes with old apis
        val bw = x.getPrimitiveWritableObject(data)
        val result = new Array[Byte](bw.getLength()) 
        System.arraycopy(bw.getBytes(), 0, result, 0, bw.getLength())
        result
      case x: DateObjectInspector if x.preferWritable() =>
        x.getPrimitiveWritableObject(data).get()
      // org.apache.hadoop.hive.serde2.io.TimestampWritable.set will reset current time object
      // if next timestamp is null, so Timestamp object is cloned
      case x: TimestampObjectInspector if x.preferWritable() =>
        x.getPrimitiveWritableObject(data).getTimestamp.clone()
      case ti: TimestampObjectInspector => ti.getPrimitiveJavaObject(data).clone()
      case _ => pi.getPrimitiveJavaObject(data)
    }
    case li: ListObjectInspector =>
      Option(li.getList(data))
        .map(_.map(unwrap(_, li.getListElementObjectInspector)).toSeq)
        .orNull
    case mi: MapObjectInspector =>
      Option(mi.getMap(data)).map(
        _.map {
          case (k, v) =>
            (unwrap(k, mi.getMapKeyObjectInspector),
              unwrap(v, mi.getMapValueObjectInspector))
        }.toMap).orNull
    // currently, hive doesn't provide the ConstantStructObjectInspector
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
      (o: Any) => HiveShim.createDecimal(o.asInstanceOf[Decimal].toJavaBigDecimal)

    case soi: StandardStructObjectInspector =>
      val wrappers = soi.getAllStructFieldRefs.map(ref => wrapperFor(ref.getFieldObjectInspector))
      (o: Any) => {
        if (o != null) {
          val struct = soi.create()
          (soi.getAllStructFieldRefs, wrappers, o.asInstanceOf[Row].toSeq).zipped.foreach {
            (field, wrapper, data) => soi.setStructFieldData(struct, field, wrapper(data))
          }
          struct
        } else {
          null
        }
      }

    case loi: ListObjectInspector =>
      val wrapper = wrapperFor(loi.getListElementObjectInspector)
      (o: Any) => if (o != null) seqAsJavaList(o.asInstanceOf[Seq[_]].map(wrapper)) else null

    case moi: MapObjectInspector =>
      // The Predef.Map is scala.collection.immutable.Map.
      // Since the map values can be mutable, we explicitly import scala.collection.Map at here.
      import scala.collection.Map

      val keyWrapper = wrapperFor(moi.getMapKeyObjectInspector)
      val valueWrapper = wrapperFor(moi.getMapValueObjectInspector)
      (o: Any) => {
        if (o != null) {
          mapAsJavaMap(o.asInstanceOf[Map[_, _]].map { case (key, value) =>
            keyWrapper(key) -> valueWrapper(value)
          })
        } else {
          null
        }
      }

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
   *
   * Strictly follows the following order in wrapping (constant OI has the higher priority):
   *   Constant object inspector => return the bundled value of Constant object inspector
   *   Check whether the `a` is null => return null if true
   *   If object inspector prefers writable object => return a Writable for the given data `a`
   *   Map the catalyst data to the boxed java primitive
   *
   *  NOTICE: the complex data type requires recursive wrapping.
   */
  def wrap(a: Any, oi: ObjectInspector): AnyRef = oi match {
    case x: ConstantObjectInspector => x.getWritableConstantValue
    case _ if a == null => null
    case x: PrimitiveObjectInspector => x match {
      // TODO we don't support the HiveVarcharObjectInspector yet.
      case _: StringObjectInspector if x.preferWritable() => HiveShim.getStringWritable(a)
      case _: StringObjectInspector => a.asInstanceOf[java.lang.String]
      case _: IntObjectInspector if x.preferWritable() => HiveShim.getIntWritable(a)
      case _: IntObjectInspector => a.asInstanceOf[java.lang.Integer]
      case _: BooleanObjectInspector if x.preferWritable() => HiveShim.getBooleanWritable(a)
      case _: BooleanObjectInspector => a.asInstanceOf[java.lang.Boolean]
      case _: FloatObjectInspector if x.preferWritable() => HiveShim.getFloatWritable(a)
      case _: FloatObjectInspector => a.asInstanceOf[java.lang.Float]
      case _: DoubleObjectInspector if x.preferWritable() => HiveShim.getDoubleWritable(a)
      case _: DoubleObjectInspector => a.asInstanceOf[java.lang.Double]
      case _: LongObjectInspector if x.preferWritable() => HiveShim.getLongWritable(a)
      case _: LongObjectInspector => a.asInstanceOf[java.lang.Long]
      case _: ShortObjectInspector if x.preferWritable() => HiveShim.getShortWritable(a)
      case _: ShortObjectInspector => a.asInstanceOf[java.lang.Short]
      case _: ByteObjectInspector if x.preferWritable() => HiveShim.getByteWritable(a)
      case _: ByteObjectInspector => a.asInstanceOf[java.lang.Byte]
      case _: HiveDecimalObjectInspector if x.preferWritable() =>
        HiveShim.getDecimalWritable(a.asInstanceOf[Decimal])
      case _: HiveDecimalObjectInspector =>
        HiveShim.createDecimal(a.asInstanceOf[Decimal].toJavaBigDecimal)
      case _: BinaryObjectInspector if x.preferWritable() => HiveShim.getBinaryWritable(a)
      case _: BinaryObjectInspector => a.asInstanceOf[Array[Byte]]
      case _: DateObjectInspector if x.preferWritable() => HiveShim.getDateWritable(a)
      case _: DateObjectInspector => a.asInstanceOf[java.sql.Date]
      case _: TimestampObjectInspector if x.preferWritable() => HiveShim.getTimestampWritable(a)
      case _: TimestampObjectInspector => a.asInstanceOf[java.sql.Timestamp]
    }
    case x: SettableStructObjectInspector =>
      val fieldRefs = x.getAllStructFieldRefs
      val row = a.asInstanceOf[Row]
      // 1. create the pojo (most likely) object
      val result = x.create()
      var i = 0
      while (i < fieldRefs.length) {
        // 2. set the property for the pojo
        x.setStructFieldData(
          result,
          fieldRefs.get(i),
          wrap(row(i), fieldRefs.get(i).getFieldObjectInspector))
        i += 1
      }

      result
    case x: StructObjectInspector =>
      val fieldRefs = x.getAllStructFieldRefs
      val row = a.asInstanceOf[Row]
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

  def wrap(
      row: Row,
      inspectors: Seq[ObjectInspector],
      cache: Array[AnyRef]): Array[AnyRef] = {
    var i = 0
    while (i < inspectors.length) {
      cache(i) = wrap(row(i), inspectors(i))
      i += 1
    }
    cache
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
        java.util.Arrays.asList(fields.map(f => f.name) :_*),
        java.util.Arrays.asList(fields.map(f => toInspector(f.dataType)) :_*))
  }

  /**
   * Map the catalyst expression to ObjectInspector, however,
   * if the expression is [[Literal]] or foldable, a constant writable object inspector returns;
   * Otherwise, we always get the object inspector according to its data type(in catalyst)
   * @param expr Catalyst expression to be mapped
   * @return Hive java objectinspector (recursively).
   */
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
    // We will enumerate all of the possible constant expressions, throw exception if we missed
    case Literal(_, dt) => sys.error(s"Hive doesn't support the constant type [$dt].")
    // ideally, we don't test the foldable here(but in optimizer), however, some of the
    // Hive UDF / UDAF requires its argument to be constant objectinspector, we do it eagerly.
    case _ if expr.foldable => toInspector(Literal(expr.eval(), expr.dataType))
    // For those non constant expression, map to object inspector according to its data type
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
        getStructTypeInfo(
          java.util.Arrays.asList(fields.map(_.name) :_*),
          java.util.Arrays.asList(fields.map(_.dataType.toTypeInfo) :_*))
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
