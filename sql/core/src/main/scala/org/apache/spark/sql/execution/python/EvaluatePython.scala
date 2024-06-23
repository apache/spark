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

package org.apache.spark.sql.execution.python

import java.io.OutputStream
import java.nio.charset.StandardCharsets

import scala.jdk.CollectionConverters._

import net.razorvine.pickle.{IObjectPickler, Opcodes, Pickler}

import org.apache.spark.SparkIllegalArgumentException
import org.apache.spark.api.python.SerDeUtil
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.catalyst.util.{ArrayBasedMapData, ArrayData, GenericArrayData, MapData}
import org.apache.spark.sql.types._
import org.apache.spark.unsafe.types.UTF8String

object EvaluatePython {

  def needConversionInPython(dt: DataType): Boolean = dt match {
    case DateType | TimestampType | TimestampNTZType | _: DayTimeIntervalType => true
    case _: StructType => true
    case _: UserDefinedType[_] => true
    case ArrayType(elementType, _) => needConversionInPython(elementType)
    case MapType(keyType, valueType, _) =>
      needConversionInPython(keyType) || needConversionInPython(valueType)
    case _ => false
  }

  /**
   * Helper for converting from Catalyst type to java type suitable for Pickle.
   */
  def toJava(obj: Any, dataType: DataType): Any = (obj, dataType) match {
    case (null, _) => null

    case (row: InternalRow, struct: StructType) =>
      val values = new Array[Any](row.numFields)
      var i = 0
      while (i < row.numFields) {
        values(i) = toJava(row.get(i, struct.fields(i).dataType), struct.fields(i).dataType)
        i += 1
      }
      new GenericRowWithSchema(values, struct)

    case (a: ArrayData, array: ArrayType) =>
      val values = new java.util.ArrayList[Any](a.numElements())
      a.foreach(array.elementType, (_, e) => {
        values.add(toJava(e, array.elementType))
      })
      values

    case (map: MapData, mt: MapType) =>
      val jmap = new java.util.HashMap[Any, Any](map.numElements())
      map.foreach(mt.keyType, mt.valueType, (k, v) => {
        jmap.put(toJava(k, mt.keyType), toJava(v, mt.valueType))
      })
      jmap

    case (ud, udt: UserDefinedType[_]) => toJava(ud, udt.sqlType)

    case (d: Decimal, _) => d.toJavaBigDecimal

    case (s: UTF8String, StringType) => s.toString

    case (other, _) => other
  }

  /**
   * Make a converter that converts `obj` to the type specified by the data type, or returns
   * null if the type of obj is unexpected. Because Python doesn't enforce the type.
   */
  def makeFromJava(dataType: DataType): Any => Any = dataType match {
    case BooleanType => (obj: Any) => nullSafeConvert(obj) {
      case b: Boolean => b
    }

    case ByteType => (obj: Any) => nullSafeConvert(obj) {
      case c: Byte => c
      case c: Short => c.toByte
      case c: Int => c.toByte
      case c: Long => c.toByte
    }

    case ShortType => (obj: Any) => nullSafeConvert(obj) {
      case c: Byte => c.toShort
      case c: Short => c
      case c: Int => c.toShort
      case c: Long => c.toShort
    }

    case IntegerType => (obj: Any) => nullSafeConvert(obj) {
      case c: Byte => c.toInt
      case c: Short => c.toInt
      case c: Int => c
      case c: Long => c.toInt
    }

    case LongType => (obj: Any) => nullSafeConvert(obj) {
      case c: Byte => c.toLong
      case c: Short => c.toLong
      case c: Int => c.toLong
      case c: Long => c
    }

    case FloatType => (obj: Any) => nullSafeConvert(obj) {
      case c: Float => c
      case c: Double => c.toFloat
    }

    case DoubleType => (obj: Any) => nullSafeConvert(obj) {
      case c: Float => c.toDouble
      case c: Double => c
    }

    case dt: DecimalType => (obj: Any) => nullSafeConvert(obj) {
      case c: java.math.BigDecimal => Decimal(c, dt.precision, dt.scale)
    }

    case DateType => (obj: Any) => nullSafeConvert(obj) {
      case c: Int => c
    }

    case TimestampType | TimestampNTZType | _: DayTimeIntervalType => (obj: Any) =>
      nullSafeConvert(obj) {
        case c: Long => c
        // Py4J serializes values between MIN_INT and MAX_INT as Ints, not Longs
        case c: Int => c.toLong
      }

    case _: StringType => (obj: Any) => nullSafeConvert(obj) {
      case _ => UTF8String.fromString(obj.toString)
    }

    case BinaryType => (obj: Any) => nullSafeConvert(obj) {
      case c: String => c.getBytes(StandardCharsets.UTF_8)
      case c if c.getClass.isArray && c.getClass.getComponentType.getName == "byte" => c
    }

    case ArrayType(elementType, _) =>
      val elementFromJava = makeFromJava(elementType)

      (obj: Any) => nullSafeConvert(obj) {
        case c: java.util.List[_] =>
          new GenericArrayData(c.asScala.map { e => elementFromJava(e) }.toArray)
        case c if c.getClass.isArray =>
          new GenericArrayData(c.asInstanceOf[Array[_]].map(e => elementFromJava(e)))
      }

    case MapType(keyType, valueType, _) =>
      val keyFromJava = makeFromJava(keyType)
      val valueFromJava = makeFromJava(valueType)

      (obj: Any) => nullSafeConvert(obj) {
        case javaMap: java.util.Map[_, _] =>
          ArrayBasedMapData(
            javaMap,
            (key: Any) => keyFromJava(key),
            (value: Any) => valueFromJava(value))
      }

    case StructType(fields) =>
      val fieldsFromJava = fields.map(f => makeFromJava(f.dataType))

      (obj: Any) => nullSafeConvert(obj) {
        case c if c.getClass.isArray =>
          val array = c.asInstanceOf[Array[_]]
          if (array.length != fields.length) {
            throw new SparkIllegalArgumentException(
              errorClass = "STRUCT_ARRAY_LENGTH_MISMATCH",
              messageParameters = Map(
                "expected" -> fields.length.toString,
                "actual" -> array.length.toString))
          }

          val row = new GenericInternalRow(fields.length)
          var i = 0
          while (i < fields.length) {
            row(i) = fieldsFromJava(i)(array(i))
            i += 1
          }
          row
      }

    case udt: UserDefinedType[_] => makeFromJava(udt.sqlType)

    case other => (obj: Any) => nullSafeConvert(obj)(PartialFunction.empty)
  }

  private def nullSafeConvert(input: Any)(f: PartialFunction[Any, Any]): Any = {
    if (input == null) {
      null
    } else {
      f.applyOrElse(input, {
        // all other unexpected type should be null, or we will have runtime exception
        // TODO(davies): we could improve this by try to cast the object to expected type
        _: Any => null
      })
    }
  }

  private val module = "pyspark.sql.types"

  /**
   * Pickler for StructType
   */
  private class StructTypePickler extends IObjectPickler {

    private val cls = classOf[StructType]

    def register(): Unit = {
      Pickler.registerCustomPickler(cls, this)
    }

    def pickle(obj: Object, out: OutputStream, pickler: Pickler): Unit = {
      out.write(Opcodes.GLOBAL)
      out.write(
        (module + "\n" + "_parse_datatype_json_string" + "\n").getBytes(StandardCharsets.UTF_8))
      val schema = obj.asInstanceOf[StructType]
      pickler.save(schema.json)
      out.write(Opcodes.TUPLE1)
      out.write(Opcodes.REDUCE)
    }
  }

  /**
   * Pickler for external row.
   */
  private class RowPickler extends IObjectPickler {

    private val cls = classOf[GenericRowWithSchema]

    // register this to Pickler and Unpickler
    def register(): Unit = {
      Pickler.registerCustomPickler(this.getClass, this)
      Pickler.registerCustomPickler(cls, this)
    }

    def pickle(obj: Object, out: OutputStream, pickler: Pickler): Unit = {
      if (obj == this) {
        out.write(Opcodes.GLOBAL)
        out.write(
          (module + "\n" + "_create_row_inbound_converter" + "\n").getBytes(StandardCharsets.UTF_8))
      } else {
        // it will be memorized by Pickler to save some bytes
        pickler.save(this)
        val row = obj.asInstanceOf[GenericRowWithSchema]
        // schema should always be same object for memoization
        pickler.save(row.schema)
        out.write(Opcodes.TUPLE1)
        out.write(Opcodes.REDUCE)

        out.write(Opcodes.MARK)
        var i = 0
        while (i < row.values.length) {
          pickler.save(row.values(i))
          i += 1
        }
        out.write(Opcodes.TUPLE)
        out.write(Opcodes.REDUCE)
      }
    }
  }

  private[this] var registered = false

  /**
   * This should be called before trying to serialize any above classes un cluster mode,
   * this should be put in the closure
   */
  def registerPicklers(): Unit = {
    synchronized {
      if (!registered) {
        SerDeUtil.initialize()
        new StructTypePickler().register()
        new RowPickler().register()
        registered = true
      }
    }
  }

  /**
   * Convert an RDD of Java objects to an RDD of serialized Python objects, that is usable by
   * PySpark.
   */
  def javaToPython(rdd: RDD[Any]): RDD[Array[Byte]] = {
    rdd.mapPartitions { iter =>
      registerPicklers()  // let it called in executor
      new SerDeUtil.AutoBatchedPickler(iter)
    }
  }
}
