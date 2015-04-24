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

package org.apache.spark.sql.catalyst

import java.lang.{Iterable => JavaIterable}
import java.util.{Map => JavaMap}

import scala.collection.mutable.HashMap

import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.types._

/**
 * Functions to convert Scala types to Catalyst types and vice versa.
 */
object CatalystTypeConverters {
  // The Predef.Map is scala.collection.immutable.Map.
  // Since the map values can be mutable, we explicitly import scala.collection.Map at here.
  import scala.collection.Map

  /**
   * Converts Scala objects to catalyst rows / types. This method is slow, and for batch
   * conversion you should be using converter produced by createToCatalystConverter.
   * Note: This is always called after schemaFor has been called.
   *       This ordering is important for UDT registration.
   */
  def convertToCatalyst(a: Any, dataType: DataType): Any = (a, dataType) match {
    // Check UDT first since UDTs can override other types
    case (obj, udt: UserDefinedType[_]) =>
      udt.serialize(obj)

    case (o: Option[_], _) =>
      o.map(convertToCatalyst(_, dataType)).orNull

    case (s: Seq[_], arrayType: ArrayType) =>
      s.map(convertToCatalyst(_, arrayType.elementType))

    case (jit: JavaIterable[_], arrayType: ArrayType) => {
      val iter = jit.iterator
      var listOfItems: List[Any] = List()
      while (iter.hasNext) {
        val item = iter.next()
        listOfItems :+= convertToCatalyst(item, arrayType.elementType)
      }
      listOfItems
    }

    case (s: Array[_], arrayType: ArrayType) =>
      s.toSeq.map(convertToCatalyst(_, arrayType.elementType))

    case (m: Map[_, _], mapType: MapType) =>
      m.map { case (k, v) =>
        convertToCatalyst(k, mapType.keyType) -> convertToCatalyst(v, mapType.valueType)
      }

    case (jmap: JavaMap[_, _], mapType: MapType) =>
      val iter = jmap.entrySet.iterator
      var listOfEntries: List[(Any, Any)] = List()
      while (iter.hasNext) {
        val entry = iter.next()
        listOfEntries :+= (convertToCatalyst(entry.getKey, mapType.keyType),
          convertToCatalyst(entry.getValue, mapType.valueType))
      }
      listOfEntries.toMap

    case (p: Product, structType: StructType) =>
      val ar = new Array[Any](structType.size)
      val iter = p.productIterator
      var idx = 0
      while (idx < structType.size) {
        ar(idx) = convertToCatalyst(iter.next(), structType.fields(idx).dataType)
        idx += 1
      }
      new GenericRowWithSchema(ar, structType)

    case (d: String, _) =>
      UTF8String(d)

    case (d: BigDecimal, _) =>
      Decimal(d)

    case (d: java.math.BigDecimal, _) =>
      Decimal(d)

    case (d: java.sql.Date, _) =>
      DateUtils.fromJavaDate(d)

    case (r: Row, structType: StructType) =>
      val converters = structType.fields.map {
        f => (item: Any) => convertToCatalyst(item, f.dataType)
      }
      convertRowWithConverters(r, structType, converters)

    case (other, _) =>
      other
  }

  /**
   * Creates a converter function that will convert Scala objects to the specified catalyst type.
   * Typical use case would be converting a collection of rows that have the same schema. You will
   * call this function once to get a converter, and apply it to every row.
   */
  private[sql] def createToCatalystConverter(dataType: DataType): Any => Any = {
    def extractOption(item: Any): Any = item match {
      case opt: Option[_] => opt.orNull
      case other => other
    }

    dataType match {
      // Check UDT first since UDTs can override other types
      case udt: UserDefinedType[_] =>
        (item) => extractOption(item) match {
          case null => null
          case other => udt.serialize(other)
        }

      case arrayType: ArrayType =>
        val elementConverter = createToCatalystConverter(arrayType.elementType)
        (item: Any) => {
          extractOption(item) match {
            case a: Array[_] => a.toSeq.map(elementConverter)
            case s: Seq[_] => s.map(elementConverter)
            case i: JavaIterable[_] => {
              val iter = i.iterator
              var convertedIterable: List[Any] = List()
              while (iter.hasNext) {
                val item = iter.next()
                convertedIterable :+= elementConverter(item)
              }
              convertedIterable
            }
            case null => null
          }
        }

      case mapType: MapType =>
        val keyConverter = createToCatalystConverter(mapType.keyType)
        val valueConverter = createToCatalystConverter(mapType.valueType)
        (item: Any) => {
          extractOption(item) match {
            case m: Map[_, _] =>
              m.map { case (k, v) =>
                keyConverter(k) -> valueConverter(v)
              }

            case jmap: JavaMap[_, _] =>
              val iter = jmap.entrySet.iterator
              val convertedMap: HashMap[Any, Any] = HashMap()
              while (iter.hasNext) {
                val entry = iter.next()
                convertedMap(keyConverter(entry.getKey)) = valueConverter(entry.getValue)
              }
              convertedMap

            case null => null
          }
        }

      case structType: StructType =>
        val converters = structType.fields.map(f => createToCatalystConverter(f.dataType))
        (item: Any) => {
          extractOption(item) match {
            case r: Row =>
              convertRowWithConverters(r, structType, converters)

            case p: Product =>
              val ar = new Array[Any](structType.size)
              val iter = p.productIterator
              var idx = 0
              while (idx < structType.size) {
                ar(idx) = converters(idx)(iter.next())
                idx += 1
              }
              new GenericRowWithSchema(ar, structType)

            case null =>
              null
          }
        }

      case dateType: DateType => (item: Any) => extractOption(item) match {
        case d: java.sql.Date => DateUtils.fromJavaDate(d)
        case other => other
      }

      case dataType: StringType => (item: Any) => extractOption(item) match {
        case s: String => UTF8String(s)
        case other => other
      }

      case _ =>
        (item: Any) => extractOption(item) match {
          case d: BigDecimal => Decimal(d)
          case d: java.math.BigDecimal => Decimal(d)
          case other => other
        }
    }
  }

  /**
   *  Converts Scala objects to catalyst rows / types.
   *
   *  Note: This should be called before do evaluation on Row
   *        (It does not support UDT)
   *  This is used to create an RDD or test results with correct types for Catalyst.
   */
  def convertToCatalyst(a: Any): Any = a match {
    case s: String => UTF8String(s)
    case d: java.sql.Date => DateUtils.fromJavaDate(d)
    case d: BigDecimal => Decimal(d)
    case d: java.math.BigDecimal => Decimal(d)
    case seq: Seq[Any] => seq.map(convertToCatalyst)
    case r: Row => Row(r.toSeq.map(convertToCatalyst): _*)
    case arr: Array[Any] => arr.toSeq.map(convertToCatalyst).toArray
    case m: Map[Any, Any] =>
      m.map { case (k, v) => (convertToCatalyst(k), convertToCatalyst(v)) }.toMap
    case other => other
  }

  /** 
   * Converts Catalyst types used internally in rows to standard Scala types
   * This method is slow, and for batch conversion you should be using converter
   * produced by createToScalaConverter.
   */
  def convertToScala(a: Any, dataType: DataType): Any = (a, dataType) match {
    // Check UDT first since UDTs can override other types
    case (d, udt: UserDefinedType[_]) =>
      udt.deserialize(d)

    case (s: Seq[_], arrayType: ArrayType) =>
      s.map(convertToScala(_, arrayType.elementType))

    case (m: Map[_, _], mapType: MapType) =>
      m.map { case (k, v) =>
        convertToScala(k, mapType.keyType) -> convertToScala(v, mapType.valueType)
      }

    case (r: Row, s: StructType) =>
      convertRowToScala(r, s)

    case (d: Decimal, _: DecimalType) =>
      d.toJavaBigDecimal

    case (i: Int, DateType) =>
      DateUtils.toJavaDate(i)

    case (s: UTF8String, StringType) =>
      s.toString()

    case (other, _) =>
      other
  }

  /**
   * Creates a converter function that will convert Catalyst types to Scala type.
   * Typical use case would be converting a collection of rows that have the same schema. You will
   * call this function once to get a converter, and apply it to every row.
   */
  private[sql] def createToScalaConverter(dataType: DataType): Any => Any = dataType match {
    // Check UDT first since UDTs can override other types
    case udt: UserDefinedType[_] =>
      (item: Any) => if (item == null) null else udt.deserialize(item)

    case arrayType: ArrayType =>
      val elementConverter = createToScalaConverter(arrayType.elementType)
      (item: Any) => if (item == null) null else item.asInstanceOf[Seq[_]].map(elementConverter)

    case mapType: MapType =>
      val keyConverter = createToScalaConverter(mapType.keyType)
      val valueConverter = createToScalaConverter(mapType.valueType)
      (item: Any) => if (item == null) {
        null
      } else {
        item.asInstanceOf[Map[_, _]].map { case (k, v) =>
          keyConverter(k) -> valueConverter(v)
        }
      }

    case s: StructType =>
      val converters = s.fields.map(f => createToScalaConverter(f.dataType))
      (item: Any) => {
        if (item == null) {
          null
        } else {
          convertRowWithConverters(item.asInstanceOf[Row], s, converters)
        }
      }

    case _: DecimalType =>
      (item: Any) => item match {
        case d: Decimal => d.toJavaBigDecimal
        case other => other
      }

    case DateType =>
      (item: Any) => item match {
        case i: Int => DateUtils.toJavaDate(i)
        case other => other
      }

    case StringType =>
      (item: Any) => item match {
        case s: UTF8String => s.toString()
        case other => other
      }

    case other =>
      (item: Any) => item
  }

  def convertRowToScala(r: Row, schema: StructType): Row = {
    val ar = new Array[Any](r.size)
    var idx = 0
    while (idx < r.size) {
      ar(idx) = convertToScala(r(idx), schema.fields(idx).dataType)
      idx += 1
    }
    new GenericRowWithSchema(ar, schema)
  }

  /**
   * Converts a row by applying the provided set of converter functions. It is used for both
   * toScala and toCatalyst conversions.
   */
  private[sql] def convertRowWithConverters(
      row: Row,
      schema: StructType,
      converters: Array[Any => Any]): Row = {
    val ar = new Array[Any](row.size)
    var idx = 0
    while (idx < row.size) {
      ar(idx) = converters(idx)(row(idx))
      idx += 1
    }
    new GenericRowWithSchema(ar, schema)
  }
}
