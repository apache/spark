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

package org.apache.spark.examples.pythonconverters

import java.util.{Collection => JCollection, Map => JMap, HashMap => JHashMap, ArrayList => JArrayList}

import org.apache.avro.Schema.Parser
import org.apache.avro.generic.GenericData.Record

import scala.collection.JavaConverters._

import org.apache.avro.generic.{GenericData, GenericFixed, IndexedRecord}
import org.apache.avro.mapred.{AvroKey, AvroWrapper}
import org.apache.avro.Schema
import org.apache.avro.Schema.Type._

import org.apache.spark.api.python.Converter
import org.apache.spark.SparkException


object AvroConversionUtil extends Serializable {
  def fromAvro(obj: Any, schema: Schema): Any = {
    if (obj == null) {
      return null
    }
    schema.getType match {
      case UNION => unpackUnion(obj, schema)
      case ARRAY => unpackArray(obj, schema)
      case FIXED => unpackFixed(obj, schema)
      case MAP => unpackMap(obj, schema)
      case BYTES => unpackBytes(obj)
      case RECORD => unpackRecord(obj)
      case STRING => obj.toString
      case ENUM => obj.toString
      case NULL => obj
      case BOOLEAN => obj
      case DOUBLE => obj
      case FLOAT => obj
      case INT => obj
      case LONG => obj
      case other => throw new SparkException(s"Unknown Avro schema type ${other.getName}")
    }
  }

  def unpackRecord(obj: Any): JMap[String, Any] = {
    val map = new java.util.HashMap[String, Any]
    obj match {
      case record: IndexedRecord =>
        record.getSchema.getFields.asScala.zipWithIndex.foreach { case (f, i) =>
          map.put(f.name, fromAvro(record.get(i), f.schema))
        }
      case other => throw new SparkException(
        s"Unsupported RECORD type ${other.getClass.getName}")
    }
    map
  }

  def unpackMap(obj: Any, schema: Schema): JMap[String, Any] = {
    obj.asInstanceOf[JMap[_, _]].asScala.map { case (key, value) =>
      (key.toString, fromAvro(value, schema.getValueType))
    }.asJava
  }

  def unpackFixed(obj: Any, schema: Schema): Array[Byte] = {
    unpackBytes(obj.asInstanceOf[GenericFixed].bytes())
  }

  def unpackBytes(obj: Any): Array[Byte] = {
    val bytes: Array[Byte] = obj match {
      case buf: java.nio.ByteBuffer =>
        val arr = new Array[Byte](buf.remaining())
        buf.get(arr)
        arr
      case arr: Array[Byte] => arr
      case other => throw new SparkException(
        s"Unknown BYTES type ${other.getClass.getName}")
    }
    val bytearray = new Array[Byte](bytes.length)
    System.arraycopy(bytes, 0, bytearray, 0, bytes.length)
    bytearray
  }

  def unpackArray(obj: Any, schema: Schema): JCollection[Any] = obj match {
    case c: JCollection[_] =>
      c.asScala.map(fromAvro(_, schema.getElementType)).toSeq.asJava
    case arr: Array[_] if arr.getClass.getComponentType.isPrimitive =>
      arr.toSeq.asJava.asInstanceOf[JCollection[Any]]
    case arr: Array[_] =>
      arr.map(fromAvro(_, schema.getElementType)).toSeq.asJava
    case other => throw new SparkException(
      s"Unknown ARRAY type ${other.getClass.getName}")
  }

  def unpackUnion(obj: Any, schema: Schema): Any = {
    schema.getTypes.asScala.toList match {
      case List(s) => fromAvro(obj, s)
      case List(n, s) if n.getType == NULL => fromAvro(obj, s)
      case List(s, n) if n.getType == NULL => fromAvro(obj, s)
      case _ => throw new SparkException(
        "Unions may only consist of a concrete type and null")
    }
  }

  def toAvro(value: Any, schema: Schema): Any = {
    if (value == null) {
      return null
    }

    schema.getType match {
      case UNION => packUnion(value, schema)
      case ARRAY => remapArray(value, schema)
      //case FIXED => unpackFixed(value, schema)
      case MAP => packMap(value, schema)
      //case BYTES => unpackBytes(value)
      case RECORD => packRecord(value, schema)
      case STRING => value.toString
      case ENUM => value.toString
      case NULL => value
      case BOOLEAN => value
      case DOUBLE => value
      case FLOAT => value
      case INT => value
      case LONG => value
      case other => throw new SparkException(
        s"Unknown Avro schema type ${other.getName}")
    }
  }

  def packUnion(value: Any, schema: Schema): Any = {
    schema.getTypes.toList match {
      case List(s) => toAvro(value, s)
      case List(n, s) if n.getType == NULL => toAvro(value, s)
      case List(s, n) if n.getType == NULL => toAvro(value, s)
      case _ => throw new SparkException(
        "Unions may only consist of a concrete type and null")
    }
  }

  def packMap(value: Any, schema: Schema): Any = {
    val result = new JHashMap[String, Any]
    val map = value.asInstanceOf[JHashMap[String, Any]]
    map.keySet().foreach(k => {
      try {
        val v = map.get(k)
        result.put(k, toAvro(v, schema.getValueType))
      } catch {
        case e: Exception => throw new SparkException(
          s"Unknown Avro map key ${k} in ${map} with schema ${schema}", e)
      }
    })
    result
  }

  def packRecord(value: Any, schema: Schema): Record = {
    val map = value.asInstanceOf[JHashMap[String, Any]]
    val record = new Record(schema)

    map.keySet().foreach(k => {
      try {
        val f = schema.getField(k)
        val v = map.get(k)
        record.put(f.pos(), toAvro(v, f.schema()))
      } catch {
        case e: Exception => throw new SparkException(
          s"Unknown Avro record key ${k} in ${map} with schema ${schema}", e)
      }
    })
    record
  }

  def remapArray(value: Any, schema: Schema): GenericData.Array[Any] = {
    val array = value.asInstanceOf[JArrayList[Any]]
    new GenericData.Array[Any](schema, array.map(e => toAvro(e, schema.getElementType)))
  }
}

/**
 * Implementation of [[org.apache.spark.api.python.Converter]] that converts
 * an Avro IndexedRecord (e.g., derived from AvroParquetInputFormat) to a Java Map.
 */
class IndexedRecordToJavaConverter extends Converter[IndexedRecord, JMap[String, Any]]{
  override def convert(record: IndexedRecord): JMap[String, Any] = {
    if (record == null) {
      return null
    }
    val map = new java.util.HashMap[String, Any]
    AvroConversionUtil.unpackRecord(record)
  }
}

/**
 * Implementation of [[org.apache.spark.api.python.Converter]] that converts
 * an Avro Record wrapped in an AvroKey (or AvroValue) to a Java Map. It tries
 * to work with all 3 Avro data mappings (Generic, Specific and Reflect).
 */
class AvroWrapperToJavaConverter extends Converter[Any, Any] {
  override def convert(obj: Any): Any = {
    if (obj == null) {
      return null
    }
    obj.asInstanceOf[AvroWrapper[_]].datum() match {
      case null => null
      case record: IndexedRecord => AvroConversionUtil.unpackRecord(record)
      case other => throw new SparkException(
        s"Unsupported top-level Avro data type ${other.getClass.getName}")
    }
  }
}

class JavaToAvroWrapperConverter() extends Converter[Any, AvroKey[_]] {
  override def convert(obj: Any): AvroKey[_] = {
    if (obj == null) {
      return null
    }
    
    val args: Array[Any] = obj.asInstanceOf[Array[Any]]
    val schema: Schema = (new Parser).parse(args(1).asInstanceOf[String])
    val value: Any = args(0)

    new AvroKey[Any](AvroConversionUtil.toAvro(value, schema))
  }
}

