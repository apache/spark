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

import java.nio.ByteBuffer

import scala.collection.JavaConversions._

import org.apache.avro.generic.{GenericArray, GenericFixed, GenericRecord}
import org.apache.avro.mapred.AvroWrapper
import org.apache.avro.Schema
import org.apache.avro.Schema.Type._
import org.apache.avro.util.Utf8

import org.apache.spark.api.python.Converter
import org.apache.spark.SparkException


/**
 * Implementation of [[org.apache.spark.api.python.Converter]] that converts an
 * Avro Wrapper to Java object. It only works with Avro's Generic Data Model.
 */
class AvroWrapperToJavaConverter extends Converter[Any, Any] {
  override def convert(obj: Any): Any = obj.asInstanceOf[AvroWrapper[_]].datum() match {
    case record: GenericRecord => unpackRecord(record, None)
    case other => throw new SparkException(
      s"Unsupported top-level Avro data type ${other.getClass.getName}")
  }

  def unpackRecord(obj: Any, readerSchema: Option[Schema]): java.util.Map[String, Any] = {
    val record = obj.asInstanceOf[GenericRecord]
    val map = new java.util.HashMap[String, Any]
    readerSchema.getOrElse(record.getSchema).getFields.foreach { case f =>
        map.put(f.name, fromAvro(record.get(f.name), f.schema))
    }
    map
  }

  def unpackMap(obj: Any, schema: Schema): java.util.Map[String, Any] = {
    val map = new java.util.HashMap[String, Any]
    obj.asInstanceOf[Map[Utf8, _]].foreach { case (key, value) =>
      map.put(key.toString, fromAvro(value, schema.getValueType))
    }
    map
  }

  def unpackFixed(obj: Any, schema: Schema): Array[Byte] = {
    unpackBytes(obj.asInstanceOf[GenericFixed].bytes())
  }

  def unpackBytes(bytes: Array[Byte]): Array[Byte] = {
    val bytearray = new Array[Byte](bytes.length)
    System.arraycopy(bytes, 0, bytearray, 0, bytes.length)
    bytearray
  }

  def unpackArray(obj: Any, schema: Schema): java.util.List[Any] = {
    val list = new java.util.ArrayList[Any]
    obj.asInstanceOf[GenericArray[_]].foreach { element =>
      list.add(fromAvro(element, schema.getElementType))
    }
    list
  }

  def unpackUnion(obj: Any, schema: Schema): Any = {
    schema.getTypes.toList match {
      case List(s) => fromAvro(obj, s)
      case List(n, s) if n.getType == NULL => fromAvro(obj, s)
      case List(s, n) if n.getType == NULL => fromAvro(obj, s)
      case _ => throw new SparkException(
        "Unions may only consist of a concrete type and null")
    }
  }

  def fromAvro(obj: Any, schema: Schema): Any = {
    if (obj == null) {
      return null
    }
    schema.getType match {
      case UNION   => unpackUnion(obj, schema)
      case ARRAY   => unpackArray(obj, schema)
      case FIXED   => unpackFixed(obj, schema)
      case BYTES   => unpackBytes(obj.asInstanceOf[ByteBuffer].array())
      case RECORD  => unpackRecord(obj, Option(schema))
      case MAP     => unpackMap(obj, schema)
      case STRING  => obj.toString
      case ENUM    => obj.toString
      case NULL    => obj
      case BOOLEAN => obj
      case DOUBLE  => obj
      case FLOAT   => obj
      case INT     => obj
      case LONG    => obj
      case other   => throw new SparkException(
        s"Unsupported Avro schema type ${other.getName}")
    }
  }
}
