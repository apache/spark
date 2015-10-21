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

import scala.collection.JavaConverters._
import scala.util.parsing.json.JSONObject

import org.apache.spark.api.python.Converter
import org.apache.hadoop.hbase.client.{Put, Result}
import org.apache.hadoop.hbase.io.ImmutableBytesWritable
import org.apache.hadoop.hbase.util.Bytes
import org.apache.hadoop.hbase.KeyValue.Type
import org.apache.hadoop.hbase.CellUtil

/**
 * Implementation of [[org.apache.spark.api.python.Converter]] that converts all
 * the records in an HBase Result to a String
 */
class HBaseResultToStringConverter extends Converter[Any, String] {
  override def convert(obj: Any): String = {
    val result = obj.asInstanceOf[Result]
    val output = result.listCells.asScala.map(cell =>
        Map(
          "row" -> Bytes.toStringBinary(CellUtil.cloneRow(cell)),
          "columnFamily" -> Bytes.toStringBinary(CellUtil.cloneFamily(cell)),
          "qualifier" -> Bytes.toStringBinary(CellUtil.cloneQualifier(cell)),
          "timestamp" -> cell.getTimestamp.toString,
          "type" -> Type.codeToType(cell.getTypeByte).toString,
          "value" -> Bytes.toStringBinary(CellUtil.cloneValue(cell))
        )
    )
    output.map(JSONObject(_).toString()).mkString("\n")
  }
}

/**
 * Implementation of [[org.apache.spark.api.python.Converter]] that converts an
 * ImmutableBytesWritable to a String
 */
class ImmutableBytesWritableToStringConverter extends Converter[Any, String] {
  override def convert(obj: Any): String = {
    val key = obj.asInstanceOf[ImmutableBytesWritable]
    Bytes.toStringBinary(key.get())
  }
}

/**
 * Implementation of [[org.apache.spark.api.python.Converter]] that converts a
 * String to an ImmutableBytesWritable
 */
class StringToImmutableBytesWritableConverter extends Converter[Any, ImmutableBytesWritable] {
  override def convert(obj: Any): ImmutableBytesWritable = {
    val bytes = Bytes.toBytes(obj.asInstanceOf[String])
    new ImmutableBytesWritable(bytes)
  }
}

/**
 * Implementation of [[org.apache.spark.api.python.Converter]] that converts a
 * list of Strings to HBase Put
 */
class StringListToPutConverter extends Converter[Any, Put] {
  override def convert(obj: Any): Put = {
    val output = obj.asInstanceOf[java.util.ArrayList[String]].asScala.map(Bytes.toBytes).toArray
    val put = new Put(output(0))
    put.add(output(1), output(2), output(3))
  }
}
