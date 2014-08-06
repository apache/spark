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

import org.apache.spark.api.python.Converter
import java.nio.ByteBuffer
import org.apache.cassandra.utils.ByteBufferUtil
import collection.JavaConversions._


/**
 * Implementation of [[org.apache.spark.api.python.Converter]] that converts Cassandra
 * output to a Map[String, Int]
 */
class CassandraCQLKeyConverter extends Converter[Any, java.util.Map[String, Int]] {
  override def convert(obj: Any): java.util.Map[String, Int] = {
    val result = obj.asInstanceOf[java.util.Map[String, ByteBuffer]]
    mapAsJavaMap(result.mapValues(bb => ByteBufferUtil.toInt(bb)))
  }
}

/**
 * Implementation of [[org.apache.spark.api.python.Converter]] that converts Cassandra
 * output to a Map[String, String]
 */
class CassandraCQLValueConverter extends Converter[Any, java.util.Map[String, String]] {
  override def convert(obj: Any): java.util.Map[String, String] = {
    val result = obj.asInstanceOf[java.util.Map[String, ByteBuffer]]
    mapAsJavaMap(result.mapValues(bb => ByteBufferUtil.string(bb)))
  }
}

/**
 * Implementation of [[org.apache.spark.api.python.Converter]] that converts a
 * Map[String, Int] to Cassandra key
 */
class ToCassandraCQLKeyConverter extends Converter[Any, java.util.Map[String, ByteBuffer]] {
  override def convert(obj: Any): java.util.Map[String, ByteBuffer] = {
    val input = obj.asInstanceOf[java.util.Map[String, Int]]
    mapAsJavaMap(input.mapValues(i => ByteBufferUtil.bytes(i)))
  }
}

/**
 * Implementation of [[org.apache.spark.api.python.Converter]] that converts a
 * List[String] to Cassandra value
 */
class ToCassandraCQLValueConverter extends Converter[Any, java.util.List[ByteBuffer]] {
  override def convert(obj: Any): java.util.List[ByteBuffer] = {
    val input = obj.asInstanceOf[java.util.List[String]]
    seqAsJavaList(input.map(s => ByteBufferUtil.bytes(s)))
  }
}
