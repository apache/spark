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

package org.apache.spark.sql.catalyst.util

class ArrayBasedMapData(val keyArray: ArrayData, val valueArray: ArrayData) extends MapData {
  require(keyArray.numElements() == valueArray.numElements())

  override def numElements(): Int = keyArray.numElements()

  override def copy(): MapData = new ArrayBasedMapData(keyArray.copy(), valueArray.copy())

  override def toString: String = {
    s"keys: $keyArray, values: $valueArray"
  }
}

object ArrayBasedMapData {
  def apply(map: Map[Any, Any]): ArrayBasedMapData = {
    val array = map.toArray
    ArrayBasedMapData(array.map(_._1), array.map(_._2))
  }

  def apply(keys: Array[Any], values: Array[Any]): ArrayBasedMapData = {
    new ArrayBasedMapData(new GenericArrayData(keys), new GenericArrayData(values))
  }

  def toScalaMap(map: ArrayBasedMapData): Map[Any, Any] = {
    val keys = map.keyArray.asInstanceOf[GenericArrayData].array
    val values = map.valueArray.asInstanceOf[GenericArrayData].array
    keys.zip(values).toMap
  }

  def toScalaMap(keys: Array[Any], values: Array[Any]): Map[Any, Any] = {
    keys.zip(values).toMap
  }

  def toScalaMap(keys: Seq[Any], values: Seq[Any]): Map[Any, Any] = {
    keys.zip(values).toMap
  }

  def toJavaMap(keys: Array[Any], values: Array[Any]): java.util.Map[Any, Any] = {
    import scala.collection.JavaConverters._
    keys.zip(values).toMap.asJava
  }
}
