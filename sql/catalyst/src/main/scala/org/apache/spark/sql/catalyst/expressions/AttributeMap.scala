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

package org.apache.spark.sql.catalyst.expressions

/**
 * Builds a map that is keyed by an Attribute's expression id. Using the expression id allows values
 * to be looked up even when the attributes used differ cosmetically (i.e., the capitalization
 * of the name, or the expected nullability).
 */
object AttributeMap {
  def apply[A](kvs: Map[Attribute, A]): AttributeMap[A] = {
    new AttributeMap(kvs.map(kv => (kv._1.exprId, kv)))
  }

  def apply[A](kvs: Seq[(Attribute, A)]): AttributeMap[A] = {
    new AttributeMap(kvs.map(kv => (kv._1.exprId, kv)).toMap)
  }

  def apply[A](kvs: Iterable[(Attribute, A)]): AttributeMap[A] = {
    new AttributeMap(kvs.map(kv => (kv._1.exprId, kv)).toMap)
  }

  def empty[A]: AttributeMap[A] = new AttributeMap(Map.empty)
}

class AttributeMap[A](val baseMap: Map[ExprId, (Attribute, A)])
  extends Map[Attribute, A] with Serializable {

  override def get(k: Attribute): Option[A] = baseMap.get(k.exprId).map(_._2)

  override def getOrElse[B1 >: A](k: Attribute, default: => B1): B1 = get(k).getOrElse(default)

  override def contains(k: Attribute): Boolean = get(k).isDefined

  override def + [B1 >: A](kv: (Attribute, B1)): AttributeMap[B1] =
    AttributeMap(baseMap.values.toMap + kv)

  override def updated[B1 >: A](key: Attribute, value: B1): Map[Attribute, B1] =
    baseMap.values.toMap + (key -> value)

  override def iterator: Iterator[(Attribute, A)] = baseMap.valuesIterator

  override def removed(key: Attribute): Map[Attribute, A] = baseMap.values.toMap - key

  def ++(other: AttributeMap[A]): AttributeMap[A] = new AttributeMap(baseMap ++ other.baseMap)
}
