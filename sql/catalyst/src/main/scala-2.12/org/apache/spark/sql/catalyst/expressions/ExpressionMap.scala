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

import scala.collection.mutable

object ExpressionMap {
  /** Constructs a new [[ExpressionMap]] by applying [[Canonicalize]] to `expressions`. */
  def apply[T](map: mutable.Map[Expression, T]): ExpressionMap[T] = {
    val newMap = new ExpressionMap[T](map)
    map.foreach(newMap.add)
    newMap
  }
}

/**
 * A helper class created on the lines of [[AttributeMap]] and [[ExpressionSet]]
 * The key added in the Map is always stored in canonicalized form.
 *
 * @param baseMap The underlying Map object which is either empty or pre-populated with all the
 *                keys being canonicalized form of expressions
 * @tparam T The value part of the Map
 */
class ExpressionMap[T](val baseMap: mutable.Map[Expression, T] = new mutable.HashMap[Expression, T])
  extends mutable.Map[Expression, T] {

  override def get(expr: Expression): Option[T] =
    baseMap.get(expr.canonicalized)


  protected def add(tup: (Expression, T)): Unit = {

    this.baseMap += (tup._1.canonicalized -> tup._2)

  }

  override def contains(expr: Expression): Boolean =
    baseMap.contains(expr.canonicalized)

  override def +=(kv: (Expression, T)): this.type = {
    this.add(kv)
    this
  }

  override def iterator: Iterator[(Expression, T)] = baseMap.iterator

  override def -=(key: Expression): this.type = {
    this.baseMap -= key.canonicalized
    this
  }
}
