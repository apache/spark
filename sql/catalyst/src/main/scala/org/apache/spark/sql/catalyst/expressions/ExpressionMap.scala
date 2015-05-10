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
 * Builds a map that is keyed by an normalized expression. Using the expression allows values
 * to be looked up even when the attributes used differ cosmetically (i.e., the capitalization
 * of the name, or the expected nullability).
 */
sealed class ExpressionMap[A] extends Serializable {
  private val baseMap = new collection.mutable.HashMap[Expression, A]()
  def get(k: Expression): Option[A] = baseMap.get(ExpressionEquals.normalize(k))

  def add(k: Expression, value: A): Unit = {
    baseMap.put(ExpressionEquals.normalize(k), value)
  }

  def values: Iterable[A] = baseMap.values
}
