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

package org.apache.spark.sql.catalyst.analysis

import org.apache.spark.sql.catalyst.expressions.Expression
import scala.collection.mutable

/** A catalog for looking up user defined functions, used by an [[Analyzer]]. */
trait FunctionRegistry {
  type FunctionBuilder = Seq[Expression] => Expression

  def registerFunction(name: String, builder: FunctionBuilder): Unit

  def lookupFunction(name: String, children: Seq[Expression]): Expression

  def caseSensitive: Boolean
}

trait OverrideFunctionRegistry extends FunctionRegistry {

  val functionBuilders = StringKeyHashMap[FunctionBuilder](caseSensitive)

  override def registerFunction(name: String, builder: FunctionBuilder): Unit = {
    functionBuilders.put(name, builder)
  }

  abstract override def lookupFunction(name: String, children: Seq[Expression]): Expression = {
    functionBuilders.get(name).map(_(children)).getOrElse(super.lookupFunction(name, children))
  }
}

class SimpleFunctionRegistry(val caseSensitive: Boolean) extends FunctionRegistry {
  val functionBuilders = StringKeyHashMap[FunctionBuilder](caseSensitive)

  override def registerFunction(name: String, builder: FunctionBuilder): Unit = {
    functionBuilders.put(name, builder)
  }

  override def lookupFunction(name: String, children: Seq[Expression]): Expression = {
    functionBuilders(name)(children)
  }
}

/**
 * A trivial catalog that returns an error when a function is requested. Used for testing when all
 * functions are already filled in and the analyzer needs only to resolve attribute references.
 */
object EmptyFunctionRegistry extends FunctionRegistry {
  override def registerFunction(name: String, builder: FunctionBuilder): Unit = {
    throw new UnsupportedOperationException
  }

  override def lookupFunction(name: String, children: Seq[Expression]): Expression = {
    throw new UnsupportedOperationException
  }

  override def caseSensitive: Boolean = throw new UnsupportedOperationException
}

/**
 * Build a map with String type of key, and it also supports either key case
 * sensitive or insensitive.
 * TODO move this into util folder?
 */
object StringKeyHashMap {
  def apply[T](caseSensitive: Boolean): StringKeyHashMap[T] = caseSensitive match {
    case false => new StringKeyHashMap[T](_.toLowerCase)
    case true => new StringKeyHashMap[T](identity)
  }
}

class StringKeyHashMap[T](normalizer: (String) => String) {
  private val base = new collection.mutable.HashMap[String, T]()

  def apply(key: String): T = base(normalizer(key))

  def get(key: String): Option[T] = base.get(normalizer(key))
  def put(key: String, value: T): Option[T] = base.put(normalizer(key), value)
  def remove(key: String): Option[T] = base.remove(normalizer(key))
  def iterator: Iterator[(String, T)] = base.toIterator
}

