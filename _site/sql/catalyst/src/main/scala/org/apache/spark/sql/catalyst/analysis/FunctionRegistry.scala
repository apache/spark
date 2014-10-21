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
}

trait OverrideFunctionRegistry extends FunctionRegistry {

  val functionBuilders = new mutable.HashMap[String, FunctionBuilder]()

  def registerFunction(name: String, builder: FunctionBuilder) = {
    functionBuilders.put(name, builder)
  }

  abstract override def lookupFunction(name: String, children: Seq[Expression]): Expression = {
    functionBuilders.get(name).map(_(children)).getOrElse(super.lookupFunction(name,children))
  }
}

class SimpleFunctionRegistry extends FunctionRegistry {
  val functionBuilders = new mutable.HashMap[String, FunctionBuilder]()

  def registerFunction(name: String, builder: FunctionBuilder) = {
    functionBuilders.put(name, builder)
  }

  override def lookupFunction(name: String, children: Seq[Expression]): Expression = {
    functionBuilders(name)(children)
  }
}

/**
 * A trivial catalog that returns an error when a function is requested.  Used for testing when all
 * functions are already filled in and the analyser needs only to resolve attribute references.
 */
object EmptyFunctionRegistry extends FunctionRegistry {
  def registerFunction(name: String, builder: FunctionBuilder) = ???

  def lookupFunction(name: String, children: Seq[Expression]): Expression = {
    throw new UnsupportedOperationException
  }
}
