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

import scala.language.dynamics

import org.apache.spark.sql.types.DataType

/**
 * The data type representing [[DynamicRow]] values.
 */
case object DynamicType extends DataType

/**
 * Wrap a [[Row]] as a [[DynamicRow]].
 */
case class WrapDynamic(children: Seq[Attribute]) extends Expression {
  type EvaluatedType = DynamicRow

  def nullable = false

  def dataType = DynamicType

  override def eval(input: Row): DynamicRow = input match {
    // Avoid copy for generic rows.
    case g: GenericRow => new DynamicRow(children, g.values)
    case otherRowType => new DynamicRow(children, otherRowType.toArray)
  }
}

/**
 * DynamicRows use scala's Dynamic trait to emulate an ORM of in a dynamically typed language.
 * Since the type of the column is not known at compile time, all attributes are converted to
 * strings before being passed to the function.
 */
class DynamicRow(val schema: Seq[Attribute], values: Array[Any])
  extends GenericRow(values) with Dynamic {

  def selectDynamic(attributeName: String): String = {
    val ordinal = schema.indexWhere(_.name == attributeName)
    values(ordinal).toString
  }
}
