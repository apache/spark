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

import org.apache.spark.Logging
import org.apache.spark.sql.catalyst.errors.attachTree
import org.apache.spark.sql.catalyst.types._
import org.apache.spark.sql.catalyst.trees

/**
 * A bound reference points to a specific slot in the input tuple, allowing the actual value
 * to be retrieved more efficiently.  However, since operations like column pruning can change
 * the layout of intermediate tuples, BindReferences should be run after all such transformations.
 */
case class BoundReference(ordinal: Int, dataType: DataType, nullable: Boolean)
  extends Expression with trees.LeafNode[Expression] {

  type EvaluatedType = Any

  override def toString = s"input[$ordinal]"

  override def eval(input: Row): Any = input(ordinal)
}

object BindReferences extends Logging {
  def bindReference[A <: Expression](expression: A, input: Seq[Attribute]): A = {
    expression.transform { case a: AttributeReference =>
      attachTree(a, "Binding attribute") {
        val ordinal = input.indexWhere(_.exprId == a.exprId)
        if (ordinal == -1) {
          sys.error(s"Couldn't find $a in ${input.mkString("[", ",", "]")}")
        } else {
          BoundReference(ordinal, a.dataType, a.nullable)
        }
      }
    }.asInstanceOf[A] // Kind of a hack, but safe.  TODO: Tighten return type when possible.
  }
}
