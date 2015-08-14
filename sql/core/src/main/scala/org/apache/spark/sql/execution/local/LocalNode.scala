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

package org.apache.spark.sql.execution.local

import org.apache.spark.sql.Row
import org.apache.spark.sql.catalyst.{CatalystTypeConverters, InternalRow}
import org.apache.spark.sql.catalyst.expressions.Attribute
import org.apache.spark.sql.catalyst.trees.TreeNode
import org.apache.spark.sql.types.StructType

/**
 * A local physical operator, in the form of an iterator.
 *
 * Before consuming the iterator, open function must be called.
 * After consuming the iterator, close function must be called.
 */
abstract class LocalNode extends TreeNode[LocalNode] {

  def output: Seq[Attribute]

  /**
   * Initializes the iterator state. Must be called before calling `next()`.
   *
   * Implementations of this must also call the `open()` function of its children.
   */
  def open(): Unit

  /**
   * Advances the iterator to the next tuple. Returns true if there is at least one more tuple.
   */
  def next(): Boolean

  /**
   * Returns the current tuple.
   */
  def get(): InternalRow

  /**
   * Closes the iterator and releases all resources.
   *
   * Implementations of this must also call the `close()` function of its children.
   */
  def close(): Unit

  /**
   * Returns the content of the iterator from the beginning to the end in the form of a Scala Seq.
   */
  def collect(): Seq[Row] = {
    val converter = CatalystTypeConverters.createToScalaConverter(StructType.fromAttributes(output))
    val result = new scala.collection.mutable.ArrayBuffer[Row]
    open()
    while (next()) {
      result += converter.apply(get()).asInstanceOf[Row]
    }
    close()
    result
  }
}


abstract class LeafLocalNode extends LocalNode {
  override def children: Seq[LocalNode] = Seq.empty
}


abstract class UnaryLocalNode extends LocalNode {

  def child: LocalNode

  override def children: Seq[LocalNode] = Seq(child)
}
