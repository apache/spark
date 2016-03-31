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

import org.apache.spark.sql.SQLConf
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.Attribute
import org.apache.spark.sql.catalyst.plans.logical.LocalRelation

/**
 * A dummy [[LocalNode]] that just returns rows from a [[LocalRelation]].
 */
private[local] case class DummyNode(
    output: Seq[Attribute],
    relation: LocalRelation,
    conf: SQLConf)
  extends LocalNode(conf) {

  import DummyNode._

  private var index: Int = CLOSED
  private val input: Seq[InternalRow] = relation.data

  def this(output: Seq[Attribute], data: Seq[Product], conf: SQLConf = new SQLConf) {
    this(output, LocalRelation.fromProduct(output, data), conf)
  }

  def isOpen: Boolean = index != CLOSED

  override def children: Seq[LocalNode] = Seq.empty

  override def open(): Unit = {
    index = -1
  }

  override def next(): Boolean = {
    index += 1
    index < input.size
  }

  override def fetch(): InternalRow = {
    assert(index >= 0 && index < input.size)
    input(index)
  }

  override def close(): Unit = {
    index = CLOSED
  }
}

private object DummyNode {
  val CLOSED: Int = Int.MinValue
}
