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
import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.util.BoundedPriorityQueue

case class TakeOrderedAndProjectNode(
    conf: SQLConf,
    limit: Int,
    sortOrder: Seq[SortOrder],
    projectList: Option[Seq[NamedExpression]],
    child: LocalNode) extends UnaryLocalNode(conf) {

  private[this] var projection: Option[Projection] = _
  private[this] var ord: InterpretedOrdering = _
  private[this] var iterator: Iterator[InternalRow] = _
  private[this] var currentRow: InternalRow = _

  override def output: Seq[Attribute] = {
    val projectOutput = projectList.map(_.map(_.toAttribute))
    projectOutput.getOrElse(child.output)
  }

  override def open(): Unit = {
    child.open()
    projection = projectList.map(new InterpretedProjection(_, child.output))
    ord = new InterpretedOrdering(sortOrder, child.output)
    // Priority keeps the largest elements, so let's reverse the ordering.
    val queue = new BoundedPriorityQueue[InternalRow](limit)(ord.reverse)
    while (child.next()) {
      queue += child.fetch()
    }
    // Close it eagerly since we don't need it.
    child.close()
    iterator = queue.toArray.sorted(ord).iterator
  }

  override def next(): Boolean = {
    if (iterator.hasNext) {
      val _currentRow = iterator.next()
      currentRow = projection match {
        case Some(p) => p(_currentRow)
        case None => _currentRow
      }
      true
    } else {
      false
    }
  }

  override def fetch(): InternalRow = currentRow

  override def close(): Unit = child.close()

}
