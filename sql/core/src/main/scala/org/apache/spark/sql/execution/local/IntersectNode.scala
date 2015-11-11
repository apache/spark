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

import scala.collection.mutable

import org.apache.spark.sql.SQLConf
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.Attribute

case class IntersectNode(conf: SQLConf, left: LocalNode, right: LocalNode)
  extends BinaryLocalNode(conf) {

  override def output: Seq[Attribute] = left.output

  private[this] var leftRows: mutable.HashSet[InternalRow] = _

  private[this] var currentRow: InternalRow = _

  override def open(): Unit = {
    left.open()
    leftRows = mutable.HashSet[InternalRow]()
    while (left.next()) {
      leftRows += left.fetch().copy()
    }
    left.close()
    right.open()
  }

  override def next(): Boolean = {
    currentRow = null
    while (currentRow == null && right.next()) {
      currentRow = right.fetch()
      if (!leftRows.contains(currentRow)) {
        currentRow = null
      }
    }
    currentRow != null
  }

  override def fetch(): InternalRow = currentRow

  override def close(): Unit = {
    left.close()
    right.close()
  }

}
