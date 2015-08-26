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

import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.Attribute

case class UnionNode(children: Seq[LocalNode]) extends LocalNode {

  override def output: Seq[Attribute] = children.head.output

  override def execute(): OpenCloseRowIterator = new OpenCloseRowIterator {

    private var currentIter: OpenCloseRowIterator = _

    private var nextChildIndex: Int = _

    override def open(): Unit = {
      currentIter = children.head.execute()
      currentIter.open()
      nextChildIndex = 1
    }

    private def advanceToNextChild(): Boolean = {
      var found = false
      var exit = false
      while (!exit && !found) {
        if (currentIter != null) {
          currentIter.close()
        }
        if (nextChildIndex >= children.size) {
          found = false
          exit = true
        } else {
          currentIter = children(nextChildIndex).execute()
          nextChildIndex += 1
          currentIter.open()
          found = currentIter.advanceNext()
        }
      }
      found
    }

    override def close(): Unit = {
      if (currentIter != null) {
        currentIter.close()
      }
    }

    override def getRow: InternalRow = currentIter.getRow

    override def advanceNext(): Boolean = {
      if (currentIter.advanceNext()) {
        true
      } else {
        advanceToNextChild()
      }
    }
  }
}
