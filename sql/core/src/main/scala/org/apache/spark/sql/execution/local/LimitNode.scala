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


case class LimitNode(limit: Int, child: LocalNode) extends UnaryLocalNode {

  override def output: Seq[Attribute] = child.output

  override def execute(): OpenCloseRowIterator = new OpenCloseRowIterator {

    private var count = 0

    private val childIter = child.execute()

    override def open(): Unit = childIter.open()

    override def close(): Unit = childIter.close()

    override def getRow: InternalRow = childIter.getRow

    override def advanceNext(): Boolean = {
      if (count < limit) {
        count += 1
        childIter.advanceNext()
      } else {
        false
      }
    }

  }

}
