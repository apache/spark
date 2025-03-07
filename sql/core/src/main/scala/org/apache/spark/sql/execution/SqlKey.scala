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

package org.apache.spark.sql.execution

import org.apache.spark.Partitioner
import org.apache.spark.sql.catalyst.expressions.{BaseOrdering, UnsafeRow}
import org.apache.spark.util.Utils

trait SqlKey extends Serializable

object RowKey {
  def apply(row: UnsafeRow): RowKey = new RowKey(row)
}

object IntKey {
  def apply(value: Int): IntKey = new IntKey(value)
}

object SqlKeyPartitioner {
  def apply(numPartitions: Int, real: Option[Partitioner] = None): SqlKeyPartitioner =
    new SqlKeyPartitioner(numPartitions, real)
}

case class RowKey(var row: UnsafeRow) extends SqlKey {
  def this() = this(null)
}
case class IntKey(value: Int) extends SqlKey {
  def this() = this(-1)
}

class SqlKeyPartitioner(partitions: Int, real: Option[Partitioner] = None) extends Partitioner {

  override def numPartitions: Int = real.map(par => par.numPartitions).getOrElse(partitions)

  override def getPartition(key: Any): Int = key match {
    case RowKey(row) =>
      real.map(par => par.getPartition(row)).
        getOrElse(Utils.nonNegativeMod(row.hashCode(), partitions))
    case IntKey(v) => Utils.nonNegativeMod(v, partitions)
  }
}

object SqlKeyOrdering {
  def apply(ordering: BaseOrdering): SqlKeyOrdering = new SqlKeyOrdering(ordering)
}

class SqlKeyOrdering(ordering: BaseOrdering) extends Ordering[SqlKey] {

  def this() = this(null)

  override def compare(x: SqlKey, y: SqlKey): Int = {
    (x, y) match {
      case (IntKey(a), IntKey(b)) => a - b
      case (RowKey(a), RowKey(b)) => ordering.compare(a, b)
    }
  }
}