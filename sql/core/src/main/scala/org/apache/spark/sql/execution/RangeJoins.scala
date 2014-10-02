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

import org.apache.spark.annotation.DeveloperApi
import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.SQLContext

@DeveloperApi
case class RangeJoin(left: SparkPlan,
                     right: SparkPlan,
                     condition: Seq[Expression],
                     context: SQLContext) extends BinaryNode with Serializable {
  def output = left.output ++ right.output

  lazy val (buildPlan, streamedPlan) = (left, right)

  lazy val (buildKeys, streamedKeys) = (List(condition(0), condition(1)),
    List(condition(2), condition(3)))

  @transient lazy val buildKeyGenerator = new InterpretedProjection(buildKeys, buildPlan.output)
  @transient lazy val streamKeyGenerator = new InterpretedProjection(streamedKeys,
    streamedPlan.output)

  def execute() = {
    val v1 = left.execute()
    val v1kv = v1.map(x => {
      val v1Key = buildKeyGenerator(x)
      (new Interval[Long](v1Key.apply(0).asInstanceOf[Long], v1Key.apply(1).asInstanceOf[Long]),
        x.copy())
    } )
    val v2 = right.execute()
    val v2kv = v2.map(x => {
      val v2Key = streamKeyGenerator(x)
      (new Interval[Long](v2Key.apply(0).asInstanceOf[Long], v2Key.apply(1).asInstanceOf[Long]),
        x.copy())
    } )
    /* As we are going to collect v1 and build an interval tree on its intervals,
    make sure that its size is the smaller one. */
    assert(v1.count <= v2.count)
    val v3 = RangeJoinImpl.overlapJoin(context.sparkContext, v1kv, v2kv)
      .flatMap(l => l._2.map(r => (l._1, r)))
    val v4 = v3.map {
      case (l: Row, r: Row) => new JoinedRow(l, r).withLeft(l)
    }
    v4
  }
}

case class Interval[T <% Long](start: T, end: T) {
  def overlaps(other: Interval[T]): Boolean = {
    (end >= start) && (other.end >= other.start) &&
      (end > other.start && start < other.end)
  }
}
