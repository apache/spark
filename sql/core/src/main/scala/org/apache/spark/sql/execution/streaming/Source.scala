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

package org.apache.spark.sql.execution.streaming

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, SQLContext}
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.Attribute
import org.apache.spark.sql.catalyst.plans.logical.LeafNode
import org.apache.spark.sql.types.StructType

trait Source  {

  /** Returns the schema of the data from this source */
  def schema: StructType

  /** Returns the maximum offset that can be retrieved from the source. */
  def offset: Offset

  /**
   * Returns the data between the `start` and `end` offsets.  This function must always return
   * the same set of data for any given pair of offsets.
   */
  def getSlice(sqlContext: SQLContext, start: Option[Offset], end: Offset): RDD[InternalRow]

  /** For testing. */
  def restart(): Source
}

case class StreamingRelation(source: Source, output: Seq[Attribute]) extends LeafNode {
  override def toString: String = source.toString
}

object StreamingRelation {
  def apply(source: Source): StreamingRelation =
    StreamingRelation(source, source.schema.toAttributes)
}

