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

package org.apache.spark.sql.catalyst.expressions.aggregate

import org.apache.datasketches.tuple.{Intersection, Sketch, Summary, Union, UpdatableSketch, UpdatableSummary}

/**
 * Sealed trait representing the internal state of tuple sketch aggregation operations.
 * All implementations must be defined in this file to maintain exhaustiveness checking.
 */
sealed trait TupleSketchState[S <: Summary] {
  def serialize(): Array[Byte]
  def eval(): Array[Byte]
}

case class UpdatableTupleSketchBuffer[U, S <: UpdatableSummary[U]](sketch: UpdatableSketch[U, S])
    extends TupleSketchState[S] {
  override def serialize(): Array[Byte] = sketch.compact.toByteArray
  override def eval(): Array[Byte] = sketch.compact.toByteArray

  /** Returns compact form of the sketch, needed for merge operations that require Sketch type. */
  def compactSketch: Sketch[S] = sketch.compact
}

case class UnionTupleAggregationBuffer[S <: Summary](union: Union[S])
    extends TupleSketchState[S] {
  override def serialize(): Array[Byte] = union.getResult.toByteArray
  override def eval(): Array[Byte] = union.getResult.toByteArray
}

case class IntersectionTupleAggregationBuffer[S <: Summary](intersection: Intersection[S])
    extends TupleSketchState[S] {
  override def serialize(): Array[Byte] = intersection.getResult.toByteArray
  override def eval(): Array[Byte] = intersection.getResult.toByteArray
}

case class FinalizedTupleSketch[S <: Summary](sketch: Sketch[S]) extends TupleSketchState[S] {
  override def serialize(): Array[Byte] = sketch.toByteArray
  override def eval(): Array[Byte] = sketch.toByteArray
}
