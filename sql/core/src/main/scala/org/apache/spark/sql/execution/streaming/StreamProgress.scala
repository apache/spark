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

import scala.collection.mutable

class StreamProgress extends Serializable {
  private val currentWatermarks = new mutable.HashMap[Source, Watermark]

  def isEmpty: Boolean = currentWatermarks.filterNot(_._2.isEmpty).isEmpty

  def update(source: Source, newWatermark: Watermark): Unit = {
    currentWatermarks.get(source).foreach(old => assert(newWatermark > old))
    currentWatermarks.put(source, newWatermark)
  }

  def update(newWatermark: (Source, Watermark)): Unit =
    update(newWatermark._1, newWatermark._2)

  def apply(source: Source): Watermark = currentWatermarks(source)
  def get(source: Source): Option[Watermark] = currentWatermarks.get(source)

  def ++(updates: Map[Source, Watermark]): StreamProgress = {
    val updated = new StreamProgress
    currentWatermarks.foreach(updated.update)
    updates.foreach(updated.update)
    updated
  }

  def copy(): StreamProgress = {
    val copied = new StreamProgress
    currentWatermarks.foreach(copied.update)
    copied
  }

  override def toString: String =
    currentWatermarks.map { case (k, v) => s"$k: $v"}.mkString("{", ",", "}")

  override def equals(other: Any): Boolean = other match {
    case s: StreamProgress =>
      s.currentWatermarks.keys.toSet == currentWatermarks.keys.toSet &&
      s.currentWatermarks.forall(w => currentWatermarks(w._1) == w._2)
  }

  override def hashCode: Int = {
    currentWatermarks.toSeq.sortBy(_._1.toString).hashCode()
  }
}