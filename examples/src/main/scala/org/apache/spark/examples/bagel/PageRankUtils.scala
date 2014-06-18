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

package org.apache.spark.examples.bagel

import org.apache.spark._
import org.apache.spark.SparkContext._
import org.apache.spark.serializer.KryoRegistrator

import org.apache.spark.bagel._
import org.apache.spark.bagel.Bagel._

import scala.collection.mutable.ArrayBuffer

import java.io.{InputStream, OutputStream, DataInputStream, DataOutputStream}

import com.esotericsoftware.kryo._

class PageRankUtils extends Serializable {
  def computeWithCombiner(numVertices: Long, epsilon: Double)(
    self: PRVertex, messageSum: Option[Double], superstep: Int
  ): (PRVertex, Array[PRMessage]) = {
    val newValue = messageSum match {
      case Some(msgSum) if msgSum != 0 =>
        0.15 / numVertices + 0.85 * msgSum
      case _ => self.value
    }

    val terminate = superstep >= 10

    val outbox: Array[PRMessage] =
      if (!terminate) {
        self.outEdges.map(targetId => new PRMessage(targetId, newValue / self.outEdges.size))
      } else {
        Array[PRMessage]()
      }

    (new PRVertex(newValue, self.outEdges, !terminate), outbox)
  }

  def computeNoCombiner(numVertices: Long, epsilon: Double)
    (self: PRVertex, messages: Option[Array[PRMessage]], superstep: Int)
  : (PRVertex, Array[PRMessage]) =
    computeWithCombiner(numVertices, epsilon)(self, messages match {
      case Some(msgs) => Some(msgs.map(_.value).sum)
      case None => None
    }, superstep)
}

class PRCombiner extends Combiner[PRMessage, Double] with Serializable {
  def createCombiner(msg: PRMessage): Double =
    msg.value
  def mergeMsg(combiner: Double, msg: PRMessage): Double =
    combiner + msg.value
  def mergeCombiners(a: Double, b: Double): Double =
    a + b
}

class PRVertex() extends Vertex with Serializable {
  var value: Double = _
  var outEdges: Array[String] = _
  var active: Boolean = _

  def this(value: Double, outEdges: Array[String], active: Boolean = true) {
    this()
    this.value = value
    this.outEdges = outEdges
    this.active = active
  }

  override def toString(): String = {
    "PRVertex(value=%f, outEdges.length=%d, active=%s)"
      .format(value, outEdges.length, active.toString)
  }
}

class PRMessage() extends Message[String] with Serializable {
  var targetId: String = _
  var value: Double = _

  def this(targetId: String, value: Double) {
    this()
    this.targetId = targetId
    this.value = value
  }
}

class PRKryoRegistrator extends KryoRegistrator {
  def registerClasses(kryo: Kryo) {
    kryo.register(classOf[PRVertex])
    kryo.register(classOf[PRMessage])
  }
}

class CustomPartitioner(partitions: Int) extends Partitioner {
  def numPartitions = partitions

  def getPartition(key: Any): Int = {
    val hash = key match {
      case k: Long => (k & 0x00000000FFFFFFFFL).toInt
      case _ => key.hashCode
    }

    val mod = key.hashCode % partitions
    if (mod < 0) mod + partitions else mod
  }

  override def equals(other: Any): Boolean = other match {
    case c: CustomPartitioner =>
      c.numPartitions == numPartitions
    case _ => false
  }

  override def hashCode: Int = numPartitions
}
