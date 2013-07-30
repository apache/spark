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

package spark.rdd

import scala.collection.immutable.NumericRange
import scala.collection.mutable.ArrayBuffer
import scala.collection.Map
import spark._
import java.io.{ObjectInput, ObjectOutput, Externalizable}
import java.nio.ByteBuffer

private[spark] class ParallelCollectionPartition[T: ClassManifest](
                                                                    var rddId: Long,
                                                                    var slice: Int,
                                                                    var values: Seq[T])
  extends Partition with Externalizable {

  def iterator: Iterator[T] = values.iterator

  override def hashCode(): Int = (41 * (41 + rddId) + slice).toInt

  override def equals(other: Any): Boolean = other match {
    case that: ParallelCollectionPartition[_] => (this.rddId == that.rddId && this.slice == that.slice)
    case _ => false
  }

  override def index: Int = slice

  override def writeExternal(out: ObjectOutput) {
    out.writeLong(rddId)
    out.writeInt(slice)
    out.writeInt(values.size)
    val ser = SparkEnv.get.serializer.newInstance()
    values.foreach(x => {
      val bb = ser.serialize(x)
      out.writeInt(bb.remaining())
      if (bb.hasArray) {
        out.write(bb.array(), bb.arrayOffset() + bb.position(), bb.remaining())
      } else {
        val b = new Array[Byte](bb.remaining())
        bb.get(b)
        out.write(b)
      }
    })
  }

  override def readExternal(in: ObjectInput) {
    rddId = in.readLong()
    slice = in.readInt()
    val s = in.readInt()
    val ser = SparkEnv.get.serializer.newInstance()
    var bb = ByteBuffer.allocate(1024)
    values = (0 until s).map({
      val s = in.readInt()
      if (bb.capacity() < s) {
        bb = ByteBuffer.allocate(s)
      } else {
        bb.clear()
      }
      in.readFully(bb.array())
      bb.limit(s)
      ser.deserialize(bb)
    }).toSeq
  }
}

private[spark] class ParallelCollectionRDD[T: ClassManifest](
                                                              @transient sc: SparkContext,
                                                              @transient data: Seq[T],
                                                              numSlices: Int,
                                                              locationPrefs: Map[Int, Seq[String]])
  extends RDD[T](sc, Nil) {
  // TODO: Right now, each split sends along its full data, even if later down the RDD chain it gets
  // cached. It might be worthwhile to write the data to a file in the DFS and read it in the split
  // instead.
  // UPDATE: A parallel collection can be checkpointed to HDFS, which achieves this goal.

  override def getPartitions: Array[Partition] = {
    val slices = ParallelCollectionRDD.slice(data, numSlices).toArray
    slices.indices.map(i => new ParallelCollectionPartition(id, i, slices(i))).toArray
  }

  override def compute(s: Partition, context: TaskContext) =
    s.asInstanceOf[ParallelCollectionPartition[T]].iterator

  override def getPreferredLocations(s: Partition): Seq[String] = {
    locationPrefs.getOrElse(s.index, Nil)
  }
}

private object ParallelCollectionRDD {
  /**
   * Slice a collection into numSlices sub-collections. One extra thing we do here is to treat Range
   * collections specially, encoding the slices as other Ranges to minimize memory cost. This makes
   * it efficient to run Spark over RDDs representing large sets of numbers.
   */
  def slice[T: ClassManifest](seq: Seq[T], numSlices: Int): Seq[Seq[T]] = {
    if (numSlices < 1) {
      throw new IllegalArgumentException("Positive number of slices required")
    }
    seq match {
      case r: Range.Inclusive => {
        val sign = if (r.step < 0) {
          -1
        } else {
          1
        }
        slice(new Range(
          r.start, r.end + sign, r.step).asInstanceOf[Seq[T]], numSlices)
      }
      case r: Range => {
        (0 until numSlices).map(i => {
          val start = ((i * r.length.toLong) / numSlices).toInt
          val end = (((i + 1) * r.length.toLong) / numSlices).toInt
          new Range(r.start + start * r.step, r.start + end * r.step, r.step)
        }).asInstanceOf[Seq[Seq[T]]]
      }
      case nr: NumericRange[_] => {
        // For ranges of Long, Double, BigInteger, etc
        val slices = new ArrayBuffer[Seq[T]](numSlices)
        val sliceSize = (nr.size + numSlices - 1) / numSlices // Round up to catch everything
        var r = nr
        for (i <- 0 until numSlices) {
          slices += r.take(sliceSize).asInstanceOf[Seq[T]]
          r = r.drop(sliceSize)
        }
        slices
      }
      case _ => {
        val array = seq.toArray // To prevent O(n^2) operations for List etc
        (0 until numSlices).map(i => {
          val start = ((i * array.length.toLong) / numSlices).toInt
          val end = (((i + 1) * array.length.toLong) / numSlices).toInt
          array.slice(start, end).toSeq
        })
      }
    }
  }
}
