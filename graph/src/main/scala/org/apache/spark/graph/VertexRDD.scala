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

package org.apache.spark.graph

import scala.reflect.ClassTag

import org.apache.spark._
import org.apache.spark.SparkContext._
import org.apache.spark.rdd._
import org.apache.spark.storage.StorageLevel

import org.apache.spark.graph.impl.MsgRDDFunctions
import org.apache.spark.graph.impl.VertexPartition


/**
 * A `VertexRDD[VD]` extends the `RDD[(Vid, VD)]` by ensuring that there is
 * only one entry for each vertex and by pre-indexing the entries for fast,
 * efficient joins.
 *
 * @tparam VD the vertex attribute associated with each vertex in the set.
 *
 * To construct a `VertexRDD` use the singleton object:
 *
 * @example Construct a `VertexRDD` from a plain RDD
 * {{{
 * // Construct an intial vertex set
 * val someData: RDD[(Vid, SomeType)] = loadData(someFile)
 * val vset = VertexRDD(someData)
 * // If there were redundant values in someData we would use a reduceFunc
 * val vset2 = VertexRDD(someData, reduceFunc)
 * // Finally we can use the VertexRDD to index another dataset
 * val otherData: RDD[(Vid, OtherType)] = loadData(otherFile)
 * val vset3 = VertexRDD(otherData, vset.index)
 * // Now we can construct very fast joins between the two sets
 * val vset4: VertexRDD[(SomeType, OtherType)] = vset.leftJoin(vset3)
 * }}}
 *
 */
class VertexRDD[@specialized VD: ClassTag](
    val partitionsRDD: RDD[VertexPartition[VD]])
  extends RDD[(Vid, VD)](partitionsRDD.context, List(new OneToOneDependency(partitionsRDD))) {

  require(partitionsRDD.partitioner.isDefined)

  partitionsRDD.setName("VertexRDD")

  /**
   * Construct a new VertexRDD that is indexed by only the keys in the RDD.
   * The resulting VertexRDD will be based on a different index and can
   * no longer be quickly joined with this RDD.
   */
  def reindex(): VertexRDD[VD] = new VertexRDD(partitionsRDD.map(_.reindex()))

  /**
   * The partitioner is defined by the index.
   */
  override val partitioner = partitionsRDD.partitioner

  /**
   * The actual partitions are defined by the tuples.
   */
  override protected def getPartitions: Array[Partition] = partitionsRDD.partitions

  /**
   * The preferred locations are computed based on the preferred
   * locations of the tuples.
   */
  override protected def getPreferredLocations(s: Partition): Seq[String] =
    partitionsRDD.preferredLocations(s)

  /**
   * Caching a VertexRDD causes the index and values to be cached separately.
   */
  override def persist(newLevel: StorageLevel): VertexRDD[VD] = {
    partitionsRDD.persist(newLevel)
    this
  }

  /** Persist this RDD with the default storage level (`MEMORY_ONLY`). */
  override def persist(): VertexRDD[VD] = persist(StorageLevel.MEMORY_ONLY)

  /** Persist this RDD with the default storage level (`MEMORY_ONLY`). */
  override def cache(): VertexRDD[VD] = persist()

  /** Return the number of vertices in this set. */
  override def count(): Long = {
    partitionsRDD.map(_.size).reduce(_ + _)
  }

  /**
   * Provide the `RDD[(Vid, VD)]` equivalent output.
   */
  override def compute(part: Partition, context: TaskContext): Iterator[(Vid, VD)] = {
    firstParent[VertexPartition[VD]].iterator(part, context).next.iterator
  }

  /**
   * Return a new VertexRDD by applying a function to each VertexPartition of this RDD.
   */
  def mapVertexPartitions[VD2: ClassTag](f: VertexPartition[VD] => VertexPartition[VD2])
    : VertexRDD[VD2] = {
    val newPartitionsRDD = partitionsRDD.mapPartitions(_.map(f), preservesPartitioning = true)
    new VertexRDD(newPartitionsRDD)
  }


  /**
   * Restrict the vertex set to the set of vertices satisfying the
   * given predicate.
   *
   * @param pred the user defined predicate, which takes a tuple to conform to
   * the RDD[(Vid, VD)] interface
   *
   * @note The vertex set preserves the original index structure
   * which means that the returned RDD can be easily joined with
   * the original vertex-set.  Furthermore, the filter only
   * modifies the bitmap index and so no new values are allocated.
   */
  override def filter(pred: Tuple2[Vid, VD] => Boolean): VertexRDD[VD] =
    this.mapVertexPartitions(_.filter(Function.untupled(pred)))

  /**
   * Pass each vertex attribute through a map function and retain the
   * original RDD's partitioning and index.
   *
   * @tparam VD2 the type returned by the map function
   *
   * @param f the function applied to each value in the RDD
   * @return a new VertexRDD with values obtained by applying `f` to
   * each of the entries in the original VertexRDD.  The resulting
   * VertexRDD retains the same index.
   */
  def mapValues[VD2: ClassTag](f: VD => VD2): VertexRDD[VD2] =
    this.mapVertexPartitions(_.map((vid, attr) => f(attr)))

  /**
   * Pass each vertex attribute through a map function and retain the
   * original RDD's partitioning and index.
   *
   * @tparam VD2 the type returned by the map function
   *
   * @param f the function applied to each value in the RDD
   * @return a new VertexRDD with values obtained by applying `f` to
   * each of the entries in the original VertexRDD.  The resulting
   * VertexRDD retains the same index.
   */
  def mapValues[VD2: ClassTag](f: (Vid, VD) => VD2): VertexRDD[VD2] =
    this.mapVertexPartitions(_.map(f))

  /**
   * Hides vertices that are the same between this and other. For vertices that are different, keeps
   * the values from `other`.
   */
  def diff(other: VertexRDD[VD]): VertexRDD[VD] = {
    val newPartitionsRDD = partitionsRDD.zipPartitions(
      other.partitionsRDD, preservesPartitioning = true
    ) { (thisIter, otherIter) =>
      val thisPart = thisIter.next()
      val otherPart = otherIter.next()
      Iterator(thisPart.diff(otherPart))
    }
    new VertexRDD(newPartitionsRDD)
  }

  /**
   * Left join this VertexSet with another VertexSet which has the
   * same Index.  This function will fail if both VertexSets do not
   * share the same index.  The resulting vertex set contains an entry
   * for each vertex in this set.  If the other VertexSet is missing
   * any vertex in this VertexSet then a `None` attribute is generated
   *
   * @tparam VD2 the attribute type of the other VertexSet
   * @tparam VD3 the attribute type of the resulting VertexSet
   *
   * @param other the other VertexSet with which to join.
   * @param f the function mapping a vertex id and its attributes in
   * this and the other vertex set to a new vertex attribute.
   * @return a VertexRDD containing all the vertices in this
   * VertexSet with `None` attributes used for Vertices missing in the
   * other VertexSet.
   *
   */
  def leftZipJoin[VD2: ClassTag, VD3: ClassTag]
      (other: VertexRDD[VD2])(f: (Vid, VD, Option[VD2]) => VD3): VertexRDD[VD3] = {
    val newPartitionsRDD = partitionsRDD.zipPartitions(
      other.partitionsRDD, preservesPartitioning = true
    ) { (thisIter, otherIter) =>
      val thisPart = thisIter.next()
      val otherPart = otherIter.next()
      Iterator(thisPart.leftJoin(otherPart)(f))
    }
    new VertexRDD(newPartitionsRDD)
  }

  /**
   * Left join this VertexRDD with an RDD containing vertex attribute
   * pairs.  If the other RDD is backed by a VertexRDD with the same
   * index than the efficient leftZipJoin implementation is used.  The
   * resulting vertex set contains an entry for each vertex in this
   * set.  If the other VertexRDD is missing any vertex in this
   * VertexRDD then a `None` attribute is generated.
   *
   * If there are duplicates, the vertex is picked at random.
   *
   * @tparam VD2 the attribute type of the other VertexRDD
   * @tparam VD3 the attribute type of the resulting VertexRDD
   *
   * @param other the other VertexRDD with which to join.
   * @param f the function mapping a vertex id and its attributes in
   * this and the other vertex set to a new vertex attribute.
   * @return a VertexRDD containing all the vertices in this
   * VertexRDD with the attribute emitted by f.
   */
  def leftJoin[VD2: ClassTag, VD3: ClassTag]
      (other: RDD[(Vid, VD2)])
      (f: (Vid, VD, Option[VD2]) => VD3)
    : VertexRDD[VD3] =
  {
    // Test if the other vertex is a VertexRDD to choose the optimal join strategy.
    // If the other set is a VertexRDD then we use the much more efficient leftZipJoin
    other match {
      case other: VertexRDD[_] =>
        leftZipJoin(other)(f)
      case _ =>
        new VertexRDD[VD3](
          partitionsRDD.zipPartitions(
            other.partitionBy(this.partitioner.get), preservesPartitioning = true)
          { (part, msgs) =>
            val vertexPartition: VertexPartition[VD] = part.next()
            Iterator(vertexPartition.leftJoin(msgs)(f))
          }
        )
    }
  }

  /**
   * Same effect as leftJoin(other) { (vid, a, bOpt) => bOpt.getOrElse(a) }, but `this` and `other`
   * must have the same index.
   */
  def innerZipJoin[U: ClassTag, VD2: ClassTag](other: VertexRDD[U])
      (f: (Vid, VD, U) => VD2): VertexRDD[VD2] = {
    val newPartitionsRDD = partitionsRDD.zipPartitions(
      other.partitionsRDD, preservesPartitioning = true
    ) { (thisIter, otherIter) =>
      val thisPart = thisIter.next()
      val otherPart = otherIter.next()
      Iterator(thisPart.innerJoin(otherPart)(f))
    }
    new VertexRDD(newPartitionsRDD)
  }

  /**
   * Replace vertices with corresponding vertices in `other`, and drop vertices without a
   * corresponding vertex in `other`.
   */
  def innerJoin[U: ClassTag, VD2: ClassTag](other: RDD[(Vid, U)])
      (f: (Vid, VD, U) => VD2): VertexRDD[VD2] = {
    // Test if the other vertex is a VertexRDD to choose the optimal join strategy.
    // If the other set is a VertexRDD then we use the much more efficient innerZipJoin
    other match {
      case other: VertexRDD[_] =>
        innerZipJoin(other)(f)
      case _ =>
        new VertexRDD(
          partitionsRDD.zipPartitions(
            other.partitionBy(this.partitioner.get), preservesPartitioning = true)
          { (part, msgs) =>
            val vertexPartition: VertexPartition[VD] = part.next()
            Iterator(vertexPartition.innerJoin(msgs)(f))
          }
        )
    }
  }

  /**
   * Aggregate messages with the same ids using `reduceFunc`, returning a VertexRDD that is
   * co-indexed with this one.
   */
  def aggregateUsingIndex[VD2: ClassTag](
      messages: RDD[(Vid, VD2)], reduceFunc: (VD2, VD2) => VD2): VertexRDD[VD2] =
  {
    val shuffled = MsgRDDFunctions.partitionForAggregation(messages, this.partitioner.get)
    val parts = partitionsRDD.zipPartitions(shuffled, true) { (thisIter, msgIter) =>
      val vertexPartition: VertexPartition[VD] = thisIter.next()
      Iterator(vertexPartition.aggregateUsingIndex(msgIter, reduceFunc))
    }
    new VertexRDD[VD2](parts)
  }

} // end of VertexRDD


/**
 * The VertexRDD singleton is used to construct VertexRDDs
 */
object VertexRDD {

  /**
   * Construct a vertex set from an RDD of vertex-attribute pairs.
   * Duplicate entries are removed arbitrarily.
   *
   * @tparam VD the vertex attribute type
   *
   * @param rdd the collection of vertex-attribute pairs
   */
  def apply[VD: ClassTag](rdd: RDD[(Vid, VD)]): VertexRDD[VD] = {
    val partitioned: RDD[(Vid, VD)] = rdd.partitioner match {
      case Some(p) => rdd
      case None => rdd.partitionBy(new HashPartitioner(rdd.partitions.size))
    }
    val vertexPartitions = partitioned.mapPartitions(
      iter => Iterator(VertexPartition(iter)),
      preservesPartitioning = true)
    new VertexRDD(vertexPartitions)
  }

  /**
   * Construct a vertex set from an RDD of vertex-attribute pairs.
   * Duplicate entries are merged using mergeFunc.
   *
   * @tparam VD the vertex attribute type
   *
   * @param rdd the collection of vertex-attribute pairs
   * @param mergeFunc the associative, commutative merge function.
   */
  def apply[VD: ClassTag](rdd: RDD[(Vid, VD)], mergeFunc: (VD, VD) => VD): VertexRDD[VD] =
  {
    val partitioned: RDD[(Vid, VD)] = rdd.partitioner match {
      case Some(p) => rdd
      case None => rdd.partitionBy(new HashPartitioner(rdd.partitions.size))
    }
    val vertexPartitions = partitioned.mapPartitions(
      iter => Iterator(VertexPartition(iter)),
      preservesPartitioning = true)
    new VertexRDD(vertexPartitions)
  }

  def apply[VD: ClassTag](vids: RDD[Vid], rdd: RDD[(Vid, VD)], defaultVal: VD)
    : VertexRDD[VD] =
  {
    VertexRDD(vids.map(vid => (vid, defaultVal))).leftJoin(rdd) { (vid, default, value) =>
      value.getOrElse(default)
    }
  }
}
