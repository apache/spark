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

import org.apache.spark._
import org.apache.spark.SparkContext._
import org.apache.spark.rdd._
import org.apache.spark.storage.StorageLevel
import org.apache.spark.util.collection.{BitSet, OpenHashSet, PrimitiveKeyOpenHashMap}
import org.apache.spark.graph.impl.AggregationMsg
import org.apache.spark.graph.impl.MsgRDDFunctions._


/**
 * The `VertexSetIndex` maintains the per-partition mapping from
 * vertex id to the corresponding location in the per-partition values
 * array.  This class is meant to be an opaque type.
 *
 */
class VertexSetIndex(private[spark] val rdd: RDD[VertexIdToIndexMap]) {
  /**
   * The persist function behaves like the standard RDD persist
   */
  def persist(newLevel: StorageLevel): VertexSetIndex = {
    rdd.persist(newLevel)
    return this
  }

  /**
   * Returns the partitioner object of the underlying RDD.  This is
   * used by the VertexSetRDD to partition the values RDD.
   */
  def partitioner: Partitioner = rdd.partitioner.get
} // end of VertexSetIndex


/**
 * An VertexSetRDD[V] extends the RDD[(Vid,V)] by ensuring that there
 * is only one entry for each vertex and by pre-indexing the entries
 * for fast, efficient joins.
 *
 * In addition to providing the basic RDD[(Vid,V)] functionality the
 * VertexSetRDD exposes an index member which can be used to "key"
 * other VertexSetRDDs
 *
 * @tparam V the vertex attribute associated with each vertex in the
 * set.
 *
 * @param index the index which contains the vertex id information and
 * is used to organize the values in the RDD.
 * @param valuesRDD the values RDD contains the actual vertex
 * attributes organized as an array within each partition.
 *
 * To construct a `VertexSetRDD` use the singleton object:
 *
 * @example Construct a `VertexSetRDD` from a plain RDD
 * {{{
 * // Construct an intial vertex set
 * val someData: RDD[(Vid, SomeType)] = loadData(someFile)
 * val vset = VertexSetRDD(someData)
 * // If there were redundant values in someData we would use a reduceFunc
 * val vset2 = VertexSetRDD(someData, reduceFunc)
 * // Finally we can use the VertexSetRDD to index another dataset
 * val otherData: RDD[(Vid, OtherType)] = loadData(otherFile)
 * val vset3 = VertexSetRDD(otherData, vset.index)
 * // Now we can construct very fast joins between the two sets
 * val vset4: VertexSetRDD[(SomeType, OtherType)] = vset.leftJoin(vset3)
 * }}}
 *
 */
class VertexSetRDD[@specialized V: ClassManifest](
    @transient val index:  VertexSetIndex,
    @transient val valuesRDD: RDD[ ( Array[V], BitSet) ])
  extends RDD[(Vid, V)](index.rdd.context,
    List(new OneToOneDependency(index.rdd), new OneToOneDependency(valuesRDD)) ) {

  /**
   * Construct a new VertexSetRDD that is indexed by only the keys in the RDD.
   * The resulting VertexSet will be based on a different index and can
   * no longer be quickly joined with this RDD.
   */
   def reindex(): VertexSetRDD[V] = VertexSetRDD(this)

  /**
   * An internal representation which joins the block indices with the values
   * This is used by the compute function to emulate RDD[(Vid, V)]
   */
  protected[spark] val tuples =
    new ZippedRDD(index.rdd.context, index.rdd, valuesRDD)

  /**
   * The partitioner is defined by the index.
   */
  override val partitioner = index.rdd.partitioner

  /**
   * The actual partitions are defined by the tuples.
   */
  override def getPartitions: Array[Partition] = tuples.getPartitions

  /**
   * The preferred locations are computed based on the preferred
   * locations of the tuples.
   */
  override def getPreferredLocations(s: Partition): Seq[String] =
    tuples.getPreferredLocations(s)

  /**
   * Caching an VertexSetRDD causes the index and values to be cached separately.
   */
  override def persist(newLevel: StorageLevel): VertexSetRDD[V] = {
    index.persist(newLevel)
    valuesRDD.persist(newLevel)
    return this
  }

  /** Persist this RDD with the default storage level (`MEMORY_ONLY`). */
  override def persist(): VertexSetRDD[V] = persist(StorageLevel.MEMORY_ONLY)

  /** Persist this RDD with the default storage level (`MEMORY_ONLY`). */
  override def cache(): VertexSetRDD[V] = persist()

  /**
   * Provide the RDD[(K,V)] equivalent output.
   */
  override def compute(part: Partition, context: TaskContext): Iterator[(Vid, V)] = {
    tuples.compute(part, context).flatMap { case (indexMap, (values, bs) ) =>
      bs.iterator.map(ind => (indexMap.getValueSafe(ind), values(ind)))
    }
  } // end of compute

  /**
   * Restrict the vertex set to the set of vertices satisfying the
   * given predicate.
   *
   * @param pred the user defined predicate
   *
   * @note The vertex set preserves the original index structure
   * which means that the returned RDD can be easily joined with
   * the original vertex-set.  Furthermore, the filter only
   * modifies the bitmap index and so no new values are allocated.
   */
  override def filter(pred: Tuple2[Vid,V] => Boolean): VertexSetRDD[V] = {
    val cleanPred = index.rdd.context.clean(pred)
    val newValues = index.rdd.zipPartitions(valuesRDD){
      (keysIter: Iterator[VertexIdToIndexMap],
       valuesIter: Iterator[(Array[V], BitSet)]) =>
      val index = keysIter.next()
      assert(keysIter.hasNext == false)
      val (oldValues, bs) = valuesIter.next()
      assert(valuesIter.hasNext == false)
      // Allocate the array to store the results into
      val newBS = new BitSet(index.capacity)
      // Iterate over the active bits in the old bitset and
      // evaluate the predicate
      var ind = bs.nextSetBit(0)
      while(ind >= 0) {
        val k = index.getValueSafe(ind)
        if( cleanPred( (k, oldValues(ind)) ) ) {
          newBS.set(ind)
        }
        ind = bs.nextSetBit(ind+1)
      }
      Array((oldValues, newBS)).iterator
    }
    new VertexSetRDD[V](index, newValues)
  } // end of filter

  /**
   * Pass each vertex attribute through a map function and retain the
   * original RDD's partitioning and index.
   *
   * @tparam U the type returned by the map function
   *
   * @param f the function applied to each value in the RDD
   * @return a new VertexSet with values obtaind by applying `f` to
   * each of the entries in the original VertexSet.  The resulting
   * VertexSetRDD retains the same index.
   */
  def mapValues[U: ClassManifest](f: V => U): VertexSetRDD[U] = {
    val cleanF = index.rdd.context.clean(f)
    val newValuesRDD: RDD[ (Array[U], BitSet) ] =
      valuesRDD.mapPartitions(iter => iter.map{
        case (values, bs: BitSet) =>
          val newValues = new Array[U](values.size)
          bs.iterator.foreach { ind => newValues(ind) = cleanF(values(ind)) }
          (newValues, bs)
      }, preservesPartitioning = true)
    new VertexSetRDD[U](index, newValuesRDD)
  } // end of mapValues

  /**
   * Fill in missing values for all vertices in the index.
   *
   * @param missingValue the value to be used for vertices in the
   * index that don't currently have values.
   * @return A VertexSetRDD with a value for all vertices.
   */
  def fillMissing(missingValue: V): VertexSetRDD[V] = {
    val newValuesRDD: RDD[ (Array[V], BitSet) ] =
      valuesRDD.zipPartitions(index.rdd){ (valuesIter, indexIter) =>
        val index = indexIter.next
        assert(!indexIter.hasNext)
        val (values, bs: BitSet) = valuesIter.next
        assert(!valuesIter.hasNext)
        // Allocate a new values array with missing value as the default
        val newValues = Array.fill(values.size)(missingValue)
        // Copy over the old values
        bs.iterator.foreach { ind => newValues(ind) = values(ind) }
        // Create a new bitset matching the keyset
        val newBS = index.getBitSet
        Iterator((newValues, newBS))
      }
    new VertexSetRDD[V](index, newValuesRDD)
  }

  /**
   * Pass each vertex attribute along with the vertex id through a map
   * function and retain the original RDD's partitioning and index.
   *
   * @tparam U the type returned by the map function
   *
   * @param f the function applied to each vertex id and vertex
   * attribute in the RDD
   * @return a new VertexSet with values obtaind by applying `f` to
   * each of the entries in the original VertexSet.  The resulting
   * VertexSetRDD retains the same index.
   */
  def mapValuesWithKeys[U: ClassManifest](f: (Vid, V) => U): VertexSetRDD[U] = {
    val cleanF = index.rdd.context.clean(f)
    val newValues: RDD[ (Array[U], BitSet) ] =
      index.rdd.zipPartitions(valuesRDD){
        (keysIter: Iterator[VertexIdToIndexMap],
         valuesIter: Iterator[(Array[V], BitSet)]) =>
        val index = keysIter.next()
        assert(keysIter.hasNext == false)
        val (values, bs: BitSet) = valuesIter.next()
        assert(valuesIter.hasNext == false)
        // Cosntruct a view of the map transformation
        val newValues = new Array[U](index.capacity)
        bs.iterator.foreach { ind => newValues(ind) = cleanF(index.getValueSafe(ind), values(ind)) }
        Iterator((newValues, bs))
      }
    new VertexSetRDD[U](index, newValues)
  } // end of mapValuesWithKeys


  /**
   * Inner join this VertexSet with another VertexSet which has the
   * same Index.  This function will fail if both VertexSets do not
   * share the same index.  The resulting vertex set will only contain
   * vertices that are in both this and the other vertex set.
   *
   * @tparam W the attribute type of the other VertexSet
   *
   * @param other the other VertexSet with which to join.
   * @param f the function mapping a vertex id and its attributes in
   * this and the other vertex set to a new vertex attribute.
   * @return a VertexSetRDD containing only the vertices in both this
   * and the other VertexSet and with tuple attributes.
   *
   */
  def zipJoin[W: ClassManifest, Z: ClassManifest](other: VertexSetRDD[W])(f: (Vid, V, W) => Z):
    VertexSetRDD[Z] = {
    val cleanF = index.rdd.context.clean(f)
    if(index != other.index) {
      throw new SparkException("A zipJoin can only be applied to RDDs with the same index!")
    }
    val newValuesRDD: RDD[ (Array[Z], BitSet) ] =
      index.rdd.zipPartitions(valuesRDD, other.valuesRDD) { (indexIter, thisIter, otherIter) =>
        val index = indexIter.next()
        assert(!indexIter.hasNext)
        val (thisValues, thisBS: BitSet) = thisIter.next()
        assert(!thisIter.hasNext)
        val (otherValues, otherBS: BitSet) = otherIter.next()
        assert(!otherIter.hasNext)
        val newBS: BitSet = thisBS & otherBS
        val newValues = new Array[Z](index.capacity)
        newBS.iterator.foreach { ind =>
          newValues(ind) = cleanF(index.getValueSafe(ind), thisValues(ind), otherValues(ind))
        }
        Iterator((newValues, newBS))
      }
    new VertexSetRDD(index, newValuesRDD)
  }


  /**
   * Inner join this VertexSet with another VertexSet which has the
   * same Index.  This function will fail if both VertexSets do not
   * share the same index.
   *
   * @param other the vertex set to join with this vertex set
   * @param f the function mapping a vertex id and its attributes in
   * this and the other vertex set to a collection of tuples.
   * @tparam W the type of the other vertex set attributes
   * @tparam Z the type of the tuples emitted by `f`
   * @return an RDD containing the tuples emitted by `f`
   */
  def zipJoinFlatMap[W: ClassManifest, Z: ClassManifest](other: VertexSetRDD[W])(f: (Vid, V,W) => Iterator[Z]):
  RDD[Z] = {
    val cleanF = index.rdd.context.clean(f)
    if(index != other.index) {
      throw new SparkException("A zipJoin can only be applied to RDDs with the same index!")
    }
    index.rdd.zipPartitions(valuesRDD, other.valuesRDD) { (indexIter, thisIter, otherIter) =>
      val index = indexIter.next()
      assert(!indexIter.hasNext)
      val (thisValues, thisBS: BitSet) = thisIter.next()
      assert(!thisIter.hasNext)
      val (otherValues, otherBS: BitSet) = otherIter.next()
      assert(!otherIter.hasNext)
      val newBS: BitSet = thisBS & otherBS
      val newValues = new Array[Z](index.capacity)
      newBS.iterator.flatMap { ind => cleanF(index.getValueSafe(ind), thisValues(ind), otherValues(ind)) }
    }
  }


  /**
   * Left join this VertexSet with another VertexSet which has the
   * same Index.  This function will fail if both VertexSets do not
   * share the same index.  The resulting vertex set contains an entry
   * for each vertex in this set.  If the other VertexSet is missing
   * any vertex in this VertexSet then a `None` attribute is generated
   *
   * @tparam W the attribute type of the other VertexSet
   *
   * @param other the other VertexSet with which to join.
   * @param f the function mapping a vertex id and its attributes in
   * this and the other vertex set to a new vertex attribute.
   * @return a VertexSetRDD containing all the vertices in this
   * VertexSet with `None` attributes used for Vertices missing in the
   * other VertexSet.
   *
   */
  def leftZipJoin[W: ClassManifest, Z: ClassManifest](other: VertexSetRDD[W])(f: (Vid, V, Option[W]) => Z):
    VertexSetRDD[Z] = {
    if(index != other.index) {
      throw new SparkException("A zipJoin can only be applied to RDDs with the same index!")
    }
    val cleanF = index.rdd.context.clean(f)
    val newValuesRDD: RDD[(Array[Z], BitSet)] =
      index.rdd.zipPartitions(valuesRDD, other.valuesRDD) { (indexIter, thisIter, otherIter) =>
      val index = indexIter.next()
      assert(!indexIter.hasNext)
      val (thisValues, thisBS: BitSet) = thisIter.next()
      assert(!thisIter.hasNext)
      val (otherValues, otherBS: BitSet) = otherIter.next()
      assert(!otherIter.hasNext)
      val newValues = new Array[Z](index.capacity)
      thisBS.iterator.foreach { ind =>
        val otherV = if (otherBS.get(ind)) Option(otherValues(ind)) else None
        newValues(ind) = cleanF(index.getValueSafe(ind), thisValues(ind), otherV)
      }
      Iterator((newValues, thisBS))
    }
    new VertexSetRDD(index, newValuesRDD)
  } // end of leftZipJoin


  /**
   * Left join this VertexSet with an RDD containing vertex attribute
   * pairs.  If the other RDD is backed by a VertexSet with the same
   * index than the efficient leftZipJoin implementation is used.  The
   * resulting vertex set contains an entry for each vertex in this
   * set.  If the other VertexSet is missing any vertex in this
   * VertexSet then a `None` attribute is generated
   *
   * @tparam W the attribute type of the other VertexSet
   *
   * @param other the other VertexSet with which to join.
   * @param f the function mapping a vertex id and its attributes in
   * this and the other vertex set to a new vertex attribute.
   * @param merge the function used combine duplicate vertex
   * attributes
   * @return a VertexSetRDD containing all the vertices in this
   * VertexSet with the attribute emitted by f.
   *
   */
  def leftJoin[W: ClassManifest, Z: ClassManifest](other: RDD[(Vid,W)])
    (f: (Vid, V, Option[W]) => Z, merge: (W,W) => W = (a:W, b:W) => a ):
    VertexSetRDD[Z] = {
    val cleanF = index.rdd.context.clean(f)
    val cleanMerge = index.rdd.context.clean(merge)
    // Test if the other vertex is a VertexSetRDD to choose the optimal
    // join strategy
    other match {
      // If the other set is a VertexSetRDD and shares the same index as
      // this vertex set then we use the much more efficient leftZipJoin
      case other: VertexSetRDD[_] if index == other.index => {
        leftZipJoin(other)(cleanF)
      }
      case _ => {
        val indexedOther: VertexSetRDD[W] = VertexSetRDD(other, index, cleanMerge)
        leftZipJoin(indexedOther)(cleanF)
      }
    }
  } // end of leftJoin

} // End of VertexSetRDD


/**
 * The VertexSetRDD singleton is used to construct VertexSets
 */
object VertexSetRDD {

  /**
   * Construct a vertex set from an RDD of vertex-attribute pairs.
   * Duplicate entries are removed arbitrarily.
   *
   * @tparam V the vertex attribute type
   *
   * @param rdd the collection of vertex-attribute pairs
   */
  def apply[V: ClassManifest](rdd: RDD[(Vid,V)]): VertexSetRDD[V] =
    apply(rdd, (a:V, b:V) => a )

  /**
   * Construct a vertex set from an RDD of vertex-attribute pairs
   * where duplicate entries are merged using the reduceFunc
   *
   * @tparam V the vertex attribute type
   *
   * @param rdd the collection of vertex-attribute pairs
   * @param reduceFunc the function used to merge attributes of
   * duplicate vertices.
   */
  def apply[V: ClassManifest](
    rdd: RDD[(Vid,V)], reduceFunc: (V, V) => V): VertexSetRDD[V] = {
    val cReduceFunc = rdd.context.clean(reduceFunc)
    // Preaggregate and shuffle if necessary
    val preAgg = rdd.partitioner match {
      case Some(p) => rdd
      case None =>
        val partitioner = new HashPartitioner(rdd.partitions.size)
        // Preaggregation.
        val aggregator = new Aggregator[Vid, V, V](v => v, cReduceFunc, cReduceFunc)
        rdd.mapPartitions(aggregator.combineValuesByKey, true).partitionBy(partitioner)
    }

    val groups = preAgg.mapPartitions( iter => {
      val hashMap = new PrimitiveKeyOpenHashMap[Vid, V]
      for ((k,v) <- iter) {
        hashMap.setMerge(k, v, cReduceFunc)
      }
      val index = hashMap.keySet
      val values = hashMap._values
      val bs = index.getBitSet
      Iterator( (index, (values, bs)) )
      }, true).cache
    // extract the index and the values
    val index = groups.mapPartitions(_.map{ case (kMap, vAr) => kMap }, true)
    val values: RDD[(Array[V], BitSet)] =
      groups.mapPartitions(_.map{ case (kMap,vAr) => vAr }, true)
    new VertexSetRDD[V](new VertexSetIndex(index), values)
  } // end of apply

  /**
   * Construct a vertex set from an RDD using an existing index.
   *
   * @note duplicate vertices are discarded arbitrarily
   *
   * @tparam V the vertex attribute type
   * @param rdd the rdd containing vertices
   * @param index the index which must be a superset of the vertices
   * in RDD
   */
  def apply[V: ClassManifest](
    rdd: RDD[(Vid,V)], index: VertexSetIndex): VertexSetRDD[V] =
    apply(rdd, index, (a:V,b:V) => a)

  /**
   * Construct a vertex set from an RDD using an existing index and a
   * user defined `combiner` to merge duplicate vertices.
   *
   * @tparam V the vertex attribute type
   * @param rdd the rdd containing vertices
   * @param index the index which must be a superset of the vertices
   * in RDD
   * @param reduceFunc the user defined reduce function used to merge
   * duplicate vertex attributes.
   */
  def apply[V: ClassManifest](
    rdd: RDD[(Vid,V)], index: VertexSetIndex,
    reduceFunc: (V, V) => V): VertexSetRDD[V] =
    apply(rdd,index, (v:V) => v, reduceFunc, reduceFunc)

  /**
   * Construct a vertex set from an RDD of AggregationMsgs
   *
   * @tparam V the vertex attribute type
   * @param rdd the rdd containing vertices
   * @param index the index which must be a superset of the vertices
   * in RDD
   * @param reduceFunc the user defined reduce function used to merge
   * duplicate vertex attributes.
   */
  private[spark] def aggregate[V: ClassManifest](
    rdd: RDD[AggregationMsg[V]], index: VertexSetIndex,
    reduceFunc: (V, V) => V): VertexSetRDD[V] = {

    val cReduceFunc = index.rdd.context.clean(reduceFunc)
    assert(rdd.partitioner == index.rdd.partitioner)
    // Use the index to build the new values table
    val values: RDD[ (Array[V], BitSet) ] = index.rdd.zipPartitions(rdd)( (indexIter, tblIter) => {
      // There is only one map
      val index = indexIter.next()
      assert(!indexIter.hasNext)
      val values = new Array[V](index.capacity)
      val bs = new BitSet(index.capacity)
      for (msg <- tblIter) {
        // Get the location of the key in the index
        val pos = index.getPos(msg.vid)
        if ((pos & OpenHashSet.NONEXISTENCE_MASK) != 0) {
          throw new SparkException("Error: Trying to bind an external index " +
            "to an RDD which contains keys that are not in the index.")
        } else {
          // Get the actual index
          val ind = pos & OpenHashSet.POSITION_MASK
          // If this value has already been seen then merge
          if (bs.get(ind)) {
            values(ind) = cReduceFunc(values(ind), msg.data)
          } else { // otherwise just store the new value
            bs.set(ind)
            values(ind) = msg.data
          }
        }
      }
      Iterator((values, bs))
    })
    new VertexSetRDD(index, values)
  }

  /**
   * Construct a vertex set from an RDD using an existing index and a
   * user defined `combiner` to merge duplicate vertices.
   *
   * @tparam V the vertex attribute type
   * @param rdd the rdd containing vertices
   * @param index the index which must be a superset of the vertices
   * in RDD
   * @param createCombiner a user defined function to create a combiner
   * from a vertex attribute
   * @param mergeValue a user defined function to merge a vertex
   * attribute into an existing combiner
   * @param mergeCombiners a user defined function to merge combiners
   *
   */
  def apply[V: ClassManifest, C: ClassManifest](
      rdd: RDD[(Vid,V)],
      index: VertexSetIndex,
      createCombiner: V => C,
      mergeValue: (C, V) => C,
      mergeCombiners: (C, C) => C): VertexSetRDD[C] = {
    val cCreateCombiner = index.rdd.context.clean(createCombiner)
    val cMergeValue = index.rdd.context.clean(mergeValue)
    val cMergeCombiners = index.rdd.context.clean(mergeCombiners)
    // Get the index Partitioner
    val partitioner = index.rdd.partitioner match {
      case Some(p) => p
      case None => throw new SparkException("An index must have a partitioner.")
    }
    // Preaggregate and shuffle if necessary
    val partitioned =
      if (rdd.partitioner != Some(partitioner)) {
        // Preaggregation.
        val aggregator = new Aggregator[Vid, V, C](cCreateCombiner, cMergeValue,
          cMergeCombiners)
        rdd.mapPartitions(aggregator.combineValuesByKey).partitionBy(partitioner)
      } else {
        rdd.mapValues(x => createCombiner(x))
      }

    // Use the index to build the new values table
    val values: RDD[ (Array[C], BitSet) ] = index.rdd.zipPartitions(partitioned)( (indexIter, tblIter) => {
      // There is only one map
      val index = indexIter.next()
      assert(!indexIter.hasNext)
      val values = new Array[C](index.capacity)
      val bs = new BitSet(index.capacity)
      for ((k,c) <- tblIter) {
        // Get the location of the key in the index
        val pos = index.getPos(k)
        if ((pos & OpenHashSet.NONEXISTENCE_MASK) != 0) {
          throw new SparkException("Error: Trying to bind an external index " +
            "to an RDD which contains keys that are not in the index.")
        } else {
          // Get the actual index
          val ind = pos & OpenHashSet.POSITION_MASK
          // If this value has already been seen then merge
          if (bs.get(ind)) {
            values(ind) = cMergeCombiners(values(ind), c)
          } else { // otherwise just store the new value
            bs.set(ind)
            values(ind) = c
          }
        }
      }
      Iterator((values, bs))
    })
    new VertexSetRDD(index, values)
  } // end of apply

  /**
   * Construct an index of the unique vertices.  The resulting index
   * can be used to build VertexSets over subsets of the vertices in
   * the input.
   */
  def makeIndex(keys: RDD[Vid], partitionerOpt: Option[Partitioner] = None): VertexSetIndex = {
    val partitioner = partitionerOpt match {
      case Some(p) => p
      case None => Partitioner.defaultPartitioner(keys)
    }

    val preAgg: RDD[(Vid, Unit)] = keys.mapPartitions( iter => {
      val keys = new VertexIdToIndexMap
      while(iter.hasNext) { keys.add(iter.next) }
      keys.iterator.map(k => (k, ()))
    }, true).partitionBy(partitioner)

    val index = preAgg.mapPartitions( iter => {
      val index = new VertexIdToIndexMap
      while(iter.hasNext) { index.add(iter.next._1) }
      Iterator(index)
    }, true).cache

    new VertexSetIndex(index)
  }

} // end of object VertexSetRDD





