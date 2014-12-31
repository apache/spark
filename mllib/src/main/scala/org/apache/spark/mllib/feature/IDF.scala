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

package org.apache.spark.mllib.feature

import breeze.linalg.{DenseVector => BDV}

import org.apache.spark.annotation.Experimental
import org.apache.spark.api.java.JavaRDD
import org.apache.spark.mllib.linalg.{DenseVector, SparseVector, Vector, Vectors}
import org.apache.spark.mllib.rdd.RDDFunctions._
import org.apache.spark.rdd.RDD

/**
 * :: Experimental ::
 * Inverse document frequency (IDF).
 * The standard formulation is used: `idf = log((m + 1) / (d(t) + 1))`, where `m` is the total
 * number of documents and `d(t)` is the number of documents that contain term `t`.
 *
 * This implementation supports filtering out terms which do not appear in a minimum number
 * of documents (controlled by the variable `minDocFreq`). For terms that are not in
 * at least `minDocFreq` documents, the IDF is found as 0, resulting in TF-IDFs of 0.
 *
 * @param minDocFreq minimum of documents in which a term
 *                   should appear for filtering
 */
@Experimental
class IDF(val minDocFreq: Int) {

  def this() = this(0)

  // TODO: Allow different IDF formulations.

  /**
   * Computes the inverse document frequency.
   * @param dataset an RDD of term frequency vectors
   */
  def fit(dataset: RDD[Vector]): IDFModel = {
    val idf = dataset.treeAggregate(new IDF.DocumentFrequencyAggregator(
          minDocFreq = minDocFreq))(
      seqOp = (df, v) => df.add(v),
      combOp = (df1, df2) => df1.merge(df2)
    ).idf()
    new IDFModel(idf)
  }

  /**
   * Computes the inverse document frequency.
   * @param dataset a JavaRDD of term frequency vectors
   */
  def fit(dataset: JavaRDD[Vector]): IDFModel = {
    fit(dataset.rdd)
  }
}

private object IDF {

  /** Document frequency aggregator. */
  class DocumentFrequencyAggregator(val minDocFreq: Int) extends Serializable {

    /** number of documents */
    private var m = 0L
    /** document frequency vector */
    private var df: BDV[Long] = _


    def this() = this(0)

    /** Adds a new document. */
    def add(doc: Vector): this.type = {
      if (isEmpty) {
        df = BDV.zeros(doc.size)
      }
      doc match {
        case sv: SparseVector =>
          val nnz = sv.indices.size
          var k = 0
          while (k < nnz) {
            if (sv.values(k) > 0) {
              df(sv.indices(k)) += 1L
            }
            k += 1
          }
        case dv: DenseVector =>
          val n = dv.size
          var j = 0
          while (j < n) {
            if (dv.values(j) > 0.0) {
              df(j) += 1L
            }
            j += 1
          }
        case other =>
          throw new UnsupportedOperationException(
            s"Only sparse and dense vectors are supported but got ${other.getClass}.")
      }
      m += 1L
      this
    }

    /** Merges another. */
    def merge(other: DocumentFrequencyAggregator): this.type = {
      if (!other.isEmpty) {
        m += other.m
        if (df == null) {
          df = other.df.copy
        } else {
          df += other.df
        }
      }
      this
    }

    private def isEmpty: Boolean = m == 0L

    /** Returns the current IDF vector. */
    def idf(): Vector = {
      if (isEmpty) {
        throw new IllegalStateException("Haven't seen any document yet.")
      }
      val n = df.length
      val inv = new Array[Double](n)
      var j = 0
      while (j < n) {
        /*
         * If the term is not present in the minimum
         * number of documents, set IDF to 0. This
         * will cause multiplication in IDFModel to
         * set TF-IDF to 0.
         *
         * Since arrays are initialized to 0 by default,
         * we just omit changing those entries.
         */
        if(df(j) >= minDocFreq) {
          inv(j) = math.log((m + 1.0) / (df(j) + 1.0))
        }
        j += 1
      }
      Vectors.dense(inv)
    }
  }
}

/**
 * :: Experimental ::
 * Represents an IDF model that can transform term frequency vectors.
 */
@Experimental
class IDFModel private[mllib] (val idf: Vector) extends Serializable {

  /**
   * Transforms term frequency (TF) vectors to TF-IDF vectors.
   *
   * If `minDocFreq` was set for the IDF calculation,
   * the terms which occur in fewer than `minDocFreq`
   * documents will have an entry of 0.
   *
   * @param dataset an RDD of term frequency vectors
   * @return an RDD of TF-IDF vectors
   */
  def transform(dataset: RDD[Vector]): RDD[Vector] = {
    val bcIdf = dataset.context.broadcast(idf)
    dataset.mapPartitions { iter =>
      val thisIdf = bcIdf.value
      iter.map { v =>
        val n = v.size
        v match {
          case sv: SparseVector =>
            val nnz = sv.indices.size
            val newValues = new Array[Double](nnz)
            var k = 0
            while (k < nnz) {
              newValues(k) = sv.values(k) * thisIdf(sv.indices(k))
              k += 1
            }
            Vectors.sparse(n, sv.indices, newValues)
          case dv: DenseVector =>
            val newValues = new Array[Double](n)
            var j = 0
            while (j < n) {
              newValues(j) = dv.values(j) * thisIdf(j)
              j += 1
            }
            Vectors.dense(newValues)
          case other =>
            throw new UnsupportedOperationException(
              s"Only sparse and dense vectors are supported but got ${other.getClass}.")
        }
      }
    }
  }

  /**
   * Transforms term frequency (TF) vectors to TF-IDF vectors (Java version).
   * @param dataset a JavaRDD of term frequency vectors
   * @return a JavaRDD of TF-IDF vectors
   */
  def transform(dataset: JavaRDD[Vector]): JavaRDD[Vector] = {
    transform(dataset.rdd).toJavaRDD()
  }
}
