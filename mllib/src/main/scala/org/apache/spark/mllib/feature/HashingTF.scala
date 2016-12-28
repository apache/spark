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

import java.lang.{Iterable => JavaIterable}

import scala.collection.JavaConverters._
import scala.collection.mutable

import org.apache.spark.SparkException
import org.apache.spark.annotation.Since
import org.apache.spark.api.java.JavaRDD
import org.apache.spark.mllib.linalg.{Vector, Vectors}
import org.apache.spark.rdd.RDD
import org.apache.spark.unsafe.hash.Murmur3_x86_32._
import org.apache.spark.unsafe.types.UTF8String
import org.apache.spark.util.Utils

/**
 * Maps a sequence of terms to their term frequencies using the hashing trick.
 *
 * @param numFeatures number of features (default: 2^20^)
 */
@Since("1.1.0")
class HashingTF(val numFeatures: Int) extends Serializable {

  import HashingTF._

  private var binary = false
  private var hashAlgorithm = HashingTF.Murmur3

  /**
   */
  @Since("1.1.0")
  def this() = this(1 << 20)

  /**
   * If true, term frequency vector will be binary such that non-zero term counts will be set to 1
   * (default: false)
   */
  @Since("2.0.0")
  def setBinary(value: Boolean): this.type = {
    binary = value
    this
  }

  /**
   * Set the hash algorithm used when mapping term to integer.
   * (default: murmur3)
   */
  @Since("2.0.0")
  def setHashAlgorithm(value: String): this.type = {
    hashAlgorithm = value
    this
  }

  /**
   * Returns the index of the input term.
   */
  @Since("1.1.0")
  def indexOf(term: Any): Int = {
    Utils.nonNegativeMod(getHashFunction(term), numFeatures)
  }

  /**
   * Get the hash function corresponding to the current [[hashAlgorithm]] setting.
   */
  private def getHashFunction: Any => Int = hashAlgorithm match {
    case Murmur3 => murmur3Hash
    case Native => nativeHash
    case _ =>
      // This should never happen.
      throw new IllegalArgumentException(
        s"HashingTF does not recognize hash algorithm $hashAlgorithm")
  }

  /**
   * Transforms the input document into a sparse term frequency vector.
   */
  @Since("1.1.0")
  def transform(document: Iterable[_]): Vector = {
    val termFrequencies = mutable.HashMap.empty[Int, Double]
    val setTF = if (binary) (i: Int) => 1.0 else (i: Int) => termFrequencies.getOrElse(i, 0.0) + 1.0
    val hashFunc: Any => Int = getHashFunction
    document.foreach { term =>
      val i = Utils.nonNegativeMod(hashFunc(term), numFeatures)
      termFrequencies.put(i, setTF(i))
    }
    Vectors.sparse(numFeatures, termFrequencies.toSeq)
  }

  /**
   * Transforms the input document into a sparse term frequency vector (Java version).
   */
  @Since("1.1.0")
  def transform(document: JavaIterable[_]): Vector = {
    transform(document.asScala)
  }

  /**
   * Transforms the input document to term frequency vectors.
   */
  @Since("1.1.0")
  def transform[D <: Iterable[_]](dataset: RDD[D]): RDD[Vector] = {
    dataset.map(this.transform)
  }

  /**
   * Transforms the input document to term frequency vectors (Java version).
   */
  @Since("1.1.0")
  def transform[D <: JavaIterable[_]](dataset: JavaRDD[D]): JavaRDD[Vector] = {
    dataset.rdd.map(this.transform).toJavaRDD()
  }
}

object HashingTF {

  private[HashingTF] val Native: String = "native"

  private[HashingTF] val Murmur3: String = "murmur3"

  private val seed = 42

  /**
   * Calculate a hash code value for the term object using the native Scala implementation.
   * This is the default hash algorithm used in Spark 1.6 and earlier.
   */
  private[HashingTF] def nativeHash(term: Any): Int = term.##

  /**
   * Calculate a hash code value for the term object using
   * Austin Appleby's MurmurHash 3 algorithm (MurmurHash3_x86_32).
   * This is the default hash algorithm used from Spark 2.0 onwards.
   */
  private[spark] def murmur3Hash(term: Any): Int = {
    term match {
      case null => seed
      case b: Boolean => hashInt(if (b) 1 else 0, seed)
      case b: Byte => hashInt(b, seed)
      case s: Short => hashInt(s, seed)
      case i: Int => hashInt(i, seed)
      case l: Long => hashLong(l, seed)
      case f: Float => hashInt(java.lang.Float.floatToIntBits(f), seed)
      case d: Double => hashLong(java.lang.Double.doubleToLongBits(d), seed)
      case s: String =>
        val utf8 = UTF8String.fromString(s)
        hashUnsafeBytes(utf8.getBaseObject, utf8.getBaseOffset, utf8.numBytes(), seed)
      case _ => throw new SparkException("HashingTF with murmur3 algorithm does not " +
        s"support type ${term.getClass.getCanonicalName} of input data.")
    }
  }
}
