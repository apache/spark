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
package org.apache.spark.sql.execution.datasources.parquet.bcvar

import java.time.Duration
import java.util
import java.util.Comparator

import com.google.common.cache.{CacheBuilder, CacheLoader, LoadingCache}
import com.google.common.collect.Sets
import org.apache.parquet.filter2.predicate.Operators.Column

import org.apache.spark.sql.catalyst.bcvar.BroadcastedJoinKeysWrapper
import org.apache.spark.sql.execution.datasources.parquet.Converter

object BroadcastVarCache {

  private val cacheLoader = new CacheLoader[KeyIdempot, util.NavigableSet[_]] {
    /** the cache key doesn't match a cached entry, or the entry is out-of-date, so load it. */
    override def load(key: KeyIdempot): util.NavigableSet[_] = {
      // lets check the initialization here.
      // TODO: figure out a better way to initialize
      ParquetBroadcastReaper.checkInstanceInitialized()
      createNavigableSet(key)
    }
  }

  private val idempotentializer: LoadingCache[KeyIdempot, util.NavigableSet[_]] =
    CacheBuilder.newBuilder.expireAfterWrite(
    Duration.ofSeconds(BroadcastedJoinKeysWrapper.CACHE_EXPIRY)).
    maximumSize(BroadcastedJoinKeysWrapper.CACHE_SIZE).weakValues.build(cacheLoader)

  private def createNavigableSet[T](key: KeyIdempot): util.NavigableSet[T] = {
    val tempSet: util.NavigableSet[T] = Sets.newTreeSet(
      ParquetJavaDataTypeComparators.COMPARATORS.getOrElse(key.comparatorClass,
        throw new UnsupportedOperationException("comparator")).asInstanceOf[Comparator[T]])
    val array = key.bcjk.getKeysArray
    for (i <- 0 until array.getLength) {
      tempSet.add(key.parquetFormatConverter.map(converter => converter(array.get(i))).
        getOrElse(array.get(i)).asInstanceOf[T])
    }
    tempSet
  }


  def getNavigableSet[T <: Comparable[T]](
      bcVar: BroadcastedJoinKeysWrapper,
      column: Column[T],
      catalystToParquetFormatConverter: Option[Converter[T]]): util.NavigableSet[T] = {
    val key = KeyIdempot(bcVar, column.getColumnType, catalystToParquetFormatConverter)
    idempotentializer.get(key).asInstanceOf[util.NavigableSet[T]]
  }

  def removeBroadcast(id: Long): Unit = {
    import scala.jdk.CollectionConverters._
    idempotentializer.asMap.asScala.keySet.filter(_.bcjk.getBroadcastVarId == id)
      .foreach(idempotentializer.invalidate)
  }

  def invalidateBroadcastCache(): Unit = {
    idempotentializer.invalidateAll()
  }
}


object ParquetJavaDataTypeComparators {
  import java.lang.{Double => JDouble, Float => JFloat, Long => JLong}
  val COMPARATORS: Map[Class[_], Comparator[_]] = Map(
    classOf[Integer] -> Comparator.naturalOrder[Integer](),
    classOf[JLong] -> Comparator.naturalOrder[JLong](),
    classOf[JFloat] -> Comparator.naturalOrder[JFloat](),
    classOf[JDouble] -> Comparator.naturalOrder[JDouble](),
    classOf[String] -> CharSeqComparator
  )
}


private object CharSeqComparator extends Comparator[CharSequence] {
  /**
   * Java character supports only upto 3 byte UTF-8 characters. 4 byte UTF-8 character is
   * represented using two Java characters (using UTF-16 surrogate pairs). Character by character
   * comparison may yield incorrect results while comparing a 4 byte UTF-8 character to a java
   * char. Character by character comparison works as expected if both characters are <= 3 byte
   * UTF-8 character or both characters are 4 byte UTF-8 characters.
   * isCharInUTF16HighSurrogateRange method detects a 4-byte character and considers that
   * character to be lexicographically greater than any 3 byte or lower UTF-8 character.
   */
  override def compare(s1: CharSequence, s2: CharSequence): Int = {
    if (s1 eq s2) return 0
    val len = Math.min(s1.length, s2.length)
    // find the first difference and return
    for (i <- 0 until len) {
      val c1 = s1.charAt(i)
      val c2 = s2.charAt(i)
      val isC1HighSurrogate = isCharHighSurrogate(c1)
      val isC2HighSurrogate = isCharHighSurrogate(c2)
      if (isC1HighSurrogate && !isC2HighSurrogate) return 1
      if (!isC1HighSurrogate && isC2HighSurrogate) return -1
      val cmp = Character.compare(c1, c2)
      if (cmp != 0) return cmp
    }
    // if there are no differences, then the shorter seq is first
    Integer.compare(s1.length, s2.length)
  }


  /**
   * Determines if the given character value is a unicode high-surrogate code unit. The range of
   * high-surrogates is 0xD800 - 0xDBFF.
   */
  def isCharHighSurrogate(ch: Char): Boolean = {
    // scalastyle:off nonascii
    (ch & '\uFC00') == '\uD800' // 0xDC00 - 0xDFFF shouldn't match
    // scalastyle:on nonascii
  }

}

private case class KeyIdempot (
    bcjk: BroadcastedJoinKeysWrapper,
    comparatorClass: Class[_],
    parquetFormatConverter: Option[Converter[_]]) {

  override def equals(other: Any): Boolean = {
    if (other != null) {
      other match {
        case key: KeyIdempot => this.bcjk == key.bcjk

        case _ => false
      }
    } else {
      false
    }
  }

  override def hashCode(): Int = {
    this.bcjk.hashCode()
  }
}
