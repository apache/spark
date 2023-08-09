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

package org.apache.spark.scheduler

import scala.collection.mutable.ArrayBuffer

import org.apache.spark.annotation.DeveloperApi

// information about a specific split instance : handles both split instances.
// So that we do not need to worry about the differences.
@DeveloperApi
class SplitInfo(
    val inputFormatClazz: Class[_],
    val hostLocation: String,
    val path: String,
    val length: Long,
    val underlyingSplit: Any) {
  override def toString(): String = {
    "SplitInfo " + super.toString + " .. inputFormatClazz " + inputFormatClazz +
      ", hostLocation : " + hostLocation + ", path : " + path +
      ", length : " + length + ", underlyingSplit " + underlyingSplit
  }

  override def hashCode(): Int = {
    var hashCode = inputFormatClazz.hashCode
    hashCode = hashCode * 31 + hostLocation.hashCode
    hashCode = hashCode * 31 + path.hashCode
    // ignore overflow ? It is hashcode anyway !
    hashCode = hashCode * 31 + (length & 0x7fffffff).toInt
    hashCode
  }

  // This is practically useless since most of the Split impl's don't seem to implement equals :-(
  // So unless there is identity equality between underlyingSplits, it will always fail even if it
  // is pointing to same block.
  override def equals(other: Any): Boolean = other match {
    case that: SplitInfo =>
      this.hostLocation == that.hostLocation &&
        this.inputFormatClazz == that.inputFormatClazz &&
        this.path == that.path &&
        this.length == that.length &&
        // other split specific checks (like start for FileSplit)
        this.underlyingSplit == that.underlyingSplit
    case _ => false
  }
}

object SplitInfo {

  def toSplitInfo(inputFormatClazz: Class[_], path: String,
                  mapredSplit: org.apache.hadoop.mapred.InputSplit): Seq[SplitInfo] = {
    val retval = new ArrayBuffer[SplitInfo]()
    val length = mapredSplit.getLength
    for (host <- mapredSplit.getLocations) {
      retval += new SplitInfo(inputFormatClazz, host, path, length, mapredSplit)
    }
    retval.toSeq
  }

  def toSplitInfo(inputFormatClazz: Class[_], path: String,
                  mapreduceSplit: org.apache.hadoop.mapreduce.InputSplit): Seq[SplitInfo] = {
    val retval = new ArrayBuffer[SplitInfo]()
    val length = mapreduceSplit.getLength
    for (host <- mapreduceSplit.getLocations) {
      retval += new SplitInfo(inputFormatClazz, host, path, length, mapreduceSplit)
    }
    retval.toSeq
  }
}
