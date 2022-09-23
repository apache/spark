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

package org.apache.spark.sql.catalyst.util

import org.jcodings.specific.UTF8Encoding
import org.joni.{Matcher, Option, Regex}
import scala.collection.mutable.ArrayBuffer

object RegexUtils {
  def group(m: Matcher, group: Int, value: Array[Byte]): Array[Byte] = {
    if (0 == group) {
      val length = m.getEnd() - m.getBegin()
      val b = new Array[Byte](length)
      Array.copy(value, m.getBegin(), b, 0, length)
      b
    } else {
      val region = m.getRegion()
      val length = region.end(group) - region.beg(group)
      val b = new Array[Byte](length)
      Array.copy(value, region.beg(group), b, 0, length)
      b
    }
  }

  def replaceAll(
                  srcBytes: Array[Byte],
                  pattern: Array[Byte],
                  replaceBytes: Array[Byte]): Array[Byte] = {
    replaceAll(srcBytes, 0, srcBytes.length, replaceBytes, 0, replaceBytes.length, pattern)
  }

  def replaceAll(
                  srcBytes: Array[Byte],
                  srcOffset: Int,
                  srcLen: Int,
                  replaceBytes: Array[Byte],
                  replaceOffset: Int,
                  replaceLen: Int,
                  pattern: Array[Byte]): Array[Byte] = {
    case class PairInt(begin: Int, end: Int)

    val srcRange: Int = srcOffset + srcLen
    val regex: Regex = new Regex(pattern, 0, pattern.length, Option.NONE, UTF8Encoding.INSTANCE)
    val matcher: Matcher = regex.matcher(srcBytes, 0, srcRange)
    var cur: Int = srcOffset
    val searchResults: ArrayBuffer[PairInt] = new ArrayBuffer[PairInt]
    var totalBytesNeeded: Int = 0
    var flag = true
    while (flag) {
      val nextCur: Int = matcher.search(cur, srcRange, Option.DEFAULT)
      if (nextCur < 0) {
        totalBytesNeeded += srcRange - cur
        flag = false
      }

      if (flag) {
        searchResults += new PairInt(matcher.getBegin, matcher.getEnd)
        totalBytesNeeded += (nextCur - cur) + replaceLen
        cur = matcher.getEnd
      }
    }
    val ret: Array[Byte] = new Array[Byte](totalBytesNeeded)
    var curPosInSrc: Int = srcOffset
    var curPosInRet: Int = 0
    for (pair <- searchResults) {
      Array.copy(srcBytes, curPosInSrc, ret, curPosInRet, pair.begin - curPosInSrc)
      curPosInRet += pair.begin - curPosInSrc
      Array.copy(replaceBytes, replaceOffset, ret, curPosInRet, replaceLen)
      curPosInRet += replaceLen
      curPosInSrc = pair.end
    }
    Array.copy(srcBytes, curPosInSrc, ret, curPosInRet, srcRange - curPosInSrc)
    ret
  }
}