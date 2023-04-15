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

import scala.reflect.ClassTag

object HiveUtils {
   def convertActualToExpected[T](mappingActualStr: String, actualSeq: Seq[T]): Seq[T] = {
     val mappingActualPos = convertMappingStringToArray(mappingActualStr)
     mappingActualPos.map(i => actualSeq(i))
   }

  private def convertMappingStringToArray(mappingStr: String): Array[Int] =
    mappingStr.split(',').map(_.toInt)

  def convertExpectedToActual[T: ClassTag](mappingActualStr: String, expectedSeq: Seq[T]): Seq[T] =
  {
    val mappingActualPos = convertMappingStringToArray(mappingActualStr)
    val actual = Array.ofDim[T](expectedSeq.length)
    for (i <- 0 until mappingActualPos.length) {
      val ele = expectedSeq(i)
      val actualPos = mappingActualPos(i)
      actual(actualPos) = ele
    }
    actual.toSeq
  }

}
