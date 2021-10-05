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

package org.apache.spark.shuffle

import java.util.Random

import org.apache.commons.lang3.StringUtils

object TestUdfs {
  val random = new Random()

  val testValues = (1 to 1000).map(n => {
    StringUtils.repeat('a', random.nextInt(n))
  }).toList

  def intToString(intValue: Int): String = {
    "%09d".format(intValue)
  }

  def generateString(size: Int): String = {
    StringUtils.repeat('a', size)
  }

  def randomString(): String = {
    val index = random.nextInt(testValues.size)
    testValues(index)
  }
}
