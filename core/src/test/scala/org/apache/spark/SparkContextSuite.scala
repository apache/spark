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

package org.apache.spark

import org.scalatest.FunSuite

import org.apache.hadoop.io.BytesWritable

class SparkContextSuite extends FunSuite {
  test("test of writing spark scala test") {
    val bytesWritable = new BytesWritable()
    bytesWritable.set((1 to 10).map(_.toByte).toArray, 0, 10)
    bytesWritable.set((1 to 5).map(_.toByte).toArray, 0, 5)

    val converter = SparkContext.bytesWritableConverter()
    val byteArray = converter.convert(bytesWritable)
    assert(byteArray.length === 5)
  }
}
