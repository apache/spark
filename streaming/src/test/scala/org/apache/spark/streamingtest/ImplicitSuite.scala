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

package org.apache.spark.streamingtest

/**
 * A test suite to make sure all `implicit` functions work correctly.
 *
 * As `implicit` is a compiler feature, we don't need to run this class.
 * What we need to do is making the compiler happy.
 */
class ImplicitSuite {

  // We only want to test if `implict` works well with the compiler, so we don't need a real DStream.
  def mockDStream[T]: org.apache.spark.streaming.dstream.DStream[T] = null

  def testToPairDStreamFunctions(): Unit = {
    val dstream: org.apache.spark.streaming.dstream.DStream[(Int, Int)] = mockDStream
    dstream.groupByKey()
  }
}
