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
package org.apache.spark.rdd

import java.util

import org.apache.hadoop.fs.Path
import org.apache.hadoop.mapred.FileSplit
import org.scalatest.{Matchers, FunSuite}

import scala.collection.JavaConverters._

class HadoopRDDSuite extends FunSuite with Matchers {
  test("file split ordering") {
    val splits = (0 until 10).map{idx =>
      new FileSplit(new Path("/foo/bar/part-0000" + idx), 0l, 0l, Array[String]())}

    val javaShuffledSplits = new util.ArrayList[FileSplit]()
    splits.foreach{s => javaShuffledSplits.add(s)}
    java.util.Collections.shuffle(javaShuffledSplits)
    val scalaShuffledSplits = javaShuffledSplits.asScala
    scalaShuffledSplits.sorted(SplitOrdering) should be (splits)
  }
}
