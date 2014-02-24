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
package org.apache.spark.storage

import org.scalatest.FunSuite
import org.apache.spark.{LocalSparkContext, SparkContext}
import org.apache.commons.io.FileUtils
import java.io.File

class Expander(base:String, count:Int) extends Iterator[String] {
  var i = 0;
  def next() : String = {
    i += 1;
    return base + i.toString;
  }
  def hasNext() : Boolean = i < count;
}

object Expander {
  def expand(s:String, i:Int) : Iterator[String] = {
    return new Expander(s,i)
  }
}

class LargeIteratorSuite extends FunSuite with LocalSparkContext {
  /* Tests the ability of Spark to deal with user provided iterators that
   * generate more data then available memory. In any memory based persistance
   * Spark will unroll the iterator into an ArrayBuffer for caching, however in
   * the case that the use defines DISK_ONLY persistance, the iterator will be 
   * fed directly to the serializer and written to disk.
   */
  val clusterUrl = "local-cluster[1,1,512]"
  test("Flatmap iterator") {
    sc = new SparkContext(clusterUrl, "mem_test");
    val seeds = sc.parallelize( Array(
      "This is the first sentence that we will test:",
      "This is the second sentence that we will test:",
      "This is the third sentence that we will test:"
    ) );
    val out = seeds.flatMap(Expander.expand(_,10000000));
    out.map(_ + "...").persist(StorageLevel.DISK_ONLY).saveAsTextFile("./test.out")
    FileUtils.deleteDirectory(new File("./test.out"))
  }
}
