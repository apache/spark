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

package org.apache.spark.bagel

import org.scalatest.{BeforeAndAfter, FunSuite, Assertions}
import org.scalatest.concurrent.Timeouts
import org.scalatest.time.SpanSugar._

import org.apache.spark._
import org.apache.spark.storage.StorageLevel

class TestVertex(val active: Boolean, val age: Int) extends Vertex with Serializable
class TestMessage(val targetId: String) extends Message[String] with Serializable

class BagelSuite extends FunSuite with Assertions with BeforeAndAfter with Timeouts {
  
  var sc: SparkContext = _
  
  after {
    if (sc != null) {
      sc.stop()
      sc = null
    }
    // To avoid Akka rebinding to the same port, since it doesn't unbind immediately on shutdown
    System.clearProperty("spark.driver.port")
    System.clearProperty("spark.hostPort")
  }

  test("halting by voting") {
    sc = new SparkContext("local", "test")
    val verts = sc.parallelize(Array("a", "b", "c", "d").map(id => (id, new TestVertex(true, 0))))
    val msgs = sc.parallelize(Array[(String, TestMessage)]())
    val numSupersteps = 5
    val result =
      Bagel.run(sc, verts, msgs, sc.defaultParallelism) {
        (self: TestVertex, msgs: Option[Array[TestMessage]], superstep: Int) =>
          (new TestVertex(superstep < numSupersteps - 1, self.age + 1), Array[TestMessage]())
      }
    for ((id, vert) <- result.collect) {
      assert(vert.age === numSupersteps)
    }
  }

  test("halting by message silence") {
    sc = new SparkContext("local", "test")
    val verts = sc.parallelize(Array("a", "b", "c", "d").map(id => (id, new TestVertex(false, 0))))
    val msgs = sc.parallelize(Array("a" -> new TestMessage("a")))
    val numSupersteps = 5
    val result =
      Bagel.run(sc, verts, msgs, sc.defaultParallelism) {
        (self: TestVertex, msgs: Option[Array[TestMessage]], superstep: Int) =>
          val msgsOut =
            msgs match {
              case Some(ms) if (superstep < numSupersteps - 1) =>
                ms
              case _ =>
                Array[TestMessage]()
            }
        (new TestVertex(self.active, self.age + 1), msgsOut)
      }
    for ((id, vert) <- result.collect) {
      assert(vert.age === numSupersteps)
    }
  }

  test("large number of iterations") {
    // This tests whether jobs with a large number of iterations finish in a reasonable time,
    // because non-memoized recursion in RDD or DAGScheduler used to cause them to hang
    failAfter(10 seconds) {
      sc = new SparkContext("local", "test")
      val verts = sc.parallelize((1 to 4).map(id => (id.toString, new TestVertex(true, 0))))
      val msgs = sc.parallelize(Array[(String, TestMessage)]())
      val numSupersteps = 50
      val result =
        Bagel.run(sc, verts, msgs, sc.defaultParallelism) {
          (self: TestVertex, msgs: Option[Array[TestMessage]], superstep: Int) =>
            (new TestVertex(superstep < numSupersteps - 1, self.age + 1), Array[TestMessage]())
        }
      for ((id, vert) <- result.collect) {
        assert(vert.age === numSupersteps)
      }
    }
  }

  test("using non-default persistence level") {
    failAfter(10 seconds) {
      sc = new SparkContext("local", "test")
      val verts = sc.parallelize((1 to 4).map(id => (id.toString, new TestVertex(true, 0))))
      val msgs = sc.parallelize(Array[(String, TestMessage)]())
      val numSupersteps = 50
      val result =
        Bagel.run(sc, verts, msgs, sc.defaultParallelism, StorageLevel.DISK_ONLY) {
          (self: TestVertex, msgs: Option[Array[TestMessage]], superstep: Int) =>
            (new TestVertex(superstep < numSupersteps - 1, self.age + 1), Array[TestMessage]())
        }
      for ((id, vert) <- result.collect) {
        assert(vert.age === numSupersteps)
      }
    }
  }
}
