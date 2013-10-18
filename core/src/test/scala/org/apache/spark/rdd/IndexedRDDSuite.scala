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


import org.scalatest.FunSuite
import org.scalatest.prop.Checkers
import org.scalacheck.Arbitrary._
import org.scalacheck.Gen
import org.scalacheck.Prop._

import com.google.common.io.Files

import scala.collection.mutable.ArrayBuffer
import scala.collection.mutable.HashSet

import org.apache.spark.rdd.RDD
import org.apache.spark.rdd.ShuffledRDD
import org.apache.spark.rdd.IndexedRDD

import org.apache.spark.SparkContext._
import org.apache.spark._



class IndexedRDDSuite extends FunSuite with SharedSparkContext {

  def lineage(rdd: RDD[_]): collection.mutable.HashSet[RDD[_]] = {
    val set = new collection.mutable.HashSet[RDD[_]]
    def visit(rdd: RDD[_]) {
      for (dep <- rdd.dependencies) {
        set += dep.rdd
        visit(dep.rdd)
      }
    }
    visit(rdd)
    set
  }  

  test("groupByKey") {
    val pairs = sc.parallelize(Array((1, 1), (1, 2), (1, 3), (2, 1))).indexed()
    val groups = pairs.groupByKey().collect()
    assert(groups.size === 2)
    val valuesFor1 = groups.find(_._1 == 1).get._2
    assert(valuesFor1.toList.sorted === List(1, 2, 3))
    val valuesFor2 = groups.find(_._1 == 2).get._2
    assert(valuesFor2.toList.sorted === List(1))
  }

  test("groupByKey with duplicates") {
    val pairs = sc.parallelize(Array((1, 1), (1, 2), (1, 3), (1, 1), (2, 1))).indexed()
    val groups = pairs.groupByKey().collect()
    assert(groups.size === 2)
    val valuesFor1 = groups.find(_._1 == 1).get._2
    assert(valuesFor1.toList.sorted === List(1, 1, 2, 3))
    val valuesFor2 = groups.find(_._1 == 2).get._2
    assert(valuesFor2.toList.sorted === List(1))
  }

  test("groupByKey with negative key hash codes") {
    val pairs = sc.parallelize(Array((-1, 1), (-1, 2), (-1, 3), (2, 1))).indexed()
    val groups = pairs.groupByKey().collect()
    assert(groups.size === 2)
    val valuesForMinus1 = groups.find(_._1 == -1).get._2
    assert(valuesForMinus1.toList.sorted === List(1, 2, 3))
    val valuesFor2 = groups.find(_._1 == 2).get._2
    assert(valuesFor2.toList.sorted === List(1))
  }

  test("groupByKey with many output partitions") {
    val pairs = sc.parallelize(Array((1, 1), (1, 2), (1, 3), (2, 1))).indexed(10)
    val groups = pairs.groupByKey().collect()
    assert(groups.size === 2)
    val valuesFor1 = groups.find(_._1 == 1).get._2
    assert(valuesFor1.toList.sorted === List(1, 2, 3))
    val valuesFor2 = groups.find(_._1 == 2).get._2
    assert(valuesFor2.toList.sorted === List(1))
  }

  test("reduceByKey") {
    val pairs = sc.parallelize(Array((1, 1), (1, 2), (1, 3), (1, 1), (2, 1))).indexed()
    val sums = pairs.reduceByKey(_+_).collect()
    assert(sums.toSet === Set((1, 7), (2, 1)))
  }

  test("reduceByKey with collectAsMap") {
    val pairs = sc.parallelize(Array((1, 1), (1, 2), (1, 3), (1, 1), (2, 1))).indexed()
    val sums = pairs.reduceByKey(_+_).collectAsMap()
    assert(sums.size === 2)
    assert(sums(1) === 7)
    assert(sums(2) === 1)
  }

  test("reduceByKey with many output partitons") {
    val pairs = sc.parallelize(Array((1, 1), (1, 2), (1, 3), (1, 1), (2, 1))).indexed(10)
    val sums = pairs.reduceByKey(_+_).collect()
    assert(sums.toSet === Set((1, 7), (2, 1)))
  }

  test("reduceByKey with partitioner") {
    val p = new Partitioner() {
      def numPartitions = 2
      def getPartition(key: Any) = key.asInstanceOf[Int]
    }
    val pairs = sc.parallelize(Array((1, 1), (1, 2), (1, 1), (0, 1))).indexed(p)
    val sums = pairs.reduceByKey(_+_)
    assert(sums.collect().toSet === Set((1, 4), (0, 1)))
    assert(sums.partitioner === Some(p))
    // count the dependencies to make sure there is only 1 ShuffledRDD
    val deps = lineage(sums)
    
    assert(deps.filter(_.isInstanceOf[ShuffledRDD[_,_,_]]).size === 1) // ShuffledRDD, ParallelCollection
  }



  test("joinIndexVsPair") {
    val rdd1 = sc.parallelize(Array((1, 1), (1, 2), (2, 1), (3, 1))).indexed()
    val rdd2 = sc.parallelize(Array((1, 'x'), (2, 'y'), (2, 'z'), (4, 'w')))
    val joined = rdd1.join(rdd2).collect()
    assert(joined.size === 4)
    assert(joined.toSet === Set(
      (1, (1, 'x')),
      (1, (2, 'x')),
      (2, (1, 'y')),
      (2, (1, 'z'))
    ))
  }

  test("joinIndexVsIndex") {
    val rdd1 = sc.parallelize(Array((1, 1), (1, 2), (2, 1), (3, 1))).indexed()
    val rdd2 = sc.parallelize(Array((1, 'x'), (2, 'y'), (2, 'z'), (4, 'w'))).indexed()
    val joined = rdd1.join(rdd2).collect()
    assert(joined.size === 4)
    assert(joined.toSet === Set(
      (1, (1, 'x')),
      (1, (2, 'x')),
      (2, (1, 'y')),
      (2, (1, 'z'))
    ))
  }

  test("joinSharedIndex") {
    val rdd1 = sc.parallelize(Array((1, 1), (1, 2), (2, 1), (3, 1), (4,-4), (4, 4) )).indexed()
    val rdd2 = sc.parallelize(Array((1, 'x'), (2, 'y'), (2, 'z'), (4, 'w'))).indexed(rdd1.index)
    val joined = rdd1.join(rdd2).collect()
    assert(joined.size === 6)
    assert(joined.toSet === Set(
      (1, (1, 'x')),
      (1, (2, 'x')),
      (2, (1, 'y')),
      (2, (1, 'z')),
      (4, (-4, 'w')),
      (4, (4, 'w'))
    ))
  }


  test("join all-to-all") {
    val rdd1 = sc.parallelize(Array((1, 1), (1, 2), (1, 3))).indexed()
    val rdd2 = sc.parallelize(Array((1, 'x'), (1, 'y'))).indexed(rdd1.index)
    val joined = rdd1.join(rdd2).collect()
    assert(joined.size === 6)
    assert(joined.toSet === Set(
      (1, (1, 'x')),
      (1, (1, 'y')),
      (1, (2, 'x')),
      (1, (2, 'y')),
      (1, (3, 'x')),
      (1, (3, 'y'))
    ))
  }

  test("leftOuterJoinIndex") {
    val index = sc.parallelize( 1 to 6 ).makeIndex()
    val rdd1 = sc.parallelize(Array((1, 1), (1, 2), (2, 1), (3, 1))).indexed(index)
    val rdd2 = sc.parallelize(Array((1, 'x'), (2, 'y'), (2, 'z'), (4, 'w')))
    val joined = rdd1.leftOuterJoin(rdd2).collect()
    assert(joined.size === 5)
    assert(joined.toSet === Set(
      (1, (1, Some('x'))),
      (1, (2, Some('x'))),
      (2, (1, Some('y'))),
      (2, (1, Some('z'))),
      (3, (1, None))
    ))
  }

  test("leftOuterJoinIndextoIndex") {
    val rdd1 = sc.parallelize(Array((1, 1), (1, 2), (2, 1), (3, 1))).indexed()
    val rdd2 = sc.parallelize(Array((1, 'x'), (2, 'y'), (2, 'z'), (4, 'w'))).indexed()
    val joined = rdd1.leftOuterJoin(rdd2).collect()
    assert(joined.size === 5)
    assert(joined.toSet === Set(
      (1, (1, Some('x'))),
      (1, (2, Some('x'))),
      (2, (1, Some('y'))),
      (2, (1, Some('z'))),
      (3, (1, None))
    ))
  }

  test("leftOuterJoinIndextoSharedIndex") {
    val rdd1 = sc.parallelize(Array((1, 1), (1, 2), (2, 1), (3, 1), (4, -4))).indexed()
    val rdd2 = sc.parallelize(Array((1, 'x'), (2, 'y'), (2, 'z'), (4, 'w'))).indexed(rdd1.index)
    val joined = rdd1.leftOuterJoin(rdd2).collect()
    assert(joined.size === 6)
    assert(joined.toSet === Set(
      (1, (1, Some('x'))),
      (1, (2, Some('x'))),
      (2, (1, Some('y'))),
      (2, (1, Some('z'))),
      (4, (-4, Some('w'))),
      (3, (1, None))
    ))
  }

test("leftOuterJoinIndextoIndexExternal") {
    val index = sc.parallelize( 1 to 6 ).makeIndex()
    val rdd1 = sc.parallelize(Array((1, 1), (1, 2), (2, 1), (3, 1))).indexed(index)
    val rdd2 = sc.parallelize(Array((1, 'x'), (2, 'y'), (2, 'z'), (4, 'w'))).indexed(index)
    val joined = rdd1.leftOuterJoin(rdd2).collect()
    assert(joined.size === 5)
    assert(joined.toSet === Set(
      (1, (1, Some('x'))),
      (1, (2, Some('x'))),
      (2, (1, Some('y'))),
      (2, (1, Some('z'))),
      (3, (1, None))
    ))
  }


  test("rightOuterJoin") {
    val index = sc.parallelize( 1 to 6 ).makeIndex()
    val rdd1 = sc.parallelize(Array((1, 1), (1, 2), (2, 1), (3, 1))).indexed(index)
    val rdd2 = sc.parallelize(Array((1, 'x'), (2, 'y'), (2, 'z'), (4, 'w')))
    val joined = rdd1.rightOuterJoin(rdd2).collect()
    assert(joined.size === 5)
    assert(joined.toSet === Set(
      (1, (Some(1), 'x')),
      (1, (Some(2), 'x')),
      (2, (Some(1), 'y')),
      (2, (Some(1), 'z')),
      (4, (None, 'w'))
    ))
  }

  test("rightOuterJoinIndex2Index") {
    val rdd1 = sc.parallelize(Array((1, 1), (1, 2), (2, 1), (3, 1))).indexed()
    val rdd2 = sc.parallelize(Array((1, 'x'), (2, 'y'), (2, 'z'), (4, 'w'))).indexed()
    val joined = rdd1.rightOuterJoin(rdd2).collect()
    assert(joined.size === 5)
    assert(joined.toSet === Set(
      (1, (Some(1), 'x')),
      (1, (Some(2), 'x')),
      (2, (Some(1), 'y')),
      (2, (Some(1), 'z')),
      (4, (None, 'w'))
    ))
  }


  test("rightOuterJoinIndex2Indexshared") {
    val index = sc.parallelize( 1 to 6 ).makeIndex()
    val rdd1 = sc.parallelize(Array((1, 1), (1, 2), (2, 1), (3, 1))).indexed(index)
    val rdd2 = sc.parallelize(Array((1, 'x'), (2, 'y'), (2, 'z'), (4, 'w'))).indexed(index)
    val joined = rdd1.rightOuterJoin(rdd2).collect()
    assert(joined.size === 5)
    assert(joined.toSet === Set(
      (1, (Some(1), 'x')),
      (1, (Some(2), 'x')),
      (2, (Some(1), 'y')),
      (2, (Some(1), 'z')),
      (4, (None, 'w'))
    ))
  }


  test("join with no matches index") {
    val index = IndexedRDD.makeIndex( sc.parallelize( 1 to 6 ) )
    val rdd1 = sc.parallelize(Array((1, 1), (1, 2), (2, 1), (3, 1))).indexed(index)
    val rdd2 = sc.parallelize(Array((4, 'x'), (5, 'y'), (5, 'z'), (6, 'w')))
    val joined = rdd1.join(rdd2).collect()
    assert(joined.size === 0)
  }

  test("join with no matches shared index") {
    val index = IndexedRDD.makeIndex( sc.parallelize( 1 to 6 ) )
    val rdd1 = sc.parallelize(Array((1, 1), (1, 2), (2, 1), (3, 1))).indexed(index)
    val rdd2 = sc.parallelize(Array((4, 'x'), (5, 'y'), (5, 'z'), (6, 'w'))).indexed(index)
    val joined = rdd1.join(rdd2).collect()
    assert(joined.size === 0)
  }


  test("join with many output partitions") {
    val rdd1 = sc.parallelize(Array((1, 1), (1, 2), (2, 1), (3, 1))).indexed(10)
    val rdd2 = sc.parallelize(Array((1, 'x'), (2, 'y'), (2, 'z'), (4, 'w')))
    val joined = rdd1.join(rdd2).collect()
    assert(joined.size === 4)
    assert(joined.toSet === Set(
      (1, (1, 'x')),
      (1, (2, 'x')),
      (2, (1, 'y')),
      (2, (1, 'z'))
    ))
  }

  test("join with many output partitions and two indices") {
    val rdd1 = sc.parallelize(Array((1, 1), (1, 2), (2, 1), (3, 1))).indexed(10)
    val rdd2 = sc.parallelize(Array((1, 'x'), (2, 'y'), (2, 'z'), (4, 'w'))).indexed(20)
    val joined = rdd1.join(rdd2).collect()
    assert(joined.size === 4)
    assert(joined.toSet === Set(
      (1, (1, 'x')),
      (1, (2, 'x')),
      (2, (1, 'y')),
      (2, (1, 'z'))
    ))
  }


  test("groupWith") {
    val index = IndexedRDD.makeIndex( sc.parallelize( 1 to 6 ) )

    val rdd1 = sc.parallelize(Array((1, 1), (1, 2), (2, 1), (3, 1))).indexed(index)
    val rdd2 = sc.parallelize(Array((1, 'x'), (2, 'y'), (2, 'z'), (4, 'w'))).indexed(index)
    val joined = rdd1.groupWith(rdd2).collect()
    assert(joined.size === 4)
    assert(joined.toSet === Set(
      (1, (ArrayBuffer(1, 2), ArrayBuffer('x'))),
      (2, (ArrayBuffer(1), ArrayBuffer('y', 'z'))),
      (3, (ArrayBuffer(1), ArrayBuffer())),
      (4, (ArrayBuffer(), ArrayBuffer('w')))
    ))
  }

  test("zero-partition RDD") {
    val emptyDir = Files.createTempDir()
    val file = sc.textFile(emptyDir.getAbsolutePath)
    assert(file.partitions.size == 0)
    assert(file.collect().toList === Nil)
    // Test that a shuffle on the file works, because this used to be a bug
    assert(file.map(line => (line, 1)).reduceByKey(_ + _).collect().toList === Nil)
  }

  test("keys and values") {
    val rdd = sc.parallelize(Array((1, "a"), (2, "b"))).indexed()
    assert(rdd.keys.collect().toList === List(1, 2))
    assert(rdd.values.collect().toList === List("a", "b"))
  }

  test("default partitioner uses partition size") {
    // specify 2000 partitions
    val a = sc.makeRDD(Array(1, 2, 3, 4), 2000)
    // do a map, which loses the partitioner
    val b = a.map(a => (a, (a * 2).toString))
    // then a group by, and see we didn't revert to 2 partitions
    val c = b.groupByKey()
    assert(c.partitions.size === 2000)
  }

  // test("default partitioner uses largest partitioner indexed to indexed") {
  //   val a = sc.makeRDD(Array((1, "a"), (2, "b")), 2).indexed()
  //   val b = sc.makeRDD(Array((1, "a"), (2, "b")), 2000).indexed()
  //   val c = a.join(b)
  //   assert(c.partitions.size === 2000)
  // }



  test("subtract") {
    val a = sc.parallelize(Array(1, 2, 3), 2)
    val b = sc.parallelize(Array(2, 3, 4), 4)
    val c = a.subtract(b)
    assert(c.collect().toSet === Set(1))
    assert(c.partitions.size === a.partitions.size)
  }

  test("subtract with narrow dependency") {
    // use a deterministic partitioner
    val p = new Partitioner() {
      def numPartitions = 5
      def getPartition(key: Any) = key.asInstanceOf[Int]
    }
    // partitionBy so we have a narrow dependency
    val a = sc.parallelize(Array((1, "a"), (2, "b"), (3, "c"))).indexed(p)
    // more partitions/no partitioner so a shuffle dependency
    val b = sc.parallelize(Array((2, "b"), (3, "cc"), (4, "d")), 4)
    val c = a.subtract(b)
    assert(c.collect().toSet === Set((1, "a"), (3, "c")))
    // Ideally we could keep the original partitioner...
    assert(c.partitioner === None)
  }

  test("subtractByKey") {

    val a = sc.parallelize(Array((1, "a"), (1, "a"), (2, "b"), (3, "c")), 2).indexed()
    val b = sc.parallelize(Array((2, 20), (3, 30), (4, 40)), 4)
    val c = a.subtractByKey(b)
    assert(c.collect().toSet === Set((1, "a"), (1, "a")))
    assert(c.partitions.size === a.partitions.size)
  }

  // test("subtractByKey with narrow dependency") {
  //   // use a deterministic partitioner
  //   val p = new Partitioner() {
  //     def numPartitions = 5
  //     def getPartition(key: Any) = key.asInstanceOf[Int]
  //   }

  //   val index = sc.parallelize( 1 to 6 ).makeIndex(Some(p))
  //   // partitionBy so we have a narrow dependency
  //   val a = sc.parallelize(Array((1, "a"), (1, "a"), (2, "b"), (3, "c"))).indexed(index)
  //   // more partitions/no partitioner so a shuffle dependency
  //   val b = sc.parallelize(Array((2, "b"), (3, "cc"), (4, "d")), 4).indexed(index)
  //   val c = a.subtractByKey(b)
  //   assert(c.collect().toSet === Set((1, "a"), (1, "a")))
  //   assert(c.partitioner.get === p)
  // }

  test("foldByKey") {
    val index = IndexedRDD.makeIndex( sc.parallelize( 1 to 6 ) )
    val pairs = sc.parallelize(Array((1, 1), (1, 2), (1, 3), (1, 1), (2, 1))).indexed(index)
    val sums = pairs.foldByKey(0)(_+_).collect()
    assert(sums.toSet === Set((1, 7), (2, 1)))
  }

  test("foldByKey with mutable result type") {
    val index = IndexedRDD.makeIndex( sc.parallelize( 1 to 6 ) )

    val pairs = sc.parallelize(Array((1, 1), (1, 2), (1, 3), (1, 1), (2, 1))).indexed(index)
    val bufs = pairs.mapValues(v => ArrayBuffer(v)).cache()
    // Fold the values using in-place mutation
    val sums = bufs.foldByKey(new ArrayBuffer[Int])(_ ++= _).collect()
    assert(sums.toSet === Set((1, ArrayBuffer(1, 2, 3, 1)), (2, ArrayBuffer(1))))
    // Check that the mutable objects in the original RDD were not changed
    assert(bufs.collect().toSet === Set(
      (1, ArrayBuffer(1)),
      (1, ArrayBuffer(2)),
      (1, ArrayBuffer(3)),
      (1, ArrayBuffer(1)),
      (2, ArrayBuffer(1))))
  }
}
