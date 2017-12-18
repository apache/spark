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

package org.apache.spark.util

import java.io.NotSerializableException

import org.apache.spark.{SparkContext, SparkException, SparkFunSuite, TaskContext}
import org.apache.spark.LocalSparkContext._
import org.apache.spark.partial.CountEvaluator
import org.apache.spark.rdd.RDD

class ClosureCleanerSuite extends SparkFunSuite {
  test("closures inside an object") {
    assert(TestObject.run() === 30) // 6 + 7 + 8 + 9
  }

  test("closures inside a class") {
    val obj = new TestClass
    assert(obj.run() === 30) // 6 + 7 + 8 + 9
  }

  test("closures inside a class with no default constructor") {
    val obj = new TestClassWithoutDefaultConstructor(5)
    assert(obj.run() === 30) // 6 + 7 + 8 + 9
  }

  test("closures that don't use fields of the outer class") {
    val obj = new TestClassWithoutFieldAccess
    assert(obj.run() === 30) // 6 + 7 + 8 + 9
  }

  test("nested closures inside an object") {
    assert(TestObjectWithNesting.run() === 96) // 4 * (1+2+3+4) + 4 * (1+2+3+4) + 16 * 1
  }

  test("nested closures inside a class") {
    val obj = new TestClassWithNesting(1)
    assert(obj.run() === 96) // 4 * (1+2+3+4) + 4 * (1+2+3+4) + 16 * 1
  }

  test("toplevel return statements in closures are identified at cleaning time") {
    intercept[ReturnStatementInClosureException] {
      TestObjectWithBogusReturns.run()
    }
  }

  test("return statements from named functions nested in closures don't raise exceptions") {
    val result = TestObjectWithNestedReturns.run()
    assert(result === 1)
  }

  test("user provided closures are actually cleaned") {

    // We use return statements as an indication that a closure is actually being cleaned
    // We expect closure cleaner to find the return statements in the user provided closures
    def expectCorrectException(body: => Unit): Unit = {
      try {
        body
      } catch {
        case rse: ReturnStatementInClosureException => // Success!
        case e @ (_: NotSerializableException | _: SparkException) =>
          fail(s"Expected ReturnStatementInClosureException, but got $e.\n" +
            "This means the closure provided by user is not actually cleaned.")
      }
    }

    withSpark(new SparkContext("local", "test")) { sc =>
      val rdd = sc.parallelize(1 to 10)
      val pairRdd = rdd.map { i => (i, i) }
      expectCorrectException { TestUserClosuresActuallyCleaned.testMap(rdd) }
      expectCorrectException { TestUserClosuresActuallyCleaned.testFlatMap(rdd) }
      expectCorrectException { TestUserClosuresActuallyCleaned.testFilter(rdd) }
      expectCorrectException { TestUserClosuresActuallyCleaned.testSortBy(rdd) }
      expectCorrectException { TestUserClosuresActuallyCleaned.testGroupBy(rdd) }
      expectCorrectException { TestUserClosuresActuallyCleaned.testKeyBy(rdd) }
      expectCorrectException { TestUserClosuresActuallyCleaned.testMapPartitions(rdd) }
      expectCorrectException { TestUserClosuresActuallyCleaned.testMapPartitionsWithIndex(rdd) }
      expectCorrectException { TestUserClosuresActuallyCleaned.testZipPartitions2(rdd) }
      expectCorrectException { TestUserClosuresActuallyCleaned.testZipPartitions3(rdd) }
      expectCorrectException { TestUserClosuresActuallyCleaned.testZipPartitions4(rdd) }
      expectCorrectException { TestUserClosuresActuallyCleaned.testForeach(rdd) }
      expectCorrectException { TestUserClosuresActuallyCleaned.testForeachPartition(rdd) }
      expectCorrectException { TestUserClosuresActuallyCleaned.testReduce(rdd) }
      expectCorrectException { TestUserClosuresActuallyCleaned.testTreeReduce(rdd) }
      expectCorrectException { TestUserClosuresActuallyCleaned.testFold(rdd) }
      expectCorrectException { TestUserClosuresActuallyCleaned.testAggregate(rdd) }
      expectCorrectException { TestUserClosuresActuallyCleaned.testTreeAggregate(rdd) }
      expectCorrectException { TestUserClosuresActuallyCleaned.testCombineByKey(pairRdd) }
      expectCorrectException { TestUserClosuresActuallyCleaned.testAggregateByKey(pairRdd) }
      expectCorrectException { TestUserClosuresActuallyCleaned.testFoldByKey(pairRdd) }
      expectCorrectException { TestUserClosuresActuallyCleaned.testReduceByKey(pairRdd) }
      expectCorrectException { TestUserClosuresActuallyCleaned.testReduceByKeyLocally(pairRdd) }
      expectCorrectException { TestUserClosuresActuallyCleaned.testMapValues(pairRdd) }
      expectCorrectException { TestUserClosuresActuallyCleaned.testFlatMapValues(pairRdd) }
      expectCorrectException { TestUserClosuresActuallyCleaned.testForeachAsync(rdd) }
      expectCorrectException { TestUserClosuresActuallyCleaned.testForeachPartitionAsync(rdd) }
      expectCorrectException { TestUserClosuresActuallyCleaned.testRunJob1(sc) }
      expectCorrectException { TestUserClosuresActuallyCleaned.testRunJob2(sc) }
      expectCorrectException { TestUserClosuresActuallyCleaned.testRunApproximateJob(sc) }
      expectCorrectException { TestUserClosuresActuallyCleaned.testSubmitJob(sc) }
    }
  }

  test("createNullValue") {
    new TestCreateNullValue().run()
  }

  test("SPARK-22328: ClosureCleaner misses referenced superclass fields: case 1") {
    val concreteObject = new TestAbstractClass {
      val n2 = 222
      val s2 = "bbb"
      val d2 = 2.0d

      def run(): Seq[(Int, Int, String, String, Double, Double)] = {
        withSpark(new SparkContext("local", "test")) { sc =>
          val rdd = sc.parallelize(1 to 1)
          body(rdd)
        }
      }

      def body(rdd: RDD[Int]): Seq[(Int, Int, String, String, Double, Double)] = rdd.map { _ =>
        (n1, n2, s1, s2, d1, d2)
      }.collect()
    }
    assert(concreteObject.run() === Seq((111, 222, "aaa", "bbb", 1.0d, 2.0d)))
  }

  test("SPARK-22328: ClosureCleaner misses referenced superclass fields: case 2") {
    val concreteObject = new TestAbstractClass2 {
      val n2 = 222
      val s2 = "bbb"
      val d2 = 2.0d
      def getData: Int => (Int, Int, String, String, Double, Double) = _ => (n1, n2, s1, s2, d1, d2)
    }
    withSpark(new SparkContext("local", "test")) { sc =>
      val rdd = sc.parallelize(1 to 1).map(concreteObject.getData)
      assert(rdd.collect() === Seq((111, 222, "aaa", "bbb", 1.0d, 2.0d)))
    }
  }

  test("SPARK-22328: multiple outer classes have the same parent class") {
    val concreteObject = new TestAbstractClass2 {

      val innerObject = new TestAbstractClass2 {
        override val n1 = 222
        override val s1 = "bbb"
      }

      val innerObject2 = new TestAbstractClass2 {
        override val n1 = 444
        val n3 = 333
        val s3 = "ccc"
        val d3 = 3.0d

        def getData: Int => (Int, Int, String, String, Double, Double, Int, String) =
          _ => (n1, n3, s1, s3, d1, d3, innerObject.n1, innerObject.s1)
      }
    }
    withSpark(new SparkContext("local", "test")) { sc =>
      val rdd = sc.parallelize(1 to 1).map(concreteObject.innerObject2.getData)
      assert(rdd.collect() === Seq((444, 333, "aaa", "ccc", 1.0d, 3.0d, 222, "bbb")))
    }
  }
}

// A non-serializable class we create in closures to make sure that we aren't
// keeping references to unneeded variables from our outer closures.
class NonSerializable(val id: Int = -1) {
  override def hashCode(): Int = id

  override def equals(other: Any): Boolean = {
    other match {
      case o: NonSerializable => id == o.id
      case _ => false
    }
  }
}

object TestObject {
  def run(): Int = {
    var nonSer = new NonSerializable
    val x = 5
    withSpark(new SparkContext("local", "test")) { sc =>
      val nums = sc.parallelize(Array(1, 2, 3, 4))
      nums.map(_ + x).reduce(_ + _)
    }
  }
}

class TestClass extends Serializable {
  var x = 5

  def getX: Int = x

  def run(): Int = {
    var nonSer = new NonSerializable
    withSpark(new SparkContext("local", "test")) { sc =>
      val nums = sc.parallelize(Array(1, 2, 3, 4))
      nums.map(_ + getX).reduce(_ + _)
    }
  }
}

class TestClassWithoutDefaultConstructor(x: Int) extends Serializable {
  def getX: Int = x

  def run(): Int = {
    var nonSer = new NonSerializable
    withSpark(new SparkContext("local", "test")) { sc =>
      val nums = sc.parallelize(Array(1, 2, 3, 4))
      nums.map(_ + getX).reduce(_ + _)
    }
  }
}

// This class is not serializable, but we aren't using any of its fields in our
// closures, so they won't have a $outer pointing to it and should still work.
class TestClassWithoutFieldAccess {
  var nonSer = new NonSerializable

  def run(): Int = {
    var nonSer2 = new NonSerializable
    var x = 5
    withSpark(new SparkContext("local", "test")) { sc =>
      val nums = sc.parallelize(Array(1, 2, 3, 4))
      nums.map(_ + x).reduce(_ + _)
    }
  }
}

object TestObjectWithBogusReturns {
  def run(): Int = {
    withSpark(new SparkContext("local", "test")) { sc =>
      val nums = sc.parallelize(Array(1, 2, 3, 4))
      // this return is invalid since it will transfer control outside the closure
      nums.map {x => return 1 ; x * 2}
      1
    }
  }
}

object TestObjectWithNestedReturns {
  def run(): Int = {
    withSpark(new SparkContext("local", "test")) { sc =>
      val nums = sc.parallelize(Array(1, 2, 3, 4))
      nums.map {x =>
        // this return is fine since it will not transfer control outside the closure
        def foo(): Int = { return 5; 1 }
        foo()
      }
      1
    }
  }
}

object TestObjectWithNesting {
  def run(): Int = {
    var nonSer = new NonSerializable
    var answer = 0
    withSpark(new SparkContext("local", "test")) { sc =>
      val nums = sc.parallelize(Array(1, 2, 3, 4))
      var y = 1
      for (i <- 1 to 4) {
        var nonSer2 = new NonSerializable
        var x = i
        answer += nums.map(_ + x + y).reduce(_ + _)
      }
      answer
    }
  }
}

class TestClassWithNesting(val y: Int) extends Serializable {
  def getY: Int = y

  def run(): Int = {
    var nonSer = new NonSerializable
    var answer = 0
    withSpark(new SparkContext("local", "test")) { sc =>
      val nums = sc.parallelize(Array(1, 2, 3, 4))
      for (i <- 1 to 4) {
        var nonSer2 = new NonSerializable
        var x = i
        answer += nums.map(_ + x + getY).reduce(_ + _)
      }
      answer
    }
  }
}

/**
 * Test whether closures passed in through public APIs are actually cleaned.
 *
 * We put a return statement in each of these closures as a mechanism to detect whether the
 * ClosureCleaner actually cleaned our closure. If it did, then it would throw an appropriate
 * exception explicitly complaining about the return statement. Otherwise, we know the
 * ClosureCleaner did not actually clean our closure, in which case we should fail the test.
 */
private object TestUserClosuresActuallyCleaned {
  def testMap(rdd: RDD[Int]): Unit = { rdd.map { _ => return; 0 }.count() }
  def testFlatMap(rdd: RDD[Int]): Unit = { rdd.flatMap { _ => return; Seq() }.count() }
  def testFilter(rdd: RDD[Int]): Unit = { rdd.filter { _ => return; true }.count() }
  def testSortBy(rdd: RDD[Int]): Unit = { rdd.sortBy { _ => return; 1 }.count() }
  def testKeyBy(rdd: RDD[Int]): Unit = { rdd.keyBy { _ => return; 1 }.count() }
  def testGroupBy(rdd: RDD[Int]): Unit = { rdd.groupBy { _ => return; 1 }.count() }
  def testMapPartitions(rdd: RDD[Int]): Unit = { rdd.mapPartitions { it => return; it }.count() }
  def testMapPartitionsWithIndex(rdd: RDD[Int]): Unit = {
    rdd.mapPartitionsWithIndex { (_, it) => return; it }.count()
  }
  def testZipPartitions2(rdd: RDD[Int]): Unit = {
    rdd.zipPartitions(rdd) { case (it1, it2) => return; it1 }.count()
  }
  def testZipPartitions3(rdd: RDD[Int]): Unit = {
    rdd.zipPartitions(rdd, rdd) { case (it1, it2, it3) => return; it1 }.count()
  }
  def testZipPartitions4(rdd: RDD[Int]): Unit = {
    rdd.zipPartitions(rdd, rdd, rdd) { case (it1, it2, it3, it4) => return; it1 }.count()
  }
  def testForeach(rdd: RDD[Int]): Unit = { rdd.foreach { _ => return } }
  def testForeachPartition(rdd: RDD[Int]): Unit = { rdd.foreachPartition { _ => return } }
  def testReduce(rdd: RDD[Int]): Unit = { rdd.reduce { case (_, _) => return; 1 } }
  def testTreeReduce(rdd: RDD[Int]): Unit = { rdd.treeReduce { case (_, _) => return; 1 } }
  def testFold(rdd: RDD[Int]): Unit = { rdd.fold(0) { case (_, _) => return; 1 } }
  def testAggregate(rdd: RDD[Int]): Unit = {
    rdd.aggregate(0)({ case (_, _) => return; 1 }, { case (_, _) => return; 1 })
  }
  def testTreeAggregate(rdd: RDD[Int]): Unit = {
    rdd.treeAggregate(0)({ case (_, _) => return; 1 }, { case (_, _) => return; 1 })
  }

  // Test pair RDD functions
  def testCombineByKey(rdd: RDD[(Int, Int)]): Unit = {
    rdd.combineByKey(
      { _ => return; 1 }: Int => Int,
      { case (_, _) => return; 1 }: (Int, Int) => Int,
      { case (_, _) => return; 1 }: (Int, Int) => Int
    ).count()
  }
  def testAggregateByKey(rdd: RDD[(Int, Int)]): Unit = {
    rdd.aggregateByKey(0)({ case (_, _) => return; 1 }, { case (_, _) => return; 1 }).count()
  }
  def testFoldByKey(rdd: RDD[(Int, Int)]): Unit = { rdd.foldByKey(0) { case (_, _) => return; 1 } }
  def testReduceByKey(rdd: RDD[(Int, Int)]): Unit = { rdd.reduceByKey { case (_, _) => return; 1 } }
  def testReduceByKeyLocally(rdd: RDD[(Int, Int)]): Unit = {
    rdd.reduceByKeyLocally { case (_, _) => return; 1 }
  }
  def testMapValues(rdd: RDD[(Int, Int)]): Unit = { rdd.mapValues { _ => return; 1 } }
  def testFlatMapValues(rdd: RDD[(Int, Int)]): Unit = { rdd.flatMapValues { _ => return; Seq() } }

  // Test async RDD actions
  def testForeachAsync(rdd: RDD[Int]): Unit = { rdd.foreachAsync { _ => return } }
  def testForeachPartitionAsync(rdd: RDD[Int]): Unit = { rdd.foreachPartitionAsync { _ => return } }

  // Test SparkContext runJob
  def testRunJob1(sc: SparkContext): Unit = {
    val rdd = sc.parallelize(1 to 10, 10)
    sc.runJob(rdd, { (ctx: TaskContext, iter: Iterator[Int]) => return; 1 } )
  }
  def testRunJob2(sc: SparkContext): Unit = {
    val rdd = sc.parallelize(1 to 10, 10)
    sc.runJob(rdd, { iter: Iterator[Int] => return; 1 } )
  }
  def testRunApproximateJob(sc: SparkContext): Unit = {
    val rdd = sc.parallelize(1 to 10, 10)
    val evaluator = new CountEvaluator(1, 0.5)
    sc.runApproximateJob(
      rdd, { (ctx: TaskContext, iter: Iterator[Int]) => return; 1L }, evaluator, 1000)
  }
  def testSubmitJob(sc: SparkContext): Unit = {
    val rdd = sc.parallelize(1 to 10, 10)
    sc.submitJob(
      rdd,
      { _ => return; 1 }: Iterator[Int] => Int,
      Seq.empty,
      { case (_, _) => return }: (Int, Int) => Unit,
      { return }
    )
  }
}

class TestCreateNullValue {

  var x = 5

  def getX: Int = x

  def run(): Unit = {
    val bo: Boolean = true
    val c: Char = '1'
    val b: Byte = 1
    val s: Short = 1
    val i: Int = 1
    val l: Long = 1
    val f: Float = 1
    val d: Double = 1

    // Bring in all primitive types into the closure such that they become
    // parameters of the closure constructor. This allows us to test whether
    // null values are created correctly for each type.
    val nestedClosure = () => {
      // scalastyle:off println
      if (s.toString == "123") { // Don't really output them to avoid noisy
        println(bo)
        println(c)
        println(b)
        println(s)
        println(i)
        println(l)
        println(f)
        println(d)
      }

      val closure = () => {
        println(getX)
      }
      // scalastyle:on println
      ClosureCleaner.clean(closure)
    }
    nestedClosure()
  }
}

abstract class TestAbstractClass extends Serializable {
  val n1 = 111
  val s1 = "aaa"
  protected val d1 = 1.0d

  def run(): Seq[(Int, Int, String, String, Double, Double)]
  def body(rdd: RDD[Int]): Seq[(Int, Int, String, String, Double, Double)]
}

abstract class TestAbstractClass2 extends Serializable {
  val n1 = 111
  val s1 = "aaa"
  protected val d1 = 1.0d
}
