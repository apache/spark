package spark

import org.scalatest.{BeforeAndAfter, FunSuite}
import java.io.File
import rdd.{BlockRDD, CoalescedRDD, MapPartitionsWithSplitRDD}
import spark.SparkContext._
import storage.StorageLevel
import java.util.concurrent.Semaphore
import collection.mutable.ArrayBuffer

class CheckpointSuite extends FunSuite with BeforeAndAfter with Logging {
  initLogging()

  var sc: SparkContext = _
  var checkpointDir: File = _

  before {
    checkpointDir = File.createTempFile("temp", "")
    checkpointDir.delete()

    sc = new SparkContext("local", "test")
    sc.setCheckpointDir(checkpointDir.toString)
  }

  after {
    if (sc != null) {
      sc.stop()
      sc = null
    }
    // To avoid Akka rebinding to the same port, since it doesn't unbind immediately on shutdown
    System.clearProperty("spark.master.port")

    if (checkpointDir != null) {
      checkpointDir.delete()
    }
  }

  test("ParallelCollection") {
    val parCollection = sc.makeRDD(1 to 4)
    parCollection.checkpoint()
    assert(parCollection.dependencies === Nil)
    val result = parCollection.collect()
    sleep(parCollection) // slightly extra time as loading classes for the first can take some time
    assert(sc.objectFile[Int](parCollection.checkpointFile).collect() === result)
    assert(parCollection.dependencies != Nil)
    assert(parCollection.collect() === result)
  }

  test("BlockRDD") {
    val blockId = "id"
    val blockManager = SparkEnv.get.blockManager
    blockManager.putSingle(blockId, "test", StorageLevel.MEMORY_ONLY)
    val blockRDD = new BlockRDD[String](sc, Array(blockId))
    blockRDD.checkpoint()
    val result = blockRDD.collect()
    sleep(blockRDD)
    assert(sc.objectFile[String](blockRDD.checkpointFile).collect() === result)
    assert(blockRDD.dependencies != Nil)
    assert(blockRDD.collect() === result)
  }

  test("RDDs with one-to-one dependencies") {
    testCheckpointing(_.map(x => x.toString))
    testCheckpointing(_.flatMap(x => 1 to x))
    testCheckpointing(_.filter(_ % 2 == 0))
    testCheckpointing(_.sample(false, 0.5, 0))
    testCheckpointing(_.glom())
    testCheckpointing(_.mapPartitions(_.map(_.toString)))
    testCheckpointing(r => new MapPartitionsWithSplitRDD(r,
      (i: Int, iter: Iterator[Int]) => iter.map(_.toString), false))
    testCheckpointing(_.map(x => (x % 2, 1)).reduceByKey(_ + _).mapValues(_.toString), 1000)
    testCheckpointing(_.map(x => (x % 2, 1)).reduceByKey(_ + _).flatMapValues(x => 1 to x), 1000)
    testCheckpointing(_.pipe(Seq("cat")))
  }

  test("ShuffledRDD") {
    testCheckpointing(_.map(x => (x % 2, 1)).reduceByKey(_ + _))
  }

  test("UnionRDD") {
    testCheckpointing(_.union(sc.makeRDD(5 to 6, 4)))
  }

  test("CartesianRDD") {
    testCheckpointing(_.cartesian(sc.makeRDD(5 to 6, 4)), 1000)
  }

  test("CoalescedRDD") {
    testCheckpointing(new CoalescedRDD(_, 2))
  }

  test("CoGroupedRDD") {
    val rdd2 = sc.makeRDD(5 to 6, 4).map(x => (x % 2, 1))
    testCheckpointing(rdd1 => rdd1.map(x => (x % 2, 1)).cogroup(rdd2))
    testCheckpointing(rdd1 => rdd1.map(x => (x % 2, x)).join(rdd2))

    // Special test to make sure that the CoGroupSplit of CoGroupedRDD do not
    // hold on to the splits of its parent RDDs, as the splits of parent RDDs
    // may change while checkpointing. Rather the splits of parent RDDs must
    // be fetched at the time of serialization to ensure the latest splits to
    // be sent along with the task.

    val add = (x: (Seq[Int], Seq[Int])) => (x._1 ++ x._2).reduce(_ + _)

    val ones = sc.parallelize(1 to 100, 1).map(x => (x,1))
    val reduced = ones.reduceByKey(_ + _)
    val seqOfCogrouped = new ArrayBuffer[RDD[(Int, Int)]]()
    seqOfCogrouped += reduced.cogroup(ones).mapValues[Int](add)
    for(i <- 1 to 10) {
      seqOfCogrouped += seqOfCogrouped.last.cogroup(ones).mapValues(add)
    }
    val finalCogrouped = seqOfCogrouped.last
    val intermediateCogrouped = seqOfCogrouped(5)

    val bytesBeforeCheckpoint = Utils.serialize(finalCogrouped.splits)
    intermediateCogrouped.checkpoint()
    finalCogrouped.count()
    sleep(intermediateCogrouped)
    val bytesAfterCheckpoint = Utils.serialize(finalCogrouped.splits)
    println("Before = " + bytesBeforeCheckpoint.size + ", after = " + bytesAfterCheckpoint.size)
    assert(bytesAfterCheckpoint.size < bytesBeforeCheckpoint.size,
      "CoGroupedSplits still holds on to the splits of its parent RDDs")
  }
  /*
  /**
   * This test forces two ResultTasks of the same job to be launched before and after
   * the checkpointing of job's RDD is completed.
   */
  test("Threading - ResultTasks") {
    val op1 = (parCollection: RDD[Int]) => {
      parCollection.map(x => { println("1st map running on " + x); Thread.sleep(500); (x % 2, x) })
    }
    val op2 = (firstRDD: RDD[(Int, Int)]) => {
      firstRDD.map(x => { println("2nd map running on " + x); Thread.sleep(500); x })
    }
    testThreading(op1, op2)
  }

  /**
   * This test forces two ShuffleMapTasks of the same job to be launched before and after
   * the checkpointing of job's RDD is completed.
   */
  test("Threading - ShuffleMapTasks") {
    val op1 = (parCollection: RDD[Int]) => {
      parCollection.map(x => { println("1st map running on " + x); Thread.sleep(500); (x % 2, x) })
    }
    val op2 = (firstRDD: RDD[(Int, Int)]) => {
      firstRDD.groupByKey(2).map(x => { println("2nd map running on " + x); Thread.sleep(500); x })
    }
    testThreading(op1, op2)
  }
  */

  def testCheckpointing[U: ClassManifest](op: (RDD[Int]) => RDD[U], sleepTime: Long = 500) {
    val parCollection = sc.makeRDD(1 to 4, 4)
    val operatedRDD = op(parCollection)
    operatedRDD.checkpoint()
    val parentRDD = operatedRDD.dependencies.head.rdd
    val result = operatedRDD.collect()
    sleep(operatedRDD)
    //println(parentRDD + ", " + operatedRDD.dependencies.head.rdd )
    assert(sc.objectFile[U](operatedRDD.checkpointFile).collect() === result)
    assert(operatedRDD.dependencies.head.rdd != parentRDD)
    assert(operatedRDD.collect() === result)
  }
  /*
  def testThreading[U: ClassManifest, V: ClassManifest](op1: (RDD[Int]) => RDD[U], op2: (RDD[U]) => RDD[V]) {

    val parCollection = sc.makeRDD(1 to 2, 2)

    // This is the RDD that is to be checkpointed
    val firstRDD = op1(parCollection)
    val parentRDD = firstRDD.dependencies.head.rdd
    firstRDD.checkpoint()

    // This the RDD that uses firstRDD. This is designed to launch a
    // ShuffleMapTask that uses firstRDD.
    val secondRDD = op2(firstRDD)

    // Starting first job, to initiate the checkpointing
    logInfo("\nLaunching 1st job to initiate checkpointing\n")
    firstRDD.collect()

    // Checkpointing has started but not completed yet
    Thread.sleep(100)
    assert(firstRDD.dependencies.head.rdd === parentRDD)

    // Starting second job; first task of this job will be
    // launched _before_ firstRDD is marked as checkpointed
    // and the second task will be launched _after_ firstRDD
    // is marked as checkpointed
    logInfo("\nLaunching 2nd job that is designed to launch tasks " +
      "before and after checkpointing is complete\n")
    val result = secondRDD.collect()

    // Check whether firstRDD has been successfully checkpointed
    assert(firstRDD.dependencies.head.rdd != parentRDD)

    logInfo("\nRecomputing 2nd job to verify the results of the previous computation\n")
    // Check whether the result in the previous job was correct or not
    val correctResult = secondRDD.collect()
    assert(result === correctResult)
  }
  */
  def sleep(rdd: RDD[_]) {
    val startTime = System.currentTimeMillis()
    val maxWaitTime = 5000
    while(rdd.isCheckpointed == false && System.currentTimeMillis() < startTime + maxWaitTime) {
      Thread.sleep(50)
    }
    assert(rdd.isCheckpointed === true, "Waiting for checkpoint to complete took more than " + maxWaitTime + " ms")
  }
}
