package spark

import org.scalatest.{BeforeAndAfter, FunSuite}
import java.io.File
import rdd.{BlockRDD, CoalescedRDD, MapPartitionsWithSplitRDD}
import spark.SparkContext._
import storage.StorageLevel

class CheckpointSuite extends FunSuite with BeforeAndAfter {

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
      (i: Int, iter: Iterator[Int]) => iter.map(_.toString) ))
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
  }

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

  def sleep(rdd: RDD[_]) {
    val startTime = System.currentTimeMillis()
    val maxWaitTime = 5000
    while(rdd.isCheckpointed == false && System.currentTimeMillis() < startTime + maxWaitTime) {
      Thread.sleep(50)
    }
    assert(rdd.isCheckpointed === true, "Waiting for checkpoint to complete took more than " + maxWaitTime + " ms")
  }
}
