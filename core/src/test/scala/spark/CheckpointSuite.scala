package spark

import org.scalatest.FunSuite
import java.io.File
import spark.rdd._
import spark.SparkContext._
import storage.StorageLevel

class CheckpointSuite extends FunSuite with LocalSparkContext with Logging {
  initLogging()

  var checkpointDir: File = _
  val partitioner = new HashPartitioner(2)

  override def beforeEach() {
    super.beforeEach()
    checkpointDir = File.createTempFile("temp", "")
    checkpointDir.delete()
    sc = new SparkContext("local", "test")
    sc.setCheckpointDir(checkpointDir.toString)
  }

  override def afterEach() {
    super.afterEach()
    if (checkpointDir != null) {
      checkpointDir.delete()
    }
  }

  test("RDDs with one-to-one dependencies") {
    testCheckpointing(_.map(x => x.toString))
    testCheckpointing(_.flatMap(x => 1 to x))
    testCheckpointing(_.filter(_ % 2 == 0))
    testCheckpointing(_.sample(false, 0.5, 0))
    testCheckpointing(_.glom())
    testCheckpointing(_.mapPartitions(_.map(_.toString)))
    testCheckpointing(r => new MapPartitionsWithIndexRDD(r,
      (i: Int, iter: Iterator[Int]) => iter.map(_.toString), false ))
    testCheckpointing(_.map(x => (x % 2, 1)).reduceByKey(_ + _).mapValues(_.toString))
    testCheckpointing(_.map(x => (x % 2, 1)).reduceByKey(_ + _).flatMapValues(x => 1 to x))
    testCheckpointing(_.pipe(Seq("cat")))
  }

  test("ParallelCollection") {
    val parCollection = sc.makeRDD(1 to 4, 2)
    val numPartitions = parCollection.partitions.size
    parCollection.checkpoint()
    assert(parCollection.dependencies === Nil)
    val result = parCollection.collect()
    assert(sc.checkpointFile[Int](parCollection.getCheckpointFile.get).collect() === result)
    assert(parCollection.dependencies != Nil)
    assert(parCollection.partitions.length === numPartitions)
    assert(parCollection.partitions.toList === parCollection.checkpointData.get.getPartitions.toList)
    assert(parCollection.collect() === result)
  }

  test("BlockRDD") {
    val blockId = "id"
    val blockManager = SparkEnv.get.blockManager
    blockManager.putSingle(blockId, "test", StorageLevel.MEMORY_ONLY)
    val blockRDD = new BlockRDD[String](sc, Array(blockId))
    val numPartitions = blockRDD.partitions.size
    blockRDD.checkpoint()
    val result = blockRDD.collect()
    assert(sc.checkpointFile[String](blockRDD.getCheckpointFile.get).collect() === result)
    assert(blockRDD.dependencies != Nil)
    assert(blockRDD.partitions.length === numPartitions)
    assert(blockRDD.partitions.toList === blockRDD.checkpointData.get.getPartitions.toList)
    assert(blockRDD.collect() === result)
  }

  test("ShuffledRDD") {
    testCheckpointing(rdd => {
      // Creating ShuffledRDD directly as PairRDDFunctions.combineByKey produces a MapPartitionedRDD
      new ShuffledRDD(rdd.map(x => (x % 2, 1)), partitioner)
    })
  }

  test("UnionRDD") {
    def otherRDD = sc.makeRDD(1 to 10, 1)

    // Test whether the size of UnionRDDPartitions reduce in size after parent RDD is checkpointed.
    // Current implementation of UnionRDD has transient reference to parent RDDs,
    // so only the partitions will reduce in serialized size, not the RDD.
    testCheckpointing(_.union(otherRDD), false, true)
    testParentCheckpointing(_.union(otherRDD), false, true)
  }

  test("CartesianRDD") {
    def otherRDD = sc.makeRDD(1 to 10, 1)
    testCheckpointing(new CartesianRDD(sc, _, otherRDD))

    // Test whether size of CoalescedRDD reduce in size after parent RDD is checkpointed
    // Current implementation of CoalescedRDDPartition has transient reference to parent RDD,
    // so only the RDD will reduce in serialized size, not the partitions.
    testParentCheckpointing(new CartesianRDD(sc, _, otherRDD), true, false)

    // Test that the CartesianRDD updates parent partitions (CartesianRDD.s1/s2) after
    // the parent RDD has been checkpointed and parent partitions have been changed to HadoopPartitions.
    // Note that this test is very specific to the current implementation of CartesianRDD.
    val ones = sc.makeRDD(1 to 100, 10).map(x => x)
    ones.checkpoint() // checkpoint that MappedRDD
    val cartesian = new CartesianRDD(sc, ones, ones)
    val splitBeforeCheckpoint =
      serializeDeserialize(cartesian.partitions.head.asInstanceOf[CartesianPartition])
    cartesian.count() // do the checkpointing
    val splitAfterCheckpoint =
      serializeDeserialize(cartesian.partitions.head.asInstanceOf[CartesianPartition])
    assert(
      (splitAfterCheckpoint.s1 != splitBeforeCheckpoint.s1) &&
        (splitAfterCheckpoint.s2 != splitBeforeCheckpoint.s2),
      "CartesianRDD.parents not updated after parent RDD checkpointed"
    )
  }

  test("CoalescedRDD") {
    testCheckpointing(_.coalesce(2))

    // Test whether size of CoalescedRDD reduce in size after parent RDD is checkpointed
    // Current implementation of CoalescedRDDPartition has transient reference to parent RDD,
    // so only the RDD will reduce in serialized size, not the partitions.
    testParentCheckpointing(_.coalesce(2), true, false)

    // Test that the CoalescedRDDPartition updates parent partitions (CoalescedRDDPartition.parents) after
    // the parent RDD has been checkpointed and parent partitions have been changed to HadoopPartitions.
    // Note that this test is very specific to the current implementation of CoalescedRDDPartitions
    val ones = sc.makeRDD(1 to 100, 10).map(x => x)
    ones.checkpoint() // checkpoint that MappedRDD
    val coalesced = new CoalescedRDD(ones, 2)
    val splitBeforeCheckpoint =
      serializeDeserialize(coalesced.partitions.head.asInstanceOf[CoalescedRDDPartition])
    coalesced.count() // do the checkpointing
    val splitAfterCheckpoint =
      serializeDeserialize(coalesced.partitions.head.asInstanceOf[CoalescedRDDPartition])
    assert(
      splitAfterCheckpoint.parents.head != splitBeforeCheckpoint.parents.head,
      "CoalescedRDDPartition.parents not updated after parent RDD checkpointed"
    )
  }

  test("CoGroupedRDD") {
    val longLineageRDD1 = generateLongLineageRDDForCoGroupedRDD()
    testCheckpointing(rdd => {
      CheckpointSuite.cogroup(longLineageRDD1, rdd.map(x => (x % 2, 1)), partitioner)
    }, false, true)

    val longLineageRDD2 = generateLongLineageRDDForCoGroupedRDD()
    testParentCheckpointing(rdd => {
      CheckpointSuite.cogroup(
        longLineageRDD2, sc.makeRDD(1 to 2, 2).map(x => (x % 2, 1)), partitioner)
    }, false, true)
  }

  test("ZippedRDD") {
    testCheckpointing(
      rdd => new ZippedRDD(sc, rdd, rdd.map(x => x)), true, false)

    // Test whether size of ZippedRDD reduce in size after parent RDD is checkpointed
    // Current implementation of ZippedRDDPartitions has transient references to parent RDDs,
    // so only the RDD will reduce in serialized size, not the partitions.
    testParentCheckpointing(
      rdd => new ZippedRDD(sc, rdd, rdd.map(x => x)), true, false)
  }

  test("CheckpointRDD with zero partitions") {
    val rdd = new BlockRDD[Int](sc, Array[String]())
    assert(rdd.partitions.size === 0)
    assert(rdd.isCheckpointed === false)
    rdd.checkpoint()
    assert(rdd.count() === 0)
    assert(rdd.isCheckpointed === true)
    assert(rdd.partitions.size === 0)
  }

  /**
   * Test checkpointing of the final RDD generated by the given operation. By default,
   * this method tests whether the size of serialized RDD has reduced after checkpointing or not.
   * It can also test whether the size of serialized RDD partitions has reduced after checkpointing or
   * not, but this is not done by default as usually the partitions do not refer to any RDD and
   * therefore never store the lineage.
   */
  def testCheckpointing[U: ClassManifest](
      op: (RDD[Int]) => RDD[U],
      testRDDSize: Boolean = true,
      testRDDPartitionSize: Boolean = false
    ) {
    // Generate the final RDD using given RDD operation
    val baseRDD = generateLongLineageRDD()
    val operatedRDD = op(baseRDD)
    val parentRDD = operatedRDD.dependencies.headOption.orNull
    val rddType = operatedRDD.getClass.getSimpleName
    val numPartitions = operatedRDD.partitions.length

    // Find serialized sizes before and after the checkpoint
    val (rddSizeBeforeCheckpoint, splitSizeBeforeCheckpoint) = getSerializedSizes(operatedRDD)
    operatedRDD.checkpoint()
    val result = operatedRDD.collect()
    val (rddSizeAfterCheckpoint, splitSizeAfterCheckpoint) = getSerializedSizes(operatedRDD)

    // Test whether the checkpoint file has been created
    assert(sc.checkpointFile[U](operatedRDD.getCheckpointFile.get).collect() === result)

    // Test whether dependencies have been changed from its earlier parent RDD
    assert(operatedRDD.dependencies.head.rdd != parentRDD)

    // Test whether the partitions have been changed to the new Hadoop partitions
    assert(operatedRDD.partitions.toList === operatedRDD.checkpointData.get.getPartitions.toList)

    // Test whether the number of partitions is same as before
    assert(operatedRDD.partitions.length === numPartitions)

    // Test whether the data in the checkpointed RDD is same as original
    assert(operatedRDD.collect() === result)

    // Test whether serialized size of the RDD has reduced. If the RDD
    // does not have any dependency to another RDD (e.g., ParallelCollection,
    // ShuffleRDD with ShuffleDependency), it may not reduce in size after checkpointing.
    if (testRDDSize) {
      logInfo("Size of " + rddType +
        "[" + rddSizeBeforeCheckpoint + " --> " + rddSizeAfterCheckpoint + "]")
      assert(
        rddSizeAfterCheckpoint < rddSizeBeforeCheckpoint,
        "Size of " + rddType + " did not reduce after checkpointing " +
          "[" + rddSizeBeforeCheckpoint + " --> " + rddSizeAfterCheckpoint + "]"
      )
    }

    // Test whether serialized size of the partitions has reduced. If the partitions
    // do not have any non-transient reference to another RDD or another RDD's partitions, it
    // does not refer to a lineage and therefore may not reduce in size after checkpointing.
    // However, if the original partitions before checkpointing do refer to a parent RDD, the partitions
    // must be forgotten after checkpointing (to remove all reference to parent RDDs) and
    // replaced with the HadooPartitions of the checkpointed RDD.
    if (testRDDPartitionSize) {
      logInfo("Size of " + rddType + " partitions "
        + "[" + splitSizeBeforeCheckpoint + " --> " + splitSizeAfterCheckpoint + "]")
      assert(
        splitSizeAfterCheckpoint < splitSizeBeforeCheckpoint,
        "Size of " + rddType + " partitions did not reduce after checkpointing " +
          "[" + splitSizeBeforeCheckpoint + " --> " + splitSizeAfterCheckpoint + "]"
      )
    }
  }

  /**
   * Test whether checkpointing of the parent of the generated RDD also
   * truncates the lineage or not. Some RDDs like CoGroupedRDD hold on to its parent
   * RDDs partitions. So even if the parent RDD is checkpointed and its partitions changed,
   * this RDD will remember the partitions and therefore potentially the whole lineage.
   */
  def testParentCheckpointing[U: ClassManifest](
      op: (RDD[Int]) => RDD[U],
      testRDDSize: Boolean,
      testRDDPartitionSize: Boolean
    ) {
    // Generate the final RDD using given RDD operation
    val baseRDD = generateLongLineageRDD()
    val operatedRDD = op(baseRDD)
    val parentRDD = operatedRDD.dependencies.head.rdd
    val rddType = operatedRDD.getClass.getSimpleName
    val parentRDDType = parentRDD.getClass.getSimpleName

    // Get the partitions and dependencies of the parent in case they're lazily computed
    parentRDD.dependencies
    parentRDD.partitions

    // Find serialized sizes before and after the checkpoint
    val (rddSizeBeforeCheckpoint, splitSizeBeforeCheckpoint) = getSerializedSizes(operatedRDD)
    parentRDD.checkpoint()  // checkpoint the parent RDD, not the generated one
    val result = operatedRDD.collect()
    val (rddSizeAfterCheckpoint, splitSizeAfterCheckpoint) = getSerializedSizes(operatedRDD)

    // Test whether the data in the checkpointed RDD is same as original
    assert(operatedRDD.collect() === result)

    // Test whether serialized size of the RDD has reduced because of its parent being
    // checkpointed. If this RDD or its parent RDD do not have any dependency
    // to another RDD (e.g., ParallelCollection, ShuffleRDD with ShuffleDependency), it may
    // not reduce in size after checkpointing.
    if (testRDDSize) {
      assert(
        rddSizeAfterCheckpoint < rddSizeBeforeCheckpoint,
        "Size of " + rddType + " did not reduce after checkpointing parent " + parentRDDType +
          "[" + rddSizeBeforeCheckpoint + " --> " + rddSizeAfterCheckpoint + "]"
      )
    }

    // Test whether serialized size of the partitions has reduced because of its parent being
    // checkpointed. If the partitions do not have any non-transient reference to another RDD
    // or another RDD's partitions, it does not refer to a lineage and therefore may not reduce
    // in size after checkpointing. However, if the partitions do refer to the *partitions* of a parent
    // RDD, then these partitions must update reference to the parent RDD partitions as the parent RDD's
    // partitions must have changed after checkpointing.
    if (testRDDPartitionSize) {
      assert(
        splitSizeAfterCheckpoint < splitSizeBeforeCheckpoint,
        "Size of " + rddType + " partitions did not reduce after checkpointing parent " + parentRDDType +
          "[" + splitSizeBeforeCheckpoint + " --> " + splitSizeAfterCheckpoint + "]"
      )
    }

  }

  /**
   * Generate an RDD with a long lineage of one-to-one dependencies.
   */
  def generateLongLineageRDD(): RDD[Int] = {
    var rdd = sc.makeRDD(1 to 100, 4)
    for (i <- 1 to 50) {
      rdd = rdd.map(x => x + 1)
    }
    rdd
  }

  /**
   * Generate an RDD with a long lineage specifically for CoGroupedRDD.
   * A CoGroupedRDD can have a long lineage only one of its parents have a long lineage
   * and narrow dependency with this RDD. This method generate such an RDD by a sequence
   * of cogroups and mapValues which creates a long lineage of narrow dependencies.
   */
  def generateLongLineageRDDForCoGroupedRDD() = {
    val add = (x: (Seq[Int], Seq[Int])) => (x._1 ++ x._2).reduce(_ + _)

    def ones: RDD[(Int, Int)] = sc.makeRDD(1 to 2, 2).map(x => (x % 2, 1)).reduceByKey(partitioner, _ + _)

    var cogrouped: RDD[(Int, (Seq[Int], Seq[Int]))] = ones.cogroup(ones)
    for(i <- 1 to 10) {
      cogrouped = cogrouped.mapValues(add).cogroup(ones)
    }
    cogrouped.mapValues(add)
  }

  /**
   * Get serialized sizes of the RDD and its partitions, in order to test whether the size shrinks
   * upon checkpointing. Ignores the checkpointData field, which may grow when we checkpoint.
   */
  def getSerializedSizes(rdd: RDD[_]): (Int, Int) = {
    (Utils.serialize(rdd).length - Utils.serialize(rdd.checkpointData).length,
     Utils.serialize(rdd.partitions).length)
  }

  /**
   * Serialize and deserialize an object. This is useful to verify the objects
   * contents after deserialization (e.g., the contents of an RDD split after
   * it is sent to a slave along with a task)
   */
  def serializeDeserialize[T](obj: T): T = {
    val bytes = Utils.serialize(obj)
    Utils.deserialize[T](bytes)
  }
}


object CheckpointSuite {
  // This is a custom cogroup function that does not use mapValues like
  // the PairRDDFunctions.cogroup()
  def cogroup[K, V](first: RDD[(K, V)], second: RDD[(K, V)], part: Partitioner) = {
    //println("First = " + first + ", second = " + second)
    new CoGroupedRDD[K](
      Seq(first.asInstanceOf[RDD[(K, _)]], second.asInstanceOf[RDD[(K, _)]]),
      part
    ).asInstanceOf[RDD[(K, Seq[Seq[V]])]]
  }

}
