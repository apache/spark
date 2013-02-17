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
    testCheckpointing(r => new MapPartitionsWithSplitRDD(r,
      (i: Int, iter: Iterator[Int]) => iter.map(_.toString), false ))
    testCheckpointing(_.map(x => (x % 2, 1)).reduceByKey(_ + _).mapValues(_.toString))
    testCheckpointing(_.map(x => (x % 2, 1)).reduceByKey(_ + _).flatMapValues(x => 1 to x))
    testCheckpointing(_.pipe(Seq("cat")))
  }

  test("ParallelCollection") {
    val parCollection = sc.makeRDD(1 to 4, 2)
    val numSplits = parCollection.splits.size
    parCollection.checkpoint()
    assert(parCollection.dependencies === Nil)
    val result = parCollection.collect()
    assert(sc.checkpointFile[Int](parCollection.getCheckpointFile.get).collect() === result)
    assert(parCollection.dependencies != Nil)
    assert(parCollection.splits.length === numSplits)
    assert(parCollection.splits.toList === parCollection.checkpointData.get.getSplits.toList)
    assert(parCollection.collect() === result)
  }

  test("BlockRDD") {
    val blockId = "id"
    val blockManager = SparkEnv.get.blockManager
    blockManager.putSingle(blockId, "test", StorageLevel.MEMORY_ONLY)
    val blockRDD = new BlockRDD[String](sc, Array(blockId))
    val numSplits = blockRDD.splits.size
    blockRDD.checkpoint()
    val result = blockRDD.collect()
    assert(sc.checkpointFile[String](blockRDD.getCheckpointFile.get).collect() === result)
    assert(blockRDD.dependencies != Nil)
    assert(blockRDD.splits.length === numSplits)
    assert(blockRDD.splits.toList === blockRDD.checkpointData.get.getSplits.toList)
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

    // Test whether the size of UnionRDDSplits reduce in size after parent RDD is checkpointed.
    // Current implementation of UnionRDD has transient reference to parent RDDs,
    // so only the splits will reduce in serialized size, not the RDD.
    testCheckpointing(_.union(otherRDD), false, true)
    testParentCheckpointing(_.union(otherRDD), false, true)
  }

  test("CartesianRDD") {
    def otherRDD = sc.makeRDD(1 to 10, 1)
    testCheckpointing(new CartesianRDD(sc, _, otherRDD))

    // Test whether size of CoalescedRDD reduce in size after parent RDD is checkpointed
    // Current implementation of CoalescedRDDSplit has transient reference to parent RDD,
    // so only the RDD will reduce in serialized size, not the splits.
    testParentCheckpointing(new CartesianRDD(sc, _, otherRDD), true, false)

    // Test that the CartesianRDD updates parent splits (CartesianRDD.s1/s2) after
    // the parent RDD has been checkpointed and parent splits have been changed to HadoopSplits.
    // Note that this test is very specific to the current implementation of CartesianRDD.
    val ones = sc.makeRDD(1 to 100, 10).map(x => x)
    ones.checkpoint() // checkpoint that MappedRDD
    val cartesian = new CartesianRDD(sc, ones, ones)
    val splitBeforeCheckpoint =
      serializeDeserialize(cartesian.splits.head.asInstanceOf[CartesianSplit])
    cartesian.count() // do the checkpointing
    val splitAfterCheckpoint =
      serializeDeserialize(cartesian.splits.head.asInstanceOf[CartesianSplit])
    assert(
      (splitAfterCheckpoint.s1 != splitBeforeCheckpoint.s1) &&
        (splitAfterCheckpoint.s2 != splitBeforeCheckpoint.s2),
      "CartesianRDD.parents not updated after parent RDD checkpointed"
    )
  }

  test("CoalescedRDD") {
    testCheckpointing(_.coalesce(2))

    // Test whether size of CoalescedRDD reduce in size after parent RDD is checkpointed
    // Current implementation of CoalescedRDDSplit has transient reference to parent RDD,
    // so only the RDD will reduce in serialized size, not the splits.
    testParentCheckpointing(_.coalesce(2), true, false)

    // Test that the CoalescedRDDSplit updates parent splits (CoalescedRDDSplit.parents) after
    // the parent RDD has been checkpointed and parent splits have been changed to HadoopSplits.
    // Note that this test is very specific to the current implementation of CoalescedRDDSplits
    val ones = sc.makeRDD(1 to 100, 10).map(x => x)
    ones.checkpoint() // checkpoint that MappedRDD
    val coalesced = new CoalescedRDD(ones, 2)
    val splitBeforeCheckpoint =
      serializeDeserialize(coalesced.splits.head.asInstanceOf[CoalescedRDDSplit])
    coalesced.count() // do the checkpointing
    val splitAfterCheckpoint =
      serializeDeserialize(coalesced.splits.head.asInstanceOf[CoalescedRDDSplit])
    assert(
      splitAfterCheckpoint.parents.head != splitBeforeCheckpoint.parents.head,
      "CoalescedRDDSplit.parents not updated after parent RDD checkpointed"
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
    // Current implementation of ZippedRDDSplit has transient references to parent RDDs,
    // so only the RDD will reduce in serialized size, not the splits.
    testParentCheckpointing(
      rdd => new ZippedRDD(sc, rdd, rdd.map(x => x)), true, false)
  }

  /**
   * Test checkpointing of the final RDD generated by the given operation. By default,
   * this method tests whether the size of serialized RDD has reduced after checkpointing or not.
   * It can also test whether the size of serialized RDD splits has reduced after checkpointing or
   * not, but this is not done by default as usually the splits do not refer to any RDD and
   * therefore never store the lineage.
   */
  def testCheckpointing[U: ClassManifest](
      op: (RDD[Int]) => RDD[U],
      testRDDSize: Boolean = true,
      testRDDSplitSize: Boolean = false
    ) {
    // Generate the final RDD using given RDD operation
    val baseRDD = generateLongLineageRDD()
    val operatedRDD = op(baseRDD)
    val parentRDD = operatedRDD.dependencies.headOption.orNull
    val rddType = operatedRDD.getClass.getSimpleName
    val numSplits = operatedRDD.splits.length

    // Find serialized sizes before and after the checkpoint
    val (rddSizeBeforeCheckpoint, splitSizeBeforeCheckpoint) = getSerializedSizes(operatedRDD)
    operatedRDD.checkpoint()
    val result = operatedRDD.collect()
    val (rddSizeAfterCheckpoint, splitSizeAfterCheckpoint) = getSerializedSizes(operatedRDD)

    // Test whether the checkpoint file has been created
    assert(sc.checkpointFile[U](operatedRDD.getCheckpointFile.get).collect() === result)

    // Test whether dependencies have been changed from its earlier parent RDD
    assert(operatedRDD.dependencies.head.rdd != parentRDD)

    // Test whether the splits have been changed to the new Hadoop splits
    assert(operatedRDD.splits.toList === operatedRDD.checkpointData.get.getSplits.toList)

    // Test whether the number of splits is same as before
    assert(operatedRDD.splits.length === numSplits)

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

    // Test whether serialized size of the splits has reduced. If the splits
    // do not have any non-transient reference to another RDD or another RDD's splits, it
    // does not refer to a lineage and therefore may not reduce in size after checkpointing.
    // However, if the original splits before checkpointing do refer to a parent RDD, the splits
    // must be forgotten after checkpointing (to remove all reference to parent RDDs) and
    // replaced with the HadoopSplits of the checkpointed RDD.
    if (testRDDSplitSize) {
      logInfo("Size of " + rddType + " splits "
        + "[" + splitSizeBeforeCheckpoint + " --> " + splitSizeAfterCheckpoint + "]")
      assert(
        splitSizeAfterCheckpoint < splitSizeBeforeCheckpoint,
        "Size of " + rddType + " splits did not reduce after checkpointing " +
          "[" + splitSizeBeforeCheckpoint + " --> " + splitSizeAfterCheckpoint + "]"
      )
    }
  }

  /**
   * Test whether checkpointing of the parent of the generated RDD also
   * truncates the lineage or not. Some RDDs like CoGroupedRDD hold on to its parent
   * RDDs splits. So even if the parent RDD is checkpointed and its splits changed,
   * this RDD will remember the splits and therefore potentially the whole lineage.
   */
  def testParentCheckpointing[U: ClassManifest](
      op: (RDD[Int]) => RDD[U],
      testRDDSize: Boolean,
      testRDDSplitSize: Boolean
    ) {
    // Generate the final RDD using given RDD operation
    val baseRDD = generateLongLineageRDD()
    val operatedRDD = op(baseRDD)
    val parentRDD = operatedRDD.dependencies.head.rdd
    val rddType = operatedRDD.getClass.getSimpleName
    val parentRDDType = parentRDD.getClass.getSimpleName

    // Get the splits and dependencies of the parent in case they're lazily computed
    parentRDD.dependencies
    parentRDD.splits

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

    // Test whether serialized size of the splits has reduced because of its parent being
    // checkpointed. If the splits do not have any non-transient reference to another RDD
    // or another RDD's splits, it does not refer to a lineage and therefore may not reduce
    // in size after checkpointing. However, if the splits do refer to the *splits* of a parent
    // RDD, then these splits must update reference to the parent RDD splits as the parent RDD's
    // splits must have changed after checkpointing.
    if (testRDDSplitSize) {
      assert(
        splitSizeAfterCheckpoint < splitSizeBeforeCheckpoint,
        "Size of " + rddType + " splits did not reduce after checkpointing parent " + parentRDDType +
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
   * Get serialized sizes of the RDD and its splits, in order to test whether the size shrinks
   * upon checkpointing. Ignores the checkpointData field, which may grow when we checkpoint.
   */
  def getSerializedSizes(rdd: RDD[_]): (Int, Int) = {
    (Utils.serialize(rdd).length - Utils.serialize(rdd.checkpointData).length,
     Utils.serialize(rdd.splits).length)
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
