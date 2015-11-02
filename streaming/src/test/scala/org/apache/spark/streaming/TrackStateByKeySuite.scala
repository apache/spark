package org.apache.spark.streaming

import java.io.File

import scala.collection.mutable.{SynchronizedBuffer, ArrayBuffer}
import scala.reflect.ClassTag

import org.scalatest.{BeforeAndAfter, BeforeAndAfterAll}

import org.apache.spark.util.{Utils, ManualClock}
import org.apache.spark.{SparkFunSuite, SparkConf, SparkContext}
import org.apache.spark.streaming.dstream.{EmittedRecordsDStream, DStream}

/**
 * Created by tdas on 10/29/15.
 */
class TrackStateByKeySuite extends SparkFunSuite with BeforeAndAfterAll with BeforeAndAfter {

  private var sc: SparkContext = null
  private var ssc: StreamingContext = null
  private var checkpointDir: File = null
  private val batchDuration = Seconds(1)

  before {
    StreamingContext.getActive().foreach { _.stop(stopSparkContext = false) }
    checkpointDir = Utils.createTempDir("checkpoint")

    ssc = new StreamingContext(sc, batchDuration)
    ssc.checkpoint(checkpointDir.toString)
  }

  after {
    StreamingContext.getActive().foreach { _.stop(stopSparkContext = false) }
  }

  override def beforeAll(): Unit = {
    val conf = new SparkConf().setMaster("local").setAppName("TrackStateByKeySuite")
    conf.set("spark.streaming.clock", classOf[ManualClock].getName())
    sc = new SparkContext(conf)
  }

  test("basic operation") {
    val inputData =
      Seq(
        Seq(),
        Seq("a"),
        Seq("a", "b"),
        Seq("a", "b", "c"),
        Seq("a", "b"),
        Seq("a"),
        Seq()
      )

    val outputData =
      Seq(
        Seq(),
        Seq("aa"),
        Seq("aa", "bb"),
        Seq("aa", "bb", "cc"),
        Seq("aa", "bb"),
        Seq("aa"),
        Seq()
      )

    val stateData =
      Seq(
        Seq(),
        Seq(("a", 1)),
        Seq(("a", 2), ("b", 1)),
        Seq(("a", 3), ("b", 2), ("c", 1)),
        Seq(("a", 4), ("b", 3), ("c", 1)),
        Seq(("a", 5), ("b", 3), ("c", 1)),
        Seq(("a", 5), ("b", 3), ("c", 1))
      )

      // state maintains running count, key string doubled and returned
      val trackStateFunc = (key: String, value: Option[Int], state: State[Int]) => {
        val sum = value.getOrElse(0) + state.getOrElse(0)
        state.update(sum)
        Some(key * 2)
    }

    testOperation(inputData, TrackStateSpec(trackStateFunc), outputData, stateData)
  }

  test("states as emitted records") {
    val inputData =
      Seq(
        Seq(),
        Seq("a"),
        Seq("a", "b"),
        Seq("a", "b", "c"),
        Seq("a", "b"),
        Seq("a"),
        Seq()
      )

    val outputData =
      Seq(
        Seq(),
        Seq(("a", 1)),
        Seq(("a", 2), ("b", 1)),
        Seq(("a", 3), ("b", 2), ("c", 1)),
        Seq(("a", 4), ("b", 3)),
        Seq(("a", 5)),
        Seq()
      )

    val stateData =
      Seq(
        Seq(),
        Seq(("a", 1)),
        Seq(("a", 2), ("b", 1)),
        Seq(("a", 3), ("b", 2), ("c", 1)),
        Seq(("a", 4), ("b", 3), ("c", 1)),
        Seq(("a", 5), ("b", 3), ("c", 1)),
        Seq(("a", 5), ("b", 3), ("c", 1))
      )

    val trackStateFunc = (key: String, value: Option[Int], state: State[Int]) => {
      val sum = value.getOrElse(0) + state.getOrElse(0)
      val output = (key, sum)
      state.update(sum)
      Some(output)
    }

    testOperation(inputData, TrackStateSpec(trackStateFunc), outputData, stateData)
  }

  test("state removing") {
    val inputData =
      Seq(
        Seq(),
        Seq("a"),
        Seq("a", "b"),
        Seq("a", "b", "c"),
        Seq("a", "b", "c"),
        Seq("a", "b"),
        Seq("a"),
        Seq()
      )

    // States that were removed
    val outputData =
      Seq(
        Seq(),
        Seq(),
        Seq("a"),
        Seq("b"),
        Seq("a", "c"),
        Seq("b"),
        Seq("a"),
        Seq()
      )

    val stateData =
      Seq(
        Seq(),
        Seq(("a", 1)),
        Seq(("b", 1)),
        Seq(("a", 1), ("c", 1)),
        Seq(("b", 1)),
        Seq(("a", 1)),
        Seq(),
        Seq()
      )

    val trackStateFunc = (key: String, value: Option[Int], state: State[Int]) => {
      if (state.exists) {
        state.remove()
        println(s"$key: state exists, removed state, and returning key")
        Some(key)
      } else {
        state.update(value.get)
        println(s"$key: State does not exists, saving state, and not returning anything")
        None
      }
    }

    testOperation(inputData, TrackStateSpec(trackStateFunc).numPartitions(1), outputData, stateData)
  }


  private def testOperation[K: ClassTag, S: ClassTag, T: ClassTag](
      input: Seq[Seq[K]],
      trackStateSpec: TrackStateSpec[K, Int, S, T],
      expectedOutputs: Seq[Seq[T]],
      expectedStateSnapshots: Seq[Seq[(K, S)]]
    ) {

    require(expectedOutputs.size == expectedStateSnapshots.size)

    // Setup the stream computation
    val inputStream = new TestInputStream(ssc, input, numPartitions = 2)
    val trackeStateStream = inputStream.map(x => (x, 1)).trackStateByKey(trackStateSpec)
    val collectedOutputs = new ArrayBuffer[Seq[T]] with SynchronizedBuffer[Seq[T]]
    val outputStream = new TestOutputStream(trackeStateStream, collectedOutputs)
    val collectedStateSnapshots = new ArrayBuffer[Seq[(K, S)]] with SynchronizedBuffer[Seq[(K, S)]]
    val stateSnapshotStream = new TestOutputStream(
      trackeStateStream.stateSnapshots(), collectedStateSnapshots)
    outputStream.register()
    stateSnapshotStream.register()

    val batchCounter = new BatchCounter(ssc)
    ssc.start()

    val numBatches = expectedOutputs.size
    val clock = ssc.scheduler.clock.asInstanceOf[ManualClock]
    clock.advance(batchDuration.milliseconds * numBatches)

    batchCounter.waitUntilBatchesCompleted(numBatches, 10000)
    assert(expectedOutputs, collectedOutputs, "outputs")
    assert(expectedStateSnapshots, collectedStateSnapshots, "state snapshots")
  }

  private def assert[U](expected: Seq[Seq[U]], collected: Seq[Seq[U]], typ: String) {
    assert(expected.size === collected.size,
      s"number of collected $typ (${collected.size}) different from expected (${expected.size})")
    expected.zip(collected).foreach { case (c, e) =>
      assert(c.toSet === e.toSet,
        s"collected $typ is different from expected" +
          "\nExpected:\n" + expected.mkString("\n") +
          "\nCollected:\n" + collected.mkString("\n")
      )
    }
  }


}
