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

package org.apache.spark.shuffle

import scala.collection.mutable

import org.mockito.Mockito.mock

import org.apache.spark._
import org.apache.spark.internal.config.{SHUFFLE_MANAGER, SHUFFLE_MANAGER_INCREMENTAL}
import org.apache.spark.rdd.RDD
import org.apache.spark.shuffle.sort.SortShuffleManager
import org.apache.spark.shuffle.streaming.StreamingShuffleManager

/**
 * A recording ShuffleManager test double: it records the shuffle ids it was asked to register so a
 * test can assert that [[SparkEnv.shuffleManagerFor]] routed a dependency to the correct manager.
 * Every instance registers itself in a process-wide registry keyed by class, because the two
 * managers live in private SparkEnv fields and a test otherwise has no reference to them.
 * Constructor takes (SparkConf, Boolean) to match how SparkEnv instantiates managers. This base
 * declares no shuffle kind; each concrete subclass mixes in [[BlockingShuffleManager]] or
 * [[PipelinedShuffleManager]] so it is accepted in the corresponding SparkEnv slot.
 */
private abstract class RecordingShuffleManager(conf: SparkConf, isDriver: Boolean)
  extends ShuffleManager {
  RecordingShuffleManager.register(this)
  val registered = mutable.ArrayBuffer[Int]()
  val unregistered = mutable.ArrayBuffer[Int]()
  @volatile var stopped = false

  override def registerShuffle[K, V, C](
      shuffleId: Int, dependency: ShuffleDependency[K, V, C]): ShuffleHandle = {
    registered += shuffleId
    new RecordingShuffleManager.RecordingHandle(shuffleId)
  }
  override def getWriter[K, V](
      handle: ShuffleHandle, mapId: Long, context: TaskContext,
      metrics: ShuffleWriteMetricsReporter): ShuffleWriter[K, V] = null
  override def getReader[K, C](
      handle: ShuffleHandle, startMapIndex: Int, endMapIndex: Int, startPartition: Int,
      endPartition: Int, context: TaskContext, metrics: ShuffleReadMetricsReporter)
      : ShuffleReader[K, C] = null
  override def unregisterShuffle(shuffleId: Int): Boolean = {
    unregistered += shuffleId
    true
  }
  override def stop(): Unit = stopped = true
}

private object RecordingShuffleManager {
  class RecordingHandle(shuffleId: Int) extends ShuffleHandle(shuffleId)

  // Registry so tests can reach the instances SparkEnv built. Cleared per test.
  private val instances = mutable.ArrayBuffer[RecordingShuffleManager]()
  def register(m: RecordingShuffleManager): Unit = synchronized { instances += m }
  def clear(): Unit = synchronized { instances.clear() }
  def of[T <: RecordingShuffleManager](cls: Class[T]): T = synchronized {
    instances.find(cls.isInstance).getOrElse(
      throw new NoSuchElementException(s"no ${cls.getSimpleName} was instantiated")).asInstanceOf[T]
  }
}

// Distinct subclasses so SparkEnv instantiates two different classes (blocking vs pipelined) and
// tests can tell which one a dependency was routed to. Each declares its kind so it is accepted in
// the matching SparkEnv slot: the blocking manager (spark.shuffle.manager) must be a
// BlockingShuffleManager, the pipelined manager (spark.shuffle.manager.incremental) a
// PipelinedShuffleManager. The routing tests never exercise the resolver, so it returns a mock.
private class DefaultRecordingManager(conf: SparkConf, isDriver: Boolean)
  extends RecordingShuffleManager(conf, isDriver) with BlockingShuffleManager {
  override def shuffleBlockResolver: ShuffleBlockResolver = mock(classOf[ShuffleBlockResolver])
}
private class IncrementalRecordingManager(conf: SparkConf, isDriver: Boolean)
  extends RecordingShuffleManager(conf, isDriver) with PipelinedShuffleManager

/**
 * Tests routing of shuffles to the default vs. incremental [[ShuffleManager]] by dependency type,
 * done natively in [[SparkEnv.shuffleManagerFor]] with no wrapping "router" manager. A
 * [[PipelinedShuffleDependency]] routes to the manager named by spark.shuffle.manager.incremental;
 * every other dependency routes to spark.shuffle.manager. `SparkEnv.blockingShuffleManager` remains
 * the plain configured manager (nothing is installed "in front" of it). The id-only cleanup path
 * (`unregisterShuffleFromAllManagers`) notifies both managers, since a shuffleId alone does not
 * identify the owner.
 */
class PipelinedShuffleRoutingSuite extends SparkFunSuite with LocalSparkContext {

  override def beforeEach(): Unit = {
    super.beforeEach()
    RecordingShuffleManager.clear()
  }

  private def newConf(): SparkConf = new SparkConf(loadDefaults = false)
    .set(SHUFFLE_MANAGER, classOf[DefaultRecordingManager].getName)
    .set(SHUFFLE_MANAGER_INCREMENTAL, classOf[IncrementalRecordingManager].getName)

  /** Start a SparkContext with the given conf and return the live SparkEnv. */
  private def startEnv(conf: SparkConf = newConf()): SparkEnv = {
    sc = new SparkContext("local", "test", conf)
    SparkEnv.get
  }

  private def defaultMgr = RecordingShuffleManager.of(classOf[DefaultRecordingManager])
  private def incrementalMgr = RecordingShuffleManager.of(classOf[IncrementalRecordingManager])

  private def pipelinedDep(sc: SparkContext): PipelinedShuffleDependency[Int, Int, Int] = {
    val rdd: RDD[(Int, Int)] = sc.parallelize(1 to 4, 2).map(x => (x, x))
    new PipelinedShuffleDependency[Int, Int, Int](rdd, new HashPartitioner(2))
  }
  private def regularDep(sc: SparkContext): ShuffleDependency[Int, Int, Int] = {
    val rdd: RDD[(Int, Int)] = sc.parallelize(1 to 4, 2).map(x => (x, x))
    new ShuffleDependency[Int, Int, Int](rdd, new HashPartitioner(2))
  }

  /** Walk an exception's cause chain to find the first cause of the given type, if any. */
  private def findCause[T <: Throwable : reflect.ClassTag](t: Throwable): Option[T] = {
    val cls = implicitly[reflect.ClassTag[T]].runtimeClass
    Iterator.iterate(t)(_.getCause).takeWhile(_ != null)
      .find(cls.isInstance).map(_.asInstanceOf[T])
  }

  test("blockingShuffleManager is the plain configured manager (no wrapper is installed)") {
    val env = startEnv()
    // The blocking manager is exactly the configured one -- not a router/composite in front of it
    // -- so callers that inspect its type (e.g. ShuffleExchangeExec) see the real manager.
    assert(env.blockingShuffleManager.isInstanceOf[DefaultRecordingManager])
  }

  test("shuffleManagerFor routes a regular dependency to the default manager") {
    val env = startEnv()
    val dep = regularDep(sc)
    assert(env.shuffleManagerFor(dep).isInstanceOf[DefaultRecordingManager])
    // registerShuffle (in the dependency's constructor) went to the default manager, not the
    // incremental one.
    assert(defaultMgr.registered.contains(dep.shuffleId))
    assert(!incrementalMgr.registered.contains(dep.shuffleId))
  }

  test("shuffleManagerFor routes a pipelined dependency to the incremental manager") {
    val env = startEnv()
    val dep = pipelinedDep(sc)
    assert(env.shuffleManagerFor(dep).isInstanceOf[IncrementalRecordingManager])
    // registerShuffle went to the incremental manager, not the default
    assert(incrementalMgr.registered.contains(dep.shuffleId))
    assert(!defaultMgr.registered.contains(dep.shuffleId))
  }

  test("unregisterShuffleFromAllManagers notifies both managers (owner unknown from id alone)") {
    val env = startEnv()
    // The RemoveShuffle path holds only a shuffleId and cannot tell which manager owns it, and must
    // reach the owner even on a node that never did this shuffle's I/O -- so it notifies both. A
    // fresh id that was never registered exercises exactly that "don't know the owner" case.
    assert(env.unregisterShuffleFromAllManagers(4242))
    assert(defaultMgr.unregistered.contains(4242), "the default manager must be unregistered")
    assert(incrementalMgr.unregistered.contains(4242),
      "the incremental manager must be unregistered")
  }

  test("unregisterShuffleFromAllManagers notifies the default when no incremental is configured") {
    val without = new SparkConf(loadDefaults = false)
      .set(SHUFFLE_MANAGER, classOf[DefaultRecordingManager].getName)
    val env = startEnv(without)
    assert(env.unregisterShuffleFromAllManagers(4242))
    assert(defaultMgr.unregistered.contains(4242))
  }

  test("unregisterShuffleFromAllManagers is a no-op (not an NPE) before managers are initialized") {
    // Disable the incremental slot (it defaults to streaming) so there is genuinely nothing to
    // notify once the blocking manager is forced back to its pre-init null state below.
    val without = new SparkConf(loadDefaults = false)
      .set(SHUFFLE_MANAGER, classOf[DefaultRecordingManager].getName)
      .set(SHUFFLE_MANAGER_INCREMENTAL, "")
    val env = startEnv(without)
    // SPARK-45762 defers shuffle-manager init behind a latch (so user jars load first), leaving
    // `_blockingShuffleManager` null until initializeShuffleManager runs. A RemoveShuffle RPC can
    // arrive in that window; it must be skipped, not dereference the null manager. Reflectively
    // reproduce the pre-init state, since a live SparkContext finishes init during startup.
    val field = classOf[SparkEnv].getDeclaredField("_blockingShuffleManager")
    field.setAccessible(true)
    val original = field.get(env)
    field.set(env, null)
    try {
      assert(!env.isShuffleManagerInitialized, "precondition: the manager must look uninitialized")
      // No incremental manager is configured either, so there is nothing to notify: the call must
      // return false without throwing. Before the null guard this threw NullPointerException.
      assert(!env.unregisterShuffleFromAllManagers(4242))
    } finally {
      field.set(env, original)
    }
  }

  test("the handle is minted by the routed manager (unwrapped) so driver and executor agree") {
    startEnv()
    // The dependency's shuffleHandle is produced by the incremental manager's registerShuffle and
    // is that manager's own handle type -- there is no wrapping handle to route on. Executors that
    // later read dep.shuffleHandle route by the same dependency type, so they hit the same manager.
    val dep = pipelinedDep(sc)
    assert(dep.shuffleHandle.isInstanceOf[RecordingShuffleManager.RecordingHandle])
    assert(dep.shuffleHandle.shuffleId === dep.shuffleId)
  }

  test("with the incremental manager disabled, a pipelined dependency falls back to the default") {
    // The incremental slot defaults to the streaming manager; setting it empty disables it, so
    // there is no pipelined manager and a pipelined dependency falls back to the blocking manager.
    val without = new SparkConf(loadDefaults = false)
      .set(SHUFFLE_MANAGER, classOf[DefaultRecordingManager].getName)
      .set(SHUFFLE_MANAGER_INCREMENTAL, "")
    val env = startEnv(without)
    // A regular dependency routes to the default manager.
    assert(env.shuffleManagerFor(regularDep(sc)).isInstanceOf[DefaultRecordingManager])
    // A pipelined dependency also routes to the default manager (served as an ordinary materialized
    // shuffle) when the incremental manager is disabled.
    assert(env.shuffleManagerFor(pipelinedDep(sc)).isInstanceOf[DefaultRecordingManager])
  }

  test("both managers are stopped when the SparkContext stops") {
    startEnv()
    // Touch a pipelined dep so the incremental manager instance exists in the registry.
    pipelinedDep(sc)
    val default = defaultMgr
    val incremental = incrementalMgr
    // Stopping the context tears down its SparkEnv, which must stop BOTH managers (default and
    // incremental). Clear `sc` afterward so LocalSparkContext's afterEach does not double-stop.
    sc.stop()
    sc = null
    assert(default.stopped, "the default manager must be stopped")
    assert(incremental.stopped, "the incremental manager must be stopped")
  }

  test("SparkEnv initializes the streaming shuffle tracker when the incremental manager is " +
      "StreamingShuffleManager") {
    val conf = new SparkConf(loadDefaults = false)
      .set(SHUFFLE_MANAGER, classOf[DefaultRecordingManager].getName)
      .set(SHUFFLE_MANAGER_INCREMENTAL, classOf[StreamingShuffleManager].getName)
    sc = new SparkContext("local", "test", conf)
    assert(SparkEnv.get.streamingShuffleOutputTracker.isDefined)
  }

  test("SparkEnv does not initialize the tracker when the incremental manager is not streaming") {
    sc = new SparkContext("local", "test", newConf())
    assert(SparkEnv.get.streamingShuffleOutputTracker.isEmpty)
  }

  test("spark.shuffle.manager.incremental resolves the same short aliases as the default manager") {
    // "sort" is a short alias the incremental slot must resolve (to SortShuffleManager) rather than
    // treat as a class name. SortShuffleManager is blocking, so it is rejected from the pipelined
    // slot -- proving the alias resolved (a ClassNotFoundException would mean it did not). See the
    // streaming-tracker test for a valid pipelined manager in this slot.
    val conf = new SparkConf(loadDefaults = false)
      .set(SHUFFLE_MANAGER, classOf[DefaultRecordingManager].getName)
      .set(SHUFFLE_MANAGER_INCREMENTAL, "sort")
    val e = intercept[Exception] {
      sc = new SparkContext("local", "test", conf)
    }
    assert(findCause[IllegalArgumentException](e).exists(
      _.getMessage.contains("must be a PipelinedShuffleManager")),
      s"expected a PipelinedShuffleManager rejection, got: $e")
  }

  test("a blocking manager in the incremental slot is rejected at startup") {
    val conf = new SparkConf(loadDefaults = false)
      .set(SHUFFLE_MANAGER, classOf[DefaultRecordingManager].getName)
      // DefaultRecordingManager is a BlockingShuffleManager; the pipelined slot must reject it, so
      // a shuffle routed there can never write blocks unreachable through the block resolver.
      .set(SHUFFLE_MANAGER_INCREMENTAL, classOf[DefaultRecordingManager].getName)
    val e = intercept[Exception] {
      sc = new SparkContext("local", "test", conf)
    }
    assert(findCause[IllegalArgumentException](e).exists(
      _.getMessage.contains("must be a PipelinedShuffleManager")),
      s"expected a PipelinedShuffleManager rejection, got: $e")
  }

  test("a bad incremental manager class name surfaces a clear error at startup") {
    val badConf = new SparkConf(loadDefaults = false)
      .set(SHUFFLE_MANAGER, "sort")
      .set(SHUFFLE_MANAGER_INCREMENTAL, "org.apache.spark.NotAShuffleManager")
    // The incremental manager is instantiated eagerly during SparkEnv init, so a bad class name
    // fails context startup rather than silently.
    intercept[Exception] {
      sc = new SparkContext("local", "test", badConf)
    }
  }

  test("a manager's type declares its shuffle kind (blocking vs pipelined)") {
    // SortShuffleManager materializes blocks -- it is a BlockingShuffleManager with a resolver.
    val sortMgr = new SortShuffleManager(new SparkConf(loadDefaults = false))
    assert(sortMgr.isInstanceOf[BlockingShuffleManager])
    // StreamingShuffleManager serves output out-of-band -- it is a PipelinedShuffleManager with no
    // resolver (the shuffleBlockResolver method was removed from the ShuffleManager interface).
    val streaming = new StreamingShuffleManager
    assert(streaming.isInstanceOf[PipelinedShuffleManager])
    assert(!streaming.isInstanceOf[BlockingShuffleManager])
  }

  test("SparkEnv.shuffleBlockResolver is defined for the blocking manager") {
    // The blocking manager is SortShuffleManager -> a resolver is available. It is the only manager
    // that produces block-manager blocks, so block resolution always comes from it.
    val blockingConf = new SparkConf(loadDefaults = false).set(SHUFFLE_MANAGER, "sort")
    sc = new SparkContext("local", "test", blockingConf)
    assert(SparkEnv.get.shuffleBlockResolver.isDefined,
      "the blocking manager must expose a shuffleBlockResolver")
  }

  test("a pipelined manager in the default slot is rejected at startup") {
    // StreamingShuffleManager is pipelined; it cannot be the blocking manager, because block
    // resolution (reads, push-merge, decommission migration) has no source otherwise. Rejecting it
    // at startup means shuffleBlockResolver always has exactly one well-typed source.
    val pipelinedConf = new SparkConf(loadDefaults = false)
      .set(SHUFFLE_MANAGER, classOf[StreamingShuffleManager].getName)
    val e = intercept[Exception] {
      sc = new SparkContext("local", "test", pipelinedConf)
    }
    assert(findCause[IllegalArgumentException](e).exists(
      _.getMessage.contains("must be a BlockingShuffleManager")),
      s"expected a BlockingShuffleManager rejection, got: $e")
  }
}
