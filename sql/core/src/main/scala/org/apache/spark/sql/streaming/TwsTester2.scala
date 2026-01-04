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
package org.apache.spark.sql.streaming

import org.apache.spark.sql.{Dataset, Encoder, SparkSession}
import org.apache.spark.sql.catalyst.encoders.{ExpressionEncoder, encoderFor}
import org.apache.spark.sql.catalyst.expressions.UnsafeRow
import org.apache.spark.sql.classic.ClassicConversions.castToImpl
import org.apache.spark.sql.execution.streaming.operators.stateful.transformwithstate.statefulprocessor.ImplicitGroupingKeyTracker
import org.apache.spark.sql.execution.streaming.operators.stateful.transformwithstate.statevariables.MapStateImpl
import org.apache.spark.sql.execution.streaming.runtime.{MemoryStream, StreamingQueryWrapper}
import org.apache.spark.sql.execution.streaming.sources.MemorySink
import org.apache.spark.sql.execution.streaming.state.{RocksDBStateStoreProvider, StateStore}
import org.apache.spark.sql.internal.SQLConf

class TwsTester2[
    K: org.apache.spark.sql.Encoder,
    I: org.apache.spark.sql.Encoder,
    O: org.apache.spark.sql.Encoder](
    val processor: StatefulProcessor[K, I, O],
    val useRocksDB: Boolean = false
)(implicit sparkSession: SparkSession) {

  sparkSession.conf
    .set(
      SQLConf.STATE_STORE_PROVIDER_CLASS.key,
      if (useRocksDB) classOf[RocksDBStateStoreProvider].getName
      else classOf[InMemoryStateStoreProvider].getName
    )
  sparkSession.conf.set(SQLConf.SHUFFLE_PARTITIONS.key, "1")

  implicit val tupleEncoder: org.apache.spark.sql.Encoder[(K, I)] =
    org.apache.spark.sql.Encoders.tuple(
      implicitly[org.apache.spark.sql.Encoder[K]],
      implicitly[org.apache.spark.sql.Encoder[I]]
    )
  private val inputStream: MemoryStream[(K, I)] = MemoryStream[(K, I)]
  private val resultDs: Dataset[O] = inputStream
    .toDS()
    .groupByKey(_._1)
    .mapValues(_._2)
    .transformWithState(processor, TimeMode.None(), OutputMode.Append())

  // Create MemorySink and start the streaming query once
  private val sink = new MemorySink()
  private val checkpointDir =
    org.apache.spark.util.Utils.createTempDir(namePrefix = "streaming.metadata").getCanonicalPath

  private val classicSession = castToImpl(sparkSession)
  private val query = classicSession.streams
    .startQuery(
      userSpecifiedName = None,
      userSpecifiedCheckpointLocation = Some(checkpointDir),
      df = resultDs.toDF(),
      extraOptions = Map.empty,
      sink = sink,
      outputMode = OutputMode.Append(),
      useTempCheckpointLocation = false,
      recoverFromCheckpointLocation = true,
      trigger = Trigger.ProcessingTime(0),
      triggerClock = new org.apache.spark.util.SystemClock()
    )
    .asInstanceOf[StreamingQueryWrapper]
    .streamingQuery

  query.awaitInitialization(30000)

  // Encoder/decoder to convert Row to O
  private val outputEncoder = encoderFor[O]
  private val rowSerializer = ExpressionEncoder(outputEncoder.schema)
    .resolveAndBind()
    .createSerializer()
  private val rowDeserializer = outputEncoder.resolveAndBind().createDeserializer()

  private var lastBatchId: Long = -1L

  // Track the current state version for manual state modifications
  private var currentStateVersion: Long = 0

  // Key encoder for state access
  private val keyEncoder = encoderFor[K].resolveAndBind()

  // State store provider for accessing state (lazy initialization)
  private lazy val stateStoreProvider: InMemoryStateStoreProvider = {
    // Trigger initialization by processing an empty batch
    inputStream.addData(List.empty)
    query.processAllAvailable()

    InMemoryStateStoreProvider.getProvider().get
  }

  // RocksDB State store provider for accessing state (lazy initialization)
  private lazy val rocksDBStateStoreProvider: RocksDBStateStoreProvider = {
    // Trigger initialization by processing an empty batch
    inputStream.addData(List.empty)
    query.processAllAvailable()

    RocksDBStateStoreProvider.getProvider().get
  }

  def test(input: List[(K, I)]): List[O] = {
    inputStream.addData(input)
    query.processAllAvailable()

    // Get only new data since last batch
    val rows = if (lastBatchId == -1L) {
      sink.allData
    } else {
      sink.dataSinceBatch(lastBatchId)
    }
    lastBatchId = sink.latestBatchId.getOrElse(-1L)

    // Update current state version - batch N commits to version N+1
    currentStateVersion = lastBatchId + 1

    // Convert external Rows to O via InternalRow
    rows.map(row => rowDeserializer(rowSerializer(row))).toList
  }

  def testOneRow(key: K, inputRow: I): List[O] = test(List((key, inputRow)))

  def stop(): Unit = {
    query.stop()
  }

  def keyToRow(key: K) = keyEncoder.createSerializer()(key).asInstanceOf[UnsafeRow].copy()

  /**
   * Gets a WRITABLE state store at the current state version.
   * This is needed for setValueState/setListState/setMapState to work.
   */
  private def getWritableStateStore: StateStore = {
    val provider = if (useRocksDB) rocksDBStateStoreProvider else stateStoreProvider
    // Get a writable store at the current version
    provider.getStore(currentStateVersion)
  }

  /**
   * Gets a READ-ONLY state store for peeking at state.
   */
  private def getReadOnlyStateStore: StateStore = {
    if (useRocksDB) {
      rocksDBStateStoreProvider.getLatestStore()
    } else {
      stateStoreProvider.getLatestStore()
    }
  }

  /**
   * Sets the value state for a given key.
   *
   * @param stateName the name of the value state variable
   * @param key the grouping key
   * @param value the value to set in the state
   */
  def setValueState[T: Encoder](stateName: String, key: K, value: T): Unit = {
    //val valueState = processor.getHandle.getValueState[T](stateName, TTLConfig.NONE)
    //ImplicitGroupingKeyTracker.setImplicitKey(key)
    //valueState.update(value)
    val serializer = encoderFor[T].resolveAndBind().createSerializer()
    val valueRow = serializer(value).asInstanceOf[UnsafeRow].copy()
    val store = getWritableStateStore
    store.put(keyToRow(key), valueRow, stateName)
    val newVersion = store.commit()
    // Update currentStateVersion to the version we just committed
    currentStateVersion = newVersion
  }

  /**
   * Retrieves the value state for a given key without modifying it.
   *
   * @param stateName the name of the value state variable
   * @param key the grouping key
   * @return Some(value) if the state exists, None otherwise
   */
  def peekValueState[T: org.apache.spark.sql.Encoder](stateName: String, key: K): Option[T] = {
    //val valueState = processor.getHandle.getValueState[T](stateName, TTLConfig.NONE)
    //ImplicitGroupingKeyTracker.setImplicitKey(key)
    //Option(valueState.get())

    val deserializer = encoderFor[T].resolveAndBind().createDeserializer()
    println("AAA calling store.get...")
    val store = getReadOnlyStateStore
    val ans = store.get(keyToRow(key), stateName)
    store.abort() // Clean up the store after reading
    println("AAA store.get returned.")
    Option(ans).map(deserializer)
  }

  /**
   * Sets the list state for a given key.
   *
   * @param stateName the name of the list state variable
   * @param key the grouping key
   * @param values the list of values to set
   */
  def setListState[T: org.apache.spark.sql.Encoder](
      stateName: String,
      key: K,
      values: List[T]): Unit = {
    val serializer = encoderFor[T].resolveAndBind().createSerializer()
    val valueRows =
      values.map(v => serializer(v).asInstanceOf[UnsafeRow].copy()).toArray
    val store = getWritableStateStore
    store.putList(keyToRow(key), valueRows, stateName)
    val newVersion = store.commit()
    currentStateVersion = newVersion
  }

  /**
   * Retrieves the list state for a given key without modifying it.
   *
   * @param stateName the name of the list state variable
   * @param key the grouping key
   * @return the list of values, or an empty list if no state exists for the key
   */
  def peekListState[T: org.apache.spark.sql.Encoder](stateName: String, key: K): List[T] = {
    val deserializer = encoderFor[T].resolveAndBind().createDeserializer()
    val store = getReadOnlyStateStore
    val result = store.valuesIterator(keyToRow(key), stateName).map(deserializer).toList
    store.abort() // Clean up the store after reading
    result
  }

  /**
   * Sets the map state for a given key.
   *
   * @param stateName the name of the map state variable
   * @param key the grouping key
   * @param values the map of key-value pairs to set
   */
  def setMapState[MK: org.apache.spark.sql.Encoder, MV: org.apache.spark.sql.Encoder](
      stateName: String,
      key: K,
      values: Map[MK, MV]): Unit = {
    val store = getWritableStateStore
    val mapState: MapStateImpl[MK, MV] = new MapStateImpl(
      store,
      stateName,
      encoderFor[K].resolveAndBind().asInstanceOf[ExpressionEncoder[Any]],
      encoderFor[MK].resolveAndBind().asInstanceOf[ExpressionEncoder[Any]],
      encoderFor[MV].resolveAndBind().asInstanceOf[ExpressionEncoder[Any]]
    )
    ImplicitGroupingKeyTracker.setImplicitKey(key)
    mapState.clear()
    values.foreachEntry((k, v) => mapState.updateValue(k, v))
    val newVersion = store.commit()
    currentStateVersion = newVersion
  }

  /**
   * Retrieves the map state for a given key without modifying it.
   *
   * @param stateName the name of the map state variable
   * @param key the grouping key
   * @return the map of key-value pairs, or an empty map if no state exists for the key
   */
  def peekMapState[MK: Encoder, MV: Encoder](stateName: String, key: K): Map[MK, MV] = {
    val store = getReadOnlyStateStore
    val mapState = new MapStateImpl(
      store,
      stateName,
      encoderFor[K].resolveAndBind().asInstanceOf[ExpressionEncoder[Any]],
      encoderFor[MK].resolveAndBind().asInstanceOf[ExpressionEncoder[Any]],
      encoderFor[MV].resolveAndBind().asInstanceOf[ExpressionEncoder[Any]]
    )
    ImplicitGroupingKeyTracker.setImplicitKey(key)
    val result: Map[MK, MV] = mapState.iterator().toMap
    store.abort() // Clean up the store after reading
    result
  }

}
