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

import scala.collection.mutable

import org.apache.spark.sql.{Dataset, SparkSession}
import org.apache.spark.sql.catalyst.encoders.{ExpressionEncoder, encoderFor}
import org.apache.spark.sql.catalyst.expressions.UnsafeRow
import org.apache.spark.sql.classic.ClassicConversions.castToImpl
import org.apache.spark.sql.execution.streaming.runtime.{MemoryStream, StreamingQueryWrapper}
import org.apache.spark.sql.execution.streaming.sources.MemorySink
import org.apache.spark.sql.internal.SQLConf

class TwsTester2[
    K: org.apache.spark.sql.Encoder,
    I: org.apache.spark.sql.Encoder,
    O: org.apache.spark.sql.Encoder](
    val processor: StatefulProcessor[K, I, O]
)(implicit sparkSession: SparkSession) {
  
  // Save original config values and set required configs for TwsTester2
  private val originalStateStoreProvider = sparkSession.conf.getOption(SQLConf.STATE_STORE_PROVIDER_CLASS.key)
  private val originalShufflePartitions = sparkSession.conf.getOption(SQLConf.SHUFFLE_PARTITIONS.key)
  
  sparkSession.conf.set(SQLConf.STATE_STORE_PROVIDER_CLASS.key, classOf[InMemoryStateStoreProvider].getName)
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
  private val checkpointDir = org.apache.spark.util.Utils.createTempDir(
    namePrefix = "streaming.metadata").getCanonicalPath
  
  private val classicSession = castToImpl(sparkSession)
  private val query = classicSession.streams.startQuery(
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
  ).asInstanceOf[StreamingQueryWrapper].streamingQuery
  
  query.awaitInitialization(30000)
  
  // Encoder/decoder to convert Row to O
  private val outputEncoder = encoderFor[O]
  private val rowSerializer = ExpressionEncoder(outputEncoder.schema)
    .resolveAndBind().createSerializer()
  private val rowDeserializer = outputEncoder.resolveAndBind().createDeserializer()
  
  private var lastBatchId: Long = -1L
  
  // Key encoder for state access
  private val keyEncoder = encoderFor[K].resolveAndBind()
  
  // State store provider for accessing state (lazy initialization)
  private lazy val stateStoreProvider: InMemoryStateStoreProvider = {
    // Trigger initialization by processing an empty batch
    inputStream.addData(List.empty)
    query.processAllAvailable()

    InMemoryStateStoreProvider.getProvider().get
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
    
    // Convert external Rows to O via InternalRow
    rows.map(row => rowDeserializer(rowSerializer(row))).toList
  }
  
  def stop(): Unit = {
    query.stop()
    
    // Restore original config values
    originalStateStoreProvider match {
      case Some(value) => sparkSession.conf.set(SQLConf.STATE_STORE_PROVIDER_CLASS.key, value)
      case None => sparkSession.conf.unset(SQLConf.STATE_STORE_PROVIDER_CLASS.key)
    }
    originalShufflePartitions match {
      case Some(value) => sparkSession.conf.set(SQLConf.SHUFFLE_PARTITIONS.key, value)
      case None => sparkSession.conf.unset(SQLConf.SHUFFLE_PARTITIONS.key)
    }
  }
  
  /**
   * Sets the value state for a given key.
   *
   * @param stateName the name of the value state variable
   * @param key the grouping key
   * @param value the value to set in the state
   */
  def setValueState[T: org.apache.spark.sql.Encoder](stateName: String, key: K, value: T): Unit = {
    val valueEncoder = encoderFor[T].resolveAndBind()
    
    val keyRow = keyEncoder.createSerializer()(key).asInstanceOf[UnsafeRow].copy()
    val valueRow = valueEncoder.createSerializer()(value).asInstanceOf[UnsafeRow].copy()
    
    val stores = stateStoreProvider.getStores()
    val stateStore = stores.getOrElseUpdate(stateName, mutable.Map.empty[UnsafeRow, UnsafeRow])
    stateStore(keyRow) = valueRow
  }
  
  /**
   * Retrieves the value state for a given key without modifying it.
   *
   * @param stateName the name of the value state variable
   * @param key the grouping key
   * @return Some(value) if the state exists, None otherwise
   */
  def peekValueState[T: org.apache.spark.sql.Encoder](stateName: String, key: K): Option[T] = {
    val valueEncoder = encoderFor[T].resolveAndBind()
    
    val keyRow = keyEncoder.createSerializer()(key).asInstanceOf[UnsafeRow]
    
    val stores = stateStoreProvider.getStores()
    stores.get(stateName).flatMap(_.get(keyRow)).map { valueRow =>
      valueEncoder.createDeserializer()(valueRow)
    }
  }

  /**
   * Sets the list state for a given key.
   *
   * @param stateName the name of the list state variable
   * @param key the grouping key
   * @param values the list of values to set
   */
  def setListState[T: org.apache.spark.sql.Encoder](stateName: String, key: K, values: List[T]): Unit = {
    val valueEncoder = encoderFor[T].resolveAndBind()
    
    val keyRow = keyEncoder.createSerializer()(key).asInstanceOf[UnsafeRow].copy()
    val valueRows = values.map(v => valueEncoder.createSerializer()(v).asInstanceOf[UnsafeRow].copy()).toArray
    
    val listStores = stateStoreProvider.getListStores()
    val listStore = listStores.getOrElseUpdate(stateName, mutable.Map.empty[UnsafeRow, Array[UnsafeRow]])
    listStore(keyRow) = valueRows
  }

  /**
   * Retrieves the list state for a given key without modifying it.
   *
   * @param stateName the name of the list state variable
   * @param key the grouping key
   * @return the list of values, or an empty list if no state exists for the key
   */
  def peekListState[T: org.apache.spark.sql.Encoder](stateName: String, key: K): List[T] = {
    val valueEncoder = encoderFor[T].resolveAndBind()
    val deserializer = valueEncoder.createDeserializer()
    
    val keyRow = keyEncoder.createSerializer()(key).asInstanceOf[UnsafeRow]
    
    val listStores = stateStoreProvider.getListStores()
    listStores.get(stateName).flatMap(_.get(keyRow)).map { valueRows =>
      valueRows.iterator.map(row => deserializer(row)).toList
    }.getOrElse(List.empty[T])
  }

  /**
   * Sets the map state for a given key.
   *
   * @param stateName the name of the map state variable
   * @param key the grouping key
   * @param values the map of key-value pairs to set
   */
  def setMapState[MK: org.apache.spark.sql.Encoder, MV: org.apache.spark.sql.Encoder](
      stateName: String, key: K, values: Map[MK, MV]): Unit = {

  }

  /**
   * Retrieves the map state for a given key without modifying it.
   *
   * @param stateName the name of the map state variable
   * @param key the grouping key
   * @return the map of key-value pairs, or an empty map if no state exists for the key
   */
  def peekMapState[MK: org.apache.spark.sql.Encoder, MV: org.apache.spark.sql.Encoder](
      stateName: String, key: K): Map[MK, MV] = {
    Map.empty[MK, MV]
  }

}

