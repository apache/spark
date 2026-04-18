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
package org.apache.spark.udf.worker.core

import org.apache.spark.annotation.Experimental

/**
 * :: Experimental ::
 * Carries all information needed to initialize a UDF execution on a worker.
 *
 * This message is passed to [[WorkerSession#init]] and contains the function
 * definition, schemas, and any additional configuration. It is designed to be
 * extended in future versions with new fields (e.g., UDF shape, data format,
 * Spark context metadata, chaining information) without breaking existing
 * worker implementations.
 *
 * @param functionPayload serialized function (e.g., pickled Python, JVM bytes)
 * @param inputSchema     serialized input schema (e.g., Arrow schema bytes)
 * @param outputSchema    serialized output schema (e.g., Arrow schema bytes)
 * @param properties      additional key-value configuration. Can carry
 *                        protocol-specific or engine-specific metadata that
 *                        does not yet have a dedicated field.
 */
@Experimental
case class InitMessage(
    functionPayload: Array[Byte],
    inputSchema: Array[Byte],
    outputSchema: Array[Byte],
    properties: Map[String, String] = Map.empty)

/**
 * :: Experimental ::
 * One UDF execution on a worker -- the main interface Spark uses to run UDFs.
 *
 * A [[WorkerSession]] is the '''per-UDF-invocation''' handle that Spark
 * obtains from [[WorkerDispatcher#createSession]]. It carries the full
 * init / data-stream / finish lifecycle for a single UDF evaluation.
 *
 * A [[WorkerSession]] does ''not'' own the underlying worker or its
 * transport channel -- those are managed by the [[WorkerDispatcher]].
 * Multiple sessions may share the same worker when the worker supports
 * concurrency.
 *
 * '''Usage:'''
 * {{{
 *   val session = dispatcher.createSession(securityScope = None)
 *   try {
 *     session.init(InitMessage(functionPayload, inputSchema, outputSchema))
 *     val results = session.process(inputBatches)
 *     results.foreach(handleBatch)
 *   } finally {
 *     session.close()
 *   }
 * }}}
 *
 * '''Lifecycle:'''
 *  - [[init]] must be called exactly once before [[process]].
 *  - [[process]] must be called at most once per session.
 *  - [[close]] must always be called (use try-finally).
 *  - [[cancel]] may be called at any time to abort execution.
 */
@Experimental
abstract class WorkerSession extends AutoCloseable {

  /**
   * Initializes the UDF execution. Must be called exactly once before
   * [[process]].
   *
   * @param message the initialization parameters including the serialized
   *                function, input/output schemas, and configuration.
   */
  def init(message: InitMessage): Unit

  /**
   * Processes input data through the worker and returns results.
   *
   * Follows Spark's Iterator-to-Iterator pattern: input batches are streamed
   * to the worker, and result batches are lazily pulled from the returned
   * iterator. The session sends a Finish signal to the worker when the input
   * iterator is exhausted.
   *
   * Must be called at most once per session.
   *
   * @param input iterator of raw input data batches (e.g., Arrow IPC)
   * @return iterator of raw result data batches
   */
  def process(input: Iterator[Array[Byte]]): Iterator[Array[Byte]]

  /**
   * Requests cancellation of the current UDF execution.
   *
   * '''Thread-safety:''' implementations must allow [[cancel]] to be called
   * from a thread different from the one driving [[process]] (typically a
   * task interruption thread). It may be invoked at any point after
   * [[init]] and should be a no-op if execution has already finished.
   */
  def cancel(): Unit

  /** Closes this session and releases resources. */
  override def close(): Unit
}
