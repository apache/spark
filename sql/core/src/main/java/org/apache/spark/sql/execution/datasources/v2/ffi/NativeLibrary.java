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

package org.apache.spark.sql.execution.datasources.v2.ffi;

import java.lang.invoke.MethodHandle;
import java.lang.invoke.MethodHandles;
import java.lang.invoke.MethodType;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;

import org.apache.spark.sql.execution.datasources.v2.ffi.Ffm.C;
import org.apache.spark.unsafe.Platform;

/**
 * A native data source library, which implements the C interface in
 * {@code org/apache/spark/sql/datasource/spark_datasource.h}, called through the Foreign Function
 * and Memory (FFM) API of Java 22 and later, see {@link Ffm}. Each function of the library is
 * bound once, when the library is loaded. The memory that Spark passes to the library is allocated
 * with {@link Platform}.
 * <p>
 * Each library is looked up separately, so all the libraries can export the same functions.
 * Calling a function that the library does not export throws {@link MissingFunctionException}. A
 * function that fails throws a {@link RuntimeException} with the message of
 * {@code spark_ds_last_error}.
 */
final class NativeLibrary {

  /** The version of the interface that Spark supports, {@code SPARK_DS_ABI_VERSION}. */
  static final int ABI_VERSION = 1;

  /** The first version of Java with the final FFM API. */
  static final int MINIMUM_JAVA_VERSION = 22;

  /** Whether this JVM can load native data source libraries. */
  static boolean isSupported() {
    return Runtime.version().feature() >= MINIMUM_JAVA_VERSION;
  }

  private final MethodHandle lastError;
  private final MethodHandle abiVersion;
  private final MethodHandle create;
  private final MethodHandle schema;
  private final MethodHandle createReader;
  private final MethodHandle createStreamReader;
  private final MethodHandle createWriter;
  private final MethodHandle createStreamWriter;
  private final MethodHandle close;
  private final MethodHandle pushPredicates;
  private final MethodHandle pushLimit;
  private final MethodHandle pruneColumns;
  private final MethodHandle partitions;
  private final MethodHandle serializeReader;
  private final MethodHandle closeReader;
  private final MethodHandle initialOffset;
  private final MethodHandle latestOffset;
  private final MethodHandle streamPartitions;
  private final MethodHandle serializeStreamReader;
  private final MethodHandle commitOffset;
  private final MethodHandle closeStreamReader;
  private final MethodHandle read;
  private final MethodHandle serializeWriter;
  private final MethodHandle commit;
  private final MethodHandle abort;
  private final MethodHandle closeWriter;
  private final MethodHandle createDataWriter;
  private final MethodHandle write;
  private final MethodHandle commitDataWriter;
  private final MethodHandle abortDataWriter;

  private NativeLibrary(Object library) throws Throwable {
    C i32 = C.INT32;
    C i64 = C.INT64;
    C ptr = C.POINTER;
    lastError = Ffm.bind(library, "spark_ds_last_error", ptr);
    abiVersion = Ffm.bind(library, "spark_ds_abi_version", i32);
    create = Ffm.bind(library, "spark_ds_create", i32, ptr, ptr, ptr, i64, ptr);
    schema = Ffm.bind(library, "spark_ds_schema", i32, ptr, ptr);
    createReader = Ffm.bind(library, "spark_ds_create_reader", i32, ptr, ptr, ptr);
    createStreamReader = Ffm.bind(library, "spark_ds_create_stream_reader", i32, ptr, ptr, ptr);
    createWriter = Ffm.bind(library, "spark_ds_create_writer", i32, ptr, ptr, C.BOOL, ptr);
    createStreamWriter =
      Ffm.bind(library, "spark_ds_create_stream_writer", i32, ptr, ptr, C.BOOL, ptr);
    close = Ffm.bind(library, "spark_ds_close", C.VOID, ptr);
    pushPredicates = Ffm.bind(library, "spark_ds_push_predicates", i32, ptr, ptr, i64, ptr);
    pushLimit = Ffm.bind(library, "spark_ds_push_limit", i32, ptr, i32, ptr);
    pruneColumns = Ffm.bind(library, "spark_ds_prune_columns", i32, ptr, ptr, i64, ptr);
    partitions = Ffm.bind(library, "spark_ds_partitions", i32, ptr, ptr, ptr);
    serializeReader = Ffm.bind(library, "spark_ds_serialize_reader", i32, ptr, ptr, ptr);
    closeReader = Ffm.bind(library, "spark_ds_close_reader", C.VOID, ptr);
    initialOffset = Ffm.bind(library, "spark_ds_initial_offset", i32, ptr, ptr, ptr);
    latestOffset = Ffm.bind(library, "spark_ds_latest_offset", i32, ptr, ptr, ptr);
    streamPartitions =
      Ffm.bind(library, "spark_ds_stream_partitions", i32, ptr, ptr, ptr, ptr, ptr);
    serializeStreamReader =
      Ffm.bind(library, "spark_ds_serialize_stream_reader", i32, ptr, ptr, ptr);
    commitOffset = Ffm.bind(library, "spark_ds_commit_offset", i32, ptr, ptr);
    closeStreamReader = Ffm.bind(library, "spark_ds_close_stream_reader", C.VOID, ptr);
    read = Ffm.bind(library, "spark_ds_read", i32, ptr, i64, ptr, i64, ptr);
    serializeWriter = Ffm.bind(library, "spark_ds_serialize_writer", i32, ptr, ptr, ptr);
    commit = Ffm.bind(library, "spark_ds_commit", i32, ptr, i64, ptr, ptr, i64);
    abort = Ffm.bind(library, "spark_ds_abort", i32, ptr, i64, ptr, ptr, i64);
    closeWriter = Ffm.bind(library, "spark_ds_close_writer", C.VOID, ptr);
    createDataWriter =
      Ffm.bind(library, "spark_ds_create_data_writer", i32, ptr, i64, i32, i64, i64, ptr);
    write = Ffm.bind(library, "spark_ds_write", i32, ptr, ptr, ptr);
    commitDataWriter = Ffm.bind(library, "spark_ds_commit_data_writer", i32, ptr, ptr, ptr);
    abortDataWriter = Ffm.bind(library, "spark_ds_abort_data_writer", i32, ptr);
  }

  /**
   * Loads the library at the given path. The library stays loaded until the JVM exits.
   *
   * @throws LoadException if the library cannot be loaded
   */
  static NativeLibrary load(String path) throws Throwable {
    if (!isSupported()) {
      throw new UnsupportedOperationException(
        "Native data sources require Java " + MINIMUM_JAVA_VERSION + " or later.");
    }
    Object library;
    try {
      library = Ffm.libraryLookup(Path.of(path));
    } catch (IllegalArgumentException | IllegalCallerException e) {
      // The library is not a valid library, or native access is not enabled for Spark.
      throw new LoadException(e.getMessage(), e);
    } catch (LinkageError e) {
      // The FFM API could not be initialized, see Ffm: report why, such as an
      // IllegalCallerException when native access is not enabled for Spark.
      throw new LoadException(
        "Cannot use the Foreign Function and Memory API: " + Ffm.rootCause(e), e);
    }
    return new NativeLibrary(library);
  }

  /** Thrown when a library cannot be loaded. */
  static final class LoadException extends RuntimeException {
    LoadException(String message, Throwable cause) {
      super(message, cause);
    }
  }

  /** Thrown when a function is called that the library does not export, with its name. */
  static final class MissingFunctionException extends RuntimeException {
    MissingFunctionException(String name) {
      super(name);
    }
  }

  // ---------------------------------------------------------------------------------------------
  // Calls
  // ---------------------------------------------------------------------------------------------

  private static MethodHandle require(MethodHandle function, String name) {
    if (function == null) {
      throw new MissingFunctionException(name);
    }
    return function;
  }

  /** Throws the error that the library reported if a function failed. */
  private void check(int status, String name) throws Throwable {
    if (status == 0) {
      return;
    }
    long address = lastError == null ? 0 : (long) lastError.invokeExact();
    String message = address == 0 ? "" : new String(readCString(address), StandardCharsets.UTF_8);
    throw new RuntimeException(message.isEmpty()
      ? name + " failed with the error code " + status + "."
      : message);
  }

  private static long readHandle(long out, String name) {
    long handle = Platform.getLong(null, out);
    if (handle == 0) {
      throw new IllegalStateException(name + " returned a NULL handle.");
    }
    return handle;
  }

  int abiVersion() throws Throwable {
    return (int) require(abiVersion, "spark_ds_abi_version").invokeExact();
  }

  long createDataSource(String name, String[] optionKeys, String[] optionValues)
      throws Throwable {
    MethodHandle f = require(create, "spark_ds_create");
    try (Memory memory = new Memory()) {
      long out = memory.zeroed(8);
      check((int) f.invokeExact(memory.string(name), memory.strings(optionKeys),
        memory.strings(optionValues), (long) optionKeys.length, out), "spark_ds_create");
      return readHandle(out, "spark_ds_create");
    }
  }

  void schema(long dataSource, long schemaAddress) throws Throwable {
    check((int) require(schema, "spark_ds_schema").invokeExact(dataSource, schemaAddress),
      "spark_ds_schema");
  }

  long createReader(long dataSource, long schemaAddress) throws Throwable {
    return createWithSchema(createReader, "spark_ds_create_reader", dataSource, schemaAddress);
  }

  long createStreamReader(long dataSource, long schemaAddress) throws Throwable {
    return createWithSchema(
      createStreamReader, "spark_ds_create_stream_reader", dataSource, schemaAddress);
  }

  private long createWithSchema(
      MethodHandle function, String name, long dataSource, long schemaAddress) throws Throwable {
    MethodHandle f = require(function, name);
    try (Memory memory = new Memory()) {
      long out = memory.zeroed(8);
      check((int) f.invokeExact(dataSource, schemaAddress, out), name);
      return readHandle(out, name);
    }
  }

  long createWriter(long dataSource, long schemaAddress, boolean overwrite) throws Throwable {
    return createAnyWriter(
      createWriter, "spark_ds_create_writer", dataSource, schemaAddress, overwrite);
  }

  long createStreamWriter(long dataSource, long schemaAddress, boolean overwrite)
      throws Throwable {
    return createAnyWriter(
      createStreamWriter, "spark_ds_create_stream_writer", dataSource, schemaAddress, overwrite);
  }

  private long createAnyWriter(
      MethodHandle function,
      String name,
      long dataSource,
      long schemaAddress,
      boolean overwrite) throws Throwable {
    MethodHandle f = require(function, name);
    try (Memory memory = new Memory()) {
      long out = memory.zeroed(8);
      check((int) f.invokeExact(dataSource, schemaAddress, overwrite, out), name);
      return readHandle(out, name);
    }
  }

  void closeDataSource(long dataSource) throws Throwable {
    require(close, "spark_ds_close").invokeExact(dataSource);
  }

  boolean[] pushPredicates(long reader, String[] predicates) throws Throwable {
    MethodHandle f = require(pushPredicates, "spark_ds_push_predicates");
    try (Memory memory = new Memory()) {
      long evaluated = memory.zeroed(Math.max(1, predicates.length));
      check((int) f.invokeExact(reader, memory.strings(predicates), (long) predicates.length,
        evaluated), "spark_ds_push_predicates");
      boolean[] result = new boolean[predicates.length];
      for (int i = 0; i < result.length; i++) {
        result[i] = Platform.getByte(null, evaluated + i) != 0;
      }
      return result;
    }
  }

  boolean pushLimit(long reader, int limit) throws Throwable {
    MethodHandle f = require(pushLimit, "spark_ds_push_limit");
    try (Memory memory = new Memory()) {
      long applied = memory.zeroed(1);
      check((int) f.invokeExact(reader, limit, applied), "spark_ds_push_limit");
      return Platform.getByte(null, applied) != 0;
    }
  }

  boolean pruneColumns(long reader, String[] columnNames) throws Throwable {
    MethodHandle f = require(pruneColumns, "spark_ds_prune_columns");
    try (Memory memory = new Memory()) {
      long pruned = memory.zeroed(1);
      check((int) f.invokeExact(reader, memory.strings(columnNames), (long) columnNames.length,
        pruned), "spark_ds_prune_columns");
      return Platform.getByte(null, pruned) != 0;
    }
  }

  byte[][] partitions(long reader) throws Throwable {
    MethodHandle f = require(partitions, "spark_ds_partitions");
    try (Sink sink = new Sink()) {
      check((int) f.invokeExact(reader, Sinks.SINK, sink.id), "spark_ds_partitions");
      return sink.all();
    }
  }

  byte[] serializeReader(long reader) throws Throwable {
    return serialize(serializeReader, "spark_ds_serialize_reader", reader);
  }

  void closeReader(long reader) throws Throwable {
    require(closeReader, "spark_ds_close_reader").invokeExact(reader);
  }

  String initialOffset(long streamReader) throws Throwable {
    return offset(initialOffset, "spark_ds_initial_offset", streamReader);
  }

  String latestOffset(long streamReader) throws Throwable {
    return offset(latestOffset, "spark_ds_latest_offset", streamReader);
  }

  private String offset(MethodHandle function, String name, long streamReader)
      throws Throwable {
    byte[] offset = serialize(function, name, streamReader);
    if (offset == null) {
      throw new IllegalStateException(name + " returned no offset.");
    }
    return new String(offset, StandardCharsets.UTF_8);
  }

  byte[][] streamPartitions(long streamReader, String start, String end) throws Throwable {
    MethodHandle f = require(streamPartitions, "spark_ds_stream_partitions");
    try (Memory memory = new Memory(); Sink sink = new Sink()) {
      check((int) f.invokeExact(streamReader, memory.string(start), memory.string(end),
        Sinks.SINK, sink.id), "spark_ds_stream_partitions");
      return sink.all();
    }
  }

  byte[] serializeStreamReader(long streamReader) throws Throwable {
    return serialize(serializeStreamReader, "spark_ds_serialize_stream_reader", streamReader);
  }

  void commitOffset(long streamReader, String end) throws Throwable {
    MethodHandle f = require(commitOffset, "spark_ds_commit_offset");
    try (Memory memory = new Memory()) {
      check((int) f.invokeExact(streamReader, memory.string(end)), "spark_ds_commit_offset");
    }
  }

  void closeStreamReader(long streamReader) throws Throwable {
    require(closeStreamReader, "spark_ds_close_stream_reader").invokeExact(streamReader);
  }

  void read(byte[] readerState, byte[] partition, long streamAddress) throws Throwable {
    MethodHandle f = require(read, "spark_ds_read");
    try (Memory memory = new Memory()) {
      check((int) f.invokeExact(memory.bytes(readerState), (long) readerState.length,
        memory.bytes(partition), (long) partition.length, streamAddress), "spark_ds_read");
    }
  }

  byte[] serializeWriter(long writer) throws Throwable {
    return serialize(serializeWriter, "spark_ds_serialize_writer", writer);
  }

  void commit(long writer, long epochId, byte[][] messages) throws Throwable {
    finishWrite(commit, "spark_ds_commit", writer, epochId, messages);
  }

  void abort(long writer, long epochId, byte[][] messages) throws Throwable {
    finishWrite(abort, "spark_ds_abort", writer, epochId, messages);
  }

  private void finishWrite(
      MethodHandle function,
      String name,
      long writer,
      long epochId,
      byte[][] messages) throws Throwable {
    MethodHandle f = require(function, name);
    try (Memory memory = new Memory()) {
      long pointers = memory.zeroed(8L * Math.max(1, messages.length));
      long lengths = memory.zeroed(8L * Math.max(1, messages.length));
      for (int i = 0; i < messages.length; i++) {
        if (messages[i] != null) {
          Platform.putLong(null, pointers + 8L * i, memory.bytes(messages[i]));
          Platform.putLong(null, lengths + 8L * i, messages[i].length);
        }
      }
      check((int) f.invokeExact(writer, epochId, pointers, lengths, (long) messages.length),
        name);
    }
  }

  void closeWriter(long writer) throws Throwable {
    require(closeWriter, "spark_ds_close_writer").invokeExact(writer);
  }

  long createDataWriter(byte[] writerState, int partitionId, long taskId, long epochId)
      throws Throwable {
    MethodHandle f = require(createDataWriter, "spark_ds_create_data_writer");
    try (Memory memory = new Memory()) {
      long out = memory.zeroed(8);
      check((int) f.invokeExact(memory.bytes(writerState), (long) writerState.length,
        partitionId, taskId, epochId, out), "spark_ds_create_data_writer");
      return readHandle(out, "spark_ds_create_data_writer");
    }
  }

  void write(long dataWriter, long arrayAddress, long schemaAddress) throws Throwable {
    check((int) require(write, "spark_ds_write").invokeExact(
      dataWriter, arrayAddress, schemaAddress), "spark_ds_write");
  }

  byte[] commitDataWriter(long dataWriter) throws Throwable {
    byte[] message = serialize(commitDataWriter, "spark_ds_commit_data_writer", dataWriter);
    return message == null ? new byte[0] : message;
  }

  void abortDataWriter(long dataWriter) throws Throwable {
    check((int) require(abortDataWriter, "spark_ds_abort_data_writer").invokeExact(dataWriter),
      "spark_ds_abort_data_writer");
  }

  /**
   * Calls a function that returns a single byte array through a sink. Returns null if the
   * function does not call the sink.
   */
  private byte[] serialize(MethodHandle function, String name, long handle) throws Throwable {
    MethodHandle f = require(function, name);
    try (Sink sink = new Sink()) {
      check((int) f.invokeExact(handle, Sinks.SINK, sink.id), name);
      byte[][] values = sink.all();
      if (values.length > 1) {
        throw new IllegalStateException(
          name + " returned " + values.length + " values instead of one.");
      }
      return values.length == 0 ? null : values[0];
    }
  }

  // ---------------------------------------------------------------------------------------------
  // Memory
  // ---------------------------------------------------------------------------------------------

  /** Native memory for the arguments of a call, freed after the call. */
  private static final class Memory implements AutoCloseable {
    private final List<Long> allocations = new ArrayList<>();

    long zeroed(long size) {
      long address = Platform.allocateMemory(size);
      allocations.add(address);
      Platform.setMemory(address, (byte) 0, size);
      return address;
    }

    /** Copies the bytes. Empty bytes are passed as a valid pointer too. */
    long bytes(byte[] bytes) {
      long address = zeroed(Math.max(1, bytes.length));
      Platform.copyMemory(bytes, Platform.BYTE_ARRAY_OFFSET, null, address, bytes.length);
      return address;
    }

    /** Copies the string as a NUL-terminated UTF-8 string. */
    long string(String value) {
      byte[] bytes = value.getBytes(StandardCharsets.UTF_8);
      long address = zeroed(bytes.length + 1L);
      Platform.copyMemory(bytes, Platform.BYTE_ARRAY_OFFSET, null, address, bytes.length);
      return address;
    }

    /** Copies the strings, and returns the address of an array of pointers to them. */
    long strings(String[] values) {
      long array = zeroed(8L * Math.max(1, values.length));
      for (int i = 0; i < values.length; i++) {
        Platform.putLong(null, array + 8L * i, string(values[i]));
      }
      return array;
    }

    @Override
    public void close() {
      allocations.forEach(Platform::freeMemory);
    }
  }

  private static byte[] readCString(long address) {
    int length = 0;
    while (Platform.getByte(null, address + length) != 0) {
      length++;
    }
    byte[] bytes = new byte[length];
    Platform.copyMemory(null, address, bytes, Platform.BYTE_ARRAY_OFFSET, length);
    return bytes;
  }

  // ---------------------------------------------------------------------------------------------
  // Sink
  // ---------------------------------------------------------------------------------------------

  /** The C function SparkDsSink, which calls {@link #sink}. */
  private static final class Sinks {
    static final long SINK;

    static {
      try {
        SINK = Ffm.upcall(
          MethodHandles.lookup().findStatic(NativeLibrary.class, "sink",
            MethodType.methodType(void.class, long.class, long.class, long.class)),
          C.VOID, C.POINTER, C.POINTER, C.INT64);
      } catch (Throwable e) {
        throw new IllegalStateException("Cannot create the sink of native data sources.", e);
      }
    }
  }

  private static final AtomicLong nextSinkId = new AtomicLong(1);
  private static final ConcurrentHashMap<Long, Sink> sinks = new ConcurrentHashMap<>();

  /**
   * Collects the byte arrays that a library returns during a call, through the C function
   * {@link Sinks#SINK}. The library passes {@link #id} back to identify it.
   */
  private static final class Sink implements AutoCloseable {
    final long id = nextSinkId.getAndIncrement();
    final List<byte[]> values = new ArrayList<>();
    Throwable error;

    Sink() {
      sinks.put(id, this);
    }

    byte[][] all() throws Throwable {
      if (error != null) {
        throw error;
      }
      return values.toArray(new byte[0][]);
    }

    @Override
    public void close() {
      sinks.remove(id);
    }
  }

  /**
   * Called by the library, through the C function {@link Sinks#SINK}, with a byte array to copy.
   * It must not throw: an exception thrown by an upcall crashes the JVM.
   */
  private static void sink(long id, long data, long length) {
    Sink sink = null;
    try {
      sink = sinks.get(id);
      if (sink == null || sink.error != null) {
        return;
      }
      if (length < 0 || length > Integer.MAX_VALUE - 8 || (data == 0 && length > 0)) {
        throw new IllegalStateException("The library returned an invalid byte array, of " +
          length + " bytes at the address " + data + ".");
      }
      byte[] bytes = new byte[(int) length];
      Platform.copyMemory(null, data, bytes, Platform.BYTE_ARRAY_OFFSET, length);
      sink.values.add(bytes);
    } catch (Throwable e) {
      if (sink != null) {
        sink.error = e;
      }
    }
  }
}
