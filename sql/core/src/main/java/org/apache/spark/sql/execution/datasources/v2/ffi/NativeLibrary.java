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
import java.lang.reflect.Array;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;

import org.apache.spark.unsafe.Platform;

/**
 * A native data source library, which implements the C interface in
 * {@code org/apache/spark/sql/datasource/spark_datasource.h}, called through the Foreign Function
 * and Memory (FFM) API of Java 22 and later.
 * <p>
 * Spark compiles for Java 17, where the FFM API does not exist, so the API is only reached
 * through method handles, looked up once when the first library is loaded. Each function of a
 * library is bound to a method handle that takes and returns primitive values only, with
 * pointers as {@code long} addresses, so the calls themselves use no reflection. The memory that
 * Spark passes to the library is allocated with {@link Platform}.
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

  // The C types of the parameters and the results of the functions.
  private enum C { VOID, BOOL, INT32, INT64, POINTER }

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

  private NativeLibrary(Object lookup) throws Throwable {
    C i32 = C.INT32;
    C i64 = C.INT64;
    C ptr = C.POINTER;
    lastError = bind(lookup, "spark_ds_last_error", ptr);
    abiVersion = bind(lookup, "spark_ds_abi_version", i32);
    create = bind(lookup, "spark_ds_create", i32, ptr, ptr, ptr, i64, ptr);
    schema = bind(lookup, "spark_ds_schema", i32, ptr, ptr);
    createReader = bind(lookup, "spark_ds_create_reader", i32, ptr, ptr, ptr);
    createStreamReader = bind(lookup, "spark_ds_create_stream_reader", i32, ptr, ptr, ptr);
    createWriter = bind(lookup, "spark_ds_create_writer", i32, ptr, ptr, C.BOOL, ptr);
    createStreamWriter =
      bind(lookup, "spark_ds_create_stream_writer", i32, ptr, ptr, C.BOOL, ptr);
    close = bind(lookup, "spark_ds_close", C.VOID, ptr);
    pushPredicates = bind(lookup, "spark_ds_push_predicates", i32, ptr, ptr, i64, ptr);
    pushLimit = bind(lookup, "spark_ds_push_limit", i32, ptr, i32, ptr);
    pruneColumns = bind(lookup, "spark_ds_prune_columns", i32, ptr, ptr, i64, ptr);
    partitions = bind(lookup, "spark_ds_partitions", i32, ptr, ptr, ptr);
    serializeReader = bind(lookup, "spark_ds_serialize_reader", i32, ptr, ptr, ptr);
    closeReader = bind(lookup, "spark_ds_close_reader", C.VOID, ptr);
    initialOffset = bind(lookup, "spark_ds_initial_offset", i32, ptr, ptr, ptr);
    latestOffset = bind(lookup, "spark_ds_latest_offset", i32, ptr, ptr, ptr);
    streamPartitions =
      bind(lookup, "spark_ds_stream_partitions", i32, ptr, ptr, ptr, ptr, ptr);
    serializeStreamReader =
      bind(lookup, "spark_ds_serialize_stream_reader", i32, ptr, ptr, ptr);
    commitOffset = bind(lookup, "spark_ds_commit_offset", i32, ptr, ptr);
    closeStreamReader = bind(lookup, "spark_ds_close_stream_reader", C.VOID, ptr);
    read = bind(lookup, "spark_ds_read", i32, ptr, i64, ptr, i64, ptr);
    serializeWriter = bind(lookup, "spark_ds_serialize_writer", i32, ptr, ptr, ptr);
    commit = bind(lookup, "spark_ds_commit", i32, ptr, i64, ptr, ptr, i64);
    abort = bind(lookup, "spark_ds_abort", i32, ptr, i64, ptr, ptr, i64);
    closeWriter = bind(lookup, "spark_ds_close_writer", C.VOID, ptr);
    createDataWriter =
      bind(lookup, "spark_ds_create_data_writer", i32, ptr, i64, i32, i64, i64, ptr);
    write = bind(lookup, "spark_ds_write", i32, ptr, ptr, ptr);
    commitDataWriter = bind(lookup, "spark_ds_commit_data_writer", i32, ptr, ptr, ptr);
    abortDataWriter = bind(lookup, "spark_ds_abort_data_writer", i32, ptr);
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
    Object lookup;
    try {
      lookup = Ffm.LIBRARY_LOOKUP.invoke(Path.of(path), Ffm.GLOBAL_ARENA);
    } catch (IllegalArgumentException | IllegalCallerException e) {
      // The library is not a valid library, or native access is not enabled for Spark.
      throw new LoadException(e.getMessage(), e);
    } catch (LinkageError e) {
      // The FFM API could not be initialized, see Ffm: report why, such as an
      // IllegalCallerException when native access is not enabled for Spark.
      throw new LoadException(
        "Cannot use the Foreign Function and Memory API: " + rootCause(e), e);
    }
    return new NativeLibrary(lookup);
  }

  /**
   * Binds a function of the library to a method handle whose parameters and result have the
   * Java types of the given C types, with {@code long} for pointers. Returns null if the library
   * does not export the function.
   */
  private static MethodHandle bind(Object lookup, String name, C result, C... parameters)
      throws Throwable {
    Optional<?> symbol = (Optional<?>) Ffm.FIND.invoke(lookup, name);
    if (symbol.isEmpty()) {
      return null;
    }
    Object layouts = Ffm.layouts(parameters);
    Object descriptor = result == C.VOID
      ? Ffm.FUNCTION_OF_VOID.invoke(layouts)
      : Ffm.FUNCTION_OF.invoke(Ffm.layout(result), layouts);
    MethodHandle handle = (MethodHandle) Ffm.DOWNCALL.invoke(
      Ffm.LINKER, symbol.get(), descriptor, Ffm.NO_OPTIONS);
    // Passes and returns the pointers, which are MemorySegments in the FFM API, as addresses.
    for (int i = 0; i < parameters.length; i++) {
      if (parameters[i] == C.POINTER) {
        handle = MethodHandles.filterArguments(handle, i, Ffm.OF_ADDRESS);
      }
    }
    if (result == C.POINTER) {
      handle = MethodHandles.filterReturnValue(handle, Ffm.ADDRESS_OF);
    }
    return handle;
  }

  /** The method handles of the FFM API, initialized when the first library is loaded. */
  private static final class Ffm {
    static final Class<?> LAYOUT_CLASS;
    static final Object LINKER;
    static final Object GLOBAL_ARENA;
    static final Object NO_OPTIONS;
    static final MethodHandle LIBRARY_LOOKUP;
    static final MethodHandle FIND;
    static final MethodHandle DOWNCALL;
    static final MethodHandle FUNCTION_OF;
    static final MethodHandle FUNCTION_OF_VOID;
    // (long) -> MemorySegment and (MemorySegment) -> long.
    static final MethodHandle OF_ADDRESS;
    static final MethodHandle ADDRESS_OF;
    static final Object BOOL;
    static final Object INT32;
    static final Object INT64;
    static final Object POINTER;
    // The address of the C function SparkDsSink, which calls NativeLibrary.sink.
    static final long SINK;

    static {
      try {
        // A full-privilege lookup, so that the restricted methods of the FFM API see this class
        // as their caller, in the unnamed module that Spark enables native access for.
        MethodHandles.Lookup lookup = MethodHandles.lookup();
        Class<?> linker = Class.forName("java.lang.foreign.Linker");
        Class<?> option = Class.forName("java.lang.foreign.Linker$Option");
        Class<?> symbolLookup = Class.forName("java.lang.foreign.SymbolLookup");
        Class<?> arena = Class.forName("java.lang.foreign.Arena");
        Class<?> segment = Class.forName("java.lang.foreign.MemorySegment");
        Class<?> layout = Class.forName("java.lang.foreign.MemoryLayout");
        Class<?> layoutArray = Array.newInstance(layout, 0).getClass();
        Class<?> valueLayout = Class.forName("java.lang.foreign.ValueLayout");
        Class<?> descriptor = Class.forName("java.lang.foreign.FunctionDescriptor");
        Class<?> options = Array.newInstance(option, 0).getClass();

        LAYOUT_CLASS = layout;
        LINKER = lookup.findStatic(linker, "nativeLinker", MethodType.methodType(linker))
          .invoke();
        GLOBAL_ARENA = lookup.findStatic(arena, "global", MethodType.methodType(arena)).invoke();
        NO_OPTIONS = Array.newInstance(option, 0);
        LIBRARY_LOOKUP = lookup.findStatic(symbolLookup, "libraryLookup",
          MethodType.methodType(symbolLookup, Path.class, arena));
        FIND = lookup.findVirtual(symbolLookup, "find",
          MethodType.methodType(Optional.class, String.class));
        DOWNCALL = lookup.findVirtual(linker, "downcallHandle",
          MethodType.methodType(MethodHandle.class, segment, descriptor, options))
          .asFixedArity();
        FUNCTION_OF = lookup.findStatic(descriptor, "of",
          MethodType.methodType(descriptor, layout, layoutArray)).asFixedArity();
        FUNCTION_OF_VOID = lookup.findStatic(descriptor, "ofVoid",
          MethodType.methodType(descriptor, layoutArray)).asFixedArity();
        OF_ADDRESS = lookup.findStatic(segment, "ofAddress",
          MethodType.methodType(segment, long.class));
        ADDRESS_OF = lookup.findVirtual(segment, "address", MethodType.methodType(long.class));
        BOOL = valueLayout.getField("JAVA_BOOLEAN").get(null);
        INT32 = valueLayout.getField("JAVA_INT").get(null);
        INT64 = valueLayout.getField("JAVA_LONG").get(null);
        POINTER = valueLayout.getField("ADDRESS").get(null);

        // The upcall stub of the sink, which lives as long as the JVM.
        MethodHandle target = lookup.findStatic(NativeLibrary.class, "sink",
          MethodType.methodType(void.class, long.class, long.class, long.class));
        target = MethodHandles.filterArguments(target, 0, ADDRESS_OF, ADDRESS_OF);
        Object sinkDescriptor =
          FUNCTION_OF_VOID.invoke(layouts(C.POINTER, C.POINTER, C.INT64));
        Object stub = lookup.findVirtual(linker, "upcallStub",
          MethodType.methodType(segment, MethodHandle.class, descriptor, arena, options))
          .asFixedArity()
          .invoke(LINKER, target, sinkDescriptor, GLOBAL_ARENA, NO_OPTIONS);
        SINK = (long) ADDRESS_OF.invoke(stub);
      } catch (Throwable e) {
        // The message names the cause, because the JVM only keeps this message for the later
        // uses of the class.
        throw new IllegalStateException("Cannot initialize the FFM API: " + rootCause(e), e);
      }
    }

    /** Returns a MemoryLayout[] with the layouts of the given types. */
    static Object layouts(C... types) {
      Object layouts = Array.newInstance(LAYOUT_CLASS, types.length);
      for (int i = 0; i < types.length; i++) {
        Array.set(layouts, i, layout(types[i]));
      }
      return layouts;
    }

    static Object layout(C type) {
      return switch (type) {
        case BOOL -> BOOL;
        case INT32 -> INT32;
        case INT64 -> INT64;
        case POINTER -> POINTER;
        case VOID -> throw new IllegalArgumentException("void has no layout");
      };
    }
  }

  private static Throwable rootCause(Throwable e) {
    Throwable cause = e;
    while (cause.getCause() != null && cause.getCause() != cause) {
      cause = cause.getCause();
    }
    return cause;
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
      check((int) f.invokeExact(reader, Ffm.SINK, sink.id), "spark_ds_partitions");
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
        Ffm.SINK, sink.id), "spark_ds_stream_partitions");
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
      check((int) f.invokeExact(handle, Ffm.SINK, sink.id), name);
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

  private static final AtomicLong nextSinkId = new AtomicLong(1);
  private static final ConcurrentHashMap<Long, Sink> sinks = new ConcurrentHashMap<>();

  /**
   * Collects the byte arrays that a library returns during a call, through the C function
   * {@link Ffm#SINK}. The library passes {@link #id} back to identify it.
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
   * Called by the library, through the upcall stub {@link Ffm#SINK}, with a byte array to copy.
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
