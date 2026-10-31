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
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.BooleanSupplier;

import org.apache.spark.internal.LogKeys;
import org.apache.spark.internal.MDC;
import org.apache.spark.internal.SparkLogger;
import org.apache.spark.internal.SparkLoggerFactory;
import org.apache.spark.unsafe.Platform;

/**
 * A native data source library, which implements the C interface in
 * {@code org/apache/spark/sql/datasource/spark_datasource.h}, called through the Foreign Function
 * and Memory (FFM) API of Java 22 and later.
 * <p>
 * Spark compiles for Java 17, where the FFM API does not exist, so the API is only reached
 * through method handles, looked up once when the first library is loaded. The library exports
 * {@code spark_ds_init}, which returns its table of functions, {@code SparkDsLibrary}. Each
 * function of the table is bound to a method handle that takes and returns primitive values only,
 * with pointers as {@code long} addresses, so the calls themselves use no reflection. The memory
 * that Spark passes to the library is allocated with {@link Platform}, at the offsets of the
 * structs of the header, which {@link #layout()} lists for the tests.
 * <p>
 * Calling a function that the library leaves NULL in its table throws
 * {@link MissingFunctionException}. A function that fails throws a {@link RuntimeException} with
 * the message of {@code last_error}.
 */
final class NativeLibrary {

  private static final SparkLogger LOGGER = SparkLoggerFactory.getLogger(NativeLibrary.class);

  /** The version of the interface that Spark supports, {@code SPARK_DS_ABI_VERSION}. */
  static final int ABI_VERSION = 1;

  /** The first version of Java with the final FFM API. */
  static final int MINIMUM_JAVA_VERSION = 22;

  /** Whether this JVM can load native data source libraries. */
  static boolean isSupported() {
    return Runtime.version().feature() >= MINIMUM_JAVA_VERSION;
  }

  // The capabilities of a data source, SPARK_DS_BATCH_READ and the others.
  static final long BATCH_READ = 1;
  static final long MICRO_BATCH_READ = 2;
  static final long BATCH_WRITE = 4;
  static final long STREAMING_WRITE = 8;
  static final long BATCH_TRUNCATE = 16;
  static final long STREAMING_TRUNCATE = 32;
  static final long ADMISSION_CONTROL = 64;

  // The modes of a write, SPARK_DS_WRITE_APPEND and SPARK_DS_WRITE_TRUNCATE.
  static final int WRITE_APPEND = 0;
  static final int WRITE_TRUNCATE = 1;

  // The kinds of a read, SPARK_DS_READ_BATCH and SPARK_DS_READ_MICRO_BATCH.
  static final int READ_BATCH = 0;
  static final int READ_MICRO_BATCH = 1;

  // The kinds of read limits, SPARK_DS_READ_ALL_AVAILABLE and SPARK_DS_READ_MAX_ROWS.
  static final int READ_ALL_AVAILABLE = 0;
  static final int READ_MAX_ROWS = 1;

  // The C types of the parameters and the results of the functions.
  private enum C { VOID, BOOL, I32, I64, PTR }

  /** The functions of the table {@code SparkDsLibrary}, in their order in the table. */
  enum Function {
    LAST_ERROR("last_error", C.PTR),
    CREATE("create", C.I32, C.PTR, C.PTR, C.PTR),
    SCHEMA("schema", C.I32, C.PTR, C.PTR),
    CREATE_READER("create_reader", C.I32, C.PTR, C.PTR, C.PTR, C.PTR),
    CREATE_STREAM_READER("create_stream_reader", C.I32, C.PTR, C.PTR, C.PTR, C.PTR),
    CREATE_WRITER("create_writer", C.I32, C.PTR, C.PTR, C.PTR, C.PTR, C.PTR, C.PTR),
    CLOSE("close", C.VOID, C.PTR),
    PUSH_LIMIT("push_limit", C.I32, C.PTR, C.I32, C.PTR),
    PRUNE_COLUMNS("prune_columns", C.I32, C.PTR, C.PTR, C.I64, C.PTR),
    PARTITIONS("partitions", C.I32, C.PTR, C.PTR, C.PTR, C.PTR, C.PTR),
    CLOSE_READER("close_reader", C.VOID, C.PTR),
    INITIAL_OFFSET("initial_offset", C.I32, C.PTR, C.PTR, C.PTR, C.PTR),
    LATEST_OFFSET("latest_offset", C.I32, C.PTR, C.PTR, C.PTR, C.PTR),
    STREAM_PARTITIONS("stream_partitions", C.I32, C.PTR, C.PTR, C.PTR, C.PTR, C.PTR),
    COMMIT_OFFSET("commit_offset", C.I32, C.PTR, C.PTR),
    CLOSE_STREAM_READER("close_stream_reader", C.VOID, C.PTR),
    READ("read", C.I32, C.PTR, C.PTR, C.PTR),
    COMMIT("commit", C.I32, C.PTR, C.PTR),
    ABORT("abort", C.I32, C.PTR, C.PTR),
    CLOSE_WRITER("close_writer", C.VOID, C.PTR),
    CREATE_DATA_WRITER("create_data_writer", C.I32, C.PTR, C.PTR, C.PTR),
    WRITE("write", C.I32, C.PTR, C.PTR),
    COMMIT_DATA_WRITER("commit_data_writer", C.I32, C.PTR, C.PTR, C.PTR, C.PTR),
    ABORT_DATA_WRITER("abort_data_writer", C.I32, C.PTR, C.PTR),
    CLOSE_DATA_WRITER("close_data_writer", C.VOID, C.PTR),
    DEFAULT_READ_LIMIT("default_read_limit", C.I32, C.PTR, C.PTR);

    /** The name of the field of the function in the table. */
    final String fieldName;
    private final C result;
    private final C[] parameters;

    Function(String fieldName, C result, C... parameters) {
      this.fieldName = fieldName;
      this.result = result;
      this.parameters = parameters;
    }

    /** The offset of the function in the table, after the size and the version of the table. */
    long offset() {
      return TABLE_FUNCTIONS + 8L * ordinal();
    }
  }

  // The layouts of the structs of the header, on the 64-bit platforms that Spark supports. Each
  // struct starts with its size.
  private static final long TABLE_ABI_VERSION = 8;
  private static final long TABLE_FUNCTIONS = 16;
  private static final long HOST_SIZE = 32;
  private static final long HOST_ABI_VERSION = 8;
  private static final long HOST_SPARK_VERSION = 16;
  private static final long HOST_LOG = 24;
  private static final long CREATE_INFO_SIZE = 40;
  private static final long CREATE_INFO_NAME = 8;
  private static final long CREATE_INFO_OPTION_KEYS = 16;
  private static final long CREATE_INFO_OPTION_VALUES = 24;
  private static final long CREATE_INFO_NUM_OPTIONS = 32;
  private static final long READER_INFO_SIZE = 8;
  private static final long PARTITIONS_INFO_SIZE = 8;
  private static final long INITIAL_OFFSET_INFO_SIZE = 8;
  private static final long STREAM_PARTITIONS_INFO_SIZE = 24;
  private static final long STREAM_PARTITIONS_INFO_START = 8;
  private static final long STREAM_PARTITIONS_INFO_END = 16;
  private static final long COMMIT_OFFSET_INFO_SIZE = 16;
  private static final long COMMIT_OFFSET_INFO_END = 8;
  private static final long STREAM_READER_INFO_SIZE = 16;
  private static final long STREAM_READER_INFO_CHECKPOINT_LOCATION = 8;
  private static final long LATEST_OFFSET_INFO_SIZE = 32;
  private static final long LATEST_OFFSET_INFO_START = 8;
  private static final long LATEST_OFFSET_INFO_LIMIT = 16;
  private static final long LATEST_OFFSET_INFO_RUN_END = 24;
  private static final long READ_LIMIT_SIZE = 24;
  private static final long READ_LIMIT_KIND = 8;
  private static final long READ_LIMIT_MAX_ROWS = 16;
  private static final long TASK_SIZE = 40;
  private static final long TASK_STAGE_ID = 8;
  private static final long TASK_STAGE_ATTEMPT_NUMBER = 12;
  private static final long TASK_PARTITION_ID = 16;
  private static final long TASK_ATTEMPT_NUMBER = 20;
  private static final long TASK_ATTEMPT_ID = 24;
  private static final long TASK_IS_INTERRUPTED = 32;
  private static final long READ_INFO_SIZE = 56;
  private static final long READ_INFO_KIND = 8;
  private static final long READ_INFO_SCHEMA = 16;
  private static final long READ_INFO_READER_STATE = 24;
  private static final long READ_INFO_READER_STATE_LENGTH = 32;
  private static final long READ_INFO_PARTITION = 40;
  private static final long READ_INFO_PARTITION_LENGTH = 48;
  private static final long WRITE_INFO_SIZE = 24;
  private static final long WRITE_INFO_MODE = 8;
  private static final long WRITE_INFO_QUERY_ID = 16;
  private static final long DATA_WRITER_INFO_SIZE = 40;
  private static final long DATA_WRITER_INFO_SCHEMA = 8;
  private static final long DATA_WRITER_INFO_WRITER_STATE = 16;
  private static final long DATA_WRITER_INFO_WRITER_STATE_LENGTH = 24;
  private static final long DATA_WRITER_INFO_EPOCH_ID = 32;
  private static final long FINISH_WRITE_INFO_SIZE = 48;
  private static final long FINISH_WRITE_INFO_EPOCH_ID = 8;
  private static final long FINISH_WRITE_INFO_MESSAGES = 16;
  private static final long FINISH_WRITE_INFO_MESSAGE_LENGTHS = 24;
  private static final long FINISH_WRITE_INFO_NUM_MESSAGES = 32;
  private static final long FINISH_WRITE_INFO_REJECTED = 40;
  private static final long FINISH_DATA_WRITER_INFO_SIZE = 8;

  // The structs that only hold their size, which the library gets as const pointers. Until fields
  // are added to them, a single struct of each kind is allocated once, and never freed.
  private static final long READER_INFO = sizeOnlyInfo(READER_INFO_SIZE);
  private static final long PARTITIONS_INFO = sizeOnlyInfo(PARTITIONS_INFO_SIZE);
  private static final long INITIAL_OFFSET_INFO = sizeOnlyInfo(INITIAL_OFFSET_INFO_SIZE);
  private static final long FINISH_DATA_WRITER_INFO = sizeOnlyInfo(FINISH_DATA_WRITER_INFO_SIZE);

  private static long sizeOnlyInfo(long size) {
    long info = Platform.allocateMemory(size);
    Platform.putLong(null, info, size);
    return info;
  }

  /**
   * The offsets of the fields of the structs that Spark reads or writes, by {@code struct.field},
   * and the sizes of the structs, by {@code struct}, for the tests to compare with the header.
   */
  static Map<String, Long> layout() {
    Map<String, Long> layout = new LinkedHashMap<>();
    layout.put("SparkDsLibrary", TABLE_FUNCTIONS + 8L * Function.values().length);
    layout.put("SparkDsLibrary.size", 0L);
    layout.put("SparkDsLibrary.abi_version", TABLE_ABI_VERSION);
    for (Function function : Function.values()) {
      layout.put("SparkDsLibrary." + function.fieldName, function.offset());
    }
    layout.put("SparkDsHost", HOST_SIZE);
    layout.put("SparkDsHost.size", 0L);
    layout.put("SparkDsHost.abi_version", HOST_ABI_VERSION);
    layout.put("SparkDsHost.spark_version", HOST_SPARK_VERSION);
    layout.put("SparkDsHost.log", HOST_LOG);
    layout.put("SparkDsCreateInfo", CREATE_INFO_SIZE);
    layout.put("SparkDsCreateInfo.size", 0L);
    layout.put("SparkDsCreateInfo.name", CREATE_INFO_NAME);
    layout.put("SparkDsCreateInfo.option_keys", CREATE_INFO_OPTION_KEYS);
    layout.put("SparkDsCreateInfo.option_values", CREATE_INFO_OPTION_VALUES);
    layout.put("SparkDsCreateInfo.num_options", CREATE_INFO_NUM_OPTIONS);
    layout.put("SparkDsReaderInfo", READER_INFO_SIZE);
    layout.put("SparkDsReaderInfo.size", 0L);
    layout.put("SparkDsPartitionsInfo", PARTITIONS_INFO_SIZE);
    layout.put("SparkDsPartitionsInfo.size", 0L);
    layout.put("SparkDsInitialOffsetInfo", INITIAL_OFFSET_INFO_SIZE);
    layout.put("SparkDsInitialOffsetInfo.size", 0L);
    layout.put("SparkDsStreamPartitionsInfo", STREAM_PARTITIONS_INFO_SIZE);
    layout.put("SparkDsStreamPartitionsInfo.size", 0L);
    layout.put("SparkDsStreamPartitionsInfo.start", STREAM_PARTITIONS_INFO_START);
    layout.put("SparkDsStreamPartitionsInfo.end", STREAM_PARTITIONS_INFO_END);
    layout.put("SparkDsCommitOffsetInfo", COMMIT_OFFSET_INFO_SIZE);
    layout.put("SparkDsCommitOffsetInfo.size", 0L);
    layout.put("SparkDsCommitOffsetInfo.end", COMMIT_OFFSET_INFO_END);
    layout.put("SparkDsStreamReaderInfo", STREAM_READER_INFO_SIZE);
    layout.put("SparkDsStreamReaderInfo.size", 0L);
    layout.put(
      "SparkDsStreamReaderInfo.checkpoint_location", STREAM_READER_INFO_CHECKPOINT_LOCATION);
    layout.put("SparkDsLatestOffsetInfo", LATEST_OFFSET_INFO_SIZE);
    layout.put("SparkDsLatestOffsetInfo.size", 0L);
    layout.put("SparkDsLatestOffsetInfo.start", LATEST_OFFSET_INFO_START);
    layout.put("SparkDsLatestOffsetInfo.limit", LATEST_OFFSET_INFO_LIMIT);
    layout.put("SparkDsLatestOffsetInfo.run_end", LATEST_OFFSET_INFO_RUN_END);
    layout.put("SparkDsReadLimit", READ_LIMIT_SIZE);
    layout.put("SparkDsReadLimit.size", 0L);
    layout.put("SparkDsReadLimit.kind", READ_LIMIT_KIND);
    layout.put("SparkDsReadLimit.max_rows", READ_LIMIT_MAX_ROWS);
    layout.put("SparkDsTaskContext", TASK_SIZE);
    layout.put("SparkDsTaskContext.size", 0L);
    layout.put("SparkDsTaskContext.stage_id", TASK_STAGE_ID);
    layout.put("SparkDsTaskContext.stage_attempt_number", TASK_STAGE_ATTEMPT_NUMBER);
    layout.put("SparkDsTaskContext.partition_id", TASK_PARTITION_ID);
    layout.put("SparkDsTaskContext.attempt_number", TASK_ATTEMPT_NUMBER);
    layout.put("SparkDsTaskContext.task_attempt_id", TASK_ATTEMPT_ID);
    layout.put("SparkDsTaskContext.is_interrupted", TASK_IS_INTERRUPTED);
    layout.put("SparkDsReadInfo", READ_INFO_SIZE);
    layout.put("SparkDsReadInfo.size", 0L);
    layout.put("SparkDsReadInfo.kind", READ_INFO_KIND);
    layout.put("SparkDsReadInfo.schema", READ_INFO_SCHEMA);
    layout.put("SparkDsReadInfo.reader_state", READ_INFO_READER_STATE);
    layout.put("SparkDsReadInfo.reader_state_length", READ_INFO_READER_STATE_LENGTH);
    layout.put("SparkDsReadInfo.partition", READ_INFO_PARTITION);
    layout.put("SparkDsReadInfo.partition_length", READ_INFO_PARTITION_LENGTH);
    layout.put("SparkDsWriteInfo", WRITE_INFO_SIZE);
    layout.put("SparkDsWriteInfo.size", 0L);
    layout.put("SparkDsWriteInfo.mode", WRITE_INFO_MODE);
    layout.put("SparkDsWriteInfo.query_id", WRITE_INFO_QUERY_ID);
    layout.put("SparkDsDataWriterInfo", DATA_WRITER_INFO_SIZE);
    layout.put("SparkDsDataWriterInfo.size", 0L);
    layout.put("SparkDsDataWriterInfo.schema", DATA_WRITER_INFO_SCHEMA);
    layout.put("SparkDsDataWriterInfo.writer_state", DATA_WRITER_INFO_WRITER_STATE);
    layout.put(
      "SparkDsDataWriterInfo.writer_state_length", DATA_WRITER_INFO_WRITER_STATE_LENGTH);
    layout.put("SparkDsDataWriterInfo.epoch_id", DATA_WRITER_INFO_EPOCH_ID);
    layout.put("SparkDsFinishWriteInfo", FINISH_WRITE_INFO_SIZE);
    layout.put("SparkDsFinishWriteInfo.size", 0L);
    layout.put("SparkDsFinishWriteInfo.epoch_id", FINISH_WRITE_INFO_EPOCH_ID);
    layout.put("SparkDsFinishWriteInfo.messages", FINISH_WRITE_INFO_MESSAGES);
    layout.put("SparkDsFinishWriteInfo.message_lengths", FINISH_WRITE_INFO_MESSAGE_LENGTHS);
    layout.put("SparkDsFinishWriteInfo.num_messages", FINISH_WRITE_INFO_NUM_MESSAGES);
    layout.put("SparkDsFinishWriteInfo.rejected", FINISH_WRITE_INFO_REJECTED);
    layout.put("SparkDsFinishDataWriterInfo", FINISH_DATA_WRITER_INFO_SIZE);
    layout.put("SparkDsFinishDataWriterInfo.size", 0L);
    return layout;
  }

  private final int abiVersion;
  // The functions of the table, by ordinal, null for the functions that the library leaves NULL.
  private final MethodHandle[] functions = new MethodHandle[Function.values().length];

  private NativeLibrary(long table) throws Throwable {
    long size = Platform.getLong(null, table);
    if (size < TABLE_FUNCTIONS) {
      throw new LoadException(
        "spark_ds_init returned a function table with a size of " + size + " bytes.", null, true);
    }
    abiVersion = Platform.getInt(null, table + TABLE_ABI_VERSION);
    // The table of another version of the interface has another layout. Only the functions that
    // fit in the size of the table are read: the table of a library built against an earlier
    // header ends before the functions that were added later.
    if (abiVersion == ABI_VERSION) {
      for (Function function : Function.values()) {
        if (function.offset() + 8 <= size) {
          long address = Platform.getLong(null, table + function.offset());
          if (address != 0) {
            functions[function.ordinal()] = Ffm.downcall(
              Ffm.OF_ADDRESS.invoke(address), function.result, function.parameters);
          }
        }
      }
    }
  }

  /**
   * Loads the library at the given path, and gets its table with {@code spark_ds_init}. The
   * library stays loaded until the JVM exits.
   *
   * @param sparkVersion the version of Spark, which the library gets in {@code SparkDsHost}
   * @throws LoadException if the library cannot be loaded
   */
  static NativeLibrary load(String path, String sparkVersion) throws Throwable {
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
    Optional<?> init = (Optional<?>) Ffm.FIND.invoke(lookup, "spark_ds_init");
    if (init.isEmpty()) {
      throw new LoadException(
        "The library does not export the function spark_ds_init.", null, true);
    }
    MethodHandle initFunction = Ffm.downcall(init.get(), C.I32, C.PTR, C.PTR);
    // The library may keep the host until the JVM exits, even if spark_ds_init fails.
    long host = Ffm.host(path, sparkVersion);
    long out = Platform.allocateMemory(8);
    try {
      Platform.putLong(null, out, 0);
      int status = (int) initFunction.invokeExact(host, out);
      if (status != 0) {
        throw new LoadException(
          "spark_ds_init failed with the error code " + status + ".", null, true);
      }
      long table = Platform.getLong(null, out);
      if (table == 0) {
        throw new LoadException("spark_ds_init returned no function table.", null, true);
      }
      return new NativeLibrary(table);
    } finally {
      Platform.freeMemory(out);
    }
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
    static final MethodHandle UPCALL;
    static final MethodHandle FUNCTION_OF;
    static final MethodHandle FUNCTION_OF_VOID;
    // (long) -> MemorySegment and (MemorySegment) -> long.
    static final MethodHandle OF_ADDRESS;
    static final MethodHandle ADDRESS_OF;
    static final Object BOOL;
    static final Object INT32;
    static final Object INT64;
    static final Object POINTER;
    // NativeLibrary.log, which SparkDsHost.log calls with the path of its library.
    static final MethodHandle LOG;
    // The addresses of the C functions that call Java, which live as long as the JVM:
    // SparkDsSink, which calls NativeLibrary.sink, the SparkDsSink set_state of the functions
    // that also return a state, which calls NativeLibrary.stateSink, and
    // SparkDsTaskContext.is_interrupted, which calls NativeLibrary.isInterrupted.
    static final long SINK;
    static final long STATE_SINK;
    static final long IS_INTERRUPTED;

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
        UPCALL = lookup.findVirtual(linker, "upcallStub",
          MethodType.methodType(segment, MethodHandle.class, descriptor, arena, options))
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

        LOG = lookup.findStatic(NativeLibrary.class, "log",
          MethodType.methodType(void.class, String.class, int.class, long.class));
        SINK = upcall(
          lookup.findStatic(NativeLibrary.class, "sink",
            MethodType.methodType(void.class, long.class, long.class, long.class)),
          C.VOID, C.PTR, C.PTR, C.I64);
        STATE_SINK = upcall(
          lookup.findStatic(NativeLibrary.class, "stateSink",
            MethodType.methodType(void.class, long.class, long.class, long.class)),
          C.VOID, C.PTR, C.PTR, C.I64);
        IS_INTERRUPTED = upcall(
          lookup.findStatic(NativeLibrary.class, "isInterrupted",
            MethodType.methodType(boolean.class, long.class)),
          C.BOOL, C.PTR);
      } catch (Throwable e) {
        // The message names the cause, because the JVM only keeps this message for the later
        // uses of the class.
        throw new IllegalStateException("Cannot initialize the FFM API: " + rootCause(e), e);
      }
    }

    /** Returns the FunctionDescriptor of a C function with the given types. */
    static Object descriptor(C result, C... parameters) throws Throwable {
      Object layouts = Array.newInstance(LAYOUT_CLASS, parameters.length);
      for (int i = 0; i < parameters.length; i++) {
        Array.set(layouts, i, layout(parameters[i]));
      }
      return result == C.VOID
        ? FUNCTION_OF_VOID.invoke(layouts)
        : FUNCTION_OF.invoke(layout(result), layouts);
    }

    static Object layout(C type) {
      return switch (type) {
        case BOOL -> BOOL;
        case I32 -> INT32;
        case I64 -> INT64;
        case PTR -> POINTER;
        case VOID -> throw new IllegalArgumentException("void has no layout");
      };
    }

    /**
     * Binds the C function at the given address, a MemorySegment, to a method handle whose
     * parameters and result have the Java types of the given C types, with {@code long} for
     * pointers.
     */
    static MethodHandle downcall(Object address, C result, C... parameters) throws Throwable {
      MethodHandle handle = (MethodHandle) DOWNCALL.invoke(
        LINKER, address, descriptor(result, parameters), NO_OPTIONS);
      // Passes and returns the pointers, which are MemorySegments in the FFM API, as addresses.
      for (int i = 0; i < parameters.length; i++) {
        if (parameters[i] == C.PTR) {
          handle = MethodHandles.filterArguments(handle, i, OF_ADDRESS);
        }
      }
      if (result == C.PTR) {
        handle = MethodHandles.filterReturnValue(handle, ADDRESS_OF);
      }
      return handle;
    }

    /**
     * Returns the address of a C function with the given types that calls `target`, whose
     * parameters have the Java types of the C types, with {@code long} for pointers. The
     * function lives as long as the JVM.
     */
    static long upcall(MethodHandle target, C result, C... parameters) throws Throwable {
      for (int i = 0; i < parameters.length; i++) {
        if (parameters[i] == C.PTR) {
          target = MethodHandles.filterArguments(target, i, ADDRESS_OF);
        }
      }
      Object stub = UPCALL.invoke(
        LINKER, target, descriptor(result, parameters), GLOBAL_ARENA, NO_OPTIONS);
      return (long) ADDRESS_OF.invoke(stub);
    }

    /**
     * Allocates the SparkDsHost of a library. The library may keep it until the JVM exits, so it
     * is never freed.
     */
    static long host(String path, String sparkVersion) throws Throwable {
      long log = upcall(MethodHandles.insertArguments(LOG, 0, path), C.VOID, C.I32, C.PTR);
      byte[] version = sparkVersion.getBytes(StandardCharsets.UTF_8);
      long versionAddress = Platform.allocateMemory(version.length + 1L);
      Platform.copyMemory(
        version, Platform.BYTE_ARRAY_OFFSET, null, versionAddress, version.length);
      Platform.putByte(null, versionAddress + version.length, (byte) 0);
      long host = Platform.allocateMemory(HOST_SIZE);
      Platform.setMemory(host, (byte) 0, HOST_SIZE);
      Platform.putLong(null, host, HOST_SIZE);
      Platform.putInt(null, host + HOST_ABI_VERSION, ABI_VERSION);
      Platform.putLong(null, host + HOST_SPARK_VERSION, versionAddress);
      Platform.putLong(null, host + HOST_LOG, log);
      return host;
    }
  }

  private static Throwable rootCause(Throwable e) {
    Throwable cause = e;
    while (cause.getCause() != null && cause.getCause() != cause) {
      cause = cause.getCause();
    }
    return cause;
  }

  /** Thrown when a library cannot be loaded, or cannot be used. */
  static final class LoadException extends RuntimeException {
    /**
     * Whether the library was loaded, in which case it stays loaded: Spark found that it cannot
     * use it afterwards, for example because {@code spark_ds_init} failed.
     */
    final boolean loaded;

    LoadException(String message, Throwable cause) {
      this(message, cause, false);
    }

    LoadException(String message, Throwable cause, boolean loaded) {
      super(message, cause);
      this.loaded = loaded;
    }
  }

  /** Thrown when a function is called that the library leaves NULL in its table, with its name. */
  static final class MissingFunctionException extends RuntimeException {
    MissingFunctionException(String name) {
      super(name);
    }
  }

  /**
   * Called by the library, through {@code SparkDsHost.log}, with the path of the library. It must
   * not throw: an exception thrown by an upcall crashes the JVM.
   */
  private static void log(String path, int level, long message) {
    try {
      String text = message == 0 ? "" : new String(readCString(message), StandardCharsets.UTF_8);
      switch (level) {
        case 1 -> LOGGER.error("Native data source library {}: {}",
          MDC.of(LogKeys.PATH, path), MDC.of(LogKeys.MESSAGE, text));
        case 2 -> LOGGER.warn("Native data source library {}: {}",
          MDC.of(LogKeys.PATH, path), MDC.of(LogKeys.MESSAGE, text));
        case 4 -> LOGGER.debug("Native data source library {}: {}", path, text);
        case 5 -> LOGGER.trace("Native data source library {}: {}", path, text);
        default -> LOGGER.info("Native data source library {}: {}",
          MDC.of(LogKeys.PATH, path), MDC.of(LogKeys.MESSAGE, text));
      }
    } catch (Throwable e) {
      // The message is lost.
    }
  }

  // ---------------------------------------------------------------------------------------------
  // Calls
  // ---------------------------------------------------------------------------------------------

  private MethodHandle function(Function function) {
    MethodHandle handle = functions[function.ordinal()];
    if (handle == null) {
      throw new MissingFunctionException(function.fieldName);
    }
    return handle;
  }

  /** Throws the error that the library reported if a function failed. */
  private void check(int status, Function function) throws Throwable {
    if (status == 0) {
      return;
    }
    MethodHandle lastError = functions[Function.LAST_ERROR.ordinal()];
    long address = lastError == null ? 0 : (long) lastError.invokeExact();
    String message = address == 0 ? "" : new String(readCString(address), StandardCharsets.UTF_8);
    throw new RuntimeException(message.isEmpty()
      ? function.fieldName + " failed with the error code " + status + "."
      : message);
  }

  private static long readHandle(long out, Function function) {
    long handle = Platform.getLong(null, out);
    if (handle == 0) {
      throw new IllegalStateException(function.fieldName + " returned a NULL handle.");
    }
    return handle;
  }

  /** The version of the interface that the table of the library implements. */
  int abiVersion() {
    return abiVersion;
  }

  /** Whether the library implements the function, which it does not leave NULL in its table. */
  boolean hasFunction(Function function) {
    return functions[function.ordinal()] != null;
  }

  /** A data source handle, with the capabilities of the data source. */
  static final class DataSourceHandle {
    final long handle;
    final long capabilities;

    DataSourceHandle(long handle, long capabilities) {
      this.handle = handle;
      this.capabilities = capabilities;
    }
  }

  DataSourceHandle createDataSource(String name, String[] optionKeys, String[] optionValues)
      throws Throwable {
    MethodHandle f = function(Function.CREATE);
    try (Memory memory = new Memory()) {
      long info = memory.zeroed(CREATE_INFO_SIZE);
      Platform.putLong(null, info, CREATE_INFO_SIZE);
      Platform.putLong(null, info + CREATE_INFO_NAME, memory.string(name));
      Platform.putLong(null, info + CREATE_INFO_OPTION_KEYS, memory.strings(optionKeys));
      Platform.putLong(null, info + CREATE_INFO_OPTION_VALUES, memory.strings(optionValues));
      Platform.putLong(null, info + CREATE_INFO_NUM_OPTIONS, optionKeys.length);
      long capabilities = memory.zeroed(8);
      long out = memory.zeroed(8);
      check((int) f.invokeExact(info, capabilities, out), Function.CREATE);
      return new DataSourceHandle(
        readHandle(out, Function.CREATE), Platform.getLong(null, capabilities));
    }
  }

  void schema(long dataSource, long schemaAddress) throws Throwable {
    check((int) function(Function.SCHEMA).invokeExact(dataSource, schemaAddress),
      Function.SCHEMA);
  }

  long createReader(long dataSource, long schemaAddress) throws Throwable {
    MethodHandle f = function(Function.CREATE_READER);
    try (Memory memory = new Memory()) {
      long out = memory.zeroed(8);
      check((int) f.invokeExact(dataSource, schemaAddress, READER_INFO, out),
        Function.CREATE_READER);
      return readHandle(out, Function.CREATE_READER);
    }
  }

  /**
   * Creates a stream reader.
   *
   * @param checkpointLocation the location of the checkpoint of the source
   */
  long createStreamReader(long dataSource, long schemaAddress, String checkpointLocation)
      throws Throwable {
    MethodHandle f = function(Function.CREATE_STREAM_READER);
    try (Memory memory = new Memory()) {
      long info = memory.zeroed(STREAM_READER_INFO_SIZE);
      Platform.putLong(null, info, STREAM_READER_INFO_SIZE);
      Platform.putLong(
        null, info + STREAM_READER_INFO_CHECKPOINT_LOCATION, memory.string(checkpointLocation));
      long out = memory.zeroed(8);
      check((int) f.invokeExact(dataSource, schemaAddress, info, out),
        Function.CREATE_STREAM_READER);
      return readHandle(out, Function.CREATE_STREAM_READER);
    }
  }

  /** A writer handle, with the state of the writer that the data writers need. */
  static final class WriterHandle {
    final long handle;
    /** The state of the writer, or null if it has none. */
    final byte[] state;

    WriterHandle(long handle, byte[] state) {
      this.handle = handle;
      this.state = state;
    }
  }

  /**
   * Creates a writer. If the library returns an invalid state, the writer is aborted, as rejected,
   * and closed.
   *
   * @param mode {@link #WRITE_APPEND} or {@link #WRITE_TRUNCATE}
   * @param queryId the ID of the streaming query of a streaming write, or null for a batch write
   */
  WriterHandle createWriter(long dataSource, long schemaAddress, int mode, String queryId)
      throws Throwable {
    MethodHandle f = function(Function.CREATE_WRITER);
    try (Memory memory = new Memory(); Sink sink = new Sink()) {
      long info = memory.zeroed(WRITE_INFO_SIZE);
      Platform.putLong(null, info, WRITE_INFO_SIZE);
      Platform.putInt(null, info + WRITE_INFO_MODE, mode);
      Platform.putLong(
        null, info + WRITE_INFO_QUERY_ID, queryId == null ? 0 : memory.string(queryId));
      long out = memory.zeroed(8);
      check((int) f.invokeExact(dataSource, schemaAddress, info, Ffm.STATE_SINK, sink.id, out),
        Function.CREATE_WRITER);
      long handle = readHandle(out, Function.CREATE_WRITER);
      try {
        return new WriterHandle(handle, state(sink, Function.CREATE_WRITER));
      } catch (Throwable e) {
        // So that the library can undo what create_writer prepared.
        try {
          finishWrite(Function.ABORT, handle, -1, new byte[0][], true);
        } catch (Throwable abortError) {
          e.addSuppressed(abortError);
        }
        try {
          closeWriter(handle);
        } catch (Throwable closeError) {
          e.addSuppressed(closeError);
        }
        throw e;
      }
    }
  }

  void closeDataSource(long dataSource) throws Throwable {
    function(Function.CLOSE).invokeExact(dataSource);
  }

  boolean pushLimit(long reader, int limit) throws Throwable {
    MethodHandle f = function(Function.PUSH_LIMIT);
    try (Memory memory = new Memory()) {
      long applied = memory.zeroed(1);
      check((int) f.invokeExact(reader, limit, applied), Function.PUSH_LIMIT);
      return Platform.getByte(null, applied) != 0;
    }
  }

  boolean pruneColumns(long reader, String[] columnNames) throws Throwable {
    MethodHandle f = function(Function.PRUNE_COLUMNS);
    try (Memory memory = new Memory()) {
      long pruned = memory.zeroed(1);
      check((int) f.invokeExact(reader, memory.strings(columnNames), (long) columnNames.length,
        pruned), Function.PRUNE_COLUMNS);
      return Platform.getByte(null, pruned) != 0;
    }
  }

  /** The partitions that a reader planned, with the state of the reader that reads them. */
  static final class PlannedPartitions {
    final byte[][] partitions;
    /** The state of the reader, or null if it has none. */
    final byte[] state;

    PlannedPartitions(byte[][] partitions, byte[] state) {
      this.partitions = partitions;
      this.state = state;
    }
  }

  PlannedPartitions partitions(long reader) throws Throwable {
    MethodHandle f = function(Function.PARTITIONS);
    try (Sink sink = new Sink()) {
      check((int) f.invokeExact(reader, PARTITIONS_INFO, Ffm.SINK, Ffm.STATE_SINK, sink.id),
        Function.PARTITIONS);
      return new PlannedPartitions(sink.values(), state(sink, Function.PARTITIONS));
    }
  }

  void closeReader(long reader) throws Throwable {
    function(Function.CLOSE_READER).invokeExact(reader);
  }

  String initialOffset(long streamReader) throws Throwable {
    MethodHandle f = function(Function.INITIAL_OFFSET);
    try (Sink sink = new Sink()) {
      check((int) f.invokeExact(streamReader, INITIAL_OFFSET_INFO, Ffm.SINK, sink.id),
        Function.INITIAL_OFFSET);
      return offset(Function.INITIAL_OFFSET, sink);
    }
  }

  /** A limit on the data of a micro-batch, as in SparkDsReadLimit. */
  static final class ReadLimitInfo {
    /** {@link #READ_ALL_AVAILABLE} or {@link #READ_MAX_ROWS}, or what the library set. */
    final int kind;
    /** The maximum number of rows with {@link #READ_MAX_ROWS}. */
    final long maxRows;

    ReadLimitInfo(int kind, long maxRows) {
      this.kind = kind;
      this.maxRows = maxRows;
    }
  }

  /**
   * Returns the latest offset.
   *
   * @param start the offset that the next micro-batch starts after
   * @param limit the limit on the data after start
   * @param runEnd the offset that a run of Trigger.AvailableNow reads up to, or null
   */
  String latestOffset(long streamReader, String start, ReadLimitInfo limit, String runEnd)
      throws Throwable {
    MethodHandle f = function(Function.LATEST_OFFSET);
    try (Memory memory = new Memory(); Sink sink = new Sink()) {
      long readLimit = memory.zeroed(READ_LIMIT_SIZE);
      Platform.putLong(null, readLimit, READ_LIMIT_SIZE);
      Platform.putInt(null, readLimit + READ_LIMIT_KIND, limit.kind);
      Platform.putLong(null, readLimit + READ_LIMIT_MAX_ROWS, limit.maxRows);
      long info = memory.zeroed(LATEST_OFFSET_INFO_SIZE);
      Platform.putLong(null, info, LATEST_OFFSET_INFO_SIZE);
      Platform.putLong(null, info + LATEST_OFFSET_INFO_START, memory.string(start));
      Platform.putLong(null, info + LATEST_OFFSET_INFO_LIMIT, readLimit);
      Platform.putLong(null, info + LATEST_OFFSET_INFO_RUN_END,
        runEnd == null ? 0 : memory.string(runEnd));
      check((int) f.invokeExact(streamReader, info, Ffm.SINK, sink.id), Function.LATEST_OFFSET);
      return offset(Function.LATEST_OFFSET, sink);
    }
  }

  /**
   * Returns the default read limit of a stream reader, whose kind is {@link #READ_ALL_AVAILABLE}
   * if the library sets nothing.
   */
  ReadLimitInfo defaultReadLimit(long streamReader) throws Throwable {
    MethodHandle f = function(Function.DEFAULT_READ_LIMIT);
    try (Memory memory = new Memory()) {
      long limit = memory.zeroed(READ_LIMIT_SIZE);
      Platform.putLong(null, limit, READ_LIMIT_SIZE);
      Platform.putInt(null, limit + READ_LIMIT_KIND, READ_ALL_AVAILABLE);
      check((int) f.invokeExact(streamReader, limit), Function.DEFAULT_READ_LIMIT);
      return new ReadLimitInfo(
        Platform.getInt(null, limit + READ_LIMIT_KIND),
        Platform.getLong(null, limit + READ_LIMIT_MAX_ROWS));
    }
  }

  /** Returns the offset that a function passed to the sink, which it must pass. */
  private static String offset(Function function, Sink sink) throws Throwable {
    byte[] offset = single(sink, function);
    if (offset == null) {
      throw new IllegalStateException(function.fieldName + " returned no offset.");
    }
    return new String(offset, StandardCharsets.UTF_8);
  }

  PlannedPartitions streamPartitions(long streamReader, String start, String end)
      throws Throwable {
    MethodHandle f = function(Function.STREAM_PARTITIONS);
    try (Memory memory = new Memory(); Sink sink = new Sink()) {
      long info = memory.zeroed(STREAM_PARTITIONS_INFO_SIZE);
      Platform.putLong(null, info, STREAM_PARTITIONS_INFO_SIZE);
      Platform.putLong(null, info + STREAM_PARTITIONS_INFO_START, memory.string(start));
      Platform.putLong(null, info + STREAM_PARTITIONS_INFO_END, memory.string(end));
      check((int) f.invokeExact(streamReader, info, Ffm.SINK, Ffm.STATE_SINK, sink.id),
        Function.STREAM_PARTITIONS);
      return new PlannedPartitions(sink.values(), state(sink, Function.STREAM_PARTITIONS));
    }
  }

  void commitOffset(long streamReader, String end) throws Throwable {
    MethodHandle f = function(Function.COMMIT_OFFSET);
    try (Memory memory = new Memory()) {
      long info = memory.zeroed(COMMIT_OFFSET_INFO_SIZE);
      Platform.putLong(null, info, COMMIT_OFFSET_INFO_SIZE);
      Platform.putLong(null, info + COMMIT_OFFSET_INFO_END, memory.string(end));
      check((int) f.invokeExact(streamReader, info), Function.COMMIT_OFFSET);
    }
  }

  void closeStreamReader(long streamReader) throws Throwable {
    function(Function.CLOSE_STREAM_READER).invokeExact(streamReader);
  }

  /**
   * Reads a partition, into the stream at the given address.
   *
   * @param kind {@link #READ_BATCH} or {@link #READ_MICRO_BATCH}
   * @param schemaAddress the address of the {@code ArrowSchema} of the schema to read
   */
  void read(
      Task task,
      int kind,
      long schemaAddress,
      byte[] readerState,
      byte[] partition,
      long streamAddress) throws Throwable {
    MethodHandle f = function(Function.READ);
    try (Memory memory = new Memory()) {
      long info = memory.zeroed(READ_INFO_SIZE);
      Platform.putLong(null, info, READ_INFO_SIZE);
      Platform.putInt(null, info + READ_INFO_KIND, kind);
      Platform.putLong(null, info + READ_INFO_SCHEMA, schemaAddress);
      Platform.putLong(null, info + READ_INFO_READER_STATE, memory.bytes(readerState));
      Platform.putLong(null, info + READ_INFO_READER_STATE_LENGTH, readerState.length);
      Platform.putLong(null, info + READ_INFO_PARTITION, memory.bytes(partition));
      Platform.putLong(null, info + READ_INFO_PARTITION_LENGTH, partition.length);
      check((int) f.invokeExact(task.address, info, streamAddress), Function.READ);
    }
  }

  void commit(long writer, long epochId, byte[][] messages) throws Throwable {
    finishWrite(Function.COMMIT, writer, epochId, messages, false);
  }

  void abort(long writer, long epochId, byte[][] messages) throws Throwable {
    finishWrite(Function.ABORT, writer, epochId, messages, false);
  }

  /**
   * Commits or aborts a write.
   *
   * @param rejected whether Spark rejected what create_writer returned, for an abort
   */
  private void finishWrite(
      Function function,
      long writer,
      long epochId,
      byte[][] messages,
      boolean rejected) throws Throwable {
    MethodHandle f = function(function);
    try (Memory memory = new Memory()) {
      long pointers = memory.zeroed(8L * Math.max(1, messages.length));
      long lengths = memory.zeroed(8L * Math.max(1, messages.length));
      for (int i = 0; i < messages.length; i++) {
        if (messages[i] != null) {
          Platform.putLong(null, pointers + 8L * i, memory.bytes(messages[i]));
          Platform.putLong(null, lengths + 8L * i, messages[i].length);
        }
      }
      long info = memory.zeroed(FINISH_WRITE_INFO_SIZE);
      Platform.putLong(null, info, FINISH_WRITE_INFO_SIZE);
      Platform.putLong(null, info + FINISH_WRITE_INFO_EPOCH_ID, epochId);
      Platform.putLong(null, info + FINISH_WRITE_INFO_MESSAGES, pointers);
      Platform.putLong(null, info + FINISH_WRITE_INFO_MESSAGE_LENGTHS, lengths);
      Platform.putLong(null, info + FINISH_WRITE_INFO_NUM_MESSAGES, messages.length);
      Platform.putLong(null, info + FINISH_WRITE_INFO_REJECTED, rejected ? 1L : 0L);
      check((int) f.invokeExact(writer, info), function);
    }
  }

  void closeWriter(long writer) throws Throwable {
    function(Function.CLOSE_WRITER).invokeExact(writer);
  }

  /**
   * Creates a data writer.
   *
   * @param schemaAddress the address of the {@code ArrowSchema} of the data to write
   * @param epochId the ID of the micro-batch of a streaming write, or -1 for a batch write
   */
  long createDataWriter(Task task, long schemaAddress, byte[] writerState, long epochId)
      throws Throwable {
    MethodHandle f = function(Function.CREATE_DATA_WRITER);
    try (Memory memory = new Memory()) {
      long info = memory.zeroed(DATA_WRITER_INFO_SIZE);
      Platform.putLong(null, info, DATA_WRITER_INFO_SIZE);
      Platform.putLong(null, info + DATA_WRITER_INFO_SCHEMA, schemaAddress);
      Platform.putLong(null, info + DATA_WRITER_INFO_WRITER_STATE, memory.bytes(writerState));
      Platform.putLong(null, info + DATA_WRITER_INFO_WRITER_STATE_LENGTH, writerState.length);
      Platform.putLong(null, info + DATA_WRITER_INFO_EPOCH_ID, epochId);
      long out = memory.zeroed(8);
      check((int) f.invokeExact(task.address, info, out), Function.CREATE_DATA_WRITER);
      return readHandle(out, Function.CREATE_DATA_WRITER);
    }
  }

  /** Writes the array at the given address, of the schema that the data writer got. */
  void write(long dataWriter, long arrayAddress) throws Throwable {
    check((int) function(Function.WRITE).invokeExact(dataWriter, arrayAddress), Function.WRITE);
  }

  /**
   * Commits a data writer. Then {@code committed} runs right away, before the message is checked:
   * if the message is invalid, the commit fails, but the data writer must not be aborted.
   */
  byte[] commitDataWriter(long dataWriter, Runnable committed) throws Throwable {
    MethodHandle f = function(Function.COMMIT_DATA_WRITER);
    try (Sink sink = new Sink()) {
      check((int) f.invokeExact(dataWriter, FINISH_DATA_WRITER_INFO, Ffm.SINK, sink.id),
        Function.COMMIT_DATA_WRITER);
      committed.run();
      byte[] message = single(sink, Function.COMMIT_DATA_WRITER);
      return message == null ? new byte[0] : message;
    }
  }

  void abortDataWriter(long dataWriter) throws Throwable {
    check(
      (int) function(Function.ABORT_DATA_WRITER).invokeExact(dataWriter, FINISH_DATA_WRITER_INFO),
      Function.ABORT_DATA_WRITER);
  }

  void closeDataWriter(long dataWriter) throws Throwable {
    function(Function.CLOSE_DATA_WRITER).invokeExact(dataWriter);
  }

  /** Returns the single byte array that a function passed to the sink, or null if none. */
  private static byte[] single(Sink sink, Function function) throws Throwable {
    return atMostOne(sink.values(), function, "values");
  }

  /** Returns the state that a function passed to the set_state sink, or null if none. */
  private static byte[] state(Sink sink, Function function) throws Throwable {
    return atMostOne(sink.states(), function, "states");
  }

  private static byte[] atMostOne(byte[][] arrays, Function function, String what) {
    if (arrays.length > 1) {
      throw new IllegalStateException(
        function.fieldName + " returned " + arrays.length + " " + what + " instead of one.");
    }
    return arrays.length == 0 ? null : arrays[0];
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
  // Task contexts
  // ---------------------------------------------------------------------------------------------

  // Whether the task of each SparkDsTaskContext was killed, by the address of the struct.
  private static final ConcurrentHashMap<Long, BooleanSupplier> tasks = new ConcurrentHashMap<>();

  /**
   * The {@code SparkDsTaskContext} of a task that reads a partition or writes data. The reader or
   * the data writer closes it once the library no longer uses it, which frees it.
   */
  static final class Task implements AutoCloseable {
    final long address;
    private boolean closed;

    Task(
        int stageId,
        int stageAttemptNumber,
        int partitionId,
        int attemptNumber,
        long taskAttemptId,
        BooleanSupplier interrupted) {
      address = Platform.allocateMemory(TASK_SIZE);
      Platform.setMemory(address, (byte) 0, TASK_SIZE);
      Platform.putLong(null, address, TASK_SIZE);
      Platform.putInt(null, address + TASK_STAGE_ID, stageId);
      Platform.putInt(null, address + TASK_STAGE_ATTEMPT_NUMBER, stageAttemptNumber);
      Platform.putInt(null, address + TASK_PARTITION_ID, partitionId);
      Platform.putInt(null, address + TASK_ATTEMPT_NUMBER, attemptNumber);
      Platform.putLong(null, address + TASK_ATTEMPT_ID, taskAttemptId);
      Platform.putLong(null, address + TASK_IS_INTERRUPTED, Ffm.IS_INTERRUPTED);
      tasks.put(address, interrupted);
    }

    @Override
    public void close() {
      if (!closed) {
        closed = true;
        // A call to is_interrupted that is still running only looks the address up.
        tasks.remove(address);
        Platform.freeMemory(address);
      }
    }
  }

  /**
   * Called by the library, through {@code SparkDsTaskContext.is_interrupted}, with the address of
   * the struct. It must not throw: an exception thrown by an upcall crashes the JVM.
   */
  private static boolean isInterrupted(long task) {
    try {
      BooleanSupplier interrupted = tasks.get(task);
      // A freed task context belongs to a read or a write that is over.
      return interrupted == null || interrupted.getAsBoolean();
    } catch (Throwable e) {
      return false;
    }
  }

  // ---------------------------------------------------------------------------------------------
  // Sink
  // ---------------------------------------------------------------------------------------------

  private static final AtomicLong nextSinkId = new AtomicLong(1);
  private static final ConcurrentHashMap<Long, Sink> sinks = new ConcurrentHashMap<>();

  /**
   * Collects the byte arrays that a library returns during a call, through the C functions
   * {@link Ffm#SINK} and {@link Ffm#STATE_SINK}, which a function that returns values and a
   * state gets with the same sink. The library passes {@link #id} back to identify it.
   */
  private static final class Sink implements AutoCloseable {
    final long id = nextSinkId.getAndIncrement();
    final List<byte[]> values = new ArrayList<>();
    final List<byte[]> states = new ArrayList<>();
    Throwable error;

    Sink() {
      sinks.put(id, this);
    }

    /** The byte arrays passed to {@link Ffm#SINK}. */
    byte[][] values() throws Throwable {
      return collected(values);
    }

    /** The byte arrays passed to {@link Ffm#STATE_SINK}. */
    byte[][] states() throws Throwable {
      return collected(states);
    }

    private byte[][] collected(List<byte[]> arrays) throws Throwable {
      if (error != null) {
        throw error;
      }
      return arrays.toArray(new byte[0][]);
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
    collect(id, data, length, false);
  }

  /**
   * Called by the library, through the upcall stub {@link Ffm#STATE_SINK}, with a state to copy.
   * It must not throw: an exception thrown by an upcall crashes the JVM.
   */
  private static void stateSink(long id, long data, long length) {
    collect(id, data, length, true);
  }

  private static void collect(long id, long data, long length, boolean state) {
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
      (state ? sink.states : sink.values).add(bytes);
    } catch (Throwable e) {
      if (sink != null) {
        sink.error = e;
      }
    }
  }
}
