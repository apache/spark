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
  static final long TRUNCATE = 16;

  // The modes of a write, SPARK_DS_WRITE_APPEND and SPARK_DS_WRITE_TRUNCATE.
  static final int WRITE_APPEND = 0;
  static final int WRITE_TRUNCATE = 1;

  // The C types of the parameters and the results of the functions.
  private enum C { VOID, BOOL, I32, I64, PTR }

  /** The functions of the table {@code SparkDsLibrary}, in their order in the table. */
  enum Function {
    LAST_ERROR("last_error", C.PTR),
    CREATE("create", C.I32, C.PTR, C.PTR, C.PTR),
    SCHEMA("schema", C.I32, C.PTR, C.PTR),
    CREATE_READER("create_reader", C.I32, C.PTR, C.PTR, C.PTR),
    CREATE_STREAM_READER("create_stream_reader", C.I32, C.PTR, C.PTR, C.PTR),
    CREATE_WRITER("create_writer", C.I32, C.PTR, C.PTR, C.PTR, C.PTR),
    CLOSE("close", C.VOID, C.PTR),
    PUSH_LIMIT("push_limit", C.I32, C.PTR, C.I32, C.PTR),
    PRUNE_COLUMNS("prune_columns", C.I32, C.PTR, C.PTR, C.I64, C.PTR),
    PARTITIONS("partitions", C.I32, C.PTR, C.PTR, C.PTR),
    SERIALIZE_READER("serialize_reader", C.I32, C.PTR, C.PTR, C.PTR),
    CLOSE_READER("close_reader", C.VOID, C.PTR),
    INITIAL_OFFSET("initial_offset", C.I32, C.PTR, C.PTR, C.PTR),
    LATEST_OFFSET("latest_offset", C.I32, C.PTR, C.PTR, C.PTR),
    STREAM_PARTITIONS("stream_partitions", C.I32, C.PTR, C.PTR, C.PTR, C.PTR, C.PTR),
    SERIALIZE_STREAM_READER("serialize_stream_reader", C.I32, C.PTR, C.PTR, C.PTR),
    COMMIT_OFFSET("commit_offset", C.I32, C.PTR, C.PTR),
    CLOSE_STREAM_READER("close_stream_reader", C.VOID, C.PTR),
    READ("read", C.I32, C.PTR, C.PTR, C.I64, C.PTR, C.I64, C.PTR),
    SERIALIZE_WRITER("serialize_writer", C.I32, C.PTR, C.PTR, C.PTR),
    COMMIT("commit", C.I32, C.PTR, C.I64, C.PTR, C.PTR, C.I64),
    ABORT("abort", C.I32, C.PTR, C.I64, C.PTR, C.PTR, C.I64),
    CLOSE_WRITER("close_writer", C.VOID, C.PTR),
    CREATE_DATA_WRITER("create_data_writer", C.I32, C.PTR, C.PTR, C.I64, C.I64, C.PTR),
    WRITE("write", C.I32, C.PTR, C.PTR, C.PTR),
    COMMIT_DATA_WRITER("commit_data_writer", C.I32, C.PTR, C.PTR, C.PTR),
    ABORT_DATA_WRITER("abort_data_writer", C.I32, C.PTR),
    CLOSE_DATA_WRITER("close_data_writer", C.VOID, C.PTR),
    // Appended after the other functions, so that the table of a library built against a header
    // without it keeps its layout.
    PUSH_DOMAINS("push_domains", C.I32, C.PTR, C.PTR, C.I64, C.PTR);

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
  private static final long TASK_SIZE = 40;
  private static final long TASK_STAGE_ID = 8;
  private static final long TASK_STAGE_ATTEMPT_NUMBER = 12;
  private static final long TASK_PARTITION_ID = 16;
  private static final long TASK_ATTEMPT_NUMBER = 20;
  private static final long TASK_ATTEMPT_ID = 24;
  private static final long TASK_IS_INTERRUPTED = 32;
  private static final long WRITE_INFO_SIZE = 24;
  private static final long WRITE_INFO_MODE = 8;
  private static final long WRITE_INFO_QUERY_ID = 16;
  private static final long DOMAIN_SIZE = 48;
  private static final long DOMAIN_PATH = 8;
  private static final long DOMAIN_PATH_LENGTH = 16;
  private static final long DOMAIN_NULL_ALLOWED = 24;
  private static final long DOMAIN_NAN_ALLOWED = 25;
  private static final long DOMAIN_RANGES = 32;
  private static final long DOMAIN_RANGES_SCHEMA = 40;

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
    layout.put("SparkDsTaskContext", TASK_SIZE);
    layout.put("SparkDsTaskContext.size", 0L);
    layout.put("SparkDsTaskContext.stage_id", TASK_STAGE_ID);
    layout.put("SparkDsTaskContext.stage_attempt_number", TASK_STAGE_ATTEMPT_NUMBER);
    layout.put("SparkDsTaskContext.partition_id", TASK_PARTITION_ID);
    layout.put("SparkDsTaskContext.attempt_number", TASK_ATTEMPT_NUMBER);
    layout.put("SparkDsTaskContext.task_attempt_id", TASK_ATTEMPT_ID);
    layout.put("SparkDsTaskContext.is_interrupted", TASK_IS_INTERRUPTED);
    layout.put("SparkDsWriteInfo", WRITE_INFO_SIZE);
    layout.put("SparkDsWriteInfo.size", 0L);
    layout.put("SparkDsWriteInfo.mode", WRITE_INFO_MODE);
    layout.put("SparkDsWriteInfo.query_id", WRITE_INFO_QUERY_ID);
    layout.put("SparkDsDomain", DOMAIN_SIZE);
    layout.put("SparkDsDomain.size", 0L);
    layout.put("SparkDsDomain.path", DOMAIN_PATH);
    layout.put("SparkDsDomain.path_length", DOMAIN_PATH_LENGTH);
    layout.put("SparkDsDomain.null_allowed", DOMAIN_NULL_ALLOWED);
    layout.put("SparkDsDomain.nan_allowed", DOMAIN_NAN_ALLOWED);
    layout.put("SparkDsDomain.ranges", DOMAIN_RANGES);
    layout.put("SparkDsDomain.ranges_schema", DOMAIN_RANGES_SCHEMA);
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
    // SparkDsSink, which calls NativeLibrary.sink, and SparkDsTaskContext.is_interrupted, which
    // calls NativeLibrary.isInterrupted.
    static final long SINK;
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
    return createWithSchema(Function.CREATE_READER, dataSource, schemaAddress);
  }

  long createStreamReader(long dataSource, long schemaAddress) throws Throwable {
    return createWithSchema(Function.CREATE_STREAM_READER, dataSource, schemaAddress);
  }

  private long createWithSchema(Function function, long dataSource, long schemaAddress)
      throws Throwable {
    MethodHandle f = function(function);
    try (Memory memory = new Memory()) {
      long out = memory.zeroed(8);
      check((int) f.invokeExact(dataSource, schemaAddress, out), function);
      return readHandle(out, function);
    }
  }

  /**
   * Creates a writer.
   *
   * @param mode {@link #WRITE_APPEND} or {@link #WRITE_TRUNCATE}
   * @param queryId the ID of the streaming query of a streaming write, or null for a batch write
   */
  long createWriter(long dataSource, long schemaAddress, int mode, String queryId)
      throws Throwable {
    MethodHandle f = function(Function.CREATE_WRITER);
    try (Memory memory = new Memory()) {
      long info = memory.zeroed(WRITE_INFO_SIZE);
      Platform.putLong(null, info, WRITE_INFO_SIZE);
      Platform.putInt(null, info + WRITE_INFO_MODE, mode);
      Platform.putLong(
        null, info + WRITE_INFO_QUERY_ID, queryId == null ? 0 : memory.string(queryId));
      long out = memory.zeroed(8);
      check((int) f.invokeExact(dataSource, schemaAddress, info, out), Function.CREATE_WRITER);
      return readHandle(out, Function.CREATE_WRITER);
    }
  }

  void closeDataSource(long dataSource) throws Throwable {
    function(Function.CLOSE).invokeExact(dataSource);
  }

  /** The domain of a column, as push_domains gets it in a {@code SparkDsDomain}. */
  static final class Domain {
    final int[] path;
    final boolean nullAllowed;
    final boolean nanAllowed;
    // The addresses of the ArrowArray and the ArrowSchema of the ranges, or 0 for any value.
    final long ranges;
    final long rangesSchema;

    Domain(int[] path, boolean nullAllowed, boolean nanAllowed, long ranges, long rangesSchema) {
      this.path = path;
      this.nullAllowed = nullAllowed;
      this.nanAllowed = nanAllowed;
      this.ranges = ranges;
      this.rangesSchema = rangesSchema;
    }
  }

  /** Pushes the domains down to the reader, and returns whether it uses each of them. */
  boolean[] pushDomains(long reader, Domain[] domains) throws Throwable {
    MethodHandle f = function(Function.PUSH_DOMAINS);
    try (Memory memory = new Memory()) {
      long pointers = memory.zeroed(8L * Math.max(1, domains.length));
      for (int i = 0; i < domains.length; i++) {
        Domain domain = domains[i];
        long struct = memory.zeroed(DOMAIN_SIZE);
        Platform.putLong(null, struct, DOMAIN_SIZE);
        Platform.putLong(null, struct + DOMAIN_PATH, memory.ints(domain.path));
        Platform.putLong(null, struct + DOMAIN_PATH_LENGTH, domain.path.length);
        Platform.putByte(null, struct + DOMAIN_NULL_ALLOWED, (byte) (domain.nullAllowed ? 1 : 0));
        Platform.putByte(null, struct + DOMAIN_NAN_ALLOWED, (byte) (domain.nanAllowed ? 1 : 0));
        Platform.putLong(null, struct + DOMAIN_RANGES, domain.ranges);
        Platform.putLong(null, struct + DOMAIN_RANGES_SCHEMA, domain.rangesSchema);
        Platform.putLong(null, pointers + 8L * i, struct);
      }
      long used = memory.zeroed(Math.max(1, domains.length));
      check((int) f.invokeExact(reader, pointers, (long) domains.length, used),
        Function.PUSH_DOMAINS);
      boolean[] result = new boolean[domains.length];
      for (int i = 0; i < domains.length; i++) {
        result[i] = Platform.getByte(null, used + i) != 0;
      }
      return result;
    }
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

  byte[][] partitions(long reader) throws Throwable {
    MethodHandle f = function(Function.PARTITIONS);
    try (Sink sink = new Sink()) {
      check((int) f.invokeExact(reader, Ffm.SINK, sink.id), Function.PARTITIONS);
      return sink.all();
    }
  }

  byte[] serializeReader(long reader) throws Throwable {
    return serialize(Function.SERIALIZE_READER, reader);
  }

  void closeReader(long reader) throws Throwable {
    function(Function.CLOSE_READER).invokeExact(reader);
  }

  String initialOffset(long streamReader) throws Throwable {
    return offset(Function.INITIAL_OFFSET, streamReader);
  }

  String latestOffset(long streamReader) throws Throwable {
    return offset(Function.LATEST_OFFSET, streamReader);
  }

  private String offset(Function function, long streamReader) throws Throwable {
    byte[] offset = serialize(function, streamReader);
    if (offset == null) {
      throw new IllegalStateException(function.fieldName + " returned no offset.");
    }
    return new String(offset, StandardCharsets.UTF_8);
  }

  byte[][] streamPartitions(long streamReader, String start, String end) throws Throwable {
    MethodHandle f = function(Function.STREAM_PARTITIONS);
    try (Memory memory = new Memory(); Sink sink = new Sink()) {
      check((int) f.invokeExact(streamReader, memory.string(start), memory.string(end),
        Ffm.SINK, sink.id), Function.STREAM_PARTITIONS);
      return sink.all();
    }
  }

  byte[] serializeStreamReader(long streamReader) throws Throwable {
    return serialize(Function.SERIALIZE_STREAM_READER, streamReader);
  }

  void commitOffset(long streamReader, String end) throws Throwable {
    MethodHandle f = function(Function.COMMIT_OFFSET);
    try (Memory memory = new Memory()) {
      check((int) f.invokeExact(streamReader, memory.string(end)), Function.COMMIT_OFFSET);
    }
  }

  void closeStreamReader(long streamReader) throws Throwable {
    function(Function.CLOSE_STREAM_READER).invokeExact(streamReader);
  }

  void read(Task task, byte[] readerState, byte[] partition, long streamAddress)
      throws Throwable {
    MethodHandle f = function(Function.READ);
    try (Memory memory = new Memory()) {
      check((int) f.invokeExact(task.address, memory.bytes(readerState),
        (long) readerState.length, memory.bytes(partition), (long) partition.length,
        streamAddress), Function.READ);
    }
  }

  byte[] serializeWriter(long writer) throws Throwable {
    return serialize(Function.SERIALIZE_WRITER, writer);
  }

  void commit(long writer, long epochId, byte[][] messages) throws Throwable {
    finishWrite(Function.COMMIT, writer, epochId, messages);
  }

  void abort(long writer, long epochId, byte[][] messages) throws Throwable {
    finishWrite(Function.ABORT, writer, epochId, messages);
  }

  private void finishWrite(Function function, long writer, long epochId, byte[][] messages)
      throws Throwable {
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
      check((int) f.invokeExact(writer, epochId, pointers, lengths, (long) messages.length),
        function);
    }
  }

  void closeWriter(long writer) throws Throwable {
    function(Function.CLOSE_WRITER).invokeExact(writer);
  }

  long createDataWriter(Task task, byte[] writerState, long epochId) throws Throwable {
    MethodHandle f = function(Function.CREATE_DATA_WRITER);
    try (Memory memory = new Memory()) {
      long out = memory.zeroed(8);
      check((int) f.invokeExact(task.address, memory.bytes(writerState),
        (long) writerState.length, epochId, out), Function.CREATE_DATA_WRITER);
      return readHandle(out, Function.CREATE_DATA_WRITER);
    }
  }

  void write(long dataWriter, long arrayAddress, long schemaAddress) throws Throwable {
    check((int) function(Function.WRITE).invokeExact(dataWriter, arrayAddress, schemaAddress),
      Function.WRITE);
  }

  /**
   * Commits a data writer. Then {@code committed} runs right away, before the message is checked:
   * if the message is invalid, the commit fails, but the data writer must not be aborted.
   */
  byte[] commitDataWriter(long dataWriter, Runnable committed) throws Throwable {
    MethodHandle f = function(Function.COMMIT_DATA_WRITER);
    try (Sink sink = new Sink()) {
      check((int) f.invokeExact(dataWriter, Ffm.SINK, sink.id), Function.COMMIT_DATA_WRITER);
      committed.run();
      byte[] message = single(sink, Function.COMMIT_DATA_WRITER);
      return message == null ? new byte[0] : message;
    }
  }

  void abortDataWriter(long dataWriter) throws Throwable {
    check((int) function(Function.ABORT_DATA_WRITER).invokeExact(dataWriter),
      Function.ABORT_DATA_WRITER);
  }

  void closeDataWriter(long dataWriter) throws Throwable {
    function(Function.CLOSE_DATA_WRITER).invokeExact(dataWriter);
  }

  /**
   * Calls a function that returns a single byte array through a sink. Returns null if the
   * function does not call the sink.
   */
  private byte[] serialize(Function function, long handle) throws Throwable {
    MethodHandle f = function(function);
    try (Sink sink = new Sink()) {
      check((int) f.invokeExact(handle, Ffm.SINK, sink.id), function);
      return single(sink, function);
    }
  }

  /** Returns the single byte array that a function passed to the sink, or null if none. */
  private static byte[] single(Sink sink, Function function) throws Throwable {
    byte[][] values = sink.all();
    if (values.length > 1) {
      throw new IllegalStateException(
        function.fieldName + " returned " + values.length + " values instead of one.");
    }
    return values.length == 0 ? null : values[0];
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

    /** Copies the ints. Empty ints are passed as a valid pointer too. */
    long ints(int[] values) {
      long address = zeroed(4L * Math.max(1, values.length));
      for (int i = 0; i < values.length; i++) {
        Platform.putInt(null, address + 4L * i, values[i]);
      }
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
