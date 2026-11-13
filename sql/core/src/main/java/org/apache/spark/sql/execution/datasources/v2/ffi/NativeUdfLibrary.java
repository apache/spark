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
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Optional;

import org.apache.spark.internal.LogKeys;
import org.apache.spark.internal.MDC;
import org.apache.spark.internal.SparkLogger;
import org.apache.spark.internal.SparkLoggerFactory;
import org.apache.spark.sql.execution.datasources.v2.ffi.NativeLibrary.C;
import org.apache.spark.sql.execution.datasources.v2.ffi.NativeLibrary.Ffm;
import org.apache.spark.sql.execution.datasources.v2.ffi.NativeLibrary.LoadException;
import org.apache.spark.unsafe.Platform;

/**
 * A native function library, which implements the C interface in
 * {@code org/apache/spark/sql/datasource/spark_udf.h}, called through the Foreign Function and
 * Memory API with the method handles of {@link NativeLibrary}. Its host and task contexts have the
 * layouts of those of native data sources, so a task context is a {@link NativeLibrary.Task}. A
 * function that fails throws a {@link RuntimeException} with the message of {@code last_error}.
 */
final class NativeUdfLibrary {

  private static final SparkLogger LOGGER = SparkLoggerFactory.getLogger(NativeUdfLibrary.class);

  /** The version of the interface that Spark supports, {@code SPARK_UDF_ABI_VERSION}. */
  static final int ABI_VERSION = 1;

  /** The kind of a scalar function, {@code SPARK_UDF_SCALAR}. */
  static final int SCALAR = 1;

  /** The functions of the table {@code SparkUdfLibrary}, in their order in the table. */
  enum Function {
    LAST_ERROR("last_error", C.PTR),
    CREATE("create", C.I32, C.PTR, C.PTR),
    EVAL("eval", C.I32, C.PTR, C.PTR, C.PTR, C.PTR),
    CLOSE("close", C.VOID, C.PTR);

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

  private static final Function[] REQUIRED = {Function.CREATE, Function.EVAL, Function.CLOSE};

  // The layouts of the structs of the header, on the 64-bit platforms that Spark supports.
  private static final long TABLE_ABI_VERSION = 8;
  private static final long TABLE_FUNCTIONS = 16;
  private static final long HOST_SIZE = 40;
  private static final long HOST_ABI_VERSION = 8;
  private static final long HOST_SPARK_VERSION = 16;
  private static final long HOST_LOG = 24;
  private static final long HOST_IS_LOG_ENABLED = 32;
  private static final long CREATE_INFO_SIZE = 56;
  private static final long CREATE_INFO_KIND = 8;
  private static final long CREATE_INFO_NAME = 16;
  private static final long CREATE_INFO_ARGUMENTS = 24;
  private static final long CREATE_INFO_RESULT = 32;
  private static final long CREATE_INFO_SESSION_TIME_ZONE = 40;
  private static final long CREATE_INFO_TASK = 48;
  private static final long EVAL_INFO_SIZE = 8;

  // SparkUdfEvalInfo only holds its size. Until fields are added to it, a single one is
  // allocated, and never freed.
  private static final long EVAL_INFO = evalInfo();

  private static long evalInfo() {
    long info = Platform.allocateMemory(EVAL_INFO_SIZE);
    Platform.putLong(null, info, EVAL_INFO_SIZE);
    return info;
  }

  /**
   * The offsets of the fields of the structs that Spark reads or writes, by {@code struct.field},
   * and the sizes of the structs, by {@code struct}, for the tests to compare with the header.
   */
  static Map<String, Long> layout() {
    Map<String, Long> layout = new LinkedHashMap<>();
    layout.put("SparkUdfLibrary", TABLE_FUNCTIONS + 8L * Function.values().length);
    layout.put("SparkUdfLibrary.size", 0L);
    layout.put("SparkUdfLibrary.abi_version", TABLE_ABI_VERSION);
    for (Function function : Function.values()) {
      layout.put("SparkUdfLibrary." + function.fieldName, function.offset());
    }
    layout.put("SparkUdfHost", HOST_SIZE);
    layout.put("SparkUdfHost.size", 0L);
    layout.put("SparkUdfHost.abi_version", HOST_ABI_VERSION);
    layout.put("SparkUdfHost.spark_version", HOST_SPARK_VERSION);
    layout.put("SparkUdfHost.log", HOST_LOG);
    layout.put("SparkUdfHost.is_log_enabled", HOST_IS_LOG_ENABLED);
    layout.put("SparkUdfCreateInfo", CREATE_INFO_SIZE);
    layout.put("SparkUdfCreateInfo.size", 0L);
    layout.put("SparkUdfCreateInfo.kind", CREATE_INFO_KIND);
    layout.put("SparkUdfCreateInfo.name", CREATE_INFO_NAME);
    layout.put("SparkUdfCreateInfo.arguments", CREATE_INFO_ARGUMENTS);
    layout.put("SparkUdfCreateInfo.result", CREATE_INFO_RESULT);
    layout.put("SparkUdfCreateInfo.session_time_zone", CREATE_INFO_SESSION_TIME_ZONE);
    layout.put("SparkUdfCreateInfo.task", CREATE_INFO_TASK);
    layout.put("SparkUdfEvalInfo", EVAL_INFO_SIZE);
    layout.put("SparkUdfEvalInfo.size", 0L);
    // NativeLibrary.Task fills the task contexts.
    NativeLibrary.layout().forEach((key, value) -> {
      if (key.equals("SparkDsTaskContext") || key.startsWith("SparkDsTaskContext.")) {
        layout.put("SparkUdf" + key.substring("SparkDs".length()), value);
      }
    });
    return layout;
  }

  private final long size;
  private final int abiVersion;
  // The functions of the table, by ordinal, null for the functions that the library leaves NULL.
  private final MethodHandle[] functions = new MethodHandle[Function.values().length];

  private NativeUdfLibrary(long table) throws Throwable {
    size = Platform.getLong(null, table);
    if (size < TABLE_FUNCTIONS) {
      throw new LoadException(
        "spark_udf_init returned a function table with a size of " + size + " bytes.", null, true);
    }
    abiVersion = Platform.getInt(null, table + TABLE_ABI_VERSION);
    // The table of another version of the interface has another layout. Only the functions that
    // fit in the size of the table are read.
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
      for (Function function : REQUIRED) {
        if (functions[function.ordinal()] == null) {
          throw new LoadException("The function table that spark_udf_init returned, of " + size +
            " bytes, does not have the function " + function.fieldName + ".", null, true);
        }
      }
    }
  }

  /**
   * Loads the library at the given path, and gets its table with {@code spark_udf_init}. The
   * library stays loaded until the JVM exits.
   *
   * @param sparkVersion the version of Spark, which the library gets in {@code SparkUdfHost}
   * @throws LoadException if the library cannot be loaded, or cannot be used
   */
  static NativeUdfLibrary load(String path, String sparkVersion) throws Throwable {
    if (!NativeLibrary.isSupported()) {
      throw new UnsupportedOperationException(
        "Native functions require Java " + NativeLibrary.MINIMUM_JAVA_VERSION + " or later.");
    }
    Object lookup;
    try {
      lookup = Ffm.LIBRARY_LOOKUP.invoke(Path.of(path), Ffm.GLOBAL_ARENA);
    } catch (IllegalArgumentException | IllegalCallerException e) {
      throw new LoadException(e.getMessage(), e);
    } catch (LinkageError e) {
      throw new LoadException(
        "Cannot use the Foreign Function and Memory API: " + NativeLibrary.rootCause(e), e);
    }
    Optional<?> init = (Optional<?>) Ffm.FIND.invoke(lookup, "spark_udf_init");
    if (init.isEmpty()) {
      throw new LoadException(
        "The library does not export the function spark_udf_init.", null, true);
    }
    MethodHandle initFunction = Ffm.downcall(init.get(), C.I32, C.PTR, C.PTR);
    // The library may keep the host until the JVM exits, even if spark_udf_init fails.
    long host = host(path, sparkVersion);
    long out = Platform.allocateMemory(8);
    try {
      Platform.putLong(null, out, 0);
      int status = (int) initFunction.invokeExact(host, out);
      if (status != 0) {
        throw new LoadException(
          "spark_udf_init failed with the error code " + status + ".", null, true);
      }
      long table = Platform.getLong(null, out);
      if (table == 0) {
        throw new LoadException("spark_udf_init returned no function table.", null, true);
      }
      return new NativeUdfLibrary(table);
    } finally {
      Platform.freeMemory(out);
    }
  }

  /** The C functions of {@code SparkUdfHost}, created when the first library is loaded. */
  private static final class HostFunctions {
    // NativeUdfLibrary.log, which SparkUdfHost.log calls with the path of its library.
    static final MethodHandle LOG;
    // The address of SparkUdfHost.is_log_enabled, which calls NativeUdfLibrary.isLogEnabled.
    static final long IS_LOG_ENABLED;

    static {
      try {
        MethodHandles.Lookup lookup = MethodHandles.lookup();
        LOG = lookup.findStatic(NativeUdfLibrary.class, "log",
          MethodType.methodType(void.class, String.class, int.class, long.class));
        IS_LOG_ENABLED = Ffm.upcall(
          lookup.findStatic(NativeUdfLibrary.class, "isLogEnabled",
            MethodType.methodType(boolean.class, int.class)),
          C.BOOL, C.I32);
      } catch (Throwable e) {
        // The message names the cause, because the JVM only keeps this message for the later
        // uses of the class.
        throw new IllegalStateException(
          "Cannot initialize the FFM API: " + NativeLibrary.rootCause(e), e);
      }
    }
  }

  /**
   * Allocates the SparkUdfHost of a library. The library may keep it until the JVM exits, so it
   * is never freed.
   */
  private static long host(String path, String sparkVersion) throws Throwable {
    long log = Ffm.upcall(
      MethodHandles.insertArguments(HostFunctions.LOG, 0, path), C.VOID, C.I32, C.PTR);
    long versionAddress = NativeLibrary.toCString(sparkVersion);
    long host = Platform.allocateMemory(HOST_SIZE);
    Platform.setMemory(host, (byte) 0, HOST_SIZE);
    Platform.putLong(null, host, HOST_SIZE);
    Platform.putInt(null, host + HOST_ABI_VERSION, ABI_VERSION);
    Platform.putLong(null, host + HOST_SPARK_VERSION, versionAddress);
    Platform.putLong(null, host + HOST_LOG, log);
    Platform.putLong(null, host + HOST_IS_LOG_ENABLED, HostFunctions.IS_LOG_ENABLED);
    return host;
  }

  /**
   * Called by the library, through {@code SparkUdfHost.log}, with the path of the library. It must
   * not throw: an exception thrown by an upcall crashes the JVM.
   */
  private static void log(String path, int level, long message) {
    try {
      String text = message == 0
        ? "" : new String(NativeLibrary.readCString(message), StandardCharsets.UTF_8);
      switch (NativeLibrary.LogLevel.of(level)) {
        case ERROR -> LOGGER.error("Native function library {}: {}",
          MDC.of(LogKeys.PATH, path), MDC.of(LogKeys.MESSAGE, text));
        case WARN -> LOGGER.warn("Native function library {}: {}",
          MDC.of(LogKeys.PATH, path), MDC.of(LogKeys.MESSAGE, text));
        case DEBUG -> LOGGER.debug("Native function library {}: {}", path, text);
        case TRACE -> LOGGER.trace("Native function library {}: {}", path, text);
        case INFO -> LOGGER.info("Native function library {}: {}",
          MDC.of(LogKeys.PATH, path), MDC.of(LogKeys.MESSAGE, text));
      }
    } catch (Throwable e) {
      // The message is lost.
    }
  }

  /**
   * Called by the library, through {@code SparkUdfHost.is_log_enabled}, with the level of the
   * messages of {@link #log}. It must not throw: an exception thrown by an upcall crashes the JVM.
   */
  private static boolean isLogEnabled(int level) {
    try {
      return switch (NativeLibrary.LogLevel.of(level)) {
        case ERROR -> LOGGER.isErrorEnabled();
        case WARN -> LOGGER.isWarnEnabled();
        case INFO -> LOGGER.isInfoEnabled();
        case DEBUG -> LOGGER.isDebugEnabled();
        case TRACE -> LOGGER.isTraceEnabled();
      };
    } catch (Throwable e) {
      return false;
    }
  }

  /** The version of the interface that the table of the library implements. */
  int abiVersion() {
    return abiVersion;
  }

  /**
   * Creates the state of a call of a function in a task.
   *
   * @param kind the kind of the function, such as {@link #SCALAR}
   * @param argumentsAddress the address of the {@code ArrowSchema} of the arguments, a struct
   * @param resultAddress the address of the {@code ArrowSchema} of the result
   * @param task the task, which the library may use until the state is closed
   * @return the state, which is not 0
   */
  long create(
      String name,
      int kind,
      long argumentsAddress,
      long resultAddress,
      String sessionTimeZone,
      NativeLibrary.Task task) throws Throwable {
    MethodHandle f = functions[Function.CREATE.ordinal()];
    try (NativeLibrary.Memory memory = new NativeLibrary.Memory()) {
      long info = memory.zeroed(CREATE_INFO_SIZE);
      Platform.putLong(null, info, CREATE_INFO_SIZE);
      Platform.putInt(null, info + CREATE_INFO_KIND, kind);
      Platform.putLong(null, info + CREATE_INFO_NAME, memory.string(name));
      Platform.putLong(null, info + CREATE_INFO_ARGUMENTS, argumentsAddress);
      Platform.putLong(null, info + CREATE_INFO_RESULT, resultAddress);
      Platform.putLong(
        null, info + CREATE_INFO_SESSION_TIME_ZONE, memory.string(sessionTimeZone));
      Platform.putLong(null, info + CREATE_INFO_TASK, task.address);
      long out = memory.zeroed(8);
      check((int) f.invokeExact(info, out), Function.CREATE);
      long state = Platform.getLong(null, out);
      if (state == 0) {
        throw new IllegalStateException("create returned a NULL state.");
      }
      return state;
    }
  }

  /**
   * Evaluates a batch.
   *
   * @param inputAddress the address of the {@code ArrowArray} of the batch, a struct array
   * @param outAddress the address of the zeroed {@code ArrowArray} of the result
   */
  void eval(long state, long inputAddress, long outAddress) throws Throwable {
    check((int) functions[Function.EVAL.ordinal()].invokeExact(
      state, EVAL_INFO, inputAddress, outAddress), Function.EVAL);
  }

  void close(long state) throws Throwable {
    functions[Function.CLOSE.ordinal()].invokeExact(state);
  }

  /** Throws the error that the library reported if a function failed. */
  private void check(int status, Function function) throws Throwable {
    if (status == 0) {
      return;
    }
    MethodHandle lastError = functions[Function.LAST_ERROR.ordinal()];
    long address = lastError == null ? 0 : (long) lastError.invokeExact();
    String message = address == 0
      ? "" : new String(NativeLibrary.readCString(address), StandardCharsets.UTF_8);
    throw new RuntimeException(message.isEmpty()
      ? function.fieldName + " failed with the error code " + status + "."
      : message);
  }
}
