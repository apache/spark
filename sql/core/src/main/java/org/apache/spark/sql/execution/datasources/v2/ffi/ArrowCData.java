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
import java.util.ArrayList;
import java.util.Arrays;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

import org.apache.arrow.memory.ArrowBuf;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.ForeignAllocation;
import org.apache.arrow.vector.BitVectorHelper;
import org.apache.arrow.vector.FieldVector;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.ipc.message.ArrowFieldNode;
import org.apache.arrow.vector.types.DateUnit;
import org.apache.arrow.vector.types.FloatingPointPrecision;
import org.apache.arrow.vector.types.IntervalUnit;
import org.apache.arrow.vector.types.TimeUnit;
import org.apache.arrow.vector.types.UnionMode;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.DictionaryEncoding;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.FieldType;
import org.apache.arrow.vector.types.pojo.Schema;

import org.apache.spark.internal.SparkLogger;
import org.apache.spark.internal.SparkLoggerFactory;
import org.apache.spark.sql.execution.datasources.v2.ffi.Ffm.C;
import org.apache.spark.unsafe.Platform;

/**
 * The <a href="https://arrow.apache.org/docs/format/CDataInterface.html">Arrow C data
 * interface</a>, implemented with the FFM API, see {@link Ffm}: Spark exports and imports the
 * schemas and the arrays that it exchanges with native data source libraries, and reads the
 * Arrow streams that they export.
 * <p>
 * The C structs are in native memory, at {@code long} addresses, with the layout of 64-bit
 * platforms. A struct is released once its {@code release} callback is NULL. Arrays are imported
 * without copying their buffers: the imported vectors refer to the memory of the array, which is
 * released once the vectors no longer use any of its buffers. Imported arrays must have an offset
 * of 0, at every level.
 */
final class ArrowCData {

  private ArrowCData() {}

  private static final SparkLogger LOG = SparkLoggerFactory.getLogger(ArrowCData.class);

  static final long SCHEMA_SIZE = 72;
  static final long ARRAY_SIZE = 80;
  static final long STREAM_SIZE = 40;

  // The fields of struct ArrowSchema.
  private static final long SCHEMA_FORMAT = 0;
  private static final long SCHEMA_NAME = 8;
  private static final long SCHEMA_METADATA = 16;
  private static final long SCHEMA_FLAGS = 24;
  private static final long SCHEMA_N_CHILDREN = 32;
  private static final long SCHEMA_CHILDREN = 40;
  private static final long SCHEMA_DICTIONARY = 48;
  private static final long SCHEMA_RELEASE = 56;
  private static final long SCHEMA_PRIVATE_DATA = 64;

  // The fields of struct ArrowArray.
  private static final long ARRAY_LENGTH = 0;
  private static final long ARRAY_NULL_COUNT = 8;
  private static final long ARRAY_OFFSET = 16;
  private static final long ARRAY_N_BUFFERS = 24;
  private static final long ARRAY_N_CHILDREN = 32;
  private static final long ARRAY_BUFFERS = 40;
  private static final long ARRAY_CHILDREN = 48;
  private static final long ARRAY_DICTIONARY = 56;
  private static final long ARRAY_RELEASE = 64;
  private static final long ARRAY_PRIVATE_DATA = 72;

  // The fields of struct ArrowArrayStream.
  private static final long STREAM_GET_SCHEMA = 0;
  private static final long STREAM_GET_NEXT = 8;
  private static final long STREAM_GET_LAST_ERROR = 16;
  private static final long STREAM_RELEASE = 24;

  private static final long FLAG_DICTIONARY_ORDERED = 1;
  private static final long FLAG_NULLABLE = 2;
  private static final long FLAG_MAP_KEYS_SORTED = 4;

  // Deeper schemas and arrays are rejected, rather than overflowing the stack.
  private static final int MAX_DEPTH = 64;

  /** The callbacks of the C structs, created when they are first used. */
  private static final class Callbacks {
    // int (*)(void*, void*), such as get_next.
    static final MethodHandle INT_FUNCTION;
    // const char* (*)(void*), such as get_last_error.
    static final MethodHandle POINTER_FUNCTION;
    // void (*)(void*), such as release.
    static final MethodHandle VOID_FUNCTION;
    // The release callbacks of the structs that Spark exports.
    static final long RELEASE_SCHEMA;
    static final long RELEASE_ARRAY;

    static {
      try {
        INT_FUNCTION = Ffm.bindPointer(C.INT32, C.POINTER, C.POINTER);
        POINTER_FUNCTION = Ffm.bindPointer(C.POINTER, C.POINTER);
        VOID_FUNCTION = Ffm.bindPointer(C.VOID, C.POINTER);
        MethodHandles.Lookup lookup = MethodHandles.lookup();
        MethodType release = MethodType.methodType(void.class, long.class);
        RELEASE_SCHEMA = Ffm.upcall(
          lookup.findStatic(ArrowCData.class, "releaseExportedSchema", release),
          C.VOID, C.POINTER);
        RELEASE_ARRAY = Ffm.upcall(
          lookup.findStatic(ArrowCData.class, "releaseExportedArray", release),
          C.VOID, C.POINTER);
      } catch (Throwable e) {
        throw new IllegalStateException("Cannot create the Arrow C data callbacks.", e);
      }
    }
  }

  // ---------------------------------------------------------------------------------------------
  // Structs
  // ---------------------------------------------------------------------------------------------

  /** Allocates a struct in native memory, initialized to zero, which is a released struct. */
  static long allocate(long size) {
    long address = Platform.allocateMemory(size);
    Platform.setMemory(address, (byte) 0, size);
    return address;
  }

  static void free(long address) {
    Platform.freeMemory(address);
  }

  static boolean isSchemaReleased(long schema) {
    return Platform.getLong(null, schema + SCHEMA_RELEASE) == 0;
  }

  static boolean isArrayReleased(long array) {
    return Platform.getLong(null, array + ARRAY_RELEASE) == 0;
  }

  static long length(long array) {
    return Platform.getLong(null, array + ARRAY_LENGTH);
  }

  static long offset(long array) {
    return Platform.getLong(null, array + ARRAY_OFFSET);
  }

  static long bufferCount(long array) {
    return Platform.getLong(null, array + ARRAY_N_BUFFERS);
  }

  /** The address of the buffer at the given index of an array, which may be 0 (NULL). */
  static long buffer(long array, int index) {
    return Platform.getLong(null, Platform.getLong(null, array + ARRAY_BUFFERS) + 8L * index);
  }

  static long childCount(long array) {
    return Platform.getLong(null, array + ARRAY_N_CHILDREN);
  }

  static long child(long array, int index) {
    return Platform.getLong(null, Platform.getLong(null, array + ARRAY_CHILDREN) + 8L * index);
  }

  /** Releases a schema with its release callback, unless it is released already. */
  static void releaseSchema(long schema) throws Throwable {
    release(schema, SCHEMA_RELEASE);
  }

  /** Releases an array with its release callback, unless it is released already. */
  static void releaseArray(long array) throws Throwable {
    release(array, ARRAY_RELEASE);
  }

  /** Releases a stream with its release callback, unless it is released already. */
  static void releaseStream(long stream) throws Throwable {
    release(stream, STREAM_RELEASE);
  }

  private static void release(long struct, long releaseField) throws Throwable {
    long release = Platform.getLong(null, struct + releaseField);
    if (release != 0) {
      Callbacks.VOID_FUNCTION.invokeExact(release, struct);
      // The callback marks the struct as released. This makes sure that it is not released again.
      Platform.putLong(null, struct + releaseField, 0L);
    }
  }

  // ---------------------------------------------------------------------------------------------
  // Streams
  // ---------------------------------------------------------------------------------------------

  /** Calls get_schema of a stream, which initializes the schema at {@code out}. */
  static void streamSchema(long stream, long out) throws Throwable {
    long getSchema = callback(stream, STREAM_GET_SCHEMA);
    checkStream(stream, (int) Callbacks.INT_FUNCTION.invokeExact(getSchema, stream, out));
  }

  /**
   * Calls get_next of a stream, which initializes the array at {@code out}, or marks it as
   * released at the end of the stream.
   */
  static void streamNext(long stream, long out) throws Throwable {
    long getNext = callback(stream, STREAM_GET_NEXT);
    checkStream(stream, (int) Callbacks.INT_FUNCTION.invokeExact(getNext, stream, out));
  }

  /** A callback of a stream, which must not be NULL: calling NULL would crash the JVM. */
  private static long callback(long stream, long field) {
    if (Platform.getLong(null, stream + STREAM_RELEASE) == 0) {
      throw new IllegalStateException("The stream is released, or was not initialized.");
    }
    long callback = Platform.getLong(null, stream + field);
    if (callback == 0) {
      throw new IllegalStateException("A callback of the stream is NULL.");
    }
    return callback;
  }

  private static void checkStream(long stream, int errno) throws Throwable {
    if (errno == 0) {
      return;
    }
    long getLastError = Platform.getLong(null, stream + STREAM_GET_LAST_ERROR);
    long message = getLastError == 0
      ? 0 : (long) Callbacks.POINTER_FUNCTION.invokeExact(getLastError, stream);
    String text = message == 0 ? "" : readString(message);
    throw new RuntimeException(text.isEmpty() ? "The stream failed with the error " + errno + "."
      : text);
  }

  // ---------------------------------------------------------------------------------------------
  // Exporting schemas
  // ---------------------------------------------------------------------------------------------

  /**
   * The memory of an exported struct, freed by its release callback. It does not include the
   * struct itself, which belongs to its parent or to whoever allocated it.
   */
  private static final class Exported {
    final List<Long> allocations = new ArrayList<>();
    final List<ArrowBuf> buffers = new ArrayList<>();

    long allocate(long size) {
      long address = ArrowCData.allocate(size);
      allocations.add(address);
      return address;
    }

    long string(String value) {
      return bytes(value.getBytes(StandardCharsets.UTF_8), true);
    }

    long bytes(byte[] bytes, boolean terminate) {
      long address = allocate(bytes.length + (terminate ? 1 : 0));
      Platform.copyMemory(bytes, Platform.BYTE_ARRAY_OFFSET, null, address, bytes.length);
      return address;
    }

    void free() {
      for (ArrowBuf buffer : buffers) {
        // Null for a buffer exported as NULL.
        if (buffer != null) {
          buffer.getReferenceManager().release();
        }
      }
      allocations.forEach(Platform::freeMemory);
    }
  }

  private static final AtomicLong nextExportedId = new AtomicLong(1);
  private static final ConcurrentHashMap<Long, Exported> exported = new ConcurrentHashMap<>();

  /** Exports a schema as a struct type whose fields are the fields of the schema. */
  static void exportSchema(Schema schema, long out) throws Throwable {
    exportField(
      new Field("", new FieldType(false, ArrowType.Struct.INSTANCE, null,
        schema.getCustomMetadata()), schema.getFields()),
      out);
  }

  private static void exportField(Field field, long out) throws Throwable {
    if (field.getDictionary() != null) {
      throw new UnsupportedOperationException(
        "Dictionary-encoded fields cannot be exported: " + field);
    }
    String format = format(field.getType());
    List<Field> children = field.getChildren();
    long flags = field.isNullable() ? FLAG_NULLABLE : 0;
    if (field.getType() instanceof ArrowType.Map map && map.getKeysSorted()) {
      flags |= FLAG_MAP_KEYS_SORTED;
    }
    // A released struct with its callback, so that a failure below releases what was exported.
    Platform.setMemory(out, (byte) 0, SCHEMA_SIZE);
    Exported data = register(out, SCHEMA_PRIVATE_DATA);
    Platform.putLong(null, out + SCHEMA_FLAGS, flags);
    Platform.putLong(null, out + SCHEMA_RELEASE, Callbacks.RELEASE_SCHEMA);
    try {
      Platform.putLong(null, out + SCHEMA_FORMAT, data.string(format));
      String name = field.getName();
      Platform.putLong(null, out + SCHEMA_NAME, name == null ? 0L : data.string(name));
      Platform.putLong(null, out + SCHEMA_METADATA, encodeMetadata(data, field.getMetadata()));
      Platform.putLong(
        null, out + SCHEMA_CHILDREN, childStructs(data, children.size(), SCHEMA_SIZE));
      for (int i = 0; i < children.size(); i++) {
        exportField(children.get(i), schemaChild(out, i));
        // Counted once exported, so that a failure releases the exported children only.
        Platform.putLong(null, out + SCHEMA_N_CHILDREN, i + 1L);
      }
    } catch (Throwable e) {
      releaseExportedSchema(out);
      throw e;
    }
  }

  private static long schemaChild(long schema, int index) {
    return Platform.getLong(null, Platform.getLong(null, schema + SCHEMA_CHILDREN) + 8L * index);
  }

  /** Registers the private data of a struct being exported. */
  private static Exported register(long struct, long privateDataField) {
    long id = nextExportedId.getAndIncrement();
    Exported data = new Exported();
    exported.put(id, data);
    Platform.putLong(null, struct + privateDataField, id);
    return data;
  }

  /**
   * Allocates the children of an exported struct: an array of pointers to the children, followed
   * by the children themselves, initialized to zero. Returns the address of the pointers.
   */
  private static long childStructs(Exported data, int count, long structSize) {
    if (count == 0) {
      return 0;
    }
    long pointers = data.allocate(count * (8 + structSize));
    for (int i = 0; i < count; i++) {
      Platform.putLong(null, pointers + 8L * i, pointers + 8L * count + structSize * i);
    }
    return pointers;
  }

  /** The release callback of the schemas that Spark exports. */
  private static void releaseExportedSchema(long schema) {
    try {
      long children = Platform.getLong(null, schema + SCHEMA_CHILDREN);
      long count = Platform.getLong(null, schema + SCHEMA_N_CHILDREN);
      for (int i = 0; i < count; i++) {
        // A child that was moved is marked as released here, and released where it was moved.
        long child = Platform.getLong(null, children + 8L * i);
        if (!isSchemaReleased(child)) {
          releaseExportedSchema(child);
        }
      }
      Exported data = exported.remove(Platform.getLong(null, schema + SCHEMA_PRIVATE_DATA));
      if (data != null) {
        data.free();
      }
    } catch (Throwable e) {
      // An exception must not be thrown to the caller of the callback, which is native code.
      LOG.warn("Failed to release an exported Arrow schema.", e);
    }
    Platform.putLong(null, schema + SCHEMA_RELEASE, 0L);
  }

  // ---------------------------------------------------------------------------------------------
  // Importing schemas
  // ---------------------------------------------------------------------------------------------

  /**
   * Imports a schema, which must be a struct type whose fields are the fields of the schema, and
   * releases it.
   */
  static Schema importSchema(long schema) throws Throwable {
    try {
      Field field = importField(schema, 0, new AtomicLong());
      if (!(field.getType() instanceof ArrowType.Struct)) {
        throw new IllegalArgumentException(
          "The schema is not a struct type, but " + field.getType() + ".");
      }
      return new Schema(field.getChildren(), field.getMetadata());
    } finally {
      releaseSchema(schema);
    }
  }

  private static Field importField(long schema, int depth, AtomicLong dictionaryIds) {
    if (depth > MAX_DEPTH) {
      throw new IllegalArgumentException("The schema is nested more than " + MAX_DEPTH +
        " levels deep.");
    }
    long formatAddress = Platform.getLong(null, schema + SCHEMA_FORMAT);
    if (formatAddress == 0) {
      throw new IllegalArgumentException("The schema has no format.");
    }
    String format = readString(formatAddress);
    long nameAddress = Platform.getLong(null, schema + SCHEMA_NAME);
    String name = nameAddress == 0 ? "" : readString(nameAddress);
    Map<String, String> metadata =
      decodeMetadata(Platform.getLong(null, schema + SCHEMA_METADATA));
    long flags = Platform.getLong(null, schema + SCHEMA_FLAGS);
    long count = Platform.getLong(null, schema + SCHEMA_N_CHILDREN);
    List<Field> children = new ArrayList<>();
    for (int i = 0; i < count; i++) {
      children.add(importField(schemaChild(schema, i), depth + 1, dictionaryIds));
    }
    ArrowType type = parseFormat(format, flags);
    DictionaryEncoding dictionary = null;
    long dictionarySchema = Platform.getLong(null, schema + SCHEMA_DICTIONARY);
    if (dictionarySchema != 0) {
      if (!(type instanceof ArrowType.Int indexType)) {
        throw new IllegalArgumentException(
          "The indices of the dictionary of " + name + " are not integers: " + format + ".");
      }
      // Only the encoding is kept: Spark does not read dictionary-encoded data.
      importField(dictionarySchema, depth + 1, dictionaryIds);
      dictionary = new DictionaryEncoding(dictionaryIds.getAndIncrement(),
        (flags & FLAG_DICTIONARY_ORDERED) != 0, indexType);
    }
    boolean nullable = (flags & FLAG_NULLABLE) != 0;
    return new Field(name, new FieldType(nullable, type, dictionary, metadata), children);
  }

  // ---------------------------------------------------------------------------------------------
  // Formats
  // ---------------------------------------------------------------------------------------------

  /** The format string of an Arrow type. */
  static String format(ArrowType type) {
    switch (type.getTypeID()) {
      case Null: return "n";
      case Bool: return "b";
      case Int: {
        ArrowType.Int t = (ArrowType.Int) type;
        String formats = t.getIsSigned() ? "csil" : "CSIL";
        switch (t.getBitWidth()) {
          case 8: return formats.substring(0, 1);
          case 16: return formats.substring(1, 2);
          case 32: return formats.substring(2, 3);
          case 64: return formats.substring(3, 4);
          default: break;
        }
        break;
      }
      case FloatingPoint: {
        switch (((ArrowType.FloatingPoint) type).getPrecision()) {
          case HALF: return "e";
          case SINGLE: return "f";
          case DOUBLE: return "g";
          default: break;
        }
        break;
      }
      case Binary: return "z";
      case LargeBinary: return "Z";
      case BinaryView: return "vz";
      case Utf8: return "u";
      case LargeUtf8: return "U";
      case Utf8View: return "vu";
      case FixedSizeBinary:
        return "w:" + ((ArrowType.FixedSizeBinary) type).getByteWidth();
      case Decimal: {
        ArrowType.Decimal t = (ArrowType.Decimal) type;
        return "d:" + t.getPrecision() + "," + t.getScale() +
          (t.getBitWidth() == 128 ? "" : "," + t.getBitWidth());
      }
      case Date:
        return ((ArrowType.Date) type).getUnit() == DateUnit.DAY ? "tdD" : "tdm";
      case Time: {
        ArrowType.Time t = (ArrowType.Time) type;
        return "tt" + unit(t.getUnit());
      }
      case Timestamp: {
        ArrowType.Timestamp t = (ArrowType.Timestamp) type;
        String timezone = t.getTimezone();
        return "ts" + unit(t.getUnit()) + ":" + (timezone == null ? "" : timezone);
      }
      case Duration:
        return "tD" + unit(((ArrowType.Duration) type).getUnit());
      case Interval: {
        switch (((ArrowType.Interval) type).getUnit()) {
          case YEAR_MONTH: return "tiM";
          case DAY_TIME: return "tiD";
          case MONTH_DAY_NANO: return "tin";
          default: break;
        }
        break;
      }
      case List: return "+l";
      case LargeList: return "+L";
      case ListView: return "+vl";
      case LargeListView: return "+vL";
      case FixedSizeList:
        return "+w:" + ((ArrowType.FixedSizeList) type).getListSize();
      case Struct: return "+s";
      case Map: return "+m";
      default: break;
    }
    throw new UnsupportedOperationException("The Arrow type " + type + " cannot be exported.");
  }

  private static char unit(TimeUnit unit) {
    return switch (unit) {
      case SECOND -> 's';
      case MILLISECOND -> 'm';
      case MICROSECOND -> 'u';
      case NANOSECOND -> 'n';
    };
  }

  /** The Arrow type of a format string. */
  static ArrowType parseFormat(String format, long flags) {
    switch (format) {
      case "n": return ArrowType.Null.INSTANCE;
      case "b": return ArrowType.Bool.INSTANCE;
      case "c": return new ArrowType.Int(8, true);
      case "C": return new ArrowType.Int(8, false);
      case "s": return new ArrowType.Int(16, true);
      case "S": return new ArrowType.Int(16, false);
      case "i": return new ArrowType.Int(32, true);
      case "I": return new ArrowType.Int(32, false);
      case "l": return new ArrowType.Int(64, true);
      case "L": return new ArrowType.Int(64, false);
      case "e": return new ArrowType.FloatingPoint(FloatingPointPrecision.HALF);
      case "f": return new ArrowType.FloatingPoint(FloatingPointPrecision.SINGLE);
      case "g": return new ArrowType.FloatingPoint(FloatingPointPrecision.DOUBLE);
      case "z": return ArrowType.Binary.INSTANCE;
      case "Z": return ArrowType.LargeBinary.INSTANCE;
      case "vz": return ArrowType.BinaryView.INSTANCE;
      case "u": return ArrowType.Utf8.INSTANCE;
      case "U": return ArrowType.LargeUtf8.INSTANCE;
      case "vu": return ArrowType.Utf8View.INSTANCE;
      case "tdD": return new ArrowType.Date(DateUnit.DAY);
      case "tdm": return new ArrowType.Date(DateUnit.MILLISECOND);
      case "tts": return new ArrowType.Time(TimeUnit.SECOND, 32);
      case "ttm": return new ArrowType.Time(TimeUnit.MILLISECOND, 32);
      case "ttu": return new ArrowType.Time(TimeUnit.MICROSECOND, 64);
      case "ttn": return new ArrowType.Time(TimeUnit.NANOSECOND, 64);
      case "tDs": return new ArrowType.Duration(TimeUnit.SECOND);
      case "tDm": return new ArrowType.Duration(TimeUnit.MILLISECOND);
      case "tDu": return new ArrowType.Duration(TimeUnit.MICROSECOND);
      case "tDn": return new ArrowType.Duration(TimeUnit.NANOSECOND);
      case "tiM": return new ArrowType.Interval(IntervalUnit.YEAR_MONTH);
      case "tiD": return new ArrowType.Interval(IntervalUnit.DAY_TIME);
      case "tin": return new ArrowType.Interval(IntervalUnit.MONTH_DAY_NANO);
      case "+l": return ArrowType.List.INSTANCE;
      case "+L": return ArrowType.LargeList.INSTANCE;
      case "+vl": return ArrowType.ListView.INSTANCE;
      case "+vL": return ArrowType.LargeListView.INSTANCE;
      case "+s": return ArrowType.Struct.INSTANCE;
      case "+m": return new ArrowType.Map((flags & FLAG_MAP_KEYS_SORTED) != 0);
      case "+r": return ArrowType.RunEndEncoded.INSTANCE;
      default: break;
    }
    try {
      if (format.startsWith("ts") && format.length() >= 4 && format.charAt(3) == ':') {
        String timezone = format.substring(4);
        return new ArrowType.Timestamp(
          timeUnit(format.charAt(2)), timezone.isEmpty() ? null : timezone);
      } else if (format.startsWith("d:")) {
        String[] parts = format.substring(2).split(",", -1);
        if (parts.length == 2 || parts.length == 3) {
          return new ArrowType.Decimal(Integer.parseInt(parts[0]), Integer.parseInt(parts[1]),
            parts.length == 3 ? Integer.parseInt(parts[2]) : 128);
        }
      } else if (format.startsWith("w:")) {
        return new ArrowType.FixedSizeBinary(Integer.parseInt(format.substring(2)));
      } else if (format.startsWith("+w:")) {
        return new ArrowType.FixedSizeList(Integer.parseInt(format.substring(3)));
      } else if (format.startsWith("+ud:") || format.startsWith("+us:")) {
        String ids = format.substring(4);
        int[] typeIds = ids.isEmpty() ? new int[0]
          : Arrays.stream(ids.split(",", -1)).mapToInt(Integer::parseInt).toArray();
        return new ArrowType.Union(
          format.charAt(2) == 'd' ? UnionMode.Dense : UnionMode.Sparse, typeIds);
      }
    } catch (IllegalArgumentException e) {
      // Not a valid format, reported below. NumberFormatException is an IllegalArgumentException.
    }
    throw new IllegalArgumentException("Unsupported Arrow format: " + format);
  }

  private static TimeUnit timeUnit(char unit) {
    return switch (unit) {
      case 's' -> TimeUnit.SECOND;
      case 'm' -> TimeUnit.MILLISECOND;
      case 'u' -> TimeUnit.MICROSECOND;
      case 'n' -> TimeUnit.NANOSECOND;
      default -> throw new IllegalArgumentException("Unsupported time unit: " + unit);
    };
  }

  // ---------------------------------------------------------------------------------------------
  // Metadata
  // ---------------------------------------------------------------------------------------------

  // The metadata are an int32 number of entries, followed by the length and the bytes of the key
  // and of the value of each entry, with lengths as int32s in the native byte order.
  private static long encodeMetadata(Exported data, Map<String, String> metadata) {
    if (metadata == null || metadata.isEmpty()) {
      return 0;
    }
    List<byte[]> parts = new ArrayList<>();
    long size = 4;
    for (Map.Entry<String, String> entry : metadata.entrySet()) {
      byte[] key = entry.getKey().getBytes(StandardCharsets.UTF_8);
      byte[] value = entry.getValue().getBytes(StandardCharsets.UTF_8);
      parts.add(key);
      parts.add(value);
      size += 8L + key.length + value.length;
    }
    long address = data.allocate(size);
    Platform.putInt(null, address, metadata.size());
    long position = address + 4;
    for (byte[] part : parts) {
      Platform.putInt(null, position, part.length);
      Platform.copyMemory(part, Platform.BYTE_ARRAY_OFFSET, null, position + 4, part.length);
      position += 4L + part.length;
    }
    return address;
  }

  private static Map<String, String> decodeMetadata(long address) {
    if (address == 0) {
      return null;
    }
    int count = Platform.getInt(null, address);
    if (count < 0) {
      throw new IllegalArgumentException("The metadata of the schema are invalid.");
    }
    Map<String, String> metadata = new LinkedHashMap<>();
    long position = address + 4;
    for (int i = 0; i < count; i++) {
      String[] entry = new String[2];
      for (int j = 0; j < 2; j++) {
        int length = Platform.getInt(null, position);
        if (length < 0) {
          throw new IllegalArgumentException("The metadata of the schema are invalid.");
        }
        byte[] bytes = new byte[length];
        Platform.copyMemory(null, position + 4, bytes, Platform.BYTE_ARRAY_OFFSET, length);
        entry[j] = new String(bytes, StandardCharsets.UTF_8);
        position += 4L + length;
      }
      metadata.put(entry[0], entry[1]);
    }
    return metadata;
  }

  // ---------------------------------------------------------------------------------------------
  // Exporting arrays
  // ---------------------------------------------------------------------------------------------

  /**
   * Exports the vectors of a root as a struct array whose children are the vectors. The array
   * refers to the buffers of the vectors, which it retains until it is released.
   */
  static void exportRoot(BufferAllocator allocator, VectorSchemaRoot root, long out)
      throws Throwable {
    Exported data = startArray(out, root.getRowCount(), 0);
    try {
      // A struct array has a single buffer, the validity buffer, NULL as there are no nulls.
      Platform.putLong(null, out + ARRAY_BUFFERS, data.allocate(8));
      Platform.putLong(null, out + ARRAY_N_BUFFERS, 1L);
      exportChildren(allocator, data, root.getFieldVectors(), out);
    } catch (Throwable e) {
      releaseExportedArray(out);
      throw e;
    }
  }

  private static void exportVector(BufferAllocator allocator, FieldVector vector, long out)
      throws Throwable {
    if (vector.getField().getDictionary() != null) {
      throw new UnsupportedOperationException(
        "Dictionary-encoded vectors cannot be exported: " + vector.getField());
    }
    Exported data = startArray(out, vector.getValueCount(), vector.getNullCount());
    try {
      int count = vector.getExportedCDataBufferCount();
      ArrowBuf pointers = allocator.buffer(8L * Math.max(1, count));
      data.buffers.add(pointers);
      // Writes the addresses of the buffers, or NULL, and adds them to the buffers to release.
      vector.exportCDataBuffers(data.buffers, pointers, 0L);
      Platform.putLong(null, out + ARRAY_BUFFERS, pointers.memoryAddress());
      Platform.putLong(null, out + ARRAY_N_BUFFERS, count);
      exportChildren(allocator, data, vector.getChildrenFromFields(), out);
    } catch (Throwable e) {
      releaseExportedArray(out);
      throw e;
    }
  }

  /**
   * Initializes an exported array without buffers or children, with its callback, so that a
   * failure while exporting them releases what was exported.
   */
  private static Exported startArray(long out, long length, long nullCount) {
    Platform.setMemory(out, (byte) 0, ARRAY_SIZE);
    Exported data = register(out, ARRAY_PRIVATE_DATA);
    Platform.putLong(null, out + ARRAY_LENGTH, length);
    Platform.putLong(null, out + ARRAY_NULL_COUNT, nullCount);
    Platform.putLong(null, out + ARRAY_RELEASE, Callbacks.RELEASE_ARRAY);
    return data;
  }

  private static void exportChildren(
      BufferAllocator allocator, Exported data, List<FieldVector> children, long out)
      throws Throwable {
    Platform.putLong(null, out + ARRAY_CHILDREN, childStructs(data, children.size(), ARRAY_SIZE));
    for (int i = 0; i < children.size(); i++) {
      exportVector(allocator, children.get(i), child(out, i));
      // Counted once exported, so that a failure releases the exported children only.
      Platform.putLong(null, out + ARRAY_N_CHILDREN, i + 1L);
    }
  }

  /** The release callback of the arrays that Spark exports. */
  private static void releaseExportedArray(long array) {
    try {
      long count = childCount(array);
      for (int i = 0; i < count; i++) {
        // A child that was moved is marked as released here, and released where it was moved.
        long child = child(array, i);
        if (!isArrayReleased(child)) {
          releaseExportedArray(child);
        }
      }
      Exported data = exported.remove(Platform.getLong(null, array + ARRAY_PRIVATE_DATA));
      if (data != null) {
        data.free();
      }
    } catch (Throwable e) {
      // An exception must not be thrown to the caller of the callback, which is native code.
      LOG.warn("Failed to release an exported Arrow array.", e);
    }
    Platform.putLong(null, array + ARRAY_RELEASE, 0L);
  }

  // ---------------------------------------------------------------------------------------------
  // Importing arrays
  // ---------------------------------------------------------------------------------------------

  /**
   * An imported array, moved to memory that Spark owns. It is released once the imported buffers
   * that refer to its memory are all released, and the importer released its own reference.
   */
  private static final class Imported {
    private final long array;
    private final AtomicInteger references = new AtomicInteger(1);

    Imported(long array) {
      this.array = array;
    }

    void retain() {
      references.incrementAndGet();
    }

    void release() {
      if (references.decrementAndGet() == 0) {
        try {
          releaseArray(array);
        } catch (Throwable e) {
          LOG.warn("Failed to release an imported Arrow array.", e);
        } finally {
          free(array);
        }
      }
    }
  }

  /**
   * Imports a struct array into the vectors of a root, one child for each vector, without copying
   * the buffers. The array is moved, and marked as released: the root releases it once its vectors
   * no longer use its buffers, such as when the next array is imported or the root is closed.
   */
  static void importIntoRoot(BufferAllocator allocator, long array, VectorSchemaRoot root)
      throws Throwable {
    long moved = allocate(ARRAY_SIZE);
    Platform.copyMemory(null, array, null, moved, ARRAY_SIZE);
    Platform.putLong(null, array + ARRAY_RELEASE, 0L);
    Imported imported = new Imported(moved);
    try {
      List<FieldVector> vectors = root.getFieldVectors();
      long length = length(moved);
      if (childCount(moved) != vectors.size()) {
        throw new IllegalArgumentException("The array has " + childCount(moved) +
          " columns, but the schema has " + vectors.size() + ".");
      }
      checkLength(length);
      for (int i = 0; i < vectors.size(); i++) {
        importVector(allocator, imported, child(moved, i), vectors.get(i), 1);
      }
      root.setRowCount((int) length);
    } finally {
      imported.release();
    }
  }

  private static void importVector(
      BufferAllocator allocator,
      Imported imported,
      long array,
      FieldVector vector,
      int depth) throws Throwable {
    if (depth > MAX_DEPTH) {
      throw new IllegalArgumentException("The array is nested more than " + MAX_DEPTH +
        " levels deep.");
    }
    if (offset(array) != 0) {
      throw new IllegalArgumentException("The array has an offset of " + offset(array) +
        ", but only arrays with an offset of 0 are supported.");
    }
    ArrowType type = vector.getField().getType();
    long length = length(array);
    checkLength(length);
    List<ArrowBuf> buffers = new ArrayList<>();
    try {
      importBuffers(allocator, imported, array, type, length, buffers);
      long nullCount = Platform.getLong(null, array + ARRAY_NULL_COUNT);
      if (nullCount < 0) {
        // Unknown: computed from the validity buffer, which is NULL when there are no nulls.
        ArrowBuf validity = buffers.isEmpty() ? null : buffers.get(0);
        nullCount = type instanceof ArrowType.Null ? length
          : validity == null || validity.capacity() == 0 ? 0
          : BitVectorHelper.getNullCount(validity, (int) length);
      } else if (nullCount > 0 && !buffers.isEmpty() && buffers.get(0) == null &&
          !(type instanceof ArrowType.Null)) {
        throw new IllegalArgumentException("The array has " + nullCount + " nulls, but no " +
          "validity buffer.");
      }
      vector.loadFieldBuffers(new ArrowFieldNode(length, nullCount), buffers);
    } finally {
      // The vector retains the buffers that it uses.
      for (ArrowBuf buffer : buffers) {
        if (buffer != null) {
          buffer.getReferenceManager().release();
        }
      }
    }
    List<FieldVector> children = vector.getChildrenFromFields();
    if (childCount(array) != children.size()) {
      throw new IllegalArgumentException("The array of type " + type + " has " +
        childCount(array) + " children, but " + children.size() + " are expected.");
    }
    for (int i = 0; i < children.size(); i++) {
      importVector(allocator, imported, child(array, i), children.get(i), depth + 1);
    }
  }

  private static void checkLength(long length) {
    if (length < 0 || length > Integer.MAX_VALUE) {
      throw new IllegalArgumentException("The array has an unsupported length of " + length +
        ".");
    }
  }

  /** Wraps the buffers of an array for the vector of the given type, as Arrow Java loads them. */
  private static void importBuffers(
      BufferAllocator allocator,
      Imported imported,
      long array,
      ArrowType type,
      long length,
      List<ArrowBuf> buffers) {
    long count = bufferCount(array);
    switch (type.getTypeID()) {
      case Null:
        expectBuffers(type, count, 0);
        return;
      case Struct:
      case FixedSizeList:
        expectBuffers(type, count, 1);
        buffers.add(validity(allocator, imported, array, length));
        return;
      case List:
      case Map:
      case LargeList: {
        expectBuffers(type, count, 2);
        int width = type instanceof ArrowType.LargeList ? 8 : 4;
        buffers.add(validity(allocator, imported, array, length));
        buffers.add(offsets(allocator, imported, array, length, width));
        return;
      }
      case Utf8:
      case Binary:
      case LargeUtf8:
      case LargeBinary: {
        expectBuffers(type, count, 3);
        int width = type instanceof ArrowType.LargeUtf8 || type instanceof ArrowType.LargeBinary
          ? 8 : 4;
        buffers.add(validity(allocator, imported, array, length));
        ArrowBuf offsets = offsets(allocator, imported, array, length, width);
        buffers.add(offsets);
        long size = width == 8 ? offsets.getLong(8L * length) : offsets.getInt(4L * length);
        buffers.add(wrap(allocator, imported, buffer(array, 2), size, "data"));
        return;
      }
      case Utf8View:
      case BinaryView: {
        // The validity, the views, the data buffers, and the sizes of the data buffers.
        if (count < 3) {
          expectBuffers(type, count, 3);
        }
        buffers.add(validity(allocator, imported, array, length));
        buffers.add(wrap(allocator, imported, buffer(array, 1), 16L * length, "views"));
        long sizes = buffer(array, (int) count - 1);
        if (sizes == 0 && count > 3) {
          throw new IllegalArgumentException("The array of type " + type + " has no sizes of " +
            "its data buffers.");
        }
        for (int i = 2; i < count - 1; i++) {
          long size = Platform.getLong(null, sizes + 8L * (i - 2));
          buffers.add(wrap(allocator, imported, buffer(array, i), size, "data"));
        }
        return;
      }
      case Bool:
        expectBuffers(type, count, 2);
        buffers.add(validity(allocator, imported, array, length));
        buffers.add(wrap(allocator, imported, buffer(array, 1), (length + 7) / 8, "values"));
        return;
      default: {
        int width = fixedWidth(type);
        expectBuffers(type, count, 2);
        buffers.add(validity(allocator, imported, array, length));
        buffers.add(wrap(allocator, imported, buffer(array, 1), width * length, "values"));
      }
    }
  }

  /** The width in bytes of the values of a fixed-width type. */
  private static int fixedWidth(ArrowType type) {
    switch (type.getTypeID()) {
      case Int: return ((ArrowType.Int) type).getBitWidth() / 8;
      case FloatingPoint:
        switch (((ArrowType.FloatingPoint) type).getPrecision()) {
          case HALF: return 2;
          case SINGLE: return 4;
          case DOUBLE: return 8;
          default: break;
        }
        break;
      case Decimal: return ((ArrowType.Decimal) type).getBitWidth() / 8;
      case Date: return ((ArrowType.Date) type).getUnit() == DateUnit.DAY ? 4 : 8;
      case Time: return ((ArrowType.Time) type).getBitWidth() / 8;
      case Timestamp:
      case Duration:
        return 8;
      case Interval:
        switch (((ArrowType.Interval) type).getUnit()) {
          case YEAR_MONTH: return 4;
          case DAY_TIME: return 8;
          case MONTH_DAY_NANO: return 16;
          default: break;
        }
        break;
      case FixedSizeBinary: return ((ArrowType.FixedSizeBinary) type).getByteWidth();
      default: break;
    }
    throw new UnsupportedOperationException("Arrays of the Arrow type " + type + " cannot be " +
      "imported.");
  }

  private static void expectBuffers(ArrowType type, long count, int expected) {
    if (count != expected) {
      throw new IllegalArgumentException("The array of type " + type + " has " + count +
        " buffers, but " + expected + " are expected.");
    }
  }

  /** The validity buffer, or null if it is NULL, which Arrow Java loads as all valid or null. */
  private static ArrowBuf validity(
      BufferAllocator allocator, Imported imported, long array, long length) {
    long address = buffer(array, 0);
    return address == 0 ? null : wrap(allocator, imported, address, (length + 7) / 8, "validity");
  }

  /** The offsets buffer, which may be NULL in an empty array. */
  private static ArrowBuf offsets(
      BufferAllocator allocator, Imported imported, long array, long length, int width) {
    long address = buffer(array, 1);
    long size = width * (length + 1);
    if (address == 0 && length == 0) {
      ArrowBuf offsets = allocator.buffer(size);
      offsets.setZero(0, size);
      return offsets;
    }
    return wrap(allocator, imported, address, size, "offsets");
  }

  /** Wraps a buffer of an imported array, which keeps the array alive until it is released. */
  private static ArrowBuf wrap(
      BufferAllocator allocator, Imported imported, long address, long size, String buffer) {
    if (size < 0) {
      throw new IllegalArgumentException("The " + buffer + " buffer has a negative size.");
    }
    if (size == 0) {
      return allocator.getEmpty();
    }
    if (address == 0) {
      throw new IllegalArgumentException("The " + buffer + " buffer is NULL.");
    }
    imported.retain();
    try {
      return allocator.wrapForeignAllocation(new ForeignAllocation(size, address) {
        @Override
        protected void release0() {
          imported.release();
        }
      });
    } catch (Throwable e) {
      imported.release();
      throw e;
    }
  }

  // ---------------------------------------------------------------------------------------------
  // Strings
  // ---------------------------------------------------------------------------------------------

  /** Reads a NUL-terminated UTF-8 string. */
  static String readString(long address) {
    int length = 0;
    while (Platform.getByte(null, address + length) != 0) {
      length++;
    }
    byte[] bytes = new byte[length];
    Platform.copyMemory(null, address, bytes, Platform.BYTE_ARRAY_OFFSET, length);
    return new String(bytes, StandardCharsets.UTF_8);
  }
}
