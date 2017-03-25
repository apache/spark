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

package org.apache.spark.kvstore;

import java.lang.reflect.Array;
import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import static java.nio.charset.StandardCharsets.UTF_8;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import com.google.common.base.Throwables;

/**
 * Holds metadata about app-specific types stored in LevelDB. Serves as a cache for data collected
 * via reflection, to make it cheaper to access it multiple times.
 */
class LevelDBTypeInfo<T> {

  static final String ENTRY_PREFIX = "+";
  static final String END_MARKER = "-";
  static final byte KEY_SEPARATOR = 0x0;

  // These constants are used in the Index.toKey() method below when encoding numbers into keys.
  // See javadoc for that method for details.
  private static final char POSITIVE_FILL = '.';
  private static final char NEGATIVE_FILL = '~';
  private static final char POSITIVE_MARKER = '=';
  private static final char NEGATIVE_MARKER = '*';

  @VisibleForTesting
  static final int BYTE_ENCODED_LEN = String.valueOf(Byte.MAX_VALUE).length() + 1;
  @VisibleForTesting
  static final int INT_ENCODED_LEN = String.valueOf(Integer.MAX_VALUE).length() + 1;
  @VisibleForTesting
  static final int LONG_ENCODED_LEN = String.valueOf(Long.MAX_VALUE).length() + 1;
  @VisibleForTesting
  static final int SHORT_ENCODED_LEN = String.valueOf(Short.MAX_VALUE).length() + 1;

  private final LevelDB db;
  private final Class<T> type;
  private final Map<String, Index> indices;
  private final byte[] typePrefix;

  LevelDBTypeInfo(LevelDB db, Class<T> type, byte[] alias) throws Exception {
    this.db = db;
    this.type = type;
    this.indices = new HashMap<>();

    for (Field f : type.getFields()) {
      KVIndex idx = f.getAnnotation(KVIndex.class);
      if (idx != null) {
        register(idx, new FieldAccessor(f));
      }
    }

    for (Method m : type.getMethods()) {
      KVIndex idx = m.getAnnotation(KVIndex.class);
      if (idx != null) {
        Preconditions.checkArgument(m.getParameterTypes().length == 0,
          "Annotated method %s::%s should not have any parameters.", type.getName(), m.getName());
        register(idx, new MethodAccessor(m));
      }
    }

    Preconditions.checkArgument(indices.get(KVIndex.NATURAL_INDEX_NAME) != null,
        "No natural index defined for type %s.", type.getName());

    ByteArrayOutputStream typePrefix = new ByteArrayOutputStream();
    typePrefix.write(utf8(ENTRY_PREFIX));
    typePrefix.write(alias);
    this.typePrefix = typePrefix.toByteArray();
  }

  private void register(KVIndex idx, Accessor accessor) {
    Preconditions.checkArgument(idx.value() != null && !idx.value().isEmpty(),
      "No name provided for index in type %s.", type.getName());
    Preconditions.checkArgument(
      !idx.value().startsWith("_") || idx.value().equals(KVIndex.NATURAL_INDEX_NAME),
      "Index name %s (in type %s) is not allowed.", idx.value(), type.getName());
    Preconditions.checkArgument(indices.get(idx.value()) == null,
      "Duplicate index %s for type %s.", idx.value(), type.getName());
    indices.put(idx.value(), new Index(idx.value(), idx.copy(), accessor));
  }

  Class<T> type() {
    return type;
  }

  byte[] keyPrefix() {
    return buildKey(false);
  }

  Index naturalIndex() {
    return index(KVIndex.NATURAL_INDEX_NAME);
  }

  Index index(String name) {
    Index i = indices.get(name);
    Preconditions.checkArgument(i != null, "Index %s does not exist for type %s.", name,
      type.getName());
    return i;
  }

  Collection<Index> indices() {
    return indices.values();
  }

  private byte[] utf8(String s) {
    return s.getBytes(UTF_8);
  }

  private byte[] buildKey(boolean trim, String... components) {
    try {
      ByteArrayOutputStream kos = new ByteArrayOutputStream(typePrefix.length * 2);
      kos.write(typePrefix);
      for (int i = 0; i < components.length; i++) {
        kos.write(utf8(components[i]));
        if (!trim || i < components.length - 1) {
          kos.write(KEY_SEPARATOR);
        }
      }
      return kos.toByteArray();
    } catch (IOException ioe) {
      throw Throwables.propagate(ioe);
    }
  }

  /**
   * Models a single index in LevelDB. Keys are stored under the type's prefix, in sequential
   * order according to the indexed value. For non-natural indices, the key also contains the
   * entity's natural key after the indexed value, so that it's possible for multiple entities
   * to have the same indexed value.
   *
   * <p>
   * An end marker is used to mark where the index ends, and the boundaries of each indexed value
   * within the index, to make descending iteration faster, at the expense of some disk space and
   * minor overhead when iterating. A count of the number of indexed entities is kept at the end
   * marker, so that it can be cleaned up when all entries are removed from the index.
   * </p>
   */
  class Index {

    private final boolean copy;
    private final boolean isNatural;
    private final String name;

    @VisibleForTesting
    final Accessor accessor;

    private Index(String name, boolean copy, Accessor accessor) {
      this.name = name;
      this.isNatural = name.equals(KVIndex.NATURAL_INDEX_NAME);
      this.copy = isNatural || copy;
      this.accessor = accessor;
    }

    boolean isCopy() {
      return copy;
    }

    /** The prefix for all keys that belong to this index. */
    byte[] keyPrefix() {
      return buildKey(false, name);
    }

    /** The key where to start ascending iteration for entries that match the given value. */
    byte[] start(Object value) {
      return buildKey(isNatural, name, toKey(value));
    }

    /** The key for the index's end marker. */
    byte[] end() {
      return buildKey(true, name, END_MARKER);
    }

    /** The key for the end marker for index entries with the given value. */
    byte[] end(Object value) throws Exception {
      return buildKey(true, name, toKey(value), END_MARKER);
    }

    /** The key in the index that identifies the given entity. */
    byte[] entityKey(Object entity) throws Exception {
      Object indexValue = accessor.get(entity);
      Preconditions.checkNotNull(indexValue, "Null index value for %s in type %s.",
        name, type.getName());
      if (isNatural) {
        return buildKey(true, name, toKey(indexValue));
      } else {
        Object naturalKey = naturalIndex().accessor.get(entity);
        return buildKey(true, name, toKey(accessor.get(entity)), toKey(naturalKey));
      }
    }

    /**
     * Add an entry to the index.
     *
     * @param batch Write batch with other related changes.
     * @param entity The entity being added to the index.
     * @param data Serialized entity to store (when storing the entity, not a reference).
     * @param naturalKey The value's key.
     */
    void add(LevelDBWriteBatch batch, Object entity, byte[] data) throws Exception {
      byte[] stored = data;
      if (!copy) {
        stored = db.serializer.serialize(toKey(naturalIndex().accessor.get(entity)));
      }
      batch.put(entityKey(entity), stored);
      batch.updateCount(end(accessor.get(entity)), 1L);
      batch.updateCount(end(), 1L);
    }

    /**
     * Remove a value from the index.
     *
     * @param batch Write batch with other related changes.
     * @param entity The entity being removed, to identify the index entry to modify.
     * @param naturalKey The value's key.
     */
    void remove(LevelDBWriteBatch batch, Object entity) throws Exception {
      batch.delete(entityKey(entity));
      batch.updateCount(end(accessor.get(entity)), -1L);
      batch.updateCount(end(), -1L);
    }

    long getCount(byte[] key) throws Exception {
      byte[] data = db.db().get(key);
      return data != null ? db.serializer.deserializeLong(data) : 0;
    }

    /**
     * Translates a value to be used as part of the store key.
     *
     * Integral numbers are encoded as a string in a way that preserves lexicographical
     * ordering. The string is always as long as the maximum value for the given type (e.g.
     * 11 characters for integers, including the character for the sign). The first character
     * represents the sign (with the character for negative coming before the one for positive,
     * which means you cannot use '-'...). The rest of the value is padded with a value that is
     * "greater than 9" for negative values, so that for example "-123" comes before "-12" (the
     * encoded value would look like "*~~~~~~~123"). For positive values, similarly, a value that
     * is "lower than 0" (".") is used for padding. The fill characters were chosen for readability
     * when looking at the encoded keys.
     *
     * Arrays are encoded by encoding each element separately, separated by KEY_SEPARATOR.
     */
    @VisibleForTesting
    String toKey(Object value) {
      StringBuilder sb = new StringBuilder(ENTRY_PREFIX);

      if (value instanceof String) {
        sb.append(value);
      } else if (value instanceof Boolean) {
        sb.append(((Boolean) value).toString().toLowerCase());
      } else if (value.getClass().isArray()) {
        int length = Array.getLength(value);
        for (int i = 0; i < length; i++) {
          sb.append(toKey(Array.get(value, i)));
          sb.append(KEY_SEPARATOR);
        }
        if (length > 0) {
          sb.setLength(sb.length() - 1);
        }
      } else {
        int encodedLen;

        if (value instanceof Integer) {
          encodedLen = INT_ENCODED_LEN;
        } else if (value instanceof Long) {
          encodedLen = LONG_ENCODED_LEN;
        } else if (value instanceof Short) {
          encodedLen = SHORT_ENCODED_LEN;
        } else if (value instanceof Byte) {
          encodedLen = BYTE_ENCODED_LEN;
        } else {
          throw new IllegalArgumentException(String.format("Type %s not allowed as key.",
            value.getClass().getName()));
        }

        long longValue = ((Number) value).longValue();
        String strVal;
        if (longValue == Long.MIN_VALUE) {
          // Math.abs() overflows for Long.MIN_VALUE.
          strVal = String.valueOf(longValue).substring(1);
        } else {
          strVal = String.valueOf(Math.abs(longValue));
        }

        sb.append(longValue >= 0 ? POSITIVE_MARKER : NEGATIVE_MARKER);

        char fill = longValue >= 0 ? POSITIVE_FILL : NEGATIVE_FILL;
        for (int i = 0; i < encodedLen - strVal.length() - 1; i++) {
          sb.append(fill);
        }

        sb.append(strVal);
      }

      return sb.toString();
    }

  }

  /**
   * Abstracts the difference between invoking a Field and a Method.
   */
  @VisibleForTesting
  interface Accessor {

    Object get(Object instance) throws Exception;

  }

  private class FieldAccessor implements Accessor {

    private final Field field;

    FieldAccessor(Field field) {
      this.field = field;
    }

    @Override
    public Object get(Object instance) throws Exception {
      return field.get(instance);
    }

  }

  private class MethodAccessor implements Accessor {

    private final Method method;

    MethodAccessor(Method method) {
      this.method = method;
    }

    @Override
    public Object get(Object instance) throws Exception {
      return method.invoke(instance);
    }

  }

}
