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

package org.apache.spark.util.kvstore;

import java.lang.reflect.Array;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import static java.nio.charset.StandardCharsets.UTF_8;

import com.google.common.base.Preconditions;
import org.iq80.leveldb.WriteBatch;

/**
 * Holds metadata about app-specific types stored in LevelDB. Serves as a cache for data collected
 * via reflection, to make it cheaper to access it multiple times.
 *
 * <p>
 * The hierarchy of keys stored in LevelDB looks roughly like the following. This hierarchy ensures
 * that iteration over indices is easy, and that updating values in the store is not overly
 * expensive. Of note, indices choose using more disk space (one value per key) instead of keeping
 * lists of pointers, which would be more expensive to update at runtime.
 * </p>
 *
 * <p>
 * Indentation defines when a sub-key lives under a parent key. In LevelDB, this means the full
 * key would be the concatenation of everything up to that point in the hierarchy, with each
 * component separated by a NULL byte.
 * </p>
 *
 * <pre>
 * +TYPE_NAME
 *   NATURAL_INDEX
 *     +NATURAL_KEY
 *     -
 *   -NATURAL_INDEX
 *   INDEX_NAME
 *     +INDEX_VALUE
 *       +NATURAL_KEY
 *     -INDEX_VALUE
 *     .INDEX_VALUE
 *       CHILD_INDEX_NAME
 *         +CHILD_INDEX_VALUE
 *           NATURAL_KEY_OR_DATA
 *         -
 *   -INDEX_NAME
 * </pre>
 *
 * <p>
 * Entity data (either the entity's natural key or a copy of the data) is stored in all keys
 * that end with "+<something>". A count of all objects that match a particular top-level index
 * value is kept at the end marker ("-<something>"). A count is also kept at the natural index's end
 * marker, to make it easy to retrieve the number of all elements of a particular type.
 * </p>
 *
 * <p>
 * To illustrate, given a type "Foo", with a natural index and a second index called "bar", you'd
 * have these keys and values in the store for two instances, one with natural key "key1" and the
 * other "key2", both with value "yes" for "bar":
 * </p>
 *
 * <pre>
 * Foo __main__ +key1   [data for instance 1]
 * Foo __main__ +key2   [data for instance 2]
 * Foo __main__ -       [count of all Foo]
 * Foo bar +yes +key1   [instance 1 key or data, depending on index type]
 * Foo bar +yes +key2   [instance 2 key or data, depending on index type]
 * Foo bar +yes -       [count of all Foo with "bar=yes" ]
 * </pre>
 *
 * <p>
 * Note that all indexed values are prepended with "+", even if the index itself does not have an
 * explicit end marker. This allows for easily skipping to the end of an index by telling LevelDB
 * to seek to the "phantom" end marker of the index. Throughout the code and comments, this part
 * of the full LevelDB key is generally referred to as the "index value" of the entity.
 * </p>
 *
 * <p>
 * Child indices are stored after their parent index. In the example above, let's assume there is
 * a child index "child", whose parent is "bar". If both instances have value "no" for this field,
 * the data in the store would look something like the following:
 * </p>
 *
 * <pre>
 * ...
 * Foo bar +yes -
 * Foo bar .yes .child +no +key1   [instance 1 key or data, depending on index type]
 * Foo bar .yes .child +no +key2   [instance 2 key or data, depending on index type]
 * ...
 * </pre>
 */
class LevelDBTypeInfo {

  static final byte[] END_MARKER = new byte[] { '-' };
  static final byte ENTRY_PREFIX = (byte) '+';
  static final byte KEY_SEPARATOR = 0x0;
  static byte TRUE = (byte) '1';
  static byte FALSE = (byte) '0';

  private static final byte SECONDARY_IDX_PREFIX = (byte) '.';
  private static final byte POSITIVE_MARKER = (byte) '=';
  private static final byte NEGATIVE_MARKER = (byte) '*';
  private static final byte[] HEX_BYTES = new byte[] {
    '0', '1', '2', '3', '4', '5', '6', '7', '8', '9', 'a', 'b', 'c', 'd', 'e', 'f'
  };

  private final LevelDB db;
  private final Class<?> type;
  private final Map<String, Index> indices;
  private final byte[] typePrefix;

  LevelDBTypeInfo(LevelDB db, Class<?> type, byte[] alias) throws Exception {
    this.db = db;
    this.type = type;
    this.indices = new HashMap<>();

    KVTypeInfo ti = new KVTypeInfo(type);

    // First create the parent indices, then the child indices.
    ti.indices().forEach(idx -> {
      // In LevelDB, there is no parent index for the NATURAL INDEX.
      if (idx.parent().isEmpty() || idx.value().equals(KVIndex.NATURAL_INDEX_NAME)) {
        indices.put(idx.value(), new Index(idx, ti.getAccessor(idx.value()), null));
      }
    });
    ti.indices().forEach(idx -> {
      if (!idx.parent().isEmpty() && !idx.value().equals(KVIndex.NATURAL_INDEX_NAME)) {
        indices.put(idx.value(), new Index(idx, ti.getAccessor(idx.value()),
          indices.get(idx.parent())));
      }
    });

    this.typePrefix = alias;
  }

  Class<?> type() {
    return type;
  }

  byte[] keyPrefix() {
    return typePrefix;
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

  byte[] buildKey(byte[]... components) {
    return buildKey(true, components);
  }

  byte[] buildKey(boolean addTypePrefix, byte[]... components) {
    int len = 0;
    if (addTypePrefix) {
      len += typePrefix.length + 1;
    }
    for (byte[] comp : components) {
      len += comp.length;
    }
    len += components.length - 1;

    byte[] dest = new byte[len];
    int written = 0;

    if (addTypePrefix) {
      System.arraycopy(typePrefix, 0, dest, 0, typePrefix.length);
      dest[typePrefix.length] = KEY_SEPARATOR;
      written += typePrefix.length + 1;
    }

    for (byte[] comp : components) {
      System.arraycopy(comp, 0, dest, written, comp.length);
      written += comp.length;
      if (written < dest.length) {
        dest[written] = KEY_SEPARATOR;
        written++;
      }
    }

    return dest;
  }

  /**
   * Models a single index in LevelDB. See top-level class's javadoc for a description of how the
   * keys are generated.
   */
  class Index {

    private final boolean copy;
    private final boolean isNatural;
    private final byte[] name;
    private final KVTypeInfo.Accessor accessor;
    private final Index parent;

    private Index(KVIndex self, KVTypeInfo.Accessor accessor, Index parent) {
      byte[] name = self.value().getBytes(UTF_8);
      if (parent != null) {
        byte[] child = new byte[name.length + 1];
        child[0] = SECONDARY_IDX_PREFIX;
        System.arraycopy(name, 0, child, 1, name.length);
      }

      this.name = name;
      this.isNatural = self.value().equals(KVIndex.NATURAL_INDEX_NAME);
      this.copy = isNatural || self.copy();
      this.accessor = accessor;
      this.parent = parent;
    }

    boolean isCopy() {
      return copy;
    }

    boolean isChild() {
      return parent != null;
    }

    Index parent() {
      return parent;
    }

    /**
     * Creates a key prefix for child indices of this index. This allows the prefix to be
     * calculated only once, avoiding redundant work when multiple child indices of the
     * same parent index exist.
     */
    byte[] childPrefix(Object value) {
      Preconditions.checkState(parent == null, "Not a parent index.");
      return buildKey(name, toParentKey(value));
    }

    /**
     * Gets the index value for a particular entity (which is the value of the field or method
     * tagged with the index annotation). This is used as part of the LevelDB key where the
     * entity (or its id) is stored.
     */
    Object getValue(Object entity) throws Exception {
      return accessor.get(entity);
    }

    private void checkParent(byte[] prefix) {
      if (prefix != null) {
        Preconditions.checkState(parent != null, "Parent prefix provided for parent index.");
      } else {
        Preconditions.checkState(parent == null, "Parent prefix missing for child index.");
      }
    }

    /** The prefix for all keys that belong to this index. */
    byte[] keyPrefix(byte[] prefix) {
      checkParent(prefix);
      return (parent != null) ? buildKey(false, prefix, name) : buildKey(name);
    }

    /**
     * The key where to start ascending iteration for entities whose value for the indexed field
     * match the given value.
     */
    byte[] start(byte[] prefix, Object value) {
      checkParent(prefix);
      return (parent != null) ? buildKey(false, prefix, name, toKey(value))
        : buildKey(name, toKey(value));
    }

    /** The key for the index's end marker. */
    byte[] end(byte[] prefix) {
      checkParent(prefix);
      return (parent != null) ? buildKey(false, prefix, name, END_MARKER)
        : buildKey(name, END_MARKER);
    }

    /** The key for the end marker for entries with the given value. */
    byte[] end(byte[] prefix, Object value) {
      checkParent(prefix);
      return (parent != null) ? buildKey(false, prefix, name, toKey(value), END_MARKER)
        : buildKey(name, toKey(value), END_MARKER);
    }

    /** The full key in the index that identifies the given entity. */
    byte[] entityKey(byte[] prefix, Object entity) throws Exception {
      Object indexValue = getValue(entity);
      Preconditions.checkNotNull(indexValue, "Null index value for %s in type %s.",
        name, type.getName());
      byte[] entityKey = start(prefix, indexValue);
      if (!isNatural) {
        entityKey = buildKey(false, entityKey, toKey(naturalIndex().getValue(entity)));
      }
      return entityKey;
    }

    private void updateCount(WriteBatch batch, byte[] key, long delta) {
      long updated = getCount(key) + delta;
      if (updated > 0) {
        batch.put(key, db.serializer.serialize(updated));
      } else {
        batch.delete(key);
      }
    }

    private void addOrRemove(
        WriteBatch batch,
        Object entity,
        Object existing,
        byte[] data,
        byte[] naturalKey,
        byte[] prefix) throws Exception {
      Object indexValue = getValue(entity);
      Preconditions.checkNotNull(indexValue, "Null index value for %s in type %s.",
        name, type.getName());

      byte[] entityKey = start(prefix, indexValue);
      if (!isNatural) {
        entityKey = buildKey(false, entityKey, naturalKey);
      }

      boolean needCountUpdate = (existing == null);

      // Check whether there's a need to update the index. The index needs to be updated in two
      // cases:
      //
      // - There is no existing value for the entity, so a new index value will be added.
      // - If there is a previously stored value for the entity, and the index value for the
      //   current index does not match the new value, the old entry needs to be deleted and
      //   the new one added.
      //
      // Natural indices don't need to be checked, because by definition both old and new entities
      // will have the same key. The put() call is all that's needed in that case.
      //
      // Also check whether we need to update the counts. If the indexed value is changing, we
      // need to decrement the count at the old index value, and the new indexed value count needs
      // to be incremented.
      if (existing != null && !isNatural) {
        byte[] oldPrefix = null;
        Object oldIndexedValue = getValue(existing);
        boolean removeExisting = !indexValue.equals(oldIndexedValue);
        if (!removeExisting && isChild()) {
          oldPrefix = parent().childPrefix(parent().getValue(existing));
          removeExisting = LevelDBIterator.compare(prefix, oldPrefix) != 0;
        }

        if (removeExisting) {
          if (oldPrefix == null && isChild()) {
            oldPrefix = parent().childPrefix(parent().getValue(existing));
          }

          byte[] oldKey = entityKey(oldPrefix, existing);
          batch.delete(oldKey);

          // If the indexed value has changed, we need to update the counts at the old and new
          // end markers for the indexed value.
          if (!isChild()) {
            byte[] oldCountKey = end(null, oldIndexedValue);
            updateCount(batch, oldCountKey, -1L);
            needCountUpdate = true;
          }
        }
      }

      if (data != null) {
        byte[] stored = copy ? data : naturalKey;
        batch.put(entityKey, stored);
      } else {
        batch.delete(entityKey);
      }

      if (needCountUpdate && !isChild()) {
        long delta = data != null ? 1L : -1L;
        byte[] countKey = isNatural ? end(prefix) : end(prefix, indexValue);
        updateCount(batch, countKey, delta);
      }
    }

    /**
     * Add an entry to the index.
     *
     * @param batch Write batch with other related changes.
     * @param entity The entity being added to the index.
     * @param existing The entity being replaced in the index, or null.
     * @param data Serialized entity to store (when storing the entity, not a reference).
     * @param naturalKey The value's natural key (to avoid re-computing it for every index).
     * @param prefix The parent index prefix, if this is a child index.
     */
    void add(
        WriteBatch batch,
        Object entity,
        Object existing,
        byte[] data,
        byte[] naturalKey,
        byte[] prefix) throws Exception {
      addOrRemove(batch, entity, existing, data, naturalKey, prefix);
    }

    /**
     * Remove a value from the index.
     *
     * @param batch Write batch with other related changes.
     * @param entity The entity being removed, to identify the index entry to modify.
     * @param naturalKey The value's natural key (to avoid re-computing it for every index).
     * @param prefix The parent index prefix, if this is a child index.
     */
    void remove(
        WriteBatch batch,
        Object entity,
        byte[] naturalKey,
        byte[] prefix) throws Exception {
      addOrRemove(batch, entity, null, null, naturalKey, prefix);
    }

    long getCount(byte[] key) {
      byte[] data = db.db().get(key);
      return data != null ? db.serializer.deserializeLong(data) : 0;
    }

    byte[] toParentKey(Object value) {
      return toKey(value, SECONDARY_IDX_PREFIX);
    }

    byte[] toKey(Object value) {
      return toKey(value, ENTRY_PREFIX);
    }

    /**
     * Translates a value to be used as part of the store key.
     *
     * Integral numbers are encoded as a string in a way that preserves lexicographical
     * ordering. The string is prepended with a marker telling whether the number is negative
     * or positive ("*" for negative and "=" for positive are used since "-" and "+" have the
     * opposite of the desired order), and then the number is encoded into a hex string (so
     * it occupies twice the number of bytes as the original type).
     *
     * Arrays are encoded by encoding each element separately, separated by KEY_SEPARATOR.
     */
    byte[] toKey(Object value, byte prefix) {
      final byte[] result;

      if (value instanceof String str) {
        byte[] bytes = str.getBytes(UTF_8);
        result = new byte[bytes.length + 1];
        result[0] = prefix;
        System.arraycopy(bytes, 0, result, 1, bytes.length);
      } else if (value instanceof Boolean bool) {
        result = new byte[] { prefix, bool ? TRUE : FALSE };
      } else if (value.getClass().isArray()) {
        int length = Array.getLength(value);
        byte[][] components = new byte[length][];
        for (int i = 0; i < length; i++) {
          components[i] = toKey(Array.get(value, i));
        }
        result = buildKey(false, components);
      } else {
        int bytes;

        if (value instanceof Integer) {
          bytes = Integer.SIZE;
        } else if (value instanceof Long) {
          bytes = Long.SIZE;
        } else if (value instanceof Short) {
          bytes = Short.SIZE;
        } else if (value instanceof Byte) {
          bytes = Byte.SIZE;
        } else {
          throw new IllegalArgumentException(String.format("Type %s not allowed as key.",
            value.getClass().getName()));
        }

        bytes = bytes / Byte.SIZE;

        byte[] key = new byte[bytes * 2 + 2];
        long longValue = ((Number) value).longValue();
        key[0] = prefix;
        key[1] = longValue >= 0 ? POSITIVE_MARKER : NEGATIVE_MARKER;

        for (int i = 0; i < key.length - 2; i++) {
          int masked = (int) ((longValue >>> (4 * i)) & 0xF);
          key[key.length - i - 1] = HEX_BYTES[masked];
        }

        result = key;
      }

      return result;
    }

  }

}
