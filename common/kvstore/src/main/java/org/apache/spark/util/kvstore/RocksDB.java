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

import java.io.File;
import java.io.IOException;
import java.lang.ref.Reference;
import java.lang.ref.WeakReference;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Collectors;

import static java.nio.charset.StandardCharsets.UTF_8;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import com.google.common.base.Throwables;
import org.rocksdb.*;

import org.apache.spark.annotation.Private;

/**
 * Implementation of KVStore that uses RocksDB as the underlying data store.
 */
@Private
public class RocksDB implements KVStore {

  static {
    org.rocksdb.RocksDB.loadLibrary();
  }

  @VisibleForTesting
  static final long STORE_VERSION = 1L;

  @VisibleForTesting
  static final byte[] STORE_VERSION_KEY = "__version__".getBytes(UTF_8);

  /** DB key where app metadata is stored. */
  private static final byte[] METADATA_KEY = "__meta__".getBytes(UTF_8);

  /** DB key where type aliases are stored. */
  private static final byte[] TYPE_ALIASES_KEY = "__types__".getBytes(UTF_8);

  /**
   * Use full filter.
   *
   * https://github.com/facebook/rocksdb/wiki/RocksDB-Bloom-Filter#full-filters-new-format
   */
  private static final BloomFilter fullFilter =
    new BloomFilter(10.0D /* BloomFilter.DEFAULT_BITS_PER_KEY */, false);

  /** Disable compression in index data. */
  private static final BlockBasedTableConfig tableFormatConfig = new BlockBasedTableConfig()
    .setFilterPolicy(fullFilter)
    .setEnableIndexCompression(false)
    .setIndexBlockRestartInterval(8)
    .setFormatVersion(5);

  /**
   * - Use ZSTD at the bottom most level to reduce the disk space
   * - Use LZ4 at the other levels because it's better than Snappy in general.
   *
   * https://github.com/facebook/rocksdb/wiki/Compression#configuration
   */
  private static final Options dbOptions = new Options()
    .setCreateIfMissing(true)
    .setBottommostCompressionType(CompressionType.ZSTD_COMPRESSION)
    .setCompressionType(CompressionType.LZ4_COMPRESSION)
    .setTableFormatConfig(tableFormatConfig);

  /**
   * - Use explicitly 'sync = false' like LevelDB KVStore implementation.
   *
   * https://github.com/google/leveldb/blob/1.23/include/leveldb/options.h#L182
   */
  private static final WriteOptions writeOptions = new WriteOptions().setSync(false);

  private final AtomicReference<org.rocksdb.RocksDB> _db;

  final KVStoreSerializer serializer;

  /**
   * Keep a mapping of class names to a shorter, unique ID managed by the store. This serves two
   * purposes: make the keys stored on disk shorter, and spread out the keys, since class names
   * will often have a long, redundant prefix (think "org.apache.spark.").
   */
  private final ConcurrentMap<String, byte[]> typeAliases;
  private final ConcurrentMap<Class<?>, RocksDBTypeInfo> types;

  /**
   * Trying to close a JNI RocksDB handle with a closed DB causes JVM crashes. This is used to
   * ensure that all iterators are correctly closed before RocksDB is closed. Use weak references
   * to ensure that the iterator can be GCed, when it is only referenced here.
   */
  private final ConcurrentLinkedQueue<Reference<RocksDBIterator<?>>> iteratorTracker;

  public RocksDB(File path) throws Exception {
    this(path, new KVStoreSerializer());
  }

  public RocksDB(File path, KVStoreSerializer serializer) throws Exception {
    this.serializer = serializer;
    this.types = new ConcurrentHashMap<>();
    this._db = new AtomicReference<>(org.rocksdb.RocksDB.open(dbOptions, path.toString()));

    byte[] versionData = db().get(STORE_VERSION_KEY);
    if (versionData != null) {
      long version = serializer.deserializeLong(versionData);
      if (version != STORE_VERSION) {
        close();
        throw new UnsupportedStoreVersionException();
      }
    } else {
      db().put(STORE_VERSION_KEY, serializer.serialize(STORE_VERSION));
    }

    Map<String, byte[]> aliases;
    try {
      aliases = get(TYPE_ALIASES_KEY, TypeAliases.class).aliases;
    } catch (NoSuchElementException e) {
      aliases = new HashMap<>();
    }
    typeAliases = new ConcurrentHashMap<>(aliases);

    iteratorTracker = new ConcurrentLinkedQueue<>();
  }

  @Override
  public <T> T getMetadata(Class<T> klass) throws Exception {
    try {
      return get(METADATA_KEY, klass);
    } catch (NoSuchElementException nsee) {
      return null;
    }
  }

  @Override
  public void setMetadata(Object value) throws Exception {
    if (value != null) {
      put(METADATA_KEY, value);
    } else {
      db().delete(METADATA_KEY);
    }
  }

  <T> T get(byte[] key, Class<T> klass) throws Exception {
    byte[] data = db().get(key);
    if (data == null) {
      throw new NoSuchElementException(new String(key, UTF_8));
    }
    return serializer.deserialize(data, klass);
  }

  private void put(byte[] key, Object value) throws Exception {
    Preconditions.checkArgument(value != null, "Null values are not allowed.");
    db().put(key, serializer.serialize(value));
  }

  @Override
  public <T> T read(Class<T> klass, Object naturalKey) throws Exception {
    Preconditions.checkArgument(naturalKey != null, "Null keys are not allowed.");
    byte[] key = getTypeInfo(klass).naturalIndex().start(null, naturalKey);
    return get(key, klass);
  }

  @Override
  public void write(Object value) throws Exception {
    Preconditions.checkArgument(value != null, "Null values are not allowed.");
    RocksDBTypeInfo ti = getTypeInfo(value.getClass());
    byte[] data = serializer.serialize(value);
    synchronized (ti) {
      try (WriteBatch writeBatch = new WriteBatch()) {
        updateBatch(writeBatch, value, data, value.getClass(), ti.naturalIndex(), ti.indices());
        db().write(writeOptions, writeBatch);
      }
    }
  }

  public void writeAll(List<?> values) throws Exception {
    Preconditions.checkArgument(values != null && !values.isEmpty(),
      "Non-empty values required.");

    // Group by class, in case there are values from different classes in the values
    // Typical usecase is for this to be a single class.
    // A NullPointerException will be thrown if values contain null object.
    for (Map.Entry<? extends Class<?>, ? extends List<?>> entry :
        values.stream().collect(Collectors.groupingBy(Object::getClass)).entrySet()) {

      final Iterator<?> valueIter = entry.getValue().iterator();
      final Iterator<byte[]> serializedValueIter;

      // Deserialize outside synchronized block
      List<byte[]> list = new ArrayList<>(entry.getValue().size());
      for (Object value : values) {
        list.add(serializer.serialize(value));
      }
      serializedValueIter = list.iterator();

      final Class<?> klass = entry.getKey();
      final RocksDBTypeInfo ti = getTypeInfo(klass);

      synchronized (ti) {
        final RocksDBTypeInfo.Index naturalIndex = ti.naturalIndex();
        final Collection<RocksDBTypeInfo.Index> indices = ti.indices();

        try (WriteBatch writeBatch = new WriteBatch()) {
          while (valueIter.hasNext()) {
            updateBatch(writeBatch, valueIter.next(), serializedValueIter.next(), klass,
                naturalIndex, indices);
          }
          db().write(writeOptions, writeBatch);
        }
      }
    }
  }

  private void updateBatch(
      WriteBatch batch,
      Object value,
      byte[] data,
      Class<?> klass,
      RocksDBTypeInfo.Index naturalIndex,
      Collection<RocksDBTypeInfo.Index> indices) throws Exception {
    Object existing;
    try {
      existing = get(naturalIndex.entityKey(null, value), klass);
    } catch (NoSuchElementException e) {
      existing = null;
    }

    PrefixCache cache = new PrefixCache(value);
    byte[] naturalKey = naturalIndex.toKey(naturalIndex.getValue(value));
    for (RocksDBTypeInfo.Index idx : indices) {
      byte[] prefix = cache.getPrefix(idx);
      idx.add(batch, value, existing, data, naturalKey, prefix);
    }
  }

  @Override
  public void delete(Class<?> type, Object naturalKey) throws Exception {
    Preconditions.checkArgument(naturalKey != null, "Null keys are not allowed.");
    try (WriteBatch writeBatch = new WriteBatch()) {
      RocksDBTypeInfo ti = getTypeInfo(type);
      byte[] key = ti.naturalIndex().start(null, naturalKey);
      synchronized (ti) {
        byte[] data = db().get(key);
        if (data != null) {
          Object existing = serializer.deserialize(data, type);
          PrefixCache cache = new PrefixCache(existing);
          byte[] keyBytes = ti.naturalIndex().toKey(ti.naturalIndex().getValue(existing));
          for (RocksDBTypeInfo.Index idx : ti.indices()) {
            idx.remove(writeBatch, existing, keyBytes, cache.getPrefix(idx));
          }
          db().write(writeOptions, writeBatch);
        }
      }
    } catch (NoSuchElementException nse) {
      // Ignore.
    }
  }

  @Override
  public <T> KVStoreView<T> view(Class<T> type) throws Exception {
    return new KVStoreView<T>() {
      @Override
      public Iterator<T> iterator() {
        try {
          RocksDBIterator<T> it = new RocksDBIterator<>(type, RocksDB.this, this);
          iteratorTracker.add(new WeakReference<>(it));
          return it;
        } catch (Exception e) {
          Throwables.throwIfUnchecked(e);
          throw new RuntimeException(e);
        }
      }
    };
  }

  @Override
  public <T> boolean removeAllByIndexValues(
      Class<T> klass,
      String index,
      Collection<?> indexValues) throws Exception {
    RocksDBTypeInfo.Index naturalIndex = getTypeInfo(klass).naturalIndex();
    boolean removed = false;
    KVStoreView<T> view = view(klass).index(index);

    for (Object indexValue : indexValues) {
      try (KVStoreIterator<T> iterator =
        view.first(indexValue).last(indexValue).closeableIterator()) {
        while (iterator.hasNext()) {
          T value = iterator.next();
          Object itemKey = naturalIndex.getValue(value);
          delete(klass, itemKey);
          removed = true;
        }
      }
    }

    return removed;
  }

  @Override
  public long count(Class<?> type) throws Exception {
    RocksDBTypeInfo.Index idx = getTypeInfo(type).naturalIndex();
    return idx.getCount(idx.end(null));
  }

  @Override
  public long count(Class<?> type, String index, Object indexedValue) throws Exception {
    RocksDBTypeInfo.Index idx = getTypeInfo(type).index(index);
    return idx.getCount(idx.end(null, indexedValue));
  }

  @Override
  public void close() throws IOException {
    synchronized (this._db) {
      org.rocksdb.RocksDB _db = this._db.getAndSet(null);
      if (_db == null) {
        return;
      }

      try {
        if (iteratorTracker != null) {
          for (Reference<RocksDBIterator<?>> ref: iteratorTracker) {
            RocksDBIterator<?> it = ref.get();
            if (it != null) {
              it.close();
            }
          }
        }
        _db.close();
      } catch (IOException ioe) {
        throw ioe;
      } catch (Exception e) {
        throw new IOException(e.getMessage(), e);
      }
    }
  }

  /**
   * Closes the given iterator if the DB is still open. Trying to close a JNI RocksDB handle
   * with a closed DB can cause JVM crashes, so this ensures that situation does not happen.
   */
  void closeIterator(RocksIterator it) {
    notifyIteratorClosed(it);
    synchronized (this._db) {
      org.rocksdb.RocksDB _db = this._db.get();
      if (_db != null) {
        it.close();
      }
    }
  }

  /**
   * Remove iterator from iterator tracker. `RocksDBIterator` calls it to notify
   * iterator is closed.
   */
  void notifyIteratorClosed(RocksIterator rocksIterator) {
    iteratorTracker.removeIf(ref -> {
      RocksDBIterator<?> rocksDBIterator = ref.get();
      return rocksDBIterator != null && rocksIterator.equals(rocksDBIterator.internalIterator());
    });
  }

  /** Returns metadata about indices for the given type. */
  RocksDBTypeInfo getTypeInfo(Class<?> type) throws Exception {
    RocksDBTypeInfo ti = types.get(type);
    if (ti == null) {
      RocksDBTypeInfo tmp = new RocksDBTypeInfo(this, type, getTypeAlias(type));
      ti = types.putIfAbsent(type, tmp);
      if (ti == null) {
        ti = tmp;
      }
    }
    return ti;
  }

  /**
   * Try to avoid use-after close since that has the tendency of crashing the JVM. This doesn't
   * prevent methods that retrieved the instance from using it after close, but hopefully will
   * catch most cases; otherwise, we'll need some kind of locking.
   */
  org.rocksdb.RocksDB db() {
    org.rocksdb.RocksDB _db = this._db.get();
    if (_db == null) {
      throw new IllegalStateException("DB is closed.");
    }
    return _db;
  }

  private byte[] getTypeAlias(Class<?> klass) throws Exception {
    byte[] alias = typeAliases.get(klass.getName());
    if (alias == null) {
      synchronized (typeAliases) {
        byte[] tmp = String.valueOf(typeAliases.size()).getBytes(UTF_8);
        alias = typeAliases.putIfAbsent(klass.getName(), tmp);
        if (alias == null) {
          alias = tmp;
          put(TYPE_ALIASES_KEY, new TypeAliases(typeAliases));
        }
      }
    }
    return alias;
  }

  /** Needs to be public for Jackson. */
  public static class TypeAliases {

    public Map<String, byte[]> aliases;

    TypeAliases(Map<String, byte[]> aliases) {
      this.aliases = aliases;
    }

    TypeAliases() {
      this(null);
    }

  }

  private static class PrefixCache {

    private final Object entity;
    private final Map<RocksDBTypeInfo.Index, byte[]> prefixes;

    PrefixCache(Object entity) {
      this.entity = entity;
      this.prefixes = new HashMap<>();
    }

    byte[] getPrefix(RocksDBTypeInfo.Index idx) throws Exception {
      byte[] prefix = null;
      if (idx.isChild()) {
        prefix = prefixes.get(idx.parent());
        if (prefix == null) {
          prefix = idx.parent().childPrefix(idx.parent().getValue(entity));
          prefixes.put(idx.parent(), prefix);
        }
      }
      return prefix;
    }

  }

}
