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
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.AtomicReference;
import static java.nio.charset.StandardCharsets.UTF_8;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import com.google.common.base.Throwables;
import org.fusesource.leveldbjni.JniDBFactory;
import org.iq80.leveldb.DB;
import org.iq80.leveldb.Options;
import org.iq80.leveldb.WriteBatch;

import org.apache.spark.annotation.Private;

/**
 * Implementation of KVStore that uses LevelDB as the underlying data store.
 */
@Private
public class LevelDB implements KVStore {

  @VisibleForTesting
  static final long STORE_VERSION = 1L;

  @VisibleForTesting
  static final byte[] STORE_VERSION_KEY = "__version__".getBytes(UTF_8);

  /** DB key where app metadata is stored. */
  private static final byte[] METADATA_KEY = "__meta__".getBytes(UTF_8);

  /** DB key where type aliases are stored. */
  private static final byte[] TYPE_ALIASES_KEY = "__types__".getBytes(UTF_8);

  final AtomicReference<DB> _db;
  final KVStoreSerializer serializer;

  /**
   * Keep a mapping of class names to a shorter, unique ID managed by the store. This serves two
   * purposes: make the keys stored on disk shorter, and spread out the keys, since class names
   * will often have a long, redundant prefix (think "org.apache.spark.").
   */
  private final ConcurrentMap<String, byte[]> typeAliases;
  private final ConcurrentMap<Class<?>, LevelDBTypeInfo> types;

  public LevelDB(File path) throws Exception {
    this(path, new KVStoreSerializer());
  }

  public LevelDB(File path, KVStoreSerializer serializer) throws Exception {
    this.serializer = serializer;
    this.types = new ConcurrentHashMap<>();

    Options options = new Options();
    options.createIfMissing(true);
    this._db = new AtomicReference<>(JniDBFactory.factory.open(path, options));

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
    LevelDBTypeInfo ti = getTypeInfo(value.getClass());

    try (WriteBatch batch = db().createWriteBatch()) {
      byte[] data = serializer.serialize(value);
      synchronized (ti) {
        Object existing;
        try {
          existing = get(ti.naturalIndex().entityKey(null, value), value.getClass());
        } catch (NoSuchElementException e) {
          existing = null;
        }

        PrefixCache cache = new PrefixCache(value);
        byte[] naturalKey = ti.naturalIndex().toKey(ti.naturalIndex().getValue(value));
        for (LevelDBTypeInfo.Index idx : ti.indices()) {
          byte[] prefix = cache.getPrefix(idx);
          idx.add(batch, value, existing, data, naturalKey, prefix);
        }
        db().write(batch);
      }
    }
  }

  @Override
  public void delete(Class<?> type, Object naturalKey) throws Exception {
    Preconditions.checkArgument(naturalKey != null, "Null keys are not allowed.");
    try (WriteBatch batch = db().createWriteBatch()) {
      LevelDBTypeInfo ti = getTypeInfo(type);
      byte[] key = ti.naturalIndex().start(null, naturalKey);
      synchronized (ti) {
        byte[] data = db().get(key);
        if (data != null) {
          Object existing = serializer.deserialize(data, type);
          PrefixCache cache = new PrefixCache(existing);
          byte[] keyBytes = ti.naturalIndex().toKey(ti.naturalIndex().getValue(existing));
          for (LevelDBTypeInfo.Index idx : ti.indices()) {
            idx.remove(batch, existing, keyBytes, cache.getPrefix(idx));
          }
          db().write(batch);
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
          return new LevelDBIterator<>(type, LevelDB.this, this);
        } catch (Exception e) {
          throw Throwables.propagate(e);
        }
      }
    };
  }

  @Override
  public <T> boolean removeAllByIndexValues(
      Class<T> klass,
      String index,
      Collection<?> indexValues) throws Exception {
    LevelDBTypeInfo.Index naturalIndex = getTypeInfo(klass).naturalIndex();
    boolean removed = false;
    KVStoreView<T> view = view(klass).index(index);

    for (Object indexValue : indexValues) {
      for (T value: view.first(indexValue).last(indexValue)) {
        Object itemKey = naturalIndex.getValue(value);
        delete(klass, itemKey);
        removed = true;
      }
    }

    return removed;
  }

  @Override
  public long count(Class<?> type) throws Exception {
    LevelDBTypeInfo.Index idx = getTypeInfo(type).naturalIndex();
    return idx.getCount(idx.end(null));
  }

  @Override
  public long count(Class<?> type, String index, Object indexedValue) throws Exception {
    LevelDBTypeInfo.Index idx = getTypeInfo(type).index(index);
    return idx.getCount(idx.end(null, indexedValue));
  }

  @Override
  public void close() throws IOException {
    synchronized (this._db) {
      DB _db = this._db.getAndSet(null);
      if (_db == null) {
        return;
      }

      try {
        _db.close();
      } catch (IOException ioe) {
        throw ioe;
      } catch (Exception e) {
        throw new IOException(e.getMessage(), e);
      }
    }
  }

  /**
   * Closes the given iterator if the DB is still open. Trying to close a JNI LevelDB handle
   * with a closed DB can cause JVM crashes, so this ensures that situation does not happen.
   */
  void closeIterator(LevelDBIterator<?> it) throws IOException {
    synchronized (this._db) {
      DB _db = this._db.get();
      if (_db != null) {
        it.close();
      }
    }
  }

  /** Returns metadata about indices for the given type. */
  LevelDBTypeInfo getTypeInfo(Class<?> type) throws Exception {
    LevelDBTypeInfo ti = types.get(type);
    if (ti == null) {
      LevelDBTypeInfo tmp = new LevelDBTypeInfo(this, type, getTypeAlias(type));
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
  DB db() {
    DB _db = this._db.get();
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
    private final Map<LevelDBTypeInfo.Index, byte[]> prefixes;

    PrefixCache(Object entity) {
      this.entity = entity;
      this.prefixes = new HashMap<>();
    }

    byte[] getPrefix(LevelDBTypeInfo.Index idx) throws Exception {
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
