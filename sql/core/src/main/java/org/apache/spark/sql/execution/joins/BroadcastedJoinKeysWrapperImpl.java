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

package org.apache.spark.sql.execution.joins;

import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.lang.ref.WeakReference;
import java.nio.ByteBuffer;
import java.util.Collection;
import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;

import scala.Function1;
import scala.collection.Iterator;
import scala.jdk.CollectionConverters;

import org.jetbrains.annotations.NotNull;

import com.google.common.base.Objects;
import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;

import org.apache.spark.broadcast.Broadcast;
import org.apache.spark.sql.catalyst.CatalystTypeConverters;
import org.apache.spark.sql.catalyst.InternalRow;
import org.apache.spark.sql.catalyst.bcvar.ArrayWrapper;
import org.apache.spark.sql.catalyst.bcvar.BroadcastedJoinKeysWrapper;
import org.apache.spark.sql.catalyst.expressions.UnsafeRow;
import org.apache.spark.sql.types.DataType;
import org.apache.spark.sql.types.IntegerType$;
import org.apache.spark.sql.types.LongType$;
import org.apache.spark.sql.types.ShortType$;

public class BroadcastedJoinKeysWrapperImpl implements BroadcastedJoinKeysWrapper {
  private Broadcast<HashedRelation> bcVar;
  private DataType[] totalKeyDataTypes;
  private int index = 0;
  private transient volatile WeakReference<Object> keysArray = null;
  private int totalJoinKeys = 0;

  private static final LoadingCache<BroadcastedJoinKeysWrapperImpl, Set<Object>>
    idempotentializerForSet = CacheBuilder.newBuilder().expireAfterWrite(
     CACHE_EXPIRY, TimeUnit.SECONDS).maximumSize(CACHE_SIZE)
      .weakValues()
      .build(
          new CacheLoader<>() {
            public Set<Object> load(BroadcastedJoinKeysWrapperImpl bcjk) {
              BroadcastJoinKeysReaper.checkInitialized();
              ArrayWrapper<? extends Object> keys = bcjk.getKeysArray();
              int len = keys.getLength();
              Set<Object> set = new HashSet<>();
              for (int i = 0; i < len; ++i) {
                set.add(keys.get(i));
              }
              return set;
            }
          });

  private static final LoadingCache<KeyIdempotForHashedRelationDeser, Object>
      idempotentializerForHashedRelationDeser =
      CacheBuilder.newBuilder().expireAfterWrite(CACHE_EXPIRY, TimeUnit.SECONDS)
      .maximumSize(CACHE_SIZE)
          .weakValues()
          .build(
              new CacheLoader<>() {
                // this will register the Reaper on the driver side as well as executor side to get
                // application life cycle events and removal of broadcast var event
                public Object load(KeyIdempotForHashedRelationDeser key) {
                  BroadcastJoinKeysReaper.checkInitialized();
                  Broadcast<HashedRelation> bcVar = key.bcjk.bcVar;
                  if (bcVar.getValue() instanceof LongHashedRelation) {
                    LongHashedRelation lhr = (LongHashedRelation) bcVar.getValue();
                    if (key.bcjk.totalJoinKeys == 1) {
                      if (key.bcjk.totalKeyDataTypes[0].equals(LongType$.MODULE$)) {
                        return CollectionConverters.SeqHasAsJava(lhr.keys().map(f -> f.get(
                            0, LongType$.MODULE$)).toList()).asJava().toArray();
                      } else if (key.bcjk.totalKeyDataTypes[0].equals(IntegerType$.MODULE$)) {
                        return CollectionConverters.SeqHasAsJava(lhr.keys().map(f -> ((Long) f.get(
                            0, LongType$.MODULE$)).intValue()).toList()).asJava().toArray();
                      } else if (key.bcjk.totalKeyDataTypes[0].equals(ShortType$.MODULE$)) {
                        return CollectionConverters.SeqHasAsJava(lhr.keys().map(f -> ((Long) f.get(
                            0, LongType$.MODULE$)).shortValue()).toList()).asJava().toArray();
                      } else {
                        return CollectionConverters.SeqHasAsJava(lhr.keys().map(f -> ((Long) f.get(
                            0, LongType$.MODULE$)).byteValue()).toList()).asJava().toArray();
                      }
                    } else if (key.bcjk.totalJoinKeys == 2) {
                      DataType key1DataType = key.bcjk.totalKeyDataTypes[0];
                      DataType key2DataType = key.bcjk.totalKeyDataTypes[1];
                      Function1<Object, Object> key1ScalaConverter =
                          CatalystTypeConverters.createToScalaConverter(key1DataType);
                      Function1<Object, Object> key2ScalaConverter =
                          CatalystTypeConverters.createToScalaConverter(key2DataType);

                      return CollectionConverters.SeqHasAsJava(
                          lhr.keys().map(ir -> {
                            long hashedKey = ir.getLong(0);

                            Object actualkey1 =
                                key1ScalaConverter.apply(
                                    Integer.valueOf(((int) (hashedKey >> 32))));
                            Object actualkey2 =
                                key2ScalaConverter.apply(
                                    Integer.valueOf((int) (hashedKey & 0xffffffffL)));

                            return new Object[] {actualkey1, actualkey2};
                          }).toList()).asJava().toArray(new Object[0][]);
                    } else {
                      // getObjects(lhr, key.bcjk);
                      throw new UnsupportedOperationException("Case not handled");
                    }
                  } else {
                    Iterator<InternalRow> keysIter = bcVar.getValue().keys();
                    if (key.bcjk.totalJoinKeys == 1) {
                      DataType keyDataType = key.bcjk.totalKeyDataTypes[0];
                      Function1<Object, Object> toScalaConverter =
                          CatalystTypeConverters.createToScalaConverter(keyDataType);
                      Iterator<Object> keysAsScala = keysIter.map(f -> {
                        Object x = f.get(0, keyDataType);
                        return conversionExcludedDataTypes.contains(keyDataType) ? x :
                            toScalaConverter.apply(x);
                      });
                      return CollectionConverters.SeqHasAsJava(keysAsScala.toList())
                          .asJava()
                          .toArray();
                    } else {
                      Function1<Object, Object>[] toScalaConverters =
                          new Function1[key.bcjk.totalJoinKeys];
                      for (int i = 0; i < key.bcjk.totalJoinKeys; ++i) {
                        DataType keyDataType = key.bcjk.totalKeyDataTypes[i];
                        toScalaConverters[i] = CatalystTypeConverters.createToScalaConverter(
                            keyDataType);
                      }
                      Iterator<Object[]> keysAsScala = keysIter.map(f -> {
                        Object[] arr = new Object[key.bcjk.totalJoinKeys];
                        for (int i = 0; i < key.bcjk.totalJoinKeys; ++i) {
                          DataType keyDataType = key.bcjk.totalKeyDataTypes[i];
                          Object x = f.get(i, keyDataType);
                          arr[i] = conversionExcludedDataTypes.contains(keyDataType) ? x :
                              toScalaConverters[i].apply(x);
                        }
                        return arr;
                      });
                      return CollectionConverters.SeqHasAsJava(keysAsScala.toList())
                          .asJava()
                          .toArray(
                              new Object[0][]);
                    }
                  }
                }
              });

  private static Object[][] getObjects(LongHashedRelation lhr,
      BroadcastedJoinKeysWrapperImpl bcjk ) {
    int totalKeysPresent = bcjk.totalJoinKeys;
    final UnsafeRow unsafeRow = new UnsafeRow(totalKeysPresent);
    final ByteBuffer buff = ByteBuffer.allocate(8);
    Function1<Object, Object>[] toScalaConverters = new Function1[bcjk.totalJoinKeys];
    for (int i = 0; i < bcjk.totalJoinKeys; ++i) {
      DataType keyDataType =bcjk.totalKeyDataTypes[i];
      toScalaConverters[i] = CatalystTypeConverters.createToScalaConverter(keyDataType);
    }
    return CollectionConverters.SeqHasAsJava(
      lhr.keys().map(ir -> {
        long hashedKey = Long.reverse(ir.getLong(0));
        buff.putLong(0, hashedKey);
        byte[] arr = buff.array();
        unsafeRow.pointTo(arr, arr.length);
        Object[] actualkeys = new Object[bcjk.totalJoinKeys];
        for (int i = 0; i < bcjk.totalJoinKeys; ++i) {
          DataType keyDataType = bcjk.totalKeyDataTypes[i];
          Object temp = unsafeRow.get(i, keyDataType);
          actualkeys[i] =  toScalaConverters[i].apply(temp);
        }
        return actualkeys;
      }).toList()).asJava().toArray(new Object[0][]);
  }

  public BroadcastedJoinKeysWrapperImpl() {}

  public BroadcastedJoinKeysWrapperImpl(
      Broadcast<HashedRelation> bcVar,
      DataType[] totalKeyDataTypes,
      int index, int totalJoinKeys) {
    this.bcVar = bcVar;
    this.totalKeyDataTypes = totalKeyDataTypes;
    this.index = index;
    this.totalJoinKeys = totalJoinKeys;
  }

  @Override
  public void writeExternal(ObjectOutput out) throws IOException {
    out.writeObject(this.bcVar);
    out.writeInt(this.index);
    out.writeInt(this.totalJoinKeys);
    out.writeObject(this.totalKeyDataTypes);
  }

  @Override
  public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException {
    this.bcVar = (Broadcast<HashedRelation>) in.readObject();
    this.index = in.readInt();
    this.totalJoinKeys = in.readInt();
    this.totalKeyDataTypes = (DataType[])in.readObject();
  }

  private Object initKeys() {
    Object actualArray;
    try {
      if (this.keysArray == null || (actualArray = this.keysArray.get()) == null) {
        actualArray =
            idempotentializerForHashedRelationDeser.get(
                new KeyIdempotForHashedRelationDeser(this));
        this.keysArray = new WeakReference<>(actualArray);
      }
      return actualArray;
    } catch (ExecutionException ee) {
      throw new RuntimeException(ee);
    }
  }

  public DataType getSingleKeyDataType() {
    return this.totalKeyDataTypes[index];
  }

  public ArrayWrapper<? extends Object> getKeysArray() {
    Object array = this.initKeys();
    return ArrayWrapper.wrapArray(array, this.totalJoinKeys == 1,  this.index);
  }

  public Set<Object> getKeysAsSet() {
    try {
      Set<Object> keyset = idempotentializerForSet.get(this);
      if (System.getProperty("debug", "false").equals("true")) {
        return new SetWrapper<>(keyset);
      } else {
        return keyset;
      }
    } catch(ExecutionException ee) {
      throw new RuntimeException(ee);
    }
  }

  @Override
  public boolean equals(Object other) {
    if (other != null) {
      return this == other || (other instanceof BroadcastedJoinKeysWrapperImpl
          && this.bcVar.id() == ((BroadcastedJoinKeysWrapperImpl) other).bcVar.id()
          && this.index == ((BroadcastedJoinKeysWrapperImpl) other).index);
    } else {
      return false;
    }
  }

  @Override
  public int hashCode() {
    return Objects.hashCode(this.bcVar.id(), this.index);
  }

  public long getBroadcastVarId() {
    return this.bcVar.id();
  }

  public int getKeyIndex() {
    return this.index;
  }

  public int getTotalJoinKeys() {
    return this.totalJoinKeys;
  }

  public HashedRelation getUnderlyingRelation() {
    return this.bcVar.getValue();
  }

  public Broadcast<HashedRelation> getUnderlyingBroadcastVar() {
    return this.bcVar;
  }

  @Override
  public void invalidateSelf() {
    removeBroadcast(this.bcVar.id());
  }

  static void removeBroadcast(long id) {
    idempotentializerForHashedRelationDeser.asMap().keySet().stream()
      .filter( key -> key.bcjk.getBroadcastVarId() == id).forEach(
            idempotentializerForHashedRelationDeser::invalidate);
    idempotentializerForSet.asMap().keySet().stream()
      .filter( bcVar -> bcVar.getBroadcastVarId() == id).forEach(
        idempotentializerForSet::invalidate);
  }

  static void invalidateBroadcastCache() {
    idempotentializerForSet.invalidateAll();
    idempotentializerForHashedRelationDeser.invalidateAll();
  }
}
class SetWrapper<T> implements Set<T> {
  private final Set<T> base;
  SetWrapper(Set<T> base) {
    this.base = base;
  }

  @Override
  public java.util.Iterator<T> iterator() {
    return this.base.iterator();
  }

  @NotNull
  @Override
  public Object[] toArray() {
    return this.base.toArray();
  }

  @Override
  public boolean add(T o) {
    return this.base.add(o);
  }

  @Override
  public boolean remove(Object o) {
    return this.base.remove(o);
  }

  @Override
  public boolean addAll(@NotNull Collection<? extends T> c) {
    return this.base.addAll(c);
  }

  @Override
  public void clear() {
    this.base.clear();
  }

  @Override
  public boolean equals(Object o) {
    return this.base.equals(o);
  }

  @Override
  public int hashCode() {
    return this.base.hashCode();
  }

  @Override
  public boolean removeAll(@NotNull Collection<?> c) {
    return this.base.removeAll(c);
  }

  @Override
  public boolean retainAll(@NotNull Collection<?> c) {
    return this.base.retainAll(c);
  }

  @Override
  public boolean containsAll(@NotNull Collection<?> c) {
    return this.base.containsAll(c);
  }

  @NotNull
  @Override
  public <T> T[] toArray(T[] a) {
    return this.base.toArray(a);
  }

  @Override
  public int size() {
    return this.base.size();
  }

  @Override
  public boolean isEmpty() {
    return this.base.isEmpty();
  }

  @Override
  public boolean contains(Object o) {
    return this.base.contains(o);
  }
}

class KeyIdempotForHashedRelationDeser {
  final BroadcastedJoinKeysWrapperImpl bcjk;

  KeyIdempotForHashedRelationDeser(BroadcastedJoinKeysWrapperImpl bcjk) {
    this.bcjk = bcjk;
  }

  @Override
  public boolean equals(Object other) {
    if (other != null) {
      return this == other ||
          (other instanceof KeyIdempotForHashedRelationDeser &&
              this.bcjk.getBroadcastVarId() ==
          ((KeyIdempotForHashedRelationDeser) other).bcjk.getBroadcastVarId());
    } else {
      return false;
    }
  }

  @Override
  public int hashCode() {
    return Objects.hashCode(this.bcjk.getBroadcastVarId());
  }
}
