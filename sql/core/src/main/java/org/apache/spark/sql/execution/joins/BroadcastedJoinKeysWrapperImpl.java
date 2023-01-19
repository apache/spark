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
import java.time.Duration;
import java.util.HashSet;
import java.util.Set;

import com.github.benmanes.caffeine.cache.Caffeine;
import com.github.benmanes.caffeine.cache.LoadingCache;

import org.apache.spark.broadcast.Broadcast;
import org.apache.spark.sql.catalyst.CatalystTypeConverters;
import org.apache.spark.sql.catalyst.InternalRow;
import org.apache.spark.sql.catalyst.bcVar.ArrayWrapper;
import org.apache.spark.sql.catalyst.bcVar.BroadcastedJoinKeysWrapper;
import org.apache.spark.sql.catalyst.expressions.UnsafeRow;
import org.apache.spark.sql.types.DataType;
import org.apache.spark.sql.types.IntegerType$;
import org.apache.spark.sql.types.LongType$;
import org.apache.spark.sql.types.ShortType$;
import scala.Function1;
import scala.collection.Iterator;
import scala.collection.JavaConverters;

public class BroadcastedJoinKeysWrapperImpl implements BroadcastedJoinKeysWrapper {
  private Broadcast<HashedRelation> bcVar;

  private DataType[] keyDataTypes;

  private int relativeKeyIndexInArray = 0;

  private int[] indexesOfInterest;
  private transient volatile WeakReference<Object> keysArray = null;

  private int totalJoinKeys = 0;

  private static final LoadingCache<BroadcastedJoinKeysWrapperImpl, Object> idempotentializer =
      Caffeine.newBuilder().expireAfterWrite(Duration.ofSeconds(CACHE_EXPIRY))
          .maximumSize(CACHE_SIZE).weakValues().build(bcjk -> {
            // this will register the Reaper on the driver side as well as executor side to get
            // application life cycle events and removal of broadcast var event
            BroadcastJoinKeysReaper.checkInitialized();
            Broadcast<HashedRelation> bcVar = bcjk.bcVar;
            if (bcVar.getValue() instanceof LongHashedRelation) {
              LongHashedRelation lhr = (LongHashedRelation) bcVar.getValue();
              if (bcjk.totalJoinKeys == 1) {
                if (bcjk.keyDataTypes[0].equals(LongType$.MODULE$)) {
                  return JavaConverters.asJavaCollection(lhr.keys().map(f -> f.get(
                          0, LongType$.MODULE$)).toList()).toArray();
                } else if (bcjk.keyDataTypes[0].equals(IntegerType$.MODULE$)) {
                  return JavaConverters.asJavaCollection(lhr.keys().map(f -> ((Long)f.get(
                      0, LongType$.MODULE$)).intValue()).toList()).toArray();
                } else if (bcjk.keyDataTypes[0].equals(ShortType$.MODULE$)) {
                  return JavaConverters.asJavaCollection(lhr.keys().map(f -> ((Long)f.get(
                      0, LongType$.MODULE$)).shortValue()).toList()).toArray();
                } else {
                  return JavaConverters.asJavaCollection(lhr.keys().map(f -> ((Long)f.get(
                      0, LongType$.MODULE$)).byteValue()).toList()).toArray();
                }
              } else {
                if (bcjk.indexesOfInterest.length == 1) {
                  return JavaConverters.asJavaCollection(
                      lhr.keys().map(ir -> {
                        long hashedKey = ir.getLong(0);
                        int actualkey;
                        if (bcjk.indexesOfInterest[0] == 0) {
                          actualkey = (int) (hashedKey >> 32);
                        } else {
                          actualkey = (int) (hashedKey & 0xffffffffL);
                        }
                         if (bcjk.keyDataTypes[0].equals(IntegerType$.MODULE$)) {
                          return actualkey;
                        } else if (bcjk.keyDataTypes[0].equals(ShortType$.MODULE$)) {
                          return (short)actualkey;
                        } else {
                          return (byte)actualkey;
                        }
                      }).toList()).toArray();
                } else {
                  return getObjects(lhr, bcjk);
                }
              }
            } else {
              Iterator<InternalRow> keysIter = bcVar.getValue().keys();
              if (bcjk.indexesOfInterest.length == 1) {
                int actualIndex = bcjk.indexesOfInterest[0];
                DataType keyDataType = bcjk.keyDataTypes[0];
                Function1<Object, Object> toScalaConverter = 
                    CatalystTypeConverters.createToScalaConverter(keyDataType);
                Iterator<Object> keysAsScala = keysIter.map(f -> {
                  Object x = f.get(actualIndex, keyDataType);
                  return toScalaConverter.apply(x);
                });
                return JavaConverters.asJavaCollection(keysAsScala.toList()).toArray();
              } else {
                Function1<Object, Object>[] toScalaConverters =
                    new Function1[bcjk.indexesOfInterest.length];
                for (int i = 0; i < bcjk.indexesOfInterest.length; ++i) {
                  DataType keyDataType = bcjk.keyDataTypes[i];
                  toScalaConverters[i] = CatalystTypeConverters.createToScalaConverter(keyDataType);
                }
                
                Iterator<Object[]> keysAsScala = keysIter.map(f -> {
                  Object[] arr = new Object[bcjk.indexesOfInterest.length];
                  for (int i = 0; i < bcjk.indexesOfInterest.length; ++i) {
                    int actualIndex = bcjk.indexesOfInterest[i];
                    DataType keyDataType = bcjk.keyDataTypes[i];
                    Object x = f.get(actualIndex, keyDataType);
                    arr[i] = toScalaConverters[i].apply(x);
                  }
                  return arr;
                });
                return JavaConverters.asJavaCollection(keysAsScala.toList()).toArray(
                  new Object[0][]);
              }
            }
          });

  private static Object[][] getObjects(LongHashedRelation lhr,
      BroadcastedJoinKeysWrapperImpl bcjk ) {
    int totalKeysPresent = bcjk.totalJoinKeys;
    final UnsafeRow unsafeRow = new UnsafeRow(totalKeysPresent);
    final ByteBuffer buff = ByteBuffer.allocate(8);
    Function1<Object, Object>[] toScalaConverters =
        new Function1[bcjk.indexesOfInterest.length];
    for (int i = 0; i < bcjk.indexesOfInterest.length; ++i) {
      DataType keyDataType = bcjk.keyDataTypes[i];
      toScalaConverters[i] = CatalystTypeConverters.createToScalaConverter(keyDataType);
    }
    return JavaConverters.asJavaCollection(
        lhr.keys().map(ir -> {
          long hashedKey = Long.reverse(ir.getLong(0));
          buff.putLong(0, hashedKey);
          byte[] arr = buff.array();
          unsafeRow.pointTo(arr, arr.length);
          Object[] actualkeys = new Object[bcjk.indexesOfInterest.length];
          for (int i = 0; i < bcjk.indexesOfInterest.length; ++i) {
            DataType keyDataType = bcjk.keyDataTypes[i];
            Object temp = unsafeRow.get(bcjk.indexesOfInterest[i], keyDataType);
            actualkeys[i] =  toScalaConverters[i].apply(temp);
          }
          return actualkeys;
        }).toList()).toArray(new Object[0][]);
    /*
    return JavaConverters.asJavaCollection(
        lhr.keys().map(ir -> {
          long hashedKey = ir.getLong(0);
          Object[] actualkeys = new Object[2];
          actualkeys[0] = (int) (hashedKey >> 32);
          actualkeys[1] = (int) (hashedKey & 0xffffffffL);

          return actualkeys;
        }).toList()).toArray(new Object[0][]);

     */
  }

  private static final LoadingCache<BroadcastedJoinKeysWrapperImpl, Set<Object>>
      idempotentializerForSet = Caffeine.newBuilder().expireAfterWrite(
          Duration.ofSeconds(CACHE_EXPIRY)).maximumSize(CACHE_SIZE).weakValues().
      build(bcjk -> {
            // this will register the Reaper on the driver side as well as executor side to get
            // application life cycle events and removal of broadcast var event
            BroadcastJoinKeysReaper.checkInitialized();
            ArrayWrapper keys = bcjk.getKeysArray();
            int len = keys.getLength();
            Set<Object> set = new HashSet<>();
            for(int i = 0; i < len; ++i) {
             set.add(keys.get(i));
            }
            return set;
          });

  public BroadcastedJoinKeysWrapperImpl() {
  }

  public BroadcastedJoinKeysWrapperImpl(Broadcast<HashedRelation> bcVar, DataType[] keyDataTypes,
      int relativeKeyIndexInArray, int[] indexArray, int totalJoinKeys) {
    this.bcVar = bcVar;
    this.keyDataTypes = keyDataTypes;
    this.relativeKeyIndexInArray = relativeKeyIndexInArray;
    this.indexesOfInterest = indexArray;
    this.totalJoinKeys = totalJoinKeys;
  }

  @Override
  public void writeExternal(ObjectOutput out) throws IOException {
    out.writeObject(this.bcVar);
    out.writeInt(this.relativeKeyIndexInArray);
    out.writeInt(this.totalJoinKeys);
    out.writeObject(this.indexesOfInterest);
    out.writeObject(this.keyDataTypes);
  }

  @Override
  public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException {
    this.bcVar = (Broadcast<HashedRelation>) in.readObject();
    this.relativeKeyIndexInArray = in.readInt();
    this.totalJoinKeys = in.readInt();
    this.indexesOfInterest = (int[])in.readObject();
    this.keyDataTypes = (DataType[])in.readObject();
  }

  private Object initKeys() {
    Object actualArray = null;
    if (this.keysArray == null || (actualArray = this.keysArray.get()) == null) {
      actualArray = idempotentializer.get(this);
      this.keysArray = new WeakReference<>(actualArray);
    }
    return actualArray;
  }
  public DataType getSingleKeyDataType() {
    return this.keyDataTypes[this.relativeKeyIndexInArray];
  }

  public ArrayWrapper getKeysArray() {
    Object array = this.initKeys();
    return new ArrayWrapper(array, this.relativeKeyIndexInArray,
        this.indexesOfInterest.length == 1);
  }

  public Set<Object> getKeysAsSet() {
    return idempotentializerForSet.get(this);
  }

  @Override
  public boolean equals(Object other) {
    if (other != null) {
      return this == other || (other instanceof BroadcastedJoinKeysWrapperImpl
            && this.bcVar.id() == ((BroadcastedJoinKeysWrapperImpl) other).bcVar.id());
    } else {
      return false;
    }
  }

  @Override
  public int hashCode() {
    return Long.valueOf(this.bcVar.id()).hashCode();
  }

  public long getBroadcastVarId() {
    return this.bcVar.id();
  }

  public int getRelativeKeyIndex() {
    return this.relativeKeyIndexInArray;
  }

  public int getTupleLength() {
    return this.indexesOfInterest.length;
  }

  public int getTotalJoinKeys() {
    return this.totalJoinKeys;
  }

  public HashedRelation getUnderlyingRelation() {
    return this.bcVar.getValue();
  }

  @Override
  public void invalidateSelf() {
    removeBroadcast(this.bcVar.id());
  }

  static void removeBroadcast(long id) {
    idempotentializer.asMap().keySet().stream()
        .filter( bcVar -> bcVar.getBroadcastVarId() == id).forEach(idempotentializer::invalidate);
    idempotentializerForSet.asMap().keySet().stream()
        .filter( bcVar -> bcVar.getBroadcastVarId() == id).forEach(
            idempotentializerForSet::invalidate);
  }

  static void invalidateBroadcastCache() {
    idempotentializerForSet.invalidateAll();
    idempotentializer.invalidateAll();
  }
}
