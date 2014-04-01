/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hadoop.hdfs.security.token.block;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableFactories;
import org.apache.hadoop.io.WritableFactory;

/**
 * Object for passing block keys
 */
public class ExportedBlockKeys implements Writable {
  public static final ExportedBlockKeys DUMMY_KEYS = new ExportedBlockKeys();
  private boolean isBlockTokenEnabled;
  private long keyUpdateInterval;
  private long tokenLifetime;
  private BlockKey currentKey;
  private BlockKey[] allKeys;

  public ExportedBlockKeys() {
    this(false, 0, 0, new BlockKey(), new BlockKey[0]);
  }

  ExportedBlockKeys(boolean isBlockTokenEnabled, long keyUpdateInterval,
      long tokenLifetime, BlockKey currentKey, BlockKey[] allKeys) {
    this.isBlockTokenEnabled = isBlockTokenEnabled;
    this.keyUpdateInterval = keyUpdateInterval;
    this.tokenLifetime = tokenLifetime;
    this.currentKey = currentKey == null ? new BlockKey() : currentKey;
    this.allKeys = allKeys == null ? new BlockKey[0] : allKeys;
  }

  public boolean isBlockTokenEnabled() {
    return isBlockTokenEnabled;
  }

  public long getKeyUpdateInterval() {
    return keyUpdateInterval;
  }

  public long getTokenLifetime() {
    return tokenLifetime;
  }

  public BlockKey getCurrentKey() {
    return currentKey;
  }

  public BlockKey[] getAllKeys() {
    return allKeys;
  }
  
  // ///////////////////////////////////////////////
  // Writable
  // ///////////////////////////////////////////////
  static { // register a ctor
    WritableFactories.setFactory(ExportedBlockKeys.class,
        new WritableFactory() {
          public Writable newInstance() {
            return new ExportedBlockKeys();
          }
        });
  }

  /**
   */
  public void write(DataOutput out) throws IOException {
    out.writeBoolean(isBlockTokenEnabled);
    out.writeLong(keyUpdateInterval);
    out.writeLong(tokenLifetime);
    currentKey.write(out);
    out.writeInt(allKeys.length);
    for (int i = 0; i < allKeys.length; i++) {
      allKeys[i].write(out);
    }
  }

  /**
   */
  public void readFields(DataInput in) throws IOException {
    isBlockTokenEnabled = in.readBoolean();
    keyUpdateInterval = in.readLong();
    tokenLifetime = in.readLong();
    currentKey.readFields(in);
    this.allKeys = new BlockKey[in.readInt()];
    for (int i = 0; i < allKeys.length; i++) {
      allKeys[i] = new BlockKey();
      allKeys[i].readFields(in);
    }
  }

}