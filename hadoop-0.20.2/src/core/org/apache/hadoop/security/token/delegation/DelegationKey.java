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

package org.apache.hadoop.security.token.delegation;

//import org.apache.hadoop.classification.InterfaceAudience;
import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import javax.crypto.SecretKey;

import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableUtils;
//import static org.apache.hadoop.classification.InterfaceAudience.LimitedPrivate.Project.HDFS;
//import static org.apache.hadoop.classification.InterfaceAudience.LimitedPrivate.Project.MAPREDUCE;

/**
 * Key used for generating and verifying delegation tokens
 */
//@InterfaceAudience.LimitedPrivate({HDFS, MAPREDUCE})
public class DelegationKey implements Writable {
  private int keyId;
  private long expiryDate;
  private SecretKey key;

  public DelegationKey() {
    this(0, 0L, null);
  }

  public DelegationKey(int keyId, long expiryDate, SecretKey key) {
    this.keyId = keyId;
    this.expiryDate = expiryDate;
    this.key = key;
  }

  public int getKeyId() {
    return keyId;
  }

  public long getExpiryDate() {
    return expiryDate;
  }

  public SecretKey getKey() {
    return key;
  }

  public void setExpiryDate(long expiryDate) {
    this.expiryDate = expiryDate;
  }

  /**
   */
  public void write(DataOutput out) throws IOException {
    WritableUtils.writeVInt(out, keyId);
    WritableUtils.writeVLong(out, expiryDate);
    if (key == null) {
      WritableUtils.writeVInt(out, -1);
    } else {
      byte[] keyBytes = key.getEncoded();
      WritableUtils.writeVInt(out, keyBytes.length);
      out.write(keyBytes);
    }
  }

  /**
   */
  public void readFields(DataInput in) throws IOException {
    keyId = WritableUtils.readVInt(in);
    expiryDate = WritableUtils.readVLong(in);
    int len = WritableUtils.readVInt(in);
    if (len == -1) {
      key = null;
    } else {
      byte[] keyBytes = new byte[len];
      in.readFully(keyBytes);
      key = AbstractDelegationTokenSecretManager.createSecretKey(keyBytes);
    }
  }
}
