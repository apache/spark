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

package org.apache.hive.service.cli;

import java.nio.ByteBuffer;
import java.util.UUID;

import org.apache.hive.service.rpc.thrift.THandleIdentifier;

/**
 * HandleIdentifier.
 *
 */
public class HandleIdentifier {
  private final UUID publicId;
  private final UUID secretId;

  public HandleIdentifier() {
    publicId = UUID.randomUUID();
    secretId = UUID.randomUUID();
  }

  public HandleIdentifier(UUID publicId, UUID secretId) {
    this.publicId = publicId;
    this.secretId = secretId;
  }

  public HandleIdentifier(THandleIdentifier tHandleId) {
    ByteBuffer bb = ByteBuffer.wrap(tHandleId.getGuid());
    this.publicId = new UUID(bb.getLong(), bb.getLong());
    bb = ByteBuffer.wrap(tHandleId.getSecret());
    this.secretId = new UUID(bb.getLong(), bb.getLong());
  }

  public UUID getPublicId() {
    return publicId;
  }

  public UUID getSecretId() {
    return secretId;
  }

  public THandleIdentifier toTHandleIdentifier() {
    byte[] guid = new byte[16];
    byte[] secret = new byte[16];
    ByteBuffer guidBB = ByteBuffer.wrap(guid);
    ByteBuffer secretBB = ByteBuffer.wrap(secret);
    guidBB.putLong(publicId.getMostSignificantBits());
    guidBB.putLong(publicId.getLeastSignificantBits());
    secretBB.putLong(secretId.getMostSignificantBits());
    secretBB.putLong(secretId.getLeastSignificantBits());
    return new THandleIdentifier(ByteBuffer.wrap(guid), ByteBuffer.wrap(secret));
  }

  @Override
  public int hashCode() {
    final int prime = 31;
    int result = 1;
    result = prime * result + ((publicId == null) ? 0 : publicId.hashCode());
    result = prime * result + ((secretId == null) ? 0 : secretId.hashCode());
    return result;
  }

  @Override
  public boolean equals(Object obj) {
    if (this == obj) {
      return true;
    }
    if (obj == null) {
      return false;
    }
    if (!(obj instanceof HandleIdentifier)) {
      return false;
    }
    HandleIdentifier other = (HandleIdentifier) obj;
    if (publicId == null) {
      if (other.publicId != null) {
        return false;
      }
    } else if (!publicId.equals(other.publicId)) {
      return false;
    }
    if (secretId == null) {
      if (other.secretId != null) {
        return false;
      }
    } else if (!secretId.equals(other.secretId)) {
      return false;
    }
    return true;
  }

  @Override
  public String toString() {
    return publicId.toString();
  }
}
