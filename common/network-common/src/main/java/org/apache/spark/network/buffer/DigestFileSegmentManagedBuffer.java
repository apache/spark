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

package org.apache.spark.network.buffer;

import java.io.File;

import com.google.common.base.Objects;

import org.apache.spark.network.util.TransportConf;

/**
 * A {@link ManagedBuffer} backed by a segment in a file with digest.
 */
public final class DigestFileSegmentManagedBuffer extends FileSegmentManagedBuffer {

  private final long digest;

  public DigestFileSegmentManagedBuffer(TransportConf conf, File file, long offset, long length,
    long digest) {
    super(conf, file, offset, length);
    this.digest = digest;
  }

  public long getDigest() { return digest; }

  @Override
  public String toString() {
    return Objects.toStringHelper(this)
      .add("file", getFile())
      .add("offset", getOffset())
      .add("length", getLength())
      .add("digest", digest)
      .toString();
  }
}
