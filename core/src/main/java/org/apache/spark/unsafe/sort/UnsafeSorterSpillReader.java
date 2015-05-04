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

package org.apache.spark.unsafe.sort;

import com.google.common.io.ByteStreams;
import org.apache.spark.storage.BlockId;
import org.apache.spark.storage.BlockManager;
import org.apache.spark.unsafe.PlatformDependent;

import java.io.*;

public final class UnsafeSorterSpillReader extends UnsafeExternalSortSpillMerger.MergeableIterator {

  private final File file;
  private InputStream in;
  private DataInputStream din;

  private long keyPrefix;
  private final byte[] arr = new byte[1024 * 1024];  // TODO: tune this (maybe grow dynamically)?
  private final Object baseObject = arr;
  private int nextRecordLength;
  private final long baseOffset = PlatformDependent.BYTE_ARRAY_OFFSET;

  public UnsafeSorterSpillReader(
      BlockManager blockManager,
      File file,
      BlockId blockId) throws IOException {
    this.file = file;
    assert (file.length() > 0);
    final BufferedInputStream bs = new BufferedInputStream(new FileInputStream(file));
    this.in = blockManager.wrapForCompression(blockId, bs);
    this.din = new DataInputStream(this.in);
    nextRecordLength = din.readInt();
  }

  @Override
  public boolean hasNext() {
    return (in != null);
  }

  @Override
  public void loadNextRecord() {
    try {
      keyPrefix = din.readLong();
      ByteStreams.readFully(in, arr, 0, nextRecordLength);
      nextRecordLength = din.readInt();
      if (nextRecordLength == UnsafeSorterSpillWriter.EOF_MARKER) {
        in.close();
        in = null;
        din = null;
      }
    } catch (Exception e) {
      PlatformDependent.throwException(e);
    }
  }

  @Override
  public long getPrefix() {
    return keyPrefix;
  }

  @Override
  public Object getBaseObject() {
    return baseObject;
  }

  @Override
  public long getBaseOffset() {
    return baseOffset;
  }
}
