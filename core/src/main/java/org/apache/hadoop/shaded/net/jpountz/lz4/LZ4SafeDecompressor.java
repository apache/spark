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

package org.apache.hadoop.shaded.net.jpountz.lz4;

/**
 * TODO(SPARK-36679): A temporary workaround for SPARK-36669. We should remove this after
 * Hadoop 3.3.2 release which fixes the LZ4 relocation in shaded Hadoop client libraries.
 * This does not need implement all net.jpountz.lz4.LZ4SafeDecompressor API, just the ones
 * used by Hadoop Lz4Decompressor.
 */
public final class LZ4SafeDecompressor {
  private net.jpountz.lz4.LZ4SafeDecompressor lz4Decompressor;

  public LZ4SafeDecompressor(net.jpountz.lz4.LZ4SafeDecompressor lz4Decompressor) {
    this.lz4Decompressor = lz4Decompressor;
  }

  public void decompress(java.nio.ByteBuffer src, java.nio.ByteBuffer dest) {
    lz4Decompressor.decompress(src, dest);
  }
}
