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

package org.apache.spark.sql.execution.datasources.parquet;

import java.io.IOException;
import java.io.InputStream;
import java.nio.ByteBuffer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.compress.CodecPool;
import org.apache.hadoop.io.compress.CompressionCodec;
import org.apache.hadoop.io.compress.Decompressor;
import org.apache.parquet.bytes.BytesInput;
import org.apache.parquet.hadoop.CodecFactory;
import org.apache.parquet.hadoop.codec.ZstandardCodec;
import org.apache.parquet.hadoop.metadata.CompressionCodecName;

/**
 * This class implements a codec factory that is used when reading from Parquet. It adds a
 * workaround for memory issues encountered when reading from zstd-compressed files. For
 * details, see <a href="https://issues.apache.org/jira/browse/PARQUET-2160">PARQUET-2160</a>
 *
 * TODO: Remove this workaround after upgrading Parquet which include PARQUET-2160.
 */
public class ParquetCodecFactory extends CodecFactory {

  public ParquetCodecFactory(Configuration configuration, int pageSize) {
    super(configuration, pageSize);
  }

  /**
   * Copied and modified from CodecFactory.HeapBytesDecompressor
   */
  @SuppressWarnings("deprecation")
  class HeapBytesDecompressor extends BytesDecompressor {

    private final CompressionCodec codec;
    private final Decompressor decompressor;

    HeapBytesDecompressor(CompressionCodecName codecName) {
      this.codec = getCodec(codecName);
      if (codec != null) {
        decompressor = CodecPool.getDecompressor(codec);
      } else {
        decompressor = null;
      }
    }

    @Override
    public BytesInput decompress(BytesInput bytes, int uncompressedSize) throws IOException {
      final BytesInput decompressed;
      if (codec != null) {
        if (decompressor != null) {
          decompressor.reset();
        }
        InputStream is = codec.createInputStream(bytes.toInputStream(), decompressor);

        if (codec instanceof ZstandardCodec) {
          // We need to explicitly close the ZstdDecompressorStream here to release the resources
          // it holds to avoid off-heap memory fragmentation issue, see PARQUET-2160.
          // This change will load the decompressor stream into heap a little earlier, since the
          // problem it solves only happens in the ZSTD codec, so this modification is only made
          // for ZSTD streams.
          decompressed = BytesInput.copy(BytesInput.from(is, uncompressedSize));
          is.close();
        } else {
          decompressed = BytesInput.from(is, uncompressedSize);
        }
      } else {
        decompressed = bytes;
      }
      return decompressed;
    }

    @Override
    public void decompress(
        ByteBuffer input, int compressedSize, ByteBuffer output, int uncompressedSize)
        throws IOException {
      ByteBuffer decompressed =
          decompress(BytesInput.from(input), uncompressedSize).toByteBuffer();
      output.put(decompressed);
    }

    @Override
    public void release() {
      if (decompressor != null) {
        CodecPool.returnDecompressor(decompressor);
      }
    }
  }

  @Override
  @SuppressWarnings("deprecation")
  protected BytesDecompressor createDecompressor(CompressionCodecName codecName) {
    return new HeapBytesDecompressor(codecName);
  }
}
