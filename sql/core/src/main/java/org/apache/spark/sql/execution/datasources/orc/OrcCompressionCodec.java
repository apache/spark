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

package org.apache.spark.sql.execution.datasources.orc;

import java.util.Arrays;
import java.util.Locale;
import java.util.Map;
import java.util.stream.Collectors;

import org.apache.orc.CompressionKind;

/**
 * A mapper class from Spark supported orc compression codecs to orc compression codecs.
 */
public enum OrcCompressionCodec {
  NONE(CompressionKind.NONE),
  UNCOMPRESSED(CompressionKind.NONE),
  ZLIB(CompressionKind.ZLIB),
  SNAPPY(CompressionKind.SNAPPY),
  LZO(CompressionKind.LZO),
  LZ4(CompressionKind.LZ4),
  ZSTD(CompressionKind.ZSTD),
  BROTLI(CompressionKind.BROTLI);

  private final CompressionKind compressionKind;

  OrcCompressionCodec(CompressionKind compressionKind) {
    this.compressionKind = compressionKind;
  }

  public CompressionKind getCompressionKind() {
    return this.compressionKind;
  }

  public static final Map<String, String> codecNameMap =
    Arrays.stream(OrcCompressionCodec.values()).collect(
      Collectors.toMap(codec -> codec.name(), codec -> codec.name().toLowerCase(Locale.ROOT)));

  public String lowerCaseName() {
    return codecNameMap.get(this.name());
  }
}
