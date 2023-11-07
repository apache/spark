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

package org.apache.spark.sql.catalyst.util;

import java.util.Arrays;
import java.util.Locale;
import java.util.Map;
import java.util.stream.Collectors;

import org.apache.hadoop.io.compress.BZip2Codec;
import org.apache.hadoop.io.compress.CompressionCodec;
import org.apache.hadoop.io.compress.DeflateCodec;
import org.apache.hadoop.io.compress.GzipCodec;
import org.apache.hadoop.io.compress.Lz4Codec;
import org.apache.hadoop.io.compress.SnappyCodec;

/**
 * A mapper class from Spark supported hadoop compression codecs to hadoop compression codecs.
 */
public enum HadoopCompressionCodec {
  NONE(null),
  UNCOMPRESSED(null),
  BZIP2(new BZip2Codec()),
  DEFLATE(new DeflateCodec()),
  GZIP(new GzipCodec()),
  LZ4(new Lz4Codec()),
  SNAPPY(new SnappyCodec());

  // TODO supports ZStandardCodec

  private final CompressionCodec compressionCodec;

  HadoopCompressionCodec(CompressionCodec compressionCodec) {
    this.compressionCodec = compressionCodec;
  }

  public CompressionCodec getCompressionCodec() {
    return this.compressionCodec;
  }

  private static final Map<String, String> codecNameMap =
    Arrays.stream(HadoopCompressionCodec.values()).collect(
      Collectors.toMap(Enum::name, codec -> codec.name().toLowerCase(Locale.ROOT)));

  public String lowerCaseName() {
    return codecNameMap.get(this.name());
  }
}
