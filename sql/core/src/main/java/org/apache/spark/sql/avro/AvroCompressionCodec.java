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

package org.apache.spark.sql.avro;

import java.util.Arrays;
import java.util.EnumMap;
import java.util.Locale;
import java.util.stream.Collectors;

import org.apache.avro.file.DataFileConstants;

/**
 * A mapper class from Spark supported avro compression codecs to avro compression codecs.
 */
public enum AvroCompressionCodec {
  UNCOMPRESSED(DataFileConstants.NULL_CODEC, false),
  DEFLATE(DataFileConstants.DEFLATE_CODEC, true),
  SNAPPY(DataFileConstants.SNAPPY_CODEC, false),
  BZIP2(DataFileConstants.BZIP2_CODEC, false),
  XZ(DataFileConstants.XZ_CODEC, true),
  ZSTANDARD(DataFileConstants.ZSTANDARD_CODEC, true);

  private final String codecName;
  private final boolean supportCompressionLevel;

  AvroCompressionCodec(
      String codecName,
      boolean supportCompressionLevel) {
    this.codecName = codecName;
    this.supportCompressionLevel = supportCompressionLevel;
  }

  public String getCodecName() {
    return this.codecName;
  }

  public boolean getSupportCompressionLevel() {
    return this.supportCompressionLevel;
  }

  public static AvroCompressionCodec fromString(String s) {
    return AvroCompressionCodec.valueOf(s.toUpperCase(Locale.ROOT));
  }

  private static final EnumMap<AvroCompressionCodec, String> codecNameMap =
    Arrays.stream(AvroCompressionCodec.values()).collect(
      Collectors.toMap(
        codec -> codec,
        codec -> codec.name().toLowerCase(Locale.ROOT),
        (oldValue, newValue) -> oldValue,
        () -> new EnumMap<>(AvroCompressionCodec.class)));

  public String lowerCaseName() {
    return codecNameMap.get(this);
  }
}
