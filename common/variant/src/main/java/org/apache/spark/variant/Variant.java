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

package org.apache.spark.variant;

/**
 * This class is structurally equivalent to {@link org.apache.spark.unsafe.types.VariantVal}. We
 * define a new class to avoid depending on or modifying Spark.
 */
public final class Variant {
  private final byte[] value;
  private final byte[] metadata;

  public Variant(byte[] value, byte[] metadata) {
    this.value = value;
    this.metadata = metadata;
  }

  public byte[] getValue() {
    return value;
  }

  public byte[] getMetadata() {
    return metadata;
  }
}
