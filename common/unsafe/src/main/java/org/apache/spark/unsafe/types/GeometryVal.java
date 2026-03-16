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

package org.apache.spark.unsafe.types;

import org.apache.spark.annotation.Unstable;

import java.io.Serializable;

// This class represents the physical type for the GEOMETRY data type.
@Unstable
public final class GeometryVal implements Comparable<GeometryVal>, Serializable {

  // The GEOMETRY type is implemented as a byte array. We provide `getBytes` and `fromBytes`
  // methods for readers and writers to access this underlying array of bytes.
  private final byte[] value;

  // We make the constructor private. We should use `fromBytes` to create new instances.
  private GeometryVal(byte[] value) {
    this.value = value;
  }

  public byte[] getBytes() {
    return value;
  }

  public static GeometryVal fromBytes(byte[] bytes) {
    if (bytes == null) {
      return null;
    } else {
      return new GeometryVal(bytes);
    }
  }

  // Comparison is not yet supported for GEOMETRY.
  public int compareTo(GeometryVal g) {
    throw new UnsupportedOperationException();
  }
}
