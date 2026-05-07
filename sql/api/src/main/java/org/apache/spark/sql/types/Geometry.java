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

package org.apache.spark.sql.types;

import org.apache.spark.annotation.Unstable;

import java.io.Serializable;
import java.util.Arrays;

// This class represents the Geometry data for clients.
@Unstable
public final class Geometry implements Serializable {
  // The GEOMETRY type is implemented as WKB bytes + SRID integer stored in class itself.
  protected final byte[] value;

  protected final int srid;

  // We make the constructor private. We should use `fromWKB` to create new instances.
  private Geometry(byte[] value, int srid) {
    this.value = value;
    this.srid = srid;
  }

  // We provide `getBytes` and `fromBytes` methods to access GEOMETRY data.

  public byte[] getBytes() {
    return value;
  }

  public int getSrid() {
    return srid;
  }

  // Default SRID value for the Geometry client class.
  public static final int DEFAULT_SRID = 0;

  // Factory methods to create new Geometry instances from WKB bytes and optional SRID.

  public static Geometry fromWKB(byte[] bytes, int srid) {
    return new Geometry(bytes, srid);
  }

  public static Geometry fromWKB(byte[] bytes) {
    return new Geometry(bytes, DEFAULT_SRID);
  }

  // Overrides for `equals` and `hashCode` methods.

  @Override
  public boolean equals(Object obj) {
    if (this == obj) {
      return true;
    }
    if (obj == null || getClass() != obj.getClass()) {
      return false;
    }
    Geometry other = (Geometry) obj;
    return srid == other.srid && java.util.Arrays.equals(value, other.value);
  }

  @Override
  public int hashCode() {
    return 31 * Arrays.hashCode(value) + Integer.hashCode(srid);
  }
}
