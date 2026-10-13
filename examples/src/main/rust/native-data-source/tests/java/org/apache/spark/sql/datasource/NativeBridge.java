/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.spark.sql.datasource;

/** Minimal test fixture with the batch-read signatures of NativeBridge ABI version 1. */
public final class NativeBridge {
  private NativeBridge() {}

  public static void load(String path) {
    System.load(path);
  }

  public static native int abiVersion();

  public static native long createDataSource(String name, String[] keys, String[] values);

  public static native void schema(long source, long schemaAddress);

  public static native long createReader(long source, long schemaAddress);

  public static native void closeDataSource(long source);

  public static native boolean[] pushPredicates(long reader, String[] predicates);

  public static native boolean pushLimit(long reader, int limit);

  public static native boolean pruneColumns(long reader, String[] columns);

  public static native byte[][] partitions(long reader);

  public static native byte[] serializeReader(long reader);

  public static native void closeReader(long reader);

  public static native void read(byte[] state, byte[] partition, long streamAddress);
}
