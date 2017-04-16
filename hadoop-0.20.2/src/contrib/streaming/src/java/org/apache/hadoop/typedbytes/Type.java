/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hadoop.typedbytes;

/**
 * The possible type codes.
 */
public enum Type {

  // codes for supported types (< 50):
  BYTES(0),
  BYTE(1),
  BOOL(2),
  INT(3),
  LONG(4),
  FLOAT(5),
  DOUBLE(6),
  STRING(7),
  VECTOR(8),
  LIST(9),
  MAP(10),
  
  // application-specific codes (50-200):
  WRITABLE(50),
  
  // low-level codes (> 200):
  MARKER(255);

  final int code;

  Type(int code) {
    this.code = code;
  }
}