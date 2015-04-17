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

package org.apache.spark.unsafe.string;

import org.apache.spark.unsafe.memory.MemoryLocation;

/**
 * A pointer to UTF8String data.
 */
public class UTF8StringPointer extends MemoryLocation {

  public long getLengthInBytes() { return UTF8StringMethods.getLengthInBytes(obj, offset); }

  public String toJavaString() { return UTF8StringMethods.toJavaString(obj, offset); }

  @Override public String toString() { return toJavaString(); }
}
