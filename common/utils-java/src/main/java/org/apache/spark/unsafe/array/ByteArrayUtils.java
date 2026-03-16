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

package org.apache.spark.unsafe.array;

public class ByteArrayUtils {
  // Some JVMs can't allocate arrays of length Integer.MAX_VALUE; actual max is somewhat smaller.
  // Be conservative and lower the cap a little.
  // Refer to "http://hg.openjdk.java.net/jdk8/jdk8/jdk/file/tip/src/share/classes/java/util/ArrayList.java#l229"
  // This value is word rounded. Use this value if the allocated byte arrays are used to store other
  // types rather than bytes.
  public static final int MAX_ROUNDED_ARRAY_LENGTH = Integer.MAX_VALUE - 15;
}
