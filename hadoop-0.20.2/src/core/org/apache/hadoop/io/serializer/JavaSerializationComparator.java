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

package org.apache.hadoop.io.serializer;

import java.io.IOException;
import java.io.Serializable;

import org.apache.hadoop.io.RawComparator;

/**
 * <p>
 * A {@link RawComparator} that uses a {@link JavaSerialization}
 * {@link Deserializer} to deserialize objects that are then compared via
 * their {@link Comparable} interfaces.
 * </p>
 * @param <T>
 * @see JavaSerialization
 */
public class JavaSerializationComparator<T extends Serializable&Comparable<T>>
  extends DeserializerComparator<T> {

  public JavaSerializationComparator() throws IOException {
    super(new JavaSerialization.JavaSerializationDeserializer<T>());
  }

  public int compare(T o1, T o2) {
    return o1.compareTo(o2);
  }

}
