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

package org.apache.spark.launcher;

import java.io.InputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectStreamClass;
import java.util.Arrays;
import java.util.List;

/**
 * An object input stream that only allows classes used by the launcher protocol to be in the
 * serialized stream. See SPARK-20922.
 */
class FilteredObjectInputStream extends ObjectInputStream {

  private static final List<String> ALLOWED_PACKAGES = Arrays.asList(
    "org.apache.spark.launcher.",
    "java.lang.");

  FilteredObjectInputStream(InputStream is) throws IOException {
    super(is);
  }

  @Override
  protected Class<?> resolveClass(ObjectStreamClass desc)
      throws IOException, ClassNotFoundException {

    boolean isValid = ALLOWED_PACKAGES.stream().anyMatch(p -> desc.getName().startsWith(p));
    if (!isValid) {
      throw new IllegalArgumentException(
        String.format("Unexpected class in stream: %s", desc.getName()));
    }
    return super.resolveClass(desc);
  }

}
