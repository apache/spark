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
package org.apache.hadoop.io;

import java.io.IOException;
import java.util.List;

/** Encapsulate a list of {@link IOException} into an {@link IOException} */
public class MultipleIOException extends IOException {
  /** Require by {@link java.io.Serializable} */
  private static final long serialVersionUID = 1L;
  
  private final List<IOException> exceptions;
  
  /** Constructor is private, use {@link #createIOException(List)}. */
  private MultipleIOException(List<IOException> exceptions) {
    super(exceptions.size() + " exceptions " + exceptions);
    this.exceptions = exceptions;
  }

  /** @return the underlying exceptions */
  public List<IOException> getExceptions() {return exceptions;}

  /** A convenient method to create an {@link IOException}. */
  public static IOException createIOException(List<IOException> exceptions) {
    if (exceptions == null || exceptions.isEmpty()) {
      return null;
    }
    if (exceptions.size() == 1) {
      return exceptions.get(0);
    }
    return new MultipleIOException(exceptions);
  }
}
