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

package org.apache.spark.network.buffer;

/**
 * An {@link IllegalStateException} which is raised when a user attempts to access a {@link ReferenceCounted} whose
 * reference count has been decreased to 0 (and consequently freed).
 */
public class IllegalReferenceCountException extends IllegalStateException {

  private static final long serialVersionUID = -2507492394288153468L;

  public IllegalReferenceCountException() { }

  public IllegalReferenceCountException(int refCnt) {
    this("refCnt: " + refCnt);
  }

  public IllegalReferenceCountException(int refCnt, int increment) {
    this("refCnt: " + refCnt + ", " + (increment > 0? "increment: " + increment : "decrement: " + -increment));
  }

  public IllegalReferenceCountException(String message) {
    super(message);
  }

  public IllegalReferenceCountException(String message, Throwable cause) {
    super(message, cause);
  }

  public IllegalReferenceCountException(Throwable cause) {
    super(cause);
  }
}
