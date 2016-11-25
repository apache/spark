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
 * A reference-counted object that requires explicit deallocation.
 * <p>
 * When a new {@link org.apache.spark.network.buffer.ReferenceCounted} is instantiated, it starts with the reference count of {@code 1}.
 * {@link #retain()} increases the reference count, and {@link #release()} decreases the reference count.
 * If the reference count is decreased to {@code 0}, the object will be deallocated explicitly, and accessing
 * the deallocated object will usually result in an access violation.
 * </p>
 * <p>
 * If an object that implements {@link org.apache.spark.network.buffer.ReferenceCounted} is a container of other objects that implement
 * {@link org.apache.spark.network.buffer.ReferenceCounted}, the contained objects will also be released via {@link #release()} when the container's
 * reference count becomes 0.
 * </p>
 */
public interface ReferenceCounted {
  /**
   * Returns the reference count of this object.  If {@code 0}, it means this object has been deallocated.
   */
  int refCnt();

  /**
   * Increases the reference count by {@code 1}.
   */
  ReferenceCounted retain();

  /**
   * Increases the reference count by the specified {@code increment}.
   */
  ReferenceCounted retain(int increment);

  /**
   * Decreases the reference count by {@code 1} and deallocates this object if the reference count reaches at
   * {@code 0}.
   *
   * @return {@code true} if and only if the reference count became {@code 0} and this object has been deallocated
   */
  boolean release();

  /**
   * Decreases the reference count by the specified {@code decrement} and deallocates this object if the reference
   * count reaches at {@code 0}.
   *
   * @return {@code true} if and only if the reference count became {@code 0} and this object has been deallocated
   */
  boolean release(int decrement);
}
