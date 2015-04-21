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

package org.apache.spark.input;

import java.io.Closeable;
import java.io.IOException;

import org.apache.spark.annotation.Experimental;

/**
 * A reader that reads records in one by one.
 */
@Experimental
public abstract class RecordReader<T> implements Closeable {

  /**
   * Fetches the next record.
   *
   * @return true if we read a record.
   */
  public abstract boolean fetchNext() throws InterruptedException, IOException;

  /**
   * Returns the current record. [[fetchNext]] must have been called before this.
   */
  public abstract T get() throws InterruptedException, IOException;

  /**
   * Returns the progress of the iterator, as a number between 0.0 and 1.0.
   */
  public abstract double getProgress();

  /**
   * Closes the reader and releases all resources. This is guaranteed to be called once
   * during the life cycle of an object.
   */
  public abstract void close();
}
