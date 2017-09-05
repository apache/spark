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

package org.apache.spark.sql.sources.v2.reader;

import java.io.Serializable;

/**
 * A read task returned by a data source reader and is responsible to create the data reader.
 * The relationship between `ReadTask` and `DataReader` is similar to `Iterable` and `Iterator`.
 *
 * Note that, the read task will be serialized and sent to executors, then the data reader will be
 * created on executors and do the actual reading.
 */
public interface ReadTask<T> extends Serializable {

  /**
   * The preferred locations where this read task can run faster, but Spark does not guarantee that
   * this task will always run on these locations. The implementations should make sure that it can
   * be run on any location. The location is a string representing the host name of an executor.
   */
  default String[] preferredLocations() {
    return new String[0];
  }

  /**
   * Returns a data reader to do the actual reading work for this read task.
   */
  DataReader<T> createReader();
}
