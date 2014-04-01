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
package org.apache.hadoop.mapred.gridmix;

import java.io.Closeable;
import java.io.IOException;

/**
 * Interface for producing records as inputs and outputs to tasks.
 */
abstract class RecordFactory implements Closeable {

  /**
   * Transform the given record or perform some operation.
   * @return true if the record should be emitted.
   */
  public abstract boolean next(GridmixKey key, GridmixRecord val)
    throws IOException;

  /**
   * Estimate of exhausted record capacity.
   */
  public abstract float getProgress() throws IOException;

}
