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

package org.apache.hadoop.contrib.utils.join;

import java.io.IOException;
import java.util.Iterator;

/**
 * This defines an iterator interface that will help the reducer class
 * re-group its input by source tags. Once the values are re-grouped,
 * the reducer will receive the cross product of values from different groups.
 */
public interface ResetableIterator extends Iterator {
  public void reset();

  public void add(Object item);

  public void close() throws IOException;
}
