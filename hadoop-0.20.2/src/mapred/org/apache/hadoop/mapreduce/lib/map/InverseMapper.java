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

package org.apache.hadoop.mapreduce.lib.map;

import java.io.IOException;

import org.apache.hadoop.mapreduce.Mapper;

/** A {@link Mapper} that swaps keys and values. */
public class InverseMapper<K, V> extends Mapper<K,V,V,K> {

  /** The inverse function.  Input keys and values are swapped.*/
  @Override
  public void map(K key, V value, Context context
                  ) throws IOException, InterruptedException {
    context.write(value, key);
  }
  
}
