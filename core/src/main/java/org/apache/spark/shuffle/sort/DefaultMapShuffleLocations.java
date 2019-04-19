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

package org.apache.spark.shuffle.sort;

import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;

import org.apache.spark.api.shuffle.MapShuffleLocations;
import org.apache.spark.api.shuffle.ShuffleLocation;
import org.apache.spark.storage.BlockManagerId;

import java.util.Objects;

public class DefaultMapShuffleLocations implements MapShuffleLocations, ShuffleLocation {

  /**
   * We borrow the cache size from the BlockManagerId's cache - around 1MB, which should be
   * feasible.
   */
  private static final LoadingCache<BlockManagerId, DefaultMapShuffleLocations>
      DEFAULT_SHUFFLE_LOCATIONS_CACHE =
          CacheBuilder.newBuilder()
              .maximumSize(BlockManagerId.blockManagerIdCacheSize())
              .build(new CacheLoader<BlockManagerId, DefaultMapShuffleLocations>() {
                @Override
                public DefaultMapShuffleLocations load(BlockManagerId blockManagerId) {
                    return new DefaultMapShuffleLocations(blockManagerId);
                }
              });

  private final BlockManagerId location;

  public DefaultMapShuffleLocations(BlockManagerId blockManagerId) {
    this.location = blockManagerId;
  }

  public static DefaultMapShuffleLocations get(BlockManagerId blockManagerId) {
    return DEFAULT_SHUFFLE_LOCATIONS_CACHE.getUnchecked(blockManagerId);
  }

  @Override
  public ShuffleLocation getLocationForBlock(int reduceId) {
    return this;
  }

  public BlockManagerId getBlockManagerId() {
    return location;
  }

  @Override
  public boolean equals(Object other) {
    return other instanceof DefaultMapShuffleLocations
        && Objects.equals(((DefaultMapShuffleLocations) other).location, location);
  }

  @Override
  public int hashCode() {
    return Objects.hashCode(location);
  }
}
