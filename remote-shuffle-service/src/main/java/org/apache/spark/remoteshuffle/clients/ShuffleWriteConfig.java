/*
 * This file is copied from Uber Remote Shuffle Service
(https://github.com/uber/RemoteShuffleService) and modified.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *     http://www.apache.org/licenses/LICENSE-2.0
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.spark.remoteshuffle.clients;

import java.util.Objects;

public class ShuffleWriteConfig {

  private final short numSplits;

  public ShuffleWriteConfig() {
    this((short) 1);
  }

  public ShuffleWriteConfig(short numSplits) {
    this.numSplits = numSplits;
  }

  public short getNumSplits() {
    return numSplits;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) return true;
    if (o == null || getClass() != o.getClass()) return false;
    ShuffleWriteConfig that = (ShuffleWriteConfig) o;
    return numSplits == that.numSplits;
  }

  @Override
  public int hashCode() {
    return Objects.hash(numSplits);
  }

  @Override
  public String toString() {
    return "ShuffleWriteConfig{" +
        ", numSplits=" + numSplits +
        '}';
  }
}
