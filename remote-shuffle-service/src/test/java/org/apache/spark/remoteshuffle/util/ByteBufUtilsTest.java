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

package org.apache.spark.remoteshuffle.util;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.PooledByteBufAllocator;
import org.testng.Assert;
import org.testng.annotations.Test;
import scala.Int;

public class ByteBufUtilsTest {

  @Test
  public void convertIntToBytes() {
    byte[] bytes = ByteBufUtils.convertIntToBytes(1);
    Assert.assertEquals(bytes.length, 4);
    Assert.assertEquals(bytes, new byte[]{0, 0, 0, 1});

    bytes = ByteBufUtils.convertIntToBytes(Int.MinValue());
    Assert.assertEquals(bytes.length, 4);

    ByteBuf buf = PooledByteBufAllocator.DEFAULT.buffer(4);
    buf.writeBytes(bytes);

    Assert.assertEquals(buf.readInt(), Int.MinValue());

    bytes = ByteBufUtils.convertIntToBytes(Int.MaxValue());
    Assert.assertEquals(bytes.length, 4);

    buf = PooledByteBufAllocator.DEFAULT.buffer(4);
    buf.writeBytes(bytes);

    Assert.assertEquals(buf.readInt(), Int.MaxValue());

    buf.release();
  }
}
