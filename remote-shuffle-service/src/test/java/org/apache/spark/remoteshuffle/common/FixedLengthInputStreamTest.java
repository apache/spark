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

package org.apache.spark.remoteshuffle.common;

import org.apache.spark.remoteshuffle.exceptions.RssEndOfStreamException;
import org.testng.Assert;
import org.testng.annotations.Test;

import java.io.ByteArrayInputStream;
import java.io.IOException;

public class FixedLengthInputStreamTest {
  @Test
  public void readEmptyStream() throws IOException {
    ByteArrayInputStream byteArrayInputStream = new ByteArrayInputStream(new byte[0]);
    FixedLengthInputStream fixedLengthInputStream =
        new FixedLengthInputStream(byteArrayInputStream, 0);
    Assert.assertEquals(fixedLengthInputStream.read(), -1);
    Assert.assertEquals(fixedLengthInputStream.read(), -1);

    byteArrayInputStream = new ByteArrayInputStream(new byte[0]);
    fixedLengthInputStream = new FixedLengthInputStream(byteArrayInputStream, 0);
    byte[] bytes = new byte[0];
    Assert.assertEquals(fixedLengthInputStream.read(bytes), 0);

    byteArrayInputStream = new ByteArrayInputStream(new byte[0]);
    fixedLengthInputStream = new FixedLengthInputStream(byteArrayInputStream, 0);
    bytes = new byte[1];
    Assert.assertEquals(fixedLengthInputStream.read(bytes), -1);

    byteArrayInputStream = new ByteArrayInputStream(new byte[]{(byte) 9});
    fixedLengthInputStream = new FixedLengthInputStream(byteArrayInputStream, 0);
    Assert.assertEquals(fixedLengthInputStream.read(), -1);
    Assert.assertEquals(fixedLengthInputStream.read(), -1);

    byteArrayInputStream = new ByteArrayInputStream(new byte[]{(byte) 9});
    fixedLengthInputStream = new FixedLengthInputStream(byteArrayInputStream, 0);
    bytes = new byte[0];
    Assert.assertEquals(fixedLengthInputStream.read(bytes), 0);

    byteArrayInputStream = new ByteArrayInputStream(new byte[]{(byte) 9});
    fixedLengthInputStream = new FixedLengthInputStream(byteArrayInputStream, 0);
    bytes = new byte[1];
    Assert.assertEquals(fixedLengthInputStream.read(bytes), -1);
  }

  @Test
  public void readOneByteStream() throws IOException {
    ByteArrayInputStream byteArrayInputStream = new ByteArrayInputStream(new byte[]{(byte) 9});
    FixedLengthInputStream fixedLengthInputStream =
        new FixedLengthInputStream(byteArrayInputStream, 1);
    Assert.assertEquals(fixedLengthInputStream.read(), 9);
    Assert.assertEquals(fixedLengthInputStream.read(), -1);

    byteArrayInputStream = new ByteArrayInputStream(new byte[]{(byte) 9});
    fixedLengthInputStream = new FixedLengthInputStream(byteArrayInputStream, 1);

    byte[] bytes = new byte[0];
    Assert.assertEquals(fixedLengthInputStream.read(bytes), 0);

    byteArrayInputStream = new ByteArrayInputStream(new byte[]{(byte) 9});
    fixedLengthInputStream = new FixedLengthInputStream(byteArrayInputStream, 1);

    bytes = new byte[1];
    Assert.assertEquals(fixedLengthInputStream.read(bytes), 1);

    byteArrayInputStream = new ByteArrayInputStream(new byte[]{(byte) 9});
    fixedLengthInputStream = new FixedLengthInputStream(byteArrayInputStream, 1);

    bytes = new byte[2];
    Assert.assertEquals(fixedLengthInputStream.read(bytes), 1);
  }

  @Test
  public void readTwoByteStream() throws IOException {
    ByteArrayInputStream byteArrayInputStream =
        new ByteArrayInputStream(new byte[]{(byte) 9, (byte) 10});
    FixedLengthInputStream fixedLengthInputStream =
        new FixedLengthInputStream(byteArrayInputStream, 1);

    Assert.assertEquals(fixedLengthInputStream.getLength(), 1);
    Assert.assertEquals(fixedLengthInputStream.getRemaining(), 1);

    Assert.assertEquals(fixedLengthInputStream.read(), 9);
    Assert.assertEquals(fixedLengthInputStream.getRemaining(), 0);
    Assert.assertEquals(fixedLengthInputStream.read(), -1);
    Assert.assertEquals(fixedLengthInputStream.getRemaining(), 0);

    Assert.assertEquals(fixedLengthInputStream.getLength(), 1);

    byteArrayInputStream = new ByteArrayInputStream(new byte[]{(byte) 9});
    fixedLengthInputStream = new FixedLengthInputStream(byteArrayInputStream, 1);

    byte[] bytes = new byte[0];
    Assert.assertEquals(fixedLengthInputStream.read(bytes), 0);
    Assert.assertEquals(fixedLengthInputStream.getRemaining(), 1);

    byteArrayInputStream = new ByteArrayInputStream(new byte[]{(byte) 9});
    fixedLengthInputStream = new FixedLengthInputStream(byteArrayInputStream, 1);

    bytes = new byte[1];
    Assert.assertEquals(fixedLengthInputStream.read(bytes), 1);
    Assert.assertEquals(fixedLengthInputStream.getRemaining(), 0);

    byteArrayInputStream = new ByteArrayInputStream(new byte[]{(byte) 9});
    fixedLengthInputStream = new FixedLengthInputStream(byteArrayInputStream, 1);

    bytes = new byte[2];
    Assert.assertEquals(fixedLengthInputStream.read(bytes), 1);
    Assert.assertEquals(bytes[0], 9);
    Assert.assertEquals(fixedLengthInputStream.getRemaining(), 0);

    byteArrayInputStream = new ByteArrayInputStream(new byte[]{(byte) 9, (byte) 10});
    fixedLengthInputStream = new FixedLengthInputStream(byteArrayInputStream, 2);

    Assert.assertEquals(fixedLengthInputStream.getLength(), 2);
    Assert.assertEquals(fixedLengthInputStream.getRemaining(), 2);

    Assert.assertEquals(fixedLengthInputStream.read(), 9);
    Assert.assertEquals(fixedLengthInputStream.getRemaining(), 1);
    Assert.assertEquals(fixedLengthInputStream.read(), 10);
    Assert.assertEquals(fixedLengthInputStream.getRemaining(), 0);
    Assert.assertEquals(fixedLengthInputStream.read(), -1);
    Assert.assertEquals(fixedLengthInputStream.getRemaining(), 0);

    Assert.assertEquals(fixedLengthInputStream.getLength(), 2);

    byteArrayInputStream = new ByteArrayInputStream(new byte[]{(byte) 9, (byte) 10});
    fixedLengthInputStream = new FixedLengthInputStream(byteArrayInputStream, 2);

    bytes = new byte[0];
    Assert.assertEquals(fixedLengthInputStream.read(bytes), 0);
    Assert.assertEquals(fixedLengthInputStream.getRemaining(), 2);

    byteArrayInputStream = new ByteArrayInputStream(new byte[]{(byte) 9, (byte) 10});
    fixedLengthInputStream = new FixedLengthInputStream(byteArrayInputStream, 1);

    bytes = new byte[1];
    Assert.assertEquals(fixedLengthInputStream.read(bytes), 1);
    Assert.assertEquals(bytes[0], 9);
    Assert.assertEquals(fixedLengthInputStream.getRemaining(), 0);

    byteArrayInputStream = new ByteArrayInputStream(new byte[]{(byte) 9, (byte) 10});
    fixedLengthInputStream = new FixedLengthInputStream(byteArrayInputStream, 2);

    bytes = new byte[2];
    Assert.assertEquals(fixedLengthInputStream.read(bytes), 2);
    Assert.assertEquals(bytes[0], 9);
    Assert.assertEquals(bytes[1], 10);
    Assert.assertEquals(fixedLengthInputStream.getRemaining(), 0);

    bytes = new byte[2];
    Assert.assertEquals(fixedLengthInputStream.read(bytes), -1);
  }

  @Test(expectedExceptions = RssEndOfStreamException.class)
  public void readEmptyStreamWithOneLength_read() throws IOException {
    ByteArrayInputStream byteArrayInputStream = new ByteArrayInputStream(new byte[0]);
    FixedLengthInputStream fixedLengthInputStream =
        new FixedLengthInputStream(byteArrayInputStream, 1);
    fixedLengthInputStream.read();
  }

  @Test(expectedExceptions = RssEndOfStreamException.class)
  public void readEmptyStreamWithOneLength_readByteArray1() throws IOException {
    ByteArrayInputStream byteArrayInputStream = new ByteArrayInputStream(new byte[0]);
    FixedLengthInputStream fixedLengthInputStream =
        new FixedLengthInputStream(byteArrayInputStream, 1);
    fixedLengthInputStream.read(new byte[1]);
  }

  @Test(expectedExceptions = RssEndOfStreamException.class)
  public void readEmptyStreamWithOneLength_readByteArray2() throws IOException {
    ByteArrayInputStream byteArrayInputStream = new ByteArrayInputStream(new byte[]{(byte) 9});
    FixedLengthInputStream fixedLengthInputStream =
        new FixedLengthInputStream(byteArrayInputStream, 2);
    Assert.assertEquals(fixedLengthInputStream.read(), 9);
    fixedLengthInputStream.read(new byte[1]);
  }
}
