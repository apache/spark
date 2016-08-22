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

package org.apache.spark.serializer;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.ByteBuffer;

import scala.reflect.ClassTag;

import org.apache.spark.annotation.Private;
import org.apache.spark.network.buffer.ChunkedByteBuffer;
import org.apache.spark.unsafe.Platform;

/**
 * Unfortunately, we need a serializer instance in order to construct a DiskBlockObjectWriter.
 * Our shuffle write path doesn't actually use this serializer (since we end up calling the
 * `write() OutputStream methods), but DiskBlockObjectWriter still calls some methods on it. To work
 * around this, we pass a dummy no-op serializer.
 */
@Private
public final class DummySerializerInstance extends SerializerInstance {

  public static final DummySerializerInstance INSTANCE = new DummySerializerInstance();

  private DummySerializerInstance() { }

  @Override
  public SerializationStream serializeStream(final OutputStream s) {
    return new SerializationStream() {
      @Override
      public void flush() {
        // Need to implement this because DiskObjectWriter uses it to flush the compression stream
        try {
          s.flush();
        } catch (IOException e) {
          Platform.throwException(e);
        }
      }

      @Override
      public <T> SerializationStream writeObject(T t, ClassTag<T> ev1) {
        throw new UnsupportedOperationException();
      }

      @Override
      public void close() {
        // Need to implement this because DiskObjectWriter uses it to close the compression stream
        try {
          s.close();
        } catch (IOException e) {
          Platform.throwException(e);
        }
      }
    };
  }

  @Override
  public <T> ChunkedByteBuffer serialize(T t, ClassTag<T> ev1) {
    throw new UnsupportedOperationException();
  }

  @Override
  public DeserializationStream deserializeStream(InputStream s) {
    throw new UnsupportedOperationException();
  }

  @Override
  public <T> T deserialize(InputStream bytes, ClassLoader loader, ClassTag<T> ev1) {
    throw new UnsupportedOperationException();
  }

  @Override
  public <T> T deserialize(InputStream bytes, ClassTag<T> ev1) {
    throw new UnsupportedOperationException();
  }
}
