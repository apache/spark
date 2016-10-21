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

import java.io.InputStream;
import java.io.OutputStream;
import java.nio.ByteBuffer;

import scala.reflect.ClassTag;


/**
 * A simple Serializer implementation to make sure the API is Java-friendly.
 */
class TestJavaSerializerImpl extends Serializer {

  @Override
  public SerializerInstance newInstance() {
    return null;
  }

  static class SerializerInstanceImpl extends SerializerInstance {
      @Override
      public <T> ByteBuffer serialize(T t, ClassTag<T> evidence$1) {
        return null;
      }

      @Override
    public <T> T deserialize(ByteBuffer bytes, ClassLoader loader, ClassTag<T> evidence$1) {
      return null;
    }

    @Override
    public <T> T deserialize(ByteBuffer bytes, ClassTag<T> evidence$1) {
      return null;
    }

    @Override
    public SerializationStream serializeStream(OutputStream s) {
      return null;
    }

    @Override
    public DeserializationStream deserializeStream(InputStream s) {
      return null;
    }
  }

  static class SerializationStreamImpl extends SerializationStream {

    @Override
    public <T> SerializationStream writeObject(T t, ClassTag<T> evidence$1) {
      return null;
    }

    @Override
    public void flush() {

    }

    @Override
    public void close() {

    }
  }

  static class DeserializationStreamImpl extends DeserializationStream {

    @Override
    public <T> T readObject(ClassTag<T> evidence$1) {
      return null;
    }

    @Override
    public void close() {

    }
  }
}
