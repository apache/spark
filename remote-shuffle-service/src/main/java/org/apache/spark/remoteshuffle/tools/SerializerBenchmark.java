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

package org.apache.spark.remoteshuffle.tools;

import org.apache.commons.lang3.StringUtils;
import org.apache.spark.SparkConf;
import org.apache.spark.serializer.DeserializationStream;
import org.apache.spark.serializer.KryoSerializer;
import org.apache.spark.serializer.SerializationStream;
import org.apache.spark.serializer.SerializerInstance;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import scala.reflect.ClassTag;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.EOFException;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Random;
import java.util.concurrent.TimeUnit;

/**
 * This tool benchmark the performance between following two approaches of serialization:
 * 1. Serialize each individual object and write to output stream
 * 2. Create a serialize stream and write each object into that stream
 */
public class SerializerBenchmark {
  private static final Logger logger = LoggerFactory.getLogger(SerializerBenchmark.class);

  private SerializerInstance serializer;

  // Total number of test objects to serialize
  private int numTestObjects = 10000;

  // Max length for test string values to uses
  private int maxStringValueLen = 10000;

  private List<HashMap<String, String>> testObjects = new ArrayList<>();

  private ByteArrayOutputStream byteArrayOutputStream;
  private byte[] bytesBuffer;

  private Random random = new Random();

  public void setSerializer(SerializerInstance serializer) {
    this.serializer = serializer;
  }

  public void prepare() {
    for (int i = 0; i < numTestObjects; i++) {
      char ch = (char) ('a' + random.nextInt(26));
      int repeats = random.nextInt(maxStringValueLen);
      String str1 = StringUtils.repeat(ch, repeats);
      String str2 = StringUtils.repeat(ch, repeats);
      HashMap<String, String> map = new HashMap<>();
      map.put(str1, str2);
      testObjects.add(map);
    }

    int objectSerializedSizeEstimate = maxStringValueLen * 4;
    byteArrayOutputStream =
        new ByteArrayOutputStream(testObjects.size() * objectSerializedSizeEstimate);
    bytesBuffer = new byte[objectSerializedSizeEstimate];
  }

  public void run(int numIterations) {
    ClassTag<HashMap<String, String>> classTag =
        scala.reflect.ClassTag$.MODULE$.apply(HashMap.class);

    long startTime = System.nanoTime();
    long serializedTotalBytes = 0;

    for (int i = 0; i < numIterations; i++) {
      byteArrayOutputStream.reset();

      // serialize objects and write to output stream

      for (HashMap<String, String> v : testObjects) {
        // serialize object
        ByteBuffer byteBuffer = serializer.serialize(v, classTag);

        // write object size to output stream
        int objectSize = byteBuffer.remaining();
        int byte1 = objectSize & 0xFF;
        int byte2 = (objectSize >> 8) & 0xFF;
        int byte3 = (objectSize >> 8 * 2) & 0xFF;
        int byte4 = (objectSize >> 8 * 3) & 0xFF;
        byteArrayOutputStream.write(byte1);
        byteArrayOutputStream.write(byte2);
        byteArrayOutputStream.write(byte3);
        byteArrayOutputStream.write(byte4);

        // write serialized bytes to output stream
        while (byteBuffer.remaining() > 0) {
          int num = Math.min(byteBuffer.remaining(), bytesBuffer.length);
          byteBuffer.get(bytesBuffer, 0, num);
          byteArrayOutputStream.write(bytesBuffer, 0, num);
        }
      }

      try {
        byteArrayOutputStream.flush();
        byteArrayOutputStream.close();
      } catch (IOException e) {
        throw new RuntimeException(e);
      }

      // read objects and deserialize

      byte[] streamBytes = byteArrayOutputStream.toByteArray();
      serializedTotalBytes += streamBytes.length;

      int numReadObjects = 0;
      ByteArrayInputStream byteArrayInputStream = new ByteArrayInputStream(streamBytes);
      int byte1 = byteArrayInputStream.read();
      while (byte1 != -1) {
        int byte2 = byteArrayInputStream.read();
        int byte3 = byteArrayInputStream.read();
        int byte4 = byteArrayInputStream.read();

        int objectSize = byte1 | byte2 << 8 | byte3 << 8 * 2 | byte4 << 8 * 3;

        if (bytesBuffer.length < objectSize) {
          throw new RuntimeException("Too small byte array size");
        }

        byteArrayInputStream.read(bytesBuffer, 0, objectSize);

        HashMap<String, String> object =
            serializer.deserialize(ByteBuffer.wrap(bytesBuffer), classTag);
        if (object == null) {
          throw new RuntimeException("Got null after deserialize");
        }

        numReadObjects++;

        byte1 = byteArrayInputStream.read();
      }

      try {
        byteArrayInputStream.close();
      } catch (IOException e) {
        throw new RuntimeException(e);
      }

      if (numReadObjects != testObjects.size()) {
        throw new RuntimeException("Invalid number of objects");
      }
    }

    long singleSerializeTotalTime = System.nanoTime() - startTime;
    long singleSerializeTotalBytes = serializedTotalBytes;

    startTime = System.nanoTime();
    serializedTotalBytes = 0;

    for (int i = 0; i < numIterations; i++) {
      byteArrayOutputStream.reset();

      // create serialize stream and write objects into it

      SerializationStream serializationStream = serializer.serializeStream(byteArrayOutputStream);
      for (HashMap<String, String> v : testObjects) {
        serializationStream.writeObject(v, classTag);
      }
      serializationStream.flush();
      serializationStream.close();

      byte[] streamBytes = byteArrayOutputStream.toByteArray();
      serializedTotalBytes += streamBytes.length;

      // create deserialize stream and read objects from it

      int numReadObjects = 0;
      ByteArrayInputStream byteArrayInputStream = new ByteArrayInputStream(streamBytes);
      DeserializationStream deserializationStream =
          serializer.deserializeStream(byteArrayInputStream);
      HashMap<String, String> object = deserializationStream.readObject(classTag);
      while (object != null) {
        numReadObjects++;
        try {
          object = deserializationStream.readObject(classTag);
        } catch (Throwable ex) {
          if (ex instanceof EOFException) {
            break;
          } else {
            throw ex;
          }
        }
      }

      try {
        byteArrayInputStream.close();
      } catch (IOException e) {
        throw new RuntimeException(e);
      }

      if (numReadObjects != testObjects.size()) {
        throw new RuntimeException("Invalid number of objects");
      }
    }

    long streamSerializeTotalTime = System.nanoTime() - startTime;
    long streamSerializeTotalBytes = serializedTotalBytes;

    logger.info(String.format(
        "Number of objects: %s, single serialize total seconds: %s (%s bytes), stream serialize total seconds: %s (%s bytes)",
        testObjects.size(),
        TimeUnit.NANOSECONDS.toSeconds(singleSerializeTotalTime),
        singleSerializeTotalBytes,
        TimeUnit.NANOSECONDS.toSeconds(streamSerializeTotalTime),
        streamSerializeTotalBytes));
  }

  public static void main(String[] args) {
    SparkConf conf = new SparkConf();
    KryoSerializer serializer = new KryoSerializer(conf);
    SerializerInstance serializerInstance = serializer.newInstance();

    SerializerBenchmark benchmark = new SerializerBenchmark();

    benchmark.setSerializer(serializerInstance);

    benchmark.prepare();

    benchmark.run(100);
    benchmark.run(100);
  }
}
