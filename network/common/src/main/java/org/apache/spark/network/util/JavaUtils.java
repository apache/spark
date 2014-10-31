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

package org.apache.spark.network.util;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.Closeable;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;

import com.google.common.io.Closeables;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class JavaUtils {
  private static final Logger logger = LoggerFactory.getLogger(JavaUtils.class);

  /** Closes the given object, ignoring IOExceptions. */
  public static void closeQuietly(Closeable closeable) {
    try {
      closeable.close();
    } catch (IOException e) {
      logger.error("IOException should not have been thrown.", e);
    }
  }

  // TODO: Make this configurable, do not use Java serialization!
  public static <T> T deserialize(byte[] bytes) {
    try {
      ObjectInputStream is = new ObjectInputStream(new ByteArrayInputStream(bytes));
      Object out = is.readObject();
      is.close();
      return (T) out;
    } catch (ClassNotFoundException e) {
      throw new RuntimeException("Could not deserialize object", e);
    } catch (IOException e) {
      throw new RuntimeException("Could not deserialize object", e);
    }
  }

  // TODO: Make this configurable, do not use Java serialization!
  public static byte[] serialize(Object object) {
    try {
      ByteArrayOutputStream baos = new ByteArrayOutputStream();
      ObjectOutputStream os = new ObjectOutputStream(baos);
      os.writeObject(object);
      os.close();
      return baos.toByteArray();
    } catch (IOException e) {
      throw new RuntimeException("Could not serialize object", e);
    }
  }

  /** Returns a hash consistent with Spark's Utils.nonNegativeHash(). */
  public static int nonNegativeHash(Object obj) {
    if (obj == null) { return 0; }
    int hash = obj.hashCode();
    return hash != Integer.MIN_VALUE ? Math.abs(hash) : 0;
  }
}
