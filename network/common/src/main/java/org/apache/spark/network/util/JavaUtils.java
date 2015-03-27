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

import java.io.Closeable;
import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;

import com.google.common.base.Charsets;
import com.google.common.base.Preconditions;
import io.netty.buffer.Unpooled;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * General utilities available in the network package. Many of these are sourced from Spark's
 * own Utils, just accessible within this package.
 */
public class JavaUtils {
  private static final Logger logger = LoggerFactory.getLogger(JavaUtils.class);

  /** Closes the given object, ignoring IOExceptions. */
  public static void closeQuietly(Closeable closeable) {
    try {
      if (closeable != null) {
        closeable.close();
      }
    } catch (IOException e) {
      logger.error("IOException should not have been thrown.", e);
    }
  }

  /** Returns a hash consistent with Spark's Utils.nonNegativeHash(). */
  public static int nonNegativeHash(Object obj) {
    if (obj == null) { return 0; }
    int hash = obj.hashCode();
    return hash != Integer.MIN_VALUE ? Math.abs(hash) : 0;
  }

  /**
   * Convert the given string to a byte buffer. The resulting buffer can be
   * converted back to the same string through {@link #bytesToString(ByteBuffer)}.
   */
  public static ByteBuffer stringToBytes(String s) {
    return Unpooled.wrappedBuffer(s.getBytes(Charsets.UTF_8)).nioBuffer();
  }

  /**
   * Convert the given byte buffer to a string. The resulting string can be
   * converted back to the same byte buffer through {@link #stringToBytes(String)}.
   */
  public static String bytesToString(ByteBuffer b) {
    return Unpooled.wrappedBuffer(b).toString(Charsets.UTF_8);
  }

  /*
   * Delete a file or directory and its contents recursively.
   * Don't follow directories if they are symlinks.
   * Throws an exception if deletion is unsuccessful.
   */
  public static void deleteRecursively(File file) throws IOException {
    if (file == null) { return; }

    if (file.isDirectory() && !isSymlink(file)) {
      IOException savedIOException = null;
      for (File child : listFilesSafely(file)) {
        try {
          deleteRecursively(child);
        } catch (IOException e) {
          // In case of multiple exceptions, only last one will be thrown
          savedIOException = e;
        }
      }
      if (savedIOException != null) {
        throw savedIOException;
      }
    }

    boolean deleted = file.delete();
    // Delete can also fail if the file simply did not exist.
    if (!deleted && file.exists()) {
      throw new IOException("Failed to delete: " + file.getAbsolutePath());
    }
  }

  private static File[] listFilesSafely(File file) throws IOException {
    if (file.exists()) {
      File[] files = file.listFiles();
      if (files == null) {
        throw new IOException("Failed to list files for dir: " + file);
      }
      return files;
    } else {
      return new File[0];
    }
  }

  private static boolean isSymlink(File file) throws IOException {
    Preconditions.checkNotNull(file);
    File fileInCanonicalDir = null;
    if (file.getParent() == null) {
      fileInCanonicalDir = file;
    } else {
      fileInCanonicalDir = new File(file.getParentFile().getCanonicalFile(), file.getName());
    }
    return !fileInCanonicalDir.getCanonicalFile().equals(fileInCanonicalDir.getAbsoluteFile());
  }

  /**
   * Convert a time parameter such as (50s, 100ms, or 250us) to microseconds for internal use
   */
  public static long timeStringToUs(String str) throws IllegalArgumentException {
    String lower = str.toLowerCase().trim();
    if (lower.endsWith("s")) {
      return Long.parseLong(lower.substring(0, lower.length()-1)) * 1000 * 1000;
    } else if (lower.endsWith("ms")) {
      return Long.parseLong(lower.substring(0, lower.length()-2)) * 1000;
    } else if (lower.endsWith("us")) {
      return Long.parseLong(lower.substring(0, lower.length()-2));
    } else {// Invalid suffix, force correct formatting
      throw new IllegalArgumentException("Time must be specified as seconds (s), " +
              "milliseconds (ms), or microseconds (us) e.g. 50s, 100ms, or 250us.");
    }
  }

  /**
   * Convert a time parameter such as (50s, 100ms, or 250us) to milliseconds for internal use.
   * Note: may round in some cases
   */
  public static long timeStringToMs(String str) throws IllegalArgumentException {
    return timeStringToUs(str)/1000;
  }

  /**
   * Convert a time parameter such as (50s, 100ms, or 250us) to seconds for internal use.
   * Note: may round in some cases
   */
  public static long timeStringToS(String str) throws IllegalArgumentException {
    return timeStringToUs(str)/1000/1000;
  }

}
