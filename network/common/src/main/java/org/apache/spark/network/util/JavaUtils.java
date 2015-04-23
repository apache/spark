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
import java.util.concurrent.TimeUnit;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import com.google.common.base.Charsets;
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableMap;
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

  private static ImmutableMap<String, TimeUnit> timeSuffixes = 
    ImmutableMap.<String, TimeUnit>builder()
      .put("us", TimeUnit.MICROSECONDS)
      .put("ms", TimeUnit.MILLISECONDS)
      .put("s", TimeUnit.SECONDS)
      .put("m", TimeUnit.MINUTES)
      .put("min", TimeUnit.MINUTES)
      .put("h", TimeUnit.HOURS)
      .put("d", TimeUnit.DAYS)
      .build();

  /**
   * Convert a passed time string (e.g. 50s, 100ms, or 250us) to a time count for
   * internal use. If no suffix is provided a direct conversion is attempted.
   */
  private static long parseTimeString(String str, TimeUnit unit) {
    String lower = str.toLowerCase().trim();
    
    try {
      String suffix;
      long val;
      Matcher m = Pattern.compile("(-?[0-9]+)([a-z]+)?").matcher(lower);
      if (m.matches()) {
        val = Long.parseLong(m.group(1));
        suffix = m.group(2);
      } else {
        throw new NumberFormatException("Failed to parse time string: " + str);
      }
      
      // Check for invalid suffixes
      if (suffix != null && !timeSuffixes.containsKey(suffix)) {
        throw new NumberFormatException("Invalid suffix: \"" + suffix + "\"");
      }
      
      // If suffix is valid use that, otherwise none was provided and use the default passed
      return unit.convert(val, suffix != null ? timeSuffixes.get(suffix) : unit);
    } catch (NumberFormatException e) {
      String timeError = "Time must be specified as seconds (s), " +
              "milliseconds (ms), microseconds (us), minutes (m or min) hour (h), or day (d). " +
              "E.g. 50s, 100ms, or 250us.";
      
      throw new NumberFormatException(timeError + "\n" + e.getMessage());
    }
  }
  
  /**
   * Convert a time parameter such as (50s, 100ms, or 250us) to milliseconds for internal use. If
   * no suffix is provided, the passed number is assumed to be in ms.
   */
  public static long timeStringAsMs(String str) {
    return parseTimeString(str, TimeUnit.MILLISECONDS);
  }

  /**
   * Convert a time parameter such as (50s, 100ms, or 250us) to seconds for internal use. If
   * no suffix is provided, the passed number is assumed to be in seconds.
   */
  public static long timeStringAsSec(String str) {
    return parseTimeString(str, TimeUnit.SECONDS);
  }

}
