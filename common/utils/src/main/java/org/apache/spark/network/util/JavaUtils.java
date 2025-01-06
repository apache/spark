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

import java.io.*;
import java.nio.ByteBuffer;
import java.nio.channels.ReadableByteChannel;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.LinkOption;
import java.nio.file.attribute.BasicFileAttributes;
import java.util.*;
import java.util.concurrent.TimeUnit;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.commons.lang3.SystemUtils;

import org.apache.spark.internal.SparkLogger;
import org.apache.spark.internal.SparkLoggerFactory;
import org.apache.spark.internal.LogKeys;
import org.apache.spark.internal.MDC;

/**
 * General utilities available in the network package. Many of these are sourced from Spark's
 * own Utils, just accessible within this package.
 */
public class JavaUtils {
  private static final SparkLogger logger = SparkLoggerFactory.getLogger(JavaUtils.class);

  /**
   * Define a default value for driver memory here since this value is referenced across the code
   * base and nearly all files already use Utils.scala
   */
  public static final long DEFAULT_DRIVER_MEM_MB = 1024;

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
    return ByteBuffer.wrap(s.getBytes(StandardCharsets.UTF_8));
  }

  /**
   * Convert the given byte buffer to a string. The resulting string can be
   * converted back to the same byte buffer through {@link #stringToBytes(String)}.
   */
  public static String bytesToString(ByteBuffer b) {
    return StandardCharsets.UTF_8.decode(b.slice()).toString();
  }

  /**
   * Delete a file or directory and its contents recursively.
   * Don't follow directories if they are symlinks.
   *
   * @param file Input file / dir to be deleted
   * @throws IOException if deletion is unsuccessful
   */
  public static void deleteRecursively(File file) throws IOException {
    deleteRecursively(file, null);
  }

  /**
   * Delete a file or directory and its contents recursively.
   * Don't follow directories if they are symlinks.
   *
   * @param file Input file / dir to be deleted
   * @param filter A filename filter that make sure only files / dirs with the satisfied filenames
   *               are deleted.
   * @throws IOException if deletion is unsuccessful
   */
  public static void deleteRecursively(File file, FilenameFilter filter) throws IOException {
    if (file == null) { return; }

    // On Unix systems, use operating system command to run faster
    // If that does not work out, fallback to the Java IO way
    // We exclude Apple Silicon test environment due to the limited resource issues.
    if (SystemUtils.IS_OS_UNIX && filter == null && !(SystemUtils.IS_OS_MAC_OSX &&
        (System.getenv("SPARK_TESTING") != null || System.getProperty("spark.testing") != null))) {
      try {
        deleteRecursivelyUsingUnixNative(file);
        return;
      } catch (IOException e) {
        logger.warn("Attempt to delete using native Unix OS command failed for path = {}. " +
          "Falling back to Java IO way", e, MDC.of(LogKeys.PATH$.MODULE$, file.getAbsolutePath()));
      }
    }

    deleteRecursivelyUsingJavaIO(file, filter);
  }

  private static void deleteRecursivelyUsingJavaIO(
      File file,
      FilenameFilter filter) throws IOException {
    BasicFileAttributes fileAttributes =
      Files.readAttributes(file.toPath(), BasicFileAttributes.class, LinkOption.NOFOLLOW_LINKS);
    // SPARK-50716: If the file does not exist and not a broken symbolic link, return directly.
    if (!file.exists() && !fileAttributes.isSymbolicLink()) return;
    if (fileAttributes.isDirectory()) {
      IOException savedIOException = null;
      for (File child : listFilesSafely(file, filter)) {
        try {
          deleteRecursively(child, filter);
        } catch (IOException e) {
          // In case of multiple exceptions, only last one will be thrown
          savedIOException = e;
        }
      }
      if (savedIOException != null) {
        throw savedIOException;
      }
    }

    // Delete file only when it's a normal file, a symbolic link, or an empty directory.
    if (fileAttributes.isRegularFile() || fileAttributes.isSymbolicLink() ||
      (fileAttributes.isDirectory() && listFilesSafely(file, null).length == 0)) {
      boolean deleted = file.delete();
      // Delete can also fail if the file simply did not exist.
      if (!deleted && file.exists()) {
        throw new IOException("Failed to delete: " + file.getAbsolutePath());
      }
    }
  }

  private static void deleteRecursivelyUsingUnixNative(File file) throws IOException {
    ProcessBuilder builder = new ProcessBuilder("rm", "-rf", file.getAbsolutePath());
    Process process = null;
    int exitCode = -1;

    try {
      // In order to avoid deadlocks, consume the stdout (and stderr) of the process
      builder.redirectErrorStream(true);
      builder.redirectOutput(new File("/dev/null"));

      process = builder.start();

      exitCode = process.waitFor();
    } catch (Exception e) {
      throw new IOException("Failed to delete: " + file.getAbsolutePath(), e);
    } finally {
      if (process != null) {
        process.destroy();
      }
    }

    if (exitCode != 0 || file.exists()) {
      throw new IOException("Failed to delete: " + file.getAbsolutePath());
    }
  }

  private static File[] listFilesSafely(File file, FilenameFilter filter) throws IOException {
    if (file.exists()) {
      File[] files = file.listFiles(filter);
      if (files == null) {
        throw new IOException("Failed to list files for dir: " + file);
      }
      return files;
    } else {
      return new File[0];
    }
  }

  private static final Map<String, TimeUnit> timeSuffixes;

  private static final Map<String, ByteUnit> byteSuffixes;

  static {
    timeSuffixes = Map.of(
      "us", TimeUnit.MICROSECONDS,
      "ms", TimeUnit.MILLISECONDS,
      "s", TimeUnit.SECONDS,
      "m", TimeUnit.MINUTES,
      "min", TimeUnit.MINUTES,
      "h", TimeUnit.HOURS,
      "d", TimeUnit.DAYS);

    byteSuffixes = Map.ofEntries(
      Map.entry("b", ByteUnit.BYTE),
      Map.entry("k", ByteUnit.KiB),
      Map.entry("kb", ByteUnit.KiB),
      Map.entry("m", ByteUnit.MiB),
      Map.entry("mb", ByteUnit.MiB),
      Map.entry("g", ByteUnit.GiB),
      Map.entry("gb", ByteUnit.GiB),
      Map.entry("t", ByteUnit.TiB),
      Map.entry("tb", ByteUnit.TiB),
      Map.entry("p", ByteUnit.PiB),
      Map.entry("pb", ByteUnit.PiB));
  }

  private static final Pattern TIME_STRING_PATTERN = Pattern.compile("(-?[0-9]+)([a-z]+)?");

  /**
   * Convert a passed time string (e.g. 50s, 100ms, or 250us) to a time count in the given unit.
   * The unit is also considered the default if the given string does not specify a unit.
   */
  public static long timeStringAs(String str, TimeUnit unit) {
    String lower = str.toLowerCase(Locale.ROOT).trim();

    try {
      Matcher m = TIME_STRING_PATTERN.matcher(lower);
      if (!m.matches()) {
        throw new NumberFormatException("Failed to parse time string: " + str);
      }

      long val = Long.parseLong(m.group(1));
      String suffix = m.group(2);

      // Check for invalid suffixes
      if (suffix != null && !timeSuffixes.containsKey(suffix)) {
        throw new NumberFormatException("Invalid suffix: \"" + suffix + "\"");
      }

      // If suffix is valid use that, otherwise none was provided and use the default passed
      return unit.convert(val, suffix != null ? timeSuffixes.get(suffix) : unit);
    } catch (NumberFormatException e) {
      String timeError = "Time must be specified as seconds (s), " +
              "milliseconds (ms), microseconds (us), minutes (m or min), hour (h), or day (d). " +
              "E.g. 50s, 100ms, or 250us.";

      throw new NumberFormatException(timeError + "\n" + e.getMessage());
    }
  }

  /**
   * Convert a time parameter such as (50s, 100ms, or 250us) to milliseconds for internal use. If
   * no suffix is provided, the passed number is assumed to be in ms.
   */
  public static long timeStringAsMs(String str) {
    return timeStringAs(str, TimeUnit.MILLISECONDS);
  }

  /**
   * Convert a time parameter such as (50s, 100ms, or 250us) to seconds for internal use. If
   * no suffix is provided, the passed number is assumed to be in seconds.
   */
  public static long timeStringAsSec(String str) {
    return timeStringAs(str, TimeUnit.SECONDS);
  }

  private static final Pattern BYTE_STRING_PATTERN =
    Pattern.compile("([0-9]+)([a-z]+)?");
  private static final Pattern BYTE_STRING_FRACTION_PATTERN =
    Pattern.compile("([0-9]+\\.[0-9]+)([a-z]+)?");

  /**
   * Convert a passed byte string (e.g. 50b, 100kb, or 250mb) to the given. If no suffix is
   * provided, a direct conversion to the provided unit is attempted.
   */
  public static long byteStringAs(String str, ByteUnit unit) {
    String lower = str.toLowerCase(Locale.ROOT).trim();

    try {
      Matcher m = BYTE_STRING_PATTERN.matcher(lower);
      Matcher fractionMatcher = BYTE_STRING_FRACTION_PATTERN.matcher(lower);

      if (m.matches()) {
        long val = Long.parseLong(m.group(1));
        String suffix = m.group(2);

        // Check for invalid suffixes
        if (suffix != null && !byteSuffixes.containsKey(suffix)) {
          throw new NumberFormatException("Invalid suffix: \"" + suffix + "\"");
        }

        // If suffix is valid use that, otherwise none was provided and use the default passed
        return unit.convertFrom(val, suffix != null ? byteSuffixes.get(suffix) : unit);
      } else if (fractionMatcher.matches()) {
        throw new NumberFormatException("Fractional values are not supported. Input was: "
          + fractionMatcher.group(1));
      } else {
        throw new NumberFormatException("Failed to parse byte string: " + str);
      }

    } catch (NumberFormatException e) {
      String byteError = "Size must be specified as bytes (b), " +
        "kibibytes (k), mebibytes (m), gibibytes (g), tebibytes (t), or pebibytes(p). " +
        "E.g. 50b, 100k, or 250m.";

      throw new NumberFormatException(byteError + "\n" + e.getMessage());
    }
  }

  /**
   * Convert a passed byte string (e.g. 50b, 100k, or 250m) to bytes for
   * internal use.
   *
   * If no suffix is provided, the passed number is assumed to be in bytes.
   */
  public static long byteStringAsBytes(String str) {
    return byteStringAs(str, ByteUnit.BYTE);
  }

  /**
   * Convert a passed byte string (e.g. 50b, 100k, or 250m) to kibibytes for
   * internal use.
   *
   * If no suffix is provided, the passed number is assumed to be in kibibytes.
   */
  public static long byteStringAsKb(String str) {
    return byteStringAs(str, ByteUnit.KiB);
  }

  /**
   * Convert a passed byte string (e.g. 50b, 100k, or 250m) to mebibytes for
   * internal use.
   *
   * If no suffix is provided, the passed number is assumed to be in mebibytes.
   */
  public static long byteStringAsMb(String str) {
    return byteStringAs(str, ByteUnit.MiB);
  }

  /**
   * Convert a passed byte string (e.g. 50b, 100k, or 250m) to gibibytes for
   * internal use.
   *
   * If no suffix is provided, the passed number is assumed to be in gibibytes.
   */
  public static long byteStringAsGb(String str) {
    return byteStringAs(str, ByteUnit.GiB);
  }

  /**
   * Returns a byte array with the buffer's contents, trying to avoid copying the data if
   * possible.
   */
  public static byte[] bufferToArray(ByteBuffer buffer) {
    if (buffer.hasArray() && buffer.arrayOffset() == 0 &&
        buffer.array().length == buffer.remaining()) {
      return buffer.array();
    } else {
      byte[] bytes = new byte[buffer.remaining()];
      buffer.get(bytes);
      return bytes;
    }
  }

  /**
   * Create a directory inside the given parent directory with default namePrefix "spark".
   * The directory is guaranteed to be newly created, and is not marked for automatic deletion.
   */
  public static File createDirectory(String root) throws IOException {
    return createDirectory(root, "spark");
  }

  /**
   * Create a directory inside the given parent directory. The directory is guaranteed to be
   * newly created, and is not marked for automatic deletion.
   */
  public static File createDirectory(String root, String namePrefix) throws IOException {
    if (namePrefix == null) namePrefix = "spark";
    int attempts = 0;
    int maxAttempts = 10;
    File dir = null;
    while (dir == null) {
      attempts += 1;
      if (attempts > maxAttempts) {
        throw new IOException("Failed to create a temp directory (under " + root + ") after " +
          maxAttempts + " attempts!");
      }
      try {
        dir = new File(root, namePrefix + "-" + UUID.randomUUID());
        Files.createDirectories(dir.toPath());
      } catch (IOException | SecurityException e) {
        logger.error("Failed to create directory {}", e, MDC.of(LogKeys.PATH$.MODULE$, dir));
        dir = null;
      }
    }
    return dir.getCanonicalFile();
  }

  /**
   * Fills a buffer with data read from the channel.
   */
  public static void readFully(ReadableByteChannel channel, ByteBuffer dst) throws IOException {
    int expected = dst.remaining();
    while (dst.hasRemaining()) {
      if (channel.read(dst) < 0) {
        throw new EOFException(String.format("Not enough bytes in channel (expected %d).",
          expected));
      }
    }
  }

}
