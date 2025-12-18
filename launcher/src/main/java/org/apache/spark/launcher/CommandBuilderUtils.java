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

package org.apache.spark.launcher;

import java.io.File;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

/**
 * Helper methods for command builders.
 */
class CommandBuilderUtils {

  static final String DEFAULT_MEM = "1g";
  static final String DEFAULT_PROPERTIES_FILE = "spark-defaults.conf";
  static final String ENV_SPARK_HOME = "SPARK_HOME";
  // This should be consistent with org.apache.spark.internal.config.SECRET_REDACTION_PATTERN
  // We maintain this copy to avoid depending on `core` module.
  static final String SECRET_REDACTION_PATTERN = "(?i)secret|password|token|access[.]?key";
  static final Pattern redactPattern = Pattern.compile(SECRET_REDACTION_PATTERN);
  static final Pattern keyValuePattern = Pattern.compile("-D(.+?)=(.+)");

  /** Returns whether the given string is null or empty. */
  static boolean isEmpty(String s) {
    return s == null || s.isEmpty();
  }

  /** Joins a list of strings using the given separator. */
  static String join(String sep, String... elements) {
    return join(sep, Arrays.asList(elements));
  }

  /** Joins a list of strings using the given separator. */
  static String join(String sep, Iterable<String> elements) {
    StringBuilder sb = new StringBuilder();
    for (String e : elements) {
      if (e != null) {
        if (!sb.isEmpty()) {
          sb.append(sep);
        }
        sb.append(e);
      }
    }
    return sb.toString();
  }

  /**
   * Returns the first non-empty value mapped to the given key in the given maps, or null otherwise.
   */
  static String firstNonEmptyValue(String key, Map<?, ?>... maps) {
    for (Map<?, ?> map : maps) {
      String value = (String) map.get(key);
      if (!isEmpty(value)) {
        return value;
      }
    }
    return null;
  }

  /** Returns the first non-empty, non-null string in the given list, or null otherwise. */
  static String firstNonEmpty(String... candidates) {
    for (String s : candidates) {
      if (!isEmpty(s)) {
        return s;
      }
    }
    return null;
  }

  /** Returns the name of the env variable that holds the native library path. */
  static String getLibPathEnvName() {
    if (isWindows()) {
      return "PATH";
    }

    String os = System.getProperty("os.name");
    if (os.startsWith("Mac OS X")) {
      return "DYLD_LIBRARY_PATH";
    } else {
      return "LD_LIBRARY_PATH";
    }
  }

  /** Returns whether the OS is Windows. */
  static boolean isWindows() {
    String os = System.getProperty("os.name");
    return os.startsWith("Windows");
  }

  /**
   * Updates the user environment, appending the given pathList to the existing value of the given
   * environment variable (or setting it if it hasn't yet been set).
   */
  static void mergeEnvPathList(Map<String, String> userEnv, String envKey, String pathList) {
    if (!isEmpty(pathList)) {
      String current = firstNonEmpty(userEnv.get(envKey), System.getenv(envKey));
      userEnv.put(envKey, join(File.pathSeparator, current, pathList));
    }
  }

  /**
   * Parse a string as if it were a list of arguments, following bash semantics.
   * For example:
   *
   * Input: "\"ab cd\" efgh 'i \" j'"
   * Output: [ "ab cd", "efgh", "i \" j" ]
   */
  static List<String> parseOptionString(String s) {
    List<String> opts = new ArrayList<>();
    StringBuilder opt = new StringBuilder();
    boolean inOpt = false;
    boolean inSingleQuote = false;
    boolean inDoubleQuote = false;
    boolean escapeNext = false;

    // This is needed to detect when a quoted empty string is used as an argument ("" or '').
    boolean hasData = false;

    for (int i = 0; i < s.length(); i++) {
      int c = s.codePointAt(i);
      if (escapeNext) {
        opt.appendCodePoint(c);
        escapeNext = false;
      } else if (inOpt) {
        switch (c) {
          case '\\' -> {
            if (inSingleQuote) {
              opt.appendCodePoint(c);
            } else {
              escapeNext = true;
            }
          }
          case '\'' -> {
            if (inDoubleQuote) {
              opt.appendCodePoint(c);
            } else {
              inSingleQuote = !inSingleQuote;
            }
          }
          case '"' -> {
            if (inSingleQuote) {
              opt.appendCodePoint(c);
            } else {
              inDoubleQuote = !inDoubleQuote;
            }
          }
          default -> {
            if (!Character.isWhitespace(c) || inSingleQuote || inDoubleQuote) {
              opt.appendCodePoint(c);
            } else {
              opts.add(opt.toString());
              opt.setLength(0);
              inOpt = false;
              hasData = false;
            }
          }
        }
      } else {
        switch (c) {
          case '\'' -> {
            inSingleQuote = true;
            inOpt = true;
            hasData = true;
          }
          case '"' -> {
            inDoubleQuote = true;
            inOpt = true;
            hasData = true;
          }
          case '\\' -> {
            escapeNext = true;
            inOpt = true;
            hasData = true;
          }
          default -> {
            if (!Character.isWhitespace(c)) {
              inOpt = true;
              hasData = true;
              opt.appendCodePoint(c);
            }
          }
        }
      }
    }

    checkArgument(!inSingleQuote && !inDoubleQuote && !escapeNext, "Invalid option string: %s", s);
    if (hasData) {
      opts.add(opt.toString());
    }
    return opts;
  }

  /** Throws IllegalArgumentException if the given object is null. */
  static void checkNotNull(Object o, String arg) {
    if (o == null) {
      throw new IllegalArgumentException(String.format("'%s' must not be null.", arg));
    }
  }

  /** Throws IllegalArgumentException with the given message if the check is false. */
  static void checkArgument(boolean check, String msg, Object... args) {
    if (!check) {
      throw new IllegalArgumentException(String.format(msg, args));
    }
  }

  /** Throws IllegalStateException with the given message if the check is false. */
  static void checkState(boolean check, String msg, Object... args) {
    if (!check) {
      throw new IllegalStateException(String.format(msg, args));
    }
  }

  /**
   * Quote a command argument for a command to be run by a Windows batch script, if the argument
   * needs quoting. Arguments only seem to need quotes in batch scripts if they have certain
   * special characters, some of which need extra (and different) escaping.
   *
   *  For example:
   *    original single argument: ab="cde fgh"
   *    quoted: "ab^=""cde fgh"""
   */
  static String quoteForBatchScript(String arg) {

    boolean needsQuotes = false;
    for (int i = 0; i < arg.length(); i++) {
      int c = arg.codePointAt(i);
      if (Character.isWhitespace(c) || c == '"' || c == '=' || c == ',' || c == ';') {
        needsQuotes = true;
        break;
      }
    }
    if (!needsQuotes) {
      return arg;
    }
    StringBuilder quoted = new StringBuilder();
    quoted.append("\"");
    for (int i = 0; i < arg.length(); i++) {
      int cp = arg.codePointAt(i);
      switch (cp) {
        case '"' -> quoted.append('"');
        default -> {}
      }
      quoted.appendCodePoint(cp);
    }
    if (arg.codePointAt(arg.length() - 1) == '\\') {
      quoted.append("\\");
    }
    quoted.append("\"");
    return quoted.toString();
  }

  /**
   * Quotes a string so that it can be used in a command string.
   * Basically, just add simple escapes. E.g.:
   *    original single argument : ab "cd" ef
   *    after: "ab \"cd\" ef"
   *
   * This can be parsed back into a single argument by python's "shlex.split()" function.
   */
  static String quoteForCommandString(String s) {
    StringBuilder quoted = new StringBuilder().append('"');
    for (int i = 0; i < s.length(); i++) {
      int cp = s.codePointAt(i);
      if (cp == '"' || cp == '\\') {
        quoted.appendCodePoint('\\');
      }
      quoted.appendCodePoint(cp);
    }
    return quoted.append('"').toString();
  }

  /**
   * Get the major version of the java version string supplied. This method
   * accepts any JEP-223-compliant strings (9-ea, 9+100), as well as legacy
   * version strings such as 1.7.0_79
   */
  static int javaMajorVersion(String javaVersion) {
    String[] version = javaVersion.split("[+.\\-]+");
    int major = Integer.parseInt(version[0]);
    // if major > 1, we're using the JEP-223 version string, e.g., 9-ea, 9+120
    // otherwise the second number is the major version
    if (major > 1) {
      return major;
    } else {
      return Integer.parseInt(version[1]);
    }
  }

  /**
   * Find the location of the Spark jars dir, depending on whether we're looking at a build
   * or a distribution directory.
   */
  static String findJarsDir(String sparkHome, String scalaVersion, boolean failIfNotFound) {
    // TODO: change to the correct directory once the assembly build is changed.
    File libdir = new File(sparkHome, "jars");
    if (!libdir.isDirectory()) {
      libdir = new File(sparkHome, String.format("assembly/target/scala-%s/jars", scalaVersion));
      if (!libdir.isDirectory()) {
        checkState(!failIfNotFound,
          "Library directory '%s' does not exist; make sure Spark is built.",
          libdir.getAbsolutePath());
        return null;
      }
    }
    return libdir.getAbsolutePath();
  }

  /**
   * Redact a command-line argument's value part which matches `-Dkey=value` pattern.
   * Note that this should be consistent with `org.apache.spark.util.Utils.redactCommandLineArgs`.
   */
  static List<String> redactCommandLineArgs(List<String> args) {
    return args.stream().map(CommandBuilderUtils::redact).collect(Collectors.toList());
  }

  /**
   * Redact a command-line argument's value part which matches `-Dkey=value` pattern.
   */
  static String redact(String arg) {
    Matcher m = keyValuePattern.matcher(arg);
    if (m.find() && redactPattern.matcher(m.group(1)).find()) {
      return String.format("-D%s=%s", m.group(1), "*********(redacted)");
    } else {
      return arg;
    }
  }
}
