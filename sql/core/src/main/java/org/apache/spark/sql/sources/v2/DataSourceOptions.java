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

package org.apache.spark.sql.sources.v2;

import java.io.IOException;
import java.util.HashMap;
import java.util.Locale;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Stream;

import com.fasterxml.jackson.databind.ObjectMapper;

import org.apache.spark.annotation.InterfaceStability;

/**
 * An immutable string-to-string map in which keys are case-insensitive. This is used to represent
 * data source options.
 *
 * Each data source implementation can define its own options and teach its users how to set them.
 * Spark doesn't have any restrictions about what options a data source should or should not have.
 * Instead Spark defines some standard options that data sources can optionally adopt. It's possible
 * that some options are very common and many data sources use them. However different data
 * sources may define the common options(key and meaning) differently, which is quite confusing to
 * end users.
 *
 * The standard options defined by Spark:
 * <table summary="standard data source options">
 *   <tr>
 *     <th><b>Option key</b></th>
 *     <th><b>Option value</b></th>
 *   </tr>
 *   <tr>
 *     <td>path</td>
 *     <td>A path string of the data files/directories, like
 *     <code>path1</code>, <code>/absolute/file2</code>, <code>path3/*</code>. The path can
 *     either be relative or absolute, points to either file or directory, and can contain
 *     wildcards. This option is commonly used by file-based data sources.</td>
 *   </tr>
 *   <tr>
 *     <td>paths</td>
 *     <td>A JSON array style paths string of the data files/directories, like
 *     <code>["path1", "/absolute/file2"]</code>. The format of each path is same as the
 *     <code>path</code> option, plus it should follow JSON string literal format, e.g. quotes
 *     should be escaped, <code>pa\"th</code> means pa"th.
 *     </td>
 *   </tr>
 *   <tr>
 *     <td>table</td>
 *     <td>A table name string representing the table name directly without any interpretation.
 *     For example, <code>db.tbl</code> means a table called db.tbl, not a table called tbl
 *     inside database db. <code>`t*b.l`</code> means a table called `t*b.l`, not t*b.l.</td>
 *   </tr>
 *   <tr>
 *     <td>database</td>
 *     <td>A database name string representing the database name directly without any
 *     interpretation, which is very similar to the table name option.</td>
 *   </tr>
 * </table>
 */
@InterfaceStability.Evolving
public class DataSourceOptions {
  private final Map<String, String> keyLowerCasedMap;

  private String toLowerCase(String key) {
    return key.toLowerCase(Locale.ROOT);
  }

  public static DataSourceOptions empty() {
    return new DataSourceOptions(new HashMap<>());
  }

  public DataSourceOptions(Map<String, String> originalMap) {
    keyLowerCasedMap = new HashMap<>(originalMap.size());
    for (Map.Entry<String, String> entry : originalMap.entrySet()) {
      keyLowerCasedMap.put(toLowerCase(entry.getKey()), entry.getValue());
    }
  }

  public Map<String, String> asMap() {
    return new HashMap<>(keyLowerCasedMap);
  }

  /**
   * Returns the option value to which the specified key is mapped, case-insensitively.
   */
  public Optional<String> get(String key) {
    return Optional.ofNullable(keyLowerCasedMap.get(toLowerCase(key)));
  }

  /**
   * Returns the boolean value to which the specified key is mapped,
   * or defaultValue if there is no mapping for the key. The key match is case-insensitive
   */
  public boolean getBoolean(String key, boolean defaultValue) {
    String lcaseKey = toLowerCase(key);
    return keyLowerCasedMap.containsKey(lcaseKey) ?
      Boolean.parseBoolean(keyLowerCasedMap.get(lcaseKey)) : defaultValue;
  }

  /**
   * Returns the integer value to which the specified key is mapped,
   * or defaultValue if there is no mapping for the key. The key match is case-insensitive
   */
  public int getInt(String key, int defaultValue) {
    String lcaseKey = toLowerCase(key);
    return keyLowerCasedMap.containsKey(lcaseKey) ?
      Integer.parseInt(keyLowerCasedMap.get(lcaseKey)) : defaultValue;
  }

  /**
   * Returns the long value to which the specified key is mapped,
   * or defaultValue if there is no mapping for the key. The key match is case-insensitive
   */
  public long getLong(String key, long defaultValue) {
    String lcaseKey = toLowerCase(key);
    return keyLowerCasedMap.containsKey(lcaseKey) ?
      Long.parseLong(keyLowerCasedMap.get(lcaseKey)) : defaultValue;
  }

  /**
   * Returns the double value to which the specified key is mapped,
   * or defaultValue if there is no mapping for the key. The key match is case-insensitive
   */
  public double getDouble(String key, double defaultValue) {
    String lcaseKey = toLowerCase(key);
    return keyLowerCasedMap.containsKey(lcaseKey) ?
      Double.parseDouble(keyLowerCasedMap.get(lcaseKey)) : defaultValue;
  }

  /**
   * The option key for singular path.
   */
  public static final String PATH_KEY = "path";

  /**
   * The option key for multiple paths.
   */
  public static final String PATHS_KEY = "paths";

  /**
   * The option key for table name.
   */
  public static final String TABLE_KEY = "table";

  /**
   * The option key for database name.
   */
  public static final String DATABASE_KEY = "database";

  /**
   * Returns all the paths specified by both the singular path option and the multiple
   * paths option.
   */
  public String[] paths() {
    String[] singularPath =
      get(PATH_KEY).map(s -> new String[]{s}).orElseGet(() -> new String[0]);
    Optional<String> pathsStr = get(PATHS_KEY);
    if (pathsStr.isPresent()) {
      ObjectMapper objectMapper = new ObjectMapper();
      try {
        String[] paths = objectMapper.readValue(pathsStr.get(), String[].class);
        return Stream.of(singularPath, paths).flatMap(Stream::of).toArray(String[]::new);
      } catch (IOException e) {
        return singularPath;
      }
    } else {
      return singularPath;
    }
  }

  /**
   * Returns the value of the table name option.
   */
  public Optional<String> tableName() {
    return get(TABLE_KEY);
  }

  /**
   * Returns the value of the database name option.
   */
  public Optional<String> databaseName() {
    return get(DATABASE_KEY);
  }
}
