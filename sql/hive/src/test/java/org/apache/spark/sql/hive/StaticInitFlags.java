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

package org.apache.spark.sql.hive;

/**
 * Test-only holder for flags flipped by the static initializers of {@link StaticInitInputFormat}
 * and {@link StaticInitOutputFormat}. The flags live on a separate class on purpose: a test can
 * read them to observe whether a format class was initialized, without initializing the format
 * class itself (which reading a field off it would do).
 */
public final class StaticInitFlags {
  public static volatile boolean inputFormatInitialized = false;
  public static volatile boolean outputFormatInitialized = false;

  private StaticInitFlags() {}
}
