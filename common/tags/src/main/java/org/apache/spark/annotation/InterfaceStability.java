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

package org.apache.spark.annotation;

import java.lang.annotation.Documented;

/**
 * Annotation to inform users of how much to rely on a particular package,
 * class or method not changing over time.
 */
public class InterfaceStability {

  /**
   * Stable APIs that retain source and binary compatibility within a major release.
   * These interfaces can change from one major release to another major release
   * (e.g. from 1.0 to 2.0).
   */
  @Documented
  public @interface Stable {};

  /**
   * APIs that are meant to evolve towards becoming stable APIs, but are not stable APIs yet.
   * Evolving interfaces can change from one feature release to another release (i.e. 2.1 to 2.2).
   */
  @Documented
  public @interface Evolving {};

  /**
   * Unstable APIs, with no guarantee on stability.
   * Classes that are unannotated are considered Unstable.
   */
  @Documented
  public @interface Unstable {};
}
