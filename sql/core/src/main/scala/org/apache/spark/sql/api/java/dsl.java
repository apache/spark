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

package org.apache.spark.sql.api.java;

import org.apache.spark.sql.Column;
import org.apache.spark.sql.DataFrame;
import org.apache.spark.sql.api.scala.dsl.package$;


/**
 * Java version of the domain-specific functions available for {@link DataFrame}.
 *
 * The Scala version is at {@link org.apache.spark.sql.api.scala.dsl}.
 */
public class dsl {
  // NOTE: Update also the Scala version when we update this version.

  private static package$ scalaDsl = package$.MODULE$;

  /**
   * Creates a column of literal value.
   */
  public static Column lit(Object literalValue) {
    return scalaDsl.lit(literalValue);
  }

  public static Column sum(Column e) {
    return scalaDsl.sum(e);
  }

  public static Column sumDistinct(Column e) {
    return scalaDsl.sumDistinct(e);
  }

  public static Column avg(Column e) {
    return scalaDsl.avg(e);
  }

  public static Column first(Column e) {
    return scalaDsl.first(e);
  }

  public static Column last(Column e) {
    return scalaDsl.last(e);
  }

  public static Column min(Column e) {
    return scalaDsl.min(e);
  }

  public static Column max(Column e) {
    return scalaDsl.max(e);
  }

  public static Column upper(Column e) {
    return scalaDsl.upper(e);
  }

  public static Column lower(Column e) {
    return scalaDsl.lower(e);
  }

  public static Column sqrt(Column e) {
    return scalaDsl.sqrt(e);
  }

  public static Column abs(Column e) {
    return scalaDsl.abs(e);
  }
}
