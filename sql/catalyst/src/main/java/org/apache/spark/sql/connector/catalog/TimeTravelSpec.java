/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.spark.sql.connector.catalog;

import org.apache.spark.annotation.Evolving;

import java.util.Objects;

/**
 * TimeTravelSpec contains the properties for time travel.
 *
 * @since 3.3.0
 */
@Evolving
public class TimeTravelSpec {

  private long timestamp;
  private String version;

  public TimeTravelSpec(long timeStamp, String version) {
    this.timestamp = timeStamp;
    this.version = version;
  }

  public String version() {
    return version;
  }

  public long timeStamp() {
    return timestamp;
  }

  @Override
  public String toString() {
    return "TimeTravelSpec{" + "version='" + version + '\'' + ", timeStamp=" + timestamp + '}';
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) return true;
    if (o == null || getClass() != o.getClass()) return false;
    TimeTravelSpec that = (TimeTravelSpec) o;
    return timestamp == that.timestamp && Objects.equals(version, that.version);
  }

  @Override
  public int hashCode() {
    return Objects.hash(version, timestamp);
  }
}
