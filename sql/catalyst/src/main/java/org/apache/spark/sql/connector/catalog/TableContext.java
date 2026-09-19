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

package org.apache.spark.sql.connector.catalog;

import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;

import org.apache.spark.SparkIllegalArgumentException;
import org.apache.spark.annotation.Evolving;
import org.apache.spark.sql.util.CaseInsensitiveStringMap;

/**
 * Encapsulates the parsed, Spark-recognized parameters of a table load request, passed from the
 * analyzer / DataFrame API to the catalog's
 * {@link TableCatalog#loadTable(Identifier, TableContext, CaseInsensitiveStringMap)} method.
 * <p>
 * A load is either a read (optionally with time travel) or a write (carrying write privileges);
 * time travel and write privileges are mutually exclusive.
 *
 * @since 4.3.0
 */
@Evolving
public class TableContext {

  // null means no time travel.
  private final TimeTravel timeTravel;
  // Never null; an empty set means no write privileges (i.e. a read).
  private final Set<TableWritePrivilege> writePrivileges;

  public TableContext(TimeTravel timeTravel, Set<TableWritePrivilege> privileges) {
    this.timeTravel = timeTravel;
    this.writePrivileges = privileges == null ? Set.of() : Set.copyOf(privileges);
    if (timeTravel != null && !writePrivileges.isEmpty()) {
      throw new SparkIllegalArgumentException(
          "INTERNAL_ERROR",
          Map.of("message", "Cannot set both time travel and write privileges"));
    }
  }

  /** Returns the time-travel spec, or empty for a current-version read. */
  public Optional<TimeTravel> timeTravel() {
    return Optional.ofNullable(timeTravel);
  }

  /** Returns the requested write privileges; empty for a read. */
  public Set<TableWritePrivilege> writePrivileges() {
    return writePrivileges;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) return true;
    if (!(o instanceof TableContext that)) return false;
    return Objects.equals(timeTravel, that.timeTravel)
        && writePrivileges.equals(that.writePrivileges);
  }

  @Override
  public int hashCode() {
    return Objects.hash(timeTravel, writePrivileges);
  }

  @Override
  public String toString() {
    return "TableContext{timeTravel=" + timeTravel +
        ", writePrivileges=" + writePrivileges + "}";
  }
}
