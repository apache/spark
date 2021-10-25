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

package test.org.apache.spark.sql.connector;

import java.util.EnumSet;
import java.util.Set;

import org.apache.spark.sql.connector.TestingV2Source;
import org.apache.spark.sql.connector.catalog.SupportsRead;
import org.apache.spark.sql.connector.catalog.Table;
import org.apache.spark.sql.connector.catalog.TableCapability;
import org.apache.spark.sql.types.StructType;

abstract class JavaSimpleBatchTable implements Table, SupportsRead {
  private static final Set<TableCapability> CAPABILITIES =
      EnumSet.of(TableCapability.BATCH_READ);
  @Override
  public StructType schema() {
    return TestingV2Source.schema();
  }

  @Override
  public String name() {
    return this.getClass().toString();
  }

  @Override
  public Set<TableCapability> capabilities() {
    return CAPABILITIES;
  }
}

