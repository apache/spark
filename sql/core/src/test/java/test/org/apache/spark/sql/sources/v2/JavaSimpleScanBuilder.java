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

package test.org.apache.spark.sql.sources.v2;

import org.apache.spark.sql.sources.v2.reader.Batch;
import org.apache.spark.sql.sources.v2.reader.PartitionReaderFactory;
import org.apache.spark.sql.sources.v2.reader.Scan;
import org.apache.spark.sql.sources.v2.reader.ScanBuilder;
import org.apache.spark.sql.types.StructType;

abstract class JavaSimpleScanBuilder implements ScanBuilder, Scan, Batch {

  @Override
  public Scan build() {
    return this;
  }

  @Override
  public Batch toBatch() {
    return this;
  }

  @Override
  public StructType readSchema() {
    return new StructType().add("i", "int").add("j", "int");
  }

  @Override
  public PartitionReaderFactory createReaderFactory() {
    return new JavaSimpleReaderFactory();
  }
}
