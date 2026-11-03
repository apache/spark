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

import java.util.ArrayList;
import java.util.List;

import org.apache.spark.sql.catalyst.expressions.EqualTo;
import org.apache.spark.sql.catalyst.expressions.Expression;
import org.apache.spark.sql.catalyst.expressions.Literal;
import org.apache.spark.sql.connector.expressions.Expressions;
import org.apache.spark.sql.connector.expressions.NamedReference;
import org.apache.spark.sql.connector.read.Batch;
import org.apache.spark.sql.connector.read.InputPartition;
import org.apache.spark.sql.connector.read.PartitionReaderFactory;
import org.apache.spark.sql.internal.connector.SupportsRuntimeCatalystFiltering;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructType;

/**
 * A scan that implements only the deprecated {@code filter}. javac adds no forwarder for
 * {@code planInputPartitionsWithRuntimeFilters}, so the method is declared on the trait, as it is
 * for a class compiled against Spark 4.3.
 */
public class JavaDeprecatedFilterScan implements SupportsRuntimeCatalystFiltering, Batch {

  public static class Part implements InputPartition {
    public final int part;

    Part(int part) {
      this.part = part;
    }
  }

  private List<Integer> parts = new ArrayList<>(List.of(0, 1, 2, 3, 4));

  public int filterCalls = 0;

  @Override
  public StructType readSchema() {
    return new StructType().add("id", DataTypes.IntegerType).add("part", DataTypes.IntegerType);
  }

  @Override
  public NamedReference[] filterAttributes() {
    return new NamedReference[] { Expressions.column("part") };
  }

  @Override
  @SuppressWarnings("deprecation")
  public void filter(Expression[] expressions) {
    filterCalls += 1;
    List<Integer> kept = new ArrayList<>();
    for (int p : parts) {
      boolean keep = true;
      for (Expression e : expressions) {
        if (e instanceof EqualTo eq && eq.right() instanceof Literal lit &&
            lit.value() instanceof Integer v && v != p) {
          keep = false;
        }
      }
      if (keep) {
        kept.add(p);
      }
    }
    parts = kept;
  }

  @Override
  public Batch toBatch() {
    return this;
  }

  @Override
  public InputPartition[] planInputPartitions() {
    return parts.stream().map(Part::new).toArray(InputPartition[]::new);
  }

  @Override
  public PartitionReaderFactory createReaderFactory() {
    throw new UnsupportedOperationException("not read");
  }
}
