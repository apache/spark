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
package org.apache.spark.sql.execution.datasources.parquet;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Random;
import java.util.concurrent.TimeUnit;

import org.apache.parquet.bytes.ByteBufferInputStream;

import org.apache.spark.sql.execution.vectorized.OffHeapColumnVector;
import org.apache.spark.sql.execution.vectorized.OnHeapColumnVector;
import org.apache.spark.sql.execution.vectorized.WritableColumnVector;
import org.apache.spark.sql.types.DataTypes;

import org.openjdk.jmh.annotations.*;
import org.openjdk.jmh.runner.Runner;
import org.openjdk.jmh.runner.options.Options;
import org.openjdk.jmh.runner.options.OptionsBuilder;

/**
 * JMH benchmark comparing old (baseline) vs new (optimized) VectorizedPlainValuesReader.
 *
 * Run via:
 *   ./build/sbt "sql/Test/runMain \
 *     org.apache.spark.sql.execution.datasources.parquet \
 *     .VectorizedPlainValuesReaderBenchmark"
 */
@BenchmarkMode(Mode.AverageTime)
@OutputTimeUnit(TimeUnit.NANOSECONDS)
@Warmup(iterations = 10, time = 2)
@Measurement(iterations = 100, time = 2)
@Fork(value = 2, jvmArgsAppend = {"-Xmx4g", "-Xms4g"})
@State(Scope.Thread)
public class VectorizedPlainValuesReaderBenchmark {

  private static final int BATCH_SIZE = 4096;

  private Random random;

  // --- Heap buffer data for each type ---
  private byte[] booleanData;

  // --- Column vectors ---
  private WritableColumnVector booleanColumn;

  @Param({"OnHeap", "OffHeap"})
  public String vectorType;

  // --- Readers ---
  private OldVectorizedPlainValuesReader oldReader;
  private VectorizedPlainValuesReader newReader;

  @Setup(Level.Trial)
  public void setup() {
    random = new Random(42);

    // Boolean data: packed bits, ceil(BATCH_SIZE / 8) bytes
    int booleanBytes = (BATCH_SIZE + 7) / 8;
    booleanData = new byte[booleanBytes];
    random.nextBytes(booleanData);

    // Allocate column vectors
    if ("OffHeap".equalsIgnoreCase(vectorType)) {
      booleanColumn = new OffHeapColumnVector(BATCH_SIZE, DataTypes.BooleanType);
    } else {
      booleanColumn = new OnHeapColumnVector(BATCH_SIZE, DataTypes.BooleanType);
    }
  }

  @TearDown(Level.Trial)
  public void tearDown() {
    booleanColumn.close();
  }

  @Setup(Level.Invocation)
  public void prepareInvocation() throws IOException {
    booleanColumn.reset();
    oldReader = initOld(wrapHeap(booleanData));
    newReader = initNew(wrapHeap(booleanData));
  }

  // --- Helper methods ---

  private ByteBufferInputStream wrapHeap(byte[] data) {
    ByteBuffer buf = ByteBuffer.wrap(data);
    return ByteBufferInputStream.wrap(buf);
  }

  private OldVectorizedPlainValuesReader initOld(ByteBufferInputStream stream) throws IOException {
    OldVectorizedPlainValuesReader reader = new OldVectorizedPlainValuesReader();
    reader.initFromPage(BATCH_SIZE, stream);
    return reader;
  }

  private VectorizedPlainValuesReader initNew(ByteBufferInputStream stream) throws IOException {
    VectorizedPlainValuesReader reader = new VectorizedPlainValuesReader();
    reader.initFromPage(BATCH_SIZE, stream);
    return reader;
  }

  // ==================== Boolean benchmarks ====================

  @Benchmark
  public void readBooleans_old() throws IOException {
    oldReader.readBooleans(BATCH_SIZE, booleanColumn, 0);
  }

  @Benchmark
  public void readBooleans_new() throws IOException {
    newReader.readBooleans(BATCH_SIZE, booleanColumn, 0);
  }

  // ==================== Main entry point ====================

  private static String getProcessorName() {
    String cpu = "Unknown processor";
    try {
      String os = System.getProperty("os.name").toLowerCase();
      if (os.contains("mac")) {
        ProcessBuilder pb =
          new ProcessBuilder("/usr/sbin/sysctl", "-n", "machdep.cpu.brand_string");
        Process p = pb.start();
        try (java.io.BufferedReader reader = new java.io.BufferedReader(
            new java.io.InputStreamReader(p.getInputStream()))) {
          cpu = reader.readLine();
        }
        p.waitFor();
      } else if (os.contains("linux")) {
        ProcessBuilder pb = new ProcessBuilder("grep", "-m", "1", "model name", "/proc/cpuinfo");
        Process p = pb.start();
        try (java.io.BufferedReader reader = new java.io.BufferedReader(
            new java.io.InputStreamReader(p.getInputStream()))) {
          String line = reader.readLine();
          if (line != null) {
            cpu = line.replaceFirst("model name\\s*:\\s*", "");
          }
        }
        p.waitFor();
      } else {
        cpu = System.getenv("PROCESSOR_IDENTIFIER");
      }
    } catch (Exception e) {
      // ignore
    }
    return cpu;
  }

  public static void main(String[] args) throws Exception {
    System.out.println("Processor: " + getProcessorName());
    Options opt = new OptionsBuilder()
        .include(VectorizedPlainValuesReaderBenchmark.class.getSimpleName())
        .build();
    new Runner(opt).run();
  }
}
