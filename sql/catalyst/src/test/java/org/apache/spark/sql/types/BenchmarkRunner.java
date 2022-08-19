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

package org.apache.spark.sql.types;

import java.time.LocalDateTime;

import com.google.common.base.StandardSystemProperty;
import org.openjdk.jmh.profile.LinuxPerfAsmProfiler;
import org.openjdk.jmh.results.format.ResultFormatType;
import org.openjdk.jmh.runner.Runner;
import org.openjdk.jmh.runner.RunnerException;
import org.openjdk.jmh.runner.options.ChainedOptionsBuilder;
import org.openjdk.jmh.runner.options.OptionsBuilder;
import org.openjdk.jmh.runner.options.VerboseMode;

import static java.lang.String.format;
import static java.time.format.DateTimeFormatter.ISO_DATE_TIME;

public class BenchmarkRunner {
  private BenchmarkRunner() {}

  public static void benchmark(Class<?> clazz) throws RunnerException {
    ChainedOptionsBuilder optionsBuilder = new OptionsBuilder()
      .verbosity(VerboseMode.NORMAL)
      .include(".*\\." + clazz.getSimpleName() + "\\..*")
      .resultFormat(ResultFormatType.JSON)
      .result(format("%s/%s-result-%s.json", System.getProperty("java.io.tmpdir"),
        clazz.getSimpleName(), ISO_DATE_TIME.format(LocalDateTime.now())));

    if (StandardSystemProperty.OS_NAME.value().equals("Linux")) {
      optionsBuilder.addProfiler(LinuxPerfAsmProfiler.class);
    }

    new Runner(optionsBuilder.build()).run();
  }
}
