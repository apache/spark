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

package org.apache.spark.io

import java.nio.file.{Files, Paths}

import org.apache.spark.benchmark.BenchmarkBase

/**
 * TPC-DS data preparation:
 * <p>
 * 1. Follow https://github.com/gregrahn/tpcds-kit.git to set up tpcds-kit
 * <p>
 * 2. Create a folder and export environment variable SPARK_TPCDS_DATA_TEXT
 * {{{
 * mkdir -p /path/of/tpcds-sf1-text
 * export SPARK_TPCDS_DATA_TEXT=/path/of/tpcds-sf1-text
 * }}}
 * <p>
 * 3. Generate TPC-DS (SF1) text data under SPARK_TPCDS_DATA_TEXT
 * {{{
 * tpcds-kit/tools/dsdgen \
 *   -DISTRIBUTIONS tpcds-kit/tools/tpcds.idx \
 *   -SCALE 1 \
 *   -DIR $SPARK_TPCDS_DATA_TEXT
 * }}}
 */
abstract class TPCDSDataBenchmark extends BenchmarkBase {

  val N = 4

  var data: Array[Byte] = _

  protected def prepareData(): Unit = {
    val tpcDsDataDir = sys.env.get("SPARK_TPCDS_DATA_TEXT")
    assert(tpcDsDataDir.nonEmpty, "Can not find env var SPARK_TPCDS_DATA_TEXT")

    val catalogSalesDatPath = Paths.get(tpcDsDataDir.get, "catalog_sales.dat")
    assert(Files.exists(catalogSalesDatPath), s"File $catalogSalesDatPath does not exists, " +
      s"please follow instruction to generate the TPC-DS (SF1) text data first.")

    // the size of TPCDS catalog_sales.dat (SF1) is about 283M
    data = Files.readAllBytes(catalogSalesDatPath)
  }

  override def afterAll(): Unit = {
    data = null
  }
}
