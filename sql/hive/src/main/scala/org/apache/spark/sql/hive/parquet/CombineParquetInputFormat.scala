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

package org.apache.spark.sql.hive.parquet

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat
import org.apache.hadoop.io.ArrayWritable
import org.apache.hadoop.mapred.lib.{CombineFileInputFormat, CombineFileRecordReader, CombineFileRecordReaderWrapper, CombineFileSplit}
import org.apache.hadoop.mapred.{InputSplit, JobConf, RecordReader, Reporter}

/**
 * Input format that is a <code>CombineFileInputFormat</code>-equivalent for
 * <code>MapredParquetInputFormat</code>.
 *
 * @see CombineFileInputFormat
 */
class CombineParquetInputFormat extends CombineFileInputFormat[Void, ArrayWritable] {
  override def getRecordReader(
      split: InputSplit,
      job: JobConf,
      reporter: Reporter): RecordReader[Void, ArrayWritable] = {
    new CombineFileRecordReader[Void, ArrayWritable](
      job,
      split.asInstanceOf[CombineFileSplit],
      reporter,
      classOf[ParquetRecordReaderWrapper].asInstanceOf[Class[RecordReader[Void, ArrayWritable]]])
  }
}

/**
 * A record reader that may be passed to <code>CombineFileRecordReader</code>
 * so that it can be used in a <code>CombineFileInputFormat</code>-equivalent
 * for <code>MapredParquetInputFormat</code>.
 *
 * @see CombineFileRecordReader
 * @see CombineFileInputFormat
 * @see MapredParquetInputFormat
 */
private class ParquetRecordReaderWrapper(
    split: CombineFileSplit,
    conf: Configuration,
    reporter: Reporter,
    idx: Integer)
  extends CombineFileRecordReaderWrapper[Void, ArrayWritable](
    new MapredParquetInputFormat(), split, conf, reporter, idx)

