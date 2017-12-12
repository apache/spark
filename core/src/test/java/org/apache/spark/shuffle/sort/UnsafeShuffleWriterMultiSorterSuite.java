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

package org.apache.spark.shuffle.sort;

import org.junit.Test;

import java.io.IOException;

import static org.junit.Assert.assertEquals;

public class UnsafeShuffleWriterMultiSorterSuite extends UnsafeShuffleWriterBaseSuite {
  @Override
  protected UnsafeShuffleWriter<Object, Object> createWriter(
    boolean transferToEnabled) throws IOException {
    conf.set("spark.file.transferTo", String.valueOf(transferToEnabled));
    conf.set("spark.shuffle.async.num.sorter", "2");
    return new UnsafeShuffleWriter<>(
      blockManager,
      shuffleBlockResolver,
      taskMemoryManager,
      new SerializedShuffleHandle<>(0, 1, shuffleDep),
      0, // map id
      taskContext,
      conf
    );
  }

  @Test
  public void writeEnoughRecordsToTriggerSortBufferExpansionAndSpillRadixOff() throws Exception {
    conf.set("spark.shuffle.sort.useRadixSort", "false");
    writeEnoughRecordsToTriggerSortBufferExpansionAndSpill(
      (UnsafeShuffleWriter.DEFAULT_INITIAL_SORT_BUFFER_SIZE * 3) / 2);
    assertEquals(2, spillFilesCreated.size());
  }

  @Test
  public void writeEnoughRecordsToTriggerSortBufferExpansionAndSpillRadixOn() throws Exception {
    conf.set("spark.shuffle.sort.useRadixSort", "true");
    writeEnoughRecordsToTriggerSortBufferExpansionAndSpill(
      (UnsafeShuffleWriter.DEFAULT_INITIAL_SORT_BUFFER_SIZE * 3) / 2);
    assertEquals(3, spillFilesCreated.size());
  }

}
