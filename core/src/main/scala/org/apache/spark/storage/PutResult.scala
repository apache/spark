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

package org.apache.spark.storage

import java.nio.ByteBuffer

/**
 * Result of adding a block into a BlockStore. This case class contains a few things:
 *   (1) The estimated size of the put,
 *   (2) The values put if the caller asked for them to be returned (e.g. for chaining
 *       replication), and
 *   (3) A list of blocks dropped as a result of this put. This is always empty for DiskStore.
 */
private[spark] case class PutResult(
    size: Long,
    data: Either[Iterator[_], ByteBuffer],
    droppedBlocks: Seq[(BlockId, BlockStatus)] = Seq.empty)
