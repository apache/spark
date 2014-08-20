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

package org.apache.spark.network.netty.server

/**
 * Header describing a block. This is used only in the server pipeline.
 *
 * [[BlockServerHandler]] creates this, and [[BlockHeaderEncoder]] encodes it.
 *
 * @param blockSize length of the block content, excluding the length itself.
 *                 If positive, this is the header for a block (not part of the header).
 *                 If negative, this is the header and content for an error message.
 * @param blockId block id
 * @param error some error message from reading the block
 */
private[server]
class BlockHeader(val blockSize: Int, val blockId: String, val error: Option[String] = None)
