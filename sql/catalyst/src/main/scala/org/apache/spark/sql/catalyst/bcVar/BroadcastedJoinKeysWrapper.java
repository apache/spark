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
package org.apache.spark.sql.catalyst.bcVar;

import java.io.Externalizable;

import java.util.Set;
import org.apache.spark.sql.types.DataType;

public interface BroadcastedJoinKeysWrapper extends Externalizable {
  DataType getSingleKeyDataType();
  ArrayWrapper getKeysArray();
  long getBroadcastVarId();

   int getRelativeKeyIndex();

   int getTupleLength();

   int getTotalJoinKeys();

   void invalidateSelf();

   Set<Object> getKeysAsSet();

   // set via system properties
   String CACHED_KEYS_EXPIRY_IN_SECONDS_KEY = "spark.bhj.cachedKeys.expiry";
   String CACHED_KEYS_EXPIRY_DEFAULT = "90";  //seconds

   String CACHE_SIZE_KEY = "spark.bhj.cachedKeys.size";
   String CACHE_SIZE_DEFAULT = "30";

   int CACHE_SIZE =  Integer.parseInt(System.getProperty(CACHE_SIZE_KEY, CACHE_SIZE_DEFAULT));
   long CACHE_EXPIRY = Long.parseLong(System.getProperty(CACHED_KEYS_EXPIRY_IN_SECONDS_KEY,
       CACHED_KEYS_EXPIRY_DEFAULT));
}
