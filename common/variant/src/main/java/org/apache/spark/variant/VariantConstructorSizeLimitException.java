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

package org.apache.spark.variant;

import scala.collection.immutable.Map$;

import org.apache.spark.QueryContext;
import org.apache.spark.SparkRuntimeException;

/**
 * An exception indicating that an external caller tried to call the Variant constructor with value
 * or metadata exceeding the 16MiB size limit. We will never construct a Variant this large, so it
 * should only be possible to encounter this exception when reading a Variant produced by another
 * tool.
 */
public class VariantConstructorSizeLimitException extends SparkRuntimeException {
  public VariantConstructorSizeLimitException() {
    super("VARIANT_CONSTRUCTOR_SIZE_LIMIT",
        Map$.MODULE$.<String, String>empty(), null, new QueryContext[]{}, "");
  }
}
