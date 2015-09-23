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

package org.apache.spark.sql.catalyst.expressions;

import org.apache.spark.annotation.DeveloperApi;

import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;

/**
 * ::DeveloperApi::

 * A function description type which can be recognized by FunctionRegistry, and will be used to
 * show the usage of the function in human language.
 *
 * `usage()` will be used for the function usage in brief way.
 * `extended()` will be used for the function usage in verbose way, suppose
 *              an example will be provided.
 *
 *  And we can refer the function name by `_FUNC_`, in `usage` and `extended`, as it's
 *  registered in `FunctionRegistry`.
 */
@DeveloperApi
@Retention(RetentionPolicy.RUNTIME)
public @interface ExpressionDescription {
    String usage() default "_FUNC_ is undocumented";
    String extended() default "No example for _FUNC_.";
}
