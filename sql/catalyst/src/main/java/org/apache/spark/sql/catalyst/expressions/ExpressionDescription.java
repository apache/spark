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
 *
 * A function description type which can be recognized by FunctionRegistry, and will be used to
 * show the usage of the function in human language.
 *
 * `usage()` will be used for the function usage in brief way.
 *
 * These below are concatenated and used for the function usage in verbose way, suppose arguments,
 * examples, note, group, source, since and deprecated will be provided.
 *
 * `arguments()` describes arguments for the expression.
 *
 * `examples()` describes examples for the expression.
 *
 * `note()` contains some notes for the expression optionally.
 *
 * `group()` describes the category that the expression belongs to. The valid value is
 * "agg_funcs", "array_funcs", "datetime_funcs", "json_funcs", "map_funcs" and "window_funcs".
 *
 * `source()` describe the source of the function. The valid value is "built-in", "hive",
 * "python_udf", "scala_udf", "java_udf".
 *
 * `since()` contains version information for the expression. Version is specified by,
 * for example, "2.2.0".
 *
 * `deprecated()` contains deprecation information for the expression optionally, for example,
 * "Deprecated since 2.2.0. Use something else instead".
 *
 * The format, in particular for `arguments()`, `examples()`,`note()`, `group()`, `source()`,
 * `since()` and `deprecated()`,  should strictly be as follows.
 *
 * <pre>
 * <code>@ExpressionDescription(
 *   ...
 *   arguments = """
 *     Arguments:
 *       * arg0 - ...
 *           ....
 *       * arg1 - ...
 *           ....
 *   """,
 *   examples = """
 *     Examples:
 *       > SELECT ...;
 *        ...
 *       > SELECT ...;
 *        ...
 *   """,
 *   note = """
 *     ...
 *   """,
 *   group = "agg_funcs",
 *   source = "built-in",
 *   since = "3.0.0",
 *   deprecated = """
 *     ...
 *   """)
 * </code>
 * </pre>
 *
 *  We can refer the function name by `_FUNC_`, in `usage()`, `arguments()`, `examples()` and
 *  `note()` as it is registered in `FunctionRegistry`.
 *
 *  Note that, if `extended()` is defined, `arguments()`, `examples()`, `note()`, `group()`,
 *  `since()` and `deprecated()` should be not defined together. `extended()` exists
 *  for backward compatibility.
 *
 *  Note this contents are used in the SparkSQL documentation for built-in functions. The contents
 *  here are considered as a Markdown text and then rendered.
 */
@DeveloperApi
@Retention(RetentionPolicy.RUNTIME)
public @interface ExpressionDescription {
    String usage() default "";
    /**
     * @deprecated This field is deprecated as of Spark 3.0. Use {@link #arguments},
     *   {@link #examples}, {@link #note}, {@link #since} and {@link #deprecated} instead
     *   to document the extended usage.
     */
    @Deprecated(since = "3.0.0")
    String extended() default "";
    String arguments() default "";
    String examples() default "";
    String note() default "";
    /**
     * Valid group names are almost the same with one defined as `groupname` in
     * `sql/functions.scala`. But, `collection_funcs` is split into fine-grained three groups:
     * `array_funcs`, `map_funcs`, and `json_funcs`. See `ExpressionInfo` for the
     * detailed group names.
     */
    String group() default "";
    String since() default "";
    String deprecated() default "";
    String source() default "built-in";
}
