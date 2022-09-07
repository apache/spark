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

import com.google.common.annotations.VisibleForTesting;

import java.util.Arrays;
import java.util.HashSet;
import java.util.Set;

/**
 * Expression information, will be used to describe a expression.
 */
public class ExpressionInfo {
    private String className;
    private String usage;
    private String name;
    private String extended;
    private String db;
    private String arguments;
    private String examples;
    private String note;
    private String group;
    private String since;
    private String deprecated;
    private String source;

    private static final Set<String> validGroups =
        new HashSet<>(Arrays.asList("agg_funcs", "array_funcs", "binary_funcs", "bitwise_funcs",
            "collection_funcs", "predicate_funcs", "conditional_funcs", "conversion_funcs",
            "csv_funcs", "datetime_funcs", "generator_funcs", "hash_funcs", "json_funcs",
            "lambda_funcs", "map_funcs", "math_funcs", "misc_funcs", "string_funcs", "struct_funcs",
            "window_funcs", "xml_funcs", "table_funcs", "url_funcs"));

    private static final Set<String> validSources =
            new HashSet<>(Arrays.asList("built-in", "hive", "python_udf", "scala_udf", "java_udf"));

    public String getClassName() {
        return className;
    }

    public String getUsage() {
        return replaceFunctionName(usage);
    }

    public String getName() {
        return name;
    }

    public String getExtended() {
        return replaceFunctionName(extended);
    }

    public String getSince() {
        return since;
    }

    public String getArguments() {
        return arguments;
    }

    @VisibleForTesting
    public String getOriginalExamples() {
        return examples;
    }

    public String getExamples() {
        return replaceFunctionName(examples);
    }

    public String getNote() {
        return note;
    }

    public String getDeprecated() {
        return deprecated;
    }

    public String getGroup() {
        return group;
    }

    public String getDb() {
        return db;
    }

    public String getSource() {
        return source;
    }

    public ExpressionInfo(
            String className,
            String db,
            String name,
            String usage,
            String arguments,
            String examples,
            String note,
            String group,
            String since,
            String deprecated,
            String source) {
        assert name != null;
        assert arguments != null;
        assert examples != null;
        assert examples.isEmpty() || examples.contains("    Examples:");
        assert note != null;
        assert group != null;
        assert since != null;
        assert deprecated != null;
        assert source != null;

        this.className = className;
        this.db = db;
        this.name = name;
        this.usage = usage;
        this.arguments = arguments;
        this.examples = examples;
        this.note = note;
        this.group = group;
        this.since = since;
        this.deprecated = deprecated;
        this.source = source;

        // Make the extended description.
        this.extended = arguments + examples;
        if (this.extended.isEmpty()) {
            this.extended = "\n    No example/argument for _FUNC_.\n";
        }
        if (!note.isEmpty()) {
            if (!note.contains("    ") || !note.endsWith("  ")) {
                throw new IllegalArgumentException("'note' is malformed in the expression [" +
                    this.name + "]. It should start with a newline and 4 leading spaces; end " +
                    "with a newline and two spaces; however, got [" + note + "].");
            }
            this.extended += "\n    Note:\n      " + note.trim() + "\n";
        }
        if (!group.isEmpty() && !validGroups.contains(group)) {
            throw new IllegalArgumentException("'group' is malformed in the expression [" +
                this.name + "]. It should be a value in " + validGroups + "; however, " +
                "got [" + group + "].");
        }
        if (!source.isEmpty() && !validSources.contains(source)) {
            throw new IllegalArgumentException("'source' is malformed in the expression [" +
                    this.name + "]. It should be a value in " + validSources + "; however, " +
                    "got [" + source + "].");
        }
        if (!since.isEmpty()) {
            if (Integer.parseInt(since.split("\\.")[0]) < 0) {
                throw new IllegalArgumentException("'since' is malformed in the expression [" +
                    this.name + "]. It should not start with a negative number; however, " +
                    "got [" + since + "].");
            }
            this.extended += "\n    Since: " + since + "\n";
        }
        if (!deprecated.isEmpty()) {
            if (!deprecated.contains("    ") || !deprecated.endsWith("  ")) {
                throw new IllegalArgumentException("'deprecated' is malformed in the " +
                    "expression [" + this.name + "]. It should start with a newline and 4 " +
                    "leading spaces; end with a newline and two spaces; however, got [" +
                    deprecated + "].");
            }
            this.extended += "\n    Deprecated:\n      " + deprecated.trim() + "\n";
        }
    }

    public ExpressionInfo(String className, String name) {
        this(className, null, name, null, "", "", "", "", "", "", "");
    }

    public ExpressionInfo(String className, String db, String name) {
        this(className, db, name, null, "", "", "", "", "", "", "");
    }

    /**
     * @deprecated This constructor is deprecated as of Spark 3.0. Use other constructors to fully
     *   specify each argument for extended usage.
     */
    @Deprecated
    public ExpressionInfo(String className, String db, String name, String usage, String extended) {
        // `arguments` and `examples` are concatenated for the extended description. So, here
        // simply pass the `extended` as `arguments` and an empty string for `examples`.
        this(className, db, name, usage, extended, "", "", "", "", "", "");
    }

    private String replaceFunctionName(String usage) {
        if (usage == null) {
            return "N/A.";
        } else {
            return usage.replaceAll("_FUNC_", name);
        }
    }
}
