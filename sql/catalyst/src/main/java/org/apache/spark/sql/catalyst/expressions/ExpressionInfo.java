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
    private String since;

    public String getClassName() {
        return className;
    }

    public String getUsage() {
        return usage;
    }

    public String getName() {
        return name;
    }

    public String getExtended() {
        return extended;
    }

    public String getSince() {
        return since;
    }

    public String getArguments() {
        return arguments;
    }

    public String getExamples() {
        return examples;
    }

    public String getNote() {
        return note;
    }

    public String getDb() {
        return db;
    }

    public ExpressionInfo(
            String className,
            String db,
            String name,
            String usage,
            String arguments,
            String examples,
            String note,
            String since) {
        assert name != null;
        assert arguments != null;
        assert arguments.isEmpty() || arguments.startsWith("\n    Arguments:");
        assert examples != null;
        assert examples.isEmpty() || examples.startsWith("\n    Examples:");
        assert note != null;
        assert since != null;

        this.className = className;
        this.db = db;
        this.name = name;
        this.arguments = arguments;
        this.examples = examples;
        this.note = note;
        this.since = since;
        if (usage == null || usage.isEmpty()) {
            this.usage = "\n    _FUNC_ is undocumented.";
        } else {
            this.usage = usage;
        }

        // Make extended description.
        this.extended = "";
        this.extended += arguments;
        this.extended += examples;
        if (arguments.isEmpty() && examples.isEmpty()) {
           this.extended += "\n    No example/argument for _FUNC_.\n";
        }
        if (!arguments.isEmpty()) {
            this.extended += "\n    Note:\n      " + note.trim() + "\n";
        }
        if (!since.isEmpty()) {
            this.extended += "\n    Since: " + since + "\n";
        }
    }

    public ExpressionInfo(String className, String name) {
        this(className, null, name, null, "", "", "", "");
    }

    public ExpressionInfo(String className, String db, String name) {
        this(className, db, name, null, "", "", "", "");
    }
}
