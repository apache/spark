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

    public String getDb() {
        return db;
    }

    public ExpressionInfo(String className, String db, String name, String usage, String extended) {
        this.className = className;
        this.db = db;
        this.name = name;
        this.usage = usage;
        this.extended = extended;
    }

    public ExpressionInfo(String className, String name) {
        this(className, null, name, null, null);
    }

    public ExpressionInfo(String className, String db, String name) {
        this(className, db, name, null, null);
    }
}
