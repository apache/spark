/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p/>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p/>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.spark.sql.thriftserver.cli.utils;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.conf.SystemVariables;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;

public class VariableSubstitution extends SystemVariables {
    private static final Logger l4j = LoggerFactory.getLogger(VariableSubstitution.class);

    private final Map<String, String> hiveVariableSource;

    public VariableSubstitution(Map<String, String> hiveVariableSource) {
        this.hiveVariableSource = hiveVariableSource;
    }

    /**
     * The super method will handle with the case of substitutions for system variables,
     * hive conf variables and env variables. In this method, it will retrieve the hive
     * variables using hiveVariableSource.
     *
     * @param conf
     * @param var
     * @return
     */
    @Override
    protected String getSubstitute(Configuration conf, String var) {
        String val = super.getSubstitute(conf, var);
        if (val == null && hiveVariableSource != null) {
            if (var.startsWith(HIVEVAR_PREFIX)) {
                val = hiveVariableSource.get(var.substring(HIVEVAR_PREFIX.length()));
            } else {
                val = hiveVariableSource.get(var);
            }
        }
        return val;
    }

    public String substitute(HiveConf conf, String expr) {
        if (expr == null) {
            return expr;
        }
        if (HiveConf.getBoolVar(conf, HiveConf.ConfVars.HIVEVARIABLESUBSTITUTE)) {
            l4j.debug("Substitution is on: " + expr);
        } else {
            return expr;
        }
        int depth = HiveConf.getIntVar(conf, HiveConf.ConfVars.HIVEVARIABLESUBSTITUTEDEPTH);
        return substitute(conf, expr, depth);
    }
}
