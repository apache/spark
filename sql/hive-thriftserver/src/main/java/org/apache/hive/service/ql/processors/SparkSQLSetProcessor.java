/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hive.service.ql.processors;

import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.ql.metadata.Hive;
import org.apache.hadoop.hive.ql.parse.VariableSubstitution;
import org.apache.hadoop.hive.ql.processors.SetProcessor;
import org.apache.hadoop.hive.ql.session.SessionState;

public class SparkSQLSetProcessor extends SetProcessor {

    public static int setVariable(String varname, String varvalue) throws Exception {
        SessionState ss = SessionState.get();
        if(varvalue.contains("\n")) {
            ss.err.println("Warning: Value had a \\n character in it.");
        }

        varname = varname.trim();
        if(varname.startsWith("env:")) {
            ss.err.println("env:* variables can not be set.");
            return 1;
        } else {
            String propName;
            if(varname.startsWith("system:")) {
                propName = varname.substring("system:".length());
                System.getProperties().setProperty(propName,
                        (new VariableSubstitution()).substitute(ss.getConf(), varvalue));
            } else if(varname.startsWith("hiveconf:")) {
                propName = varname.substring("hiveconf:".length());
                setConf(varname, propName, varvalue, true);
            } else if(varname.startsWith("hivevar:")) {
                propName = varname.substring("hivevar:".length());
                ss.getHiveVariables().put(propName,
                        (new VariableSubstitution()).substitute(ss.getConf(), varvalue));
            } else if(varname.startsWith("metaconf:")) {
                propName = varname.substring("metaconf:".length());
                Hive hive = Hive.get(ss.getConf());
                hive.setMetaConf(propName,
                        (new VariableSubstitution()).substitute(ss.getConf(), varvalue));
            } else {
                setConf(varname, varname, varvalue, true);
            }

            return 0;
        }
    }

    private static void setConf(String varname, String key, String varvalue, boolean register)
            throws IllegalArgumentException {
        HiveConf conf = SessionState.get().getConf();
        String value = (new VariableSubstitution()).substitute(conf, varvalue);
        if(conf.getBoolVar(HiveConf.ConfVars.HIVECONFVALIDATION)) {
            HiveConf.ConfVars confVars = HiveConf.getConfVars(key);
            if(confVars != null) {
                if(!confVars.isType(value)) {
                    StringBuilder fail1 = new StringBuilder();
                    fail1.append("\'SET ").append(varname).append('=').append(varvalue);
                    fail1.append("\' FAILED because ").append(key).append(" expects ");
                    fail1.append(confVars.typeString()).append(" type value.");
                    throw new IllegalArgumentException(fail1.toString());
                }

                String fail = confVars.validate(value);
                if(fail != null) {
                    StringBuilder message = new StringBuilder();
                    message.append("\'SET ").append(varname).append('=').append(varvalue);
                    message.append("\' FAILED in validation : ").append(fail).append('.');
                    throw new IllegalArgumentException(message.toString());
                }
            } else if(key.startsWith("hive.")) {
                throw new IllegalArgumentException("hive configuration " + key +
                        " does not exists.");
            }
        }

        conf.verifyAndSet(key, value);
        if(register) {
            SessionState.get().getOverriddenConfigurations().put(key, value);
        }

    }
}
