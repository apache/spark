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
package org.apache.spark.deploy.kubernetes.docker.gradle;

import java.util.List;
import java.util.stream.Collectors;
import org.gradle.api.DefaultTask;
import org.gradle.api.provider.Property;
import org.gradle.api.tasks.Input;
import org.gradle.api.tasks.TaskAction;

public class LazyExecTask extends DefaultTask {

    private List<Property<String>> commandLine;

    @Input
    public final List<Property<String>> getCommandLine() {
        return commandLine;
    }

    public final void setCommandLine(List<Property<String>> commandLine) {
        this.commandLine = commandLine;
    }

    @TaskAction
    public final void runCommand() {
        getProject().exec(exec ->
                exec.commandLine(commandLine.stream()
                        .map(Property::get)
                        .collect(Collectors.toList())));
    }
}
