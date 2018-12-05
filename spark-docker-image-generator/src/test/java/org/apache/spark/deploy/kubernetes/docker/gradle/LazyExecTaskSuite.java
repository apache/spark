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

import java.util.ArrayList;
import java.util.List;
import org.gradle.api.Project;
import org.gradle.api.provider.Property;
import org.gradle.process.ExecSpec;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.runners.MockitoJUnitRunner;

@RunWith(MockitoJUnitRunner.class)
public final class LazyExecTaskSuite {

    @Mock
    private org.apache.spark.deploy.kubernetes.docker.gradle.LazyExecTask taskUnderTest;

    @Mock
    private Project project;

    @Mock
    private ExecSpec execSpec;

    @Before
    public void before() {
        org.apache.spark.deploy.kubernetes.docker.gradle.ProjectExecUtils.invokeExecSpecAction(project, execSpec);
        Mockito.when(taskUnderTest.getProject()).thenReturn(project);
    }

    @Test
    public void testRunCommand_setsCommandLineOnExecSpec() {
        List<Property<String>> command = new ArrayList<>();
        command.add(constProperty("ls"));
        command.add(constProperty("-lahrt"));
        command.add(constProperty("git"));
        List<String> expectedCommand = new ArrayList<>();
        expectedCommand.add("ls");
        expectedCommand.add("-lahrt");
        expectedCommand.add("git");
        taskUnderTest.setCommandLine(command);
        taskUnderTest.runCommand();
        Mockito.verify(execSpec).commandLine(expectedCommand);
    }

    private <T> Property<T> constProperty(T value) {
        Property<T> property = (Property<T>) Mockito.mock(Property.class);
        Mockito.when(property.get()).thenReturn(value);
        return property;
    }
}
