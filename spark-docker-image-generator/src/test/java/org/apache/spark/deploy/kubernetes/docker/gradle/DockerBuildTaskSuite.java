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

import java.io.File;
import java.io.IOException;
import org.gradle.api.Project;
import org.gradle.api.provider.Property;
import org.gradle.process.ExecSpec;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.runners.MockitoJUnitRunner;

@RunWith(MockitoJUnitRunner.class)
public final class DockerBuildTaskSuite {

    private static final String IMAGE_NAME = "spark-test";

    @Rule
    public TemporaryFolder tempFolder = new TemporaryFolder();

    private File dockerFile;

    private File dockerBuildDirectory;

    @Mock
    private Project project;

    @Mock
    private ExecSpec execSpec;

    @Mock
    private DockerBuildTask taskUnderTest;

    @Mock
    private Property<String> imageName;

    @Before
    public void before() throws IOException {
        ProjectExecUtils.invokeExecSpecAction(project, execSpec);
        dockerFile = tempFolder.newFile("Dockerfile");
        dockerBuildDirectory = tempFolder.newFolder("docker-build-dir");
        taskUnderTest.setDockerFile(dockerFile);
        taskUnderTest.setDockerBuildDirectory(dockerBuildDirectory);
        taskUnderTest.setImageName(imageName);
        Mockito.when(imageName.get()).thenReturn(IMAGE_NAME);
        Mockito.when(taskUnderTest.getProject()).thenReturn(project);
    }

    @Test
    public void testExec_setsCommandLineToBuildImage() {
        taskUnderTest.exec();
        Mockito.verify(execSpec).commandLine(
                "docker",
                "build",
                "-f",
                dockerFile.getAbsolutePath(),
                "-t",
                IMAGE_NAME,
                dockerBuildDirectory.getAbsolutePath());
    }

}
