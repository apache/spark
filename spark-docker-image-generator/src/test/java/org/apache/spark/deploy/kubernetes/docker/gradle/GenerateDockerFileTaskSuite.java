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

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.util.List;
import java.util.stream.Collectors;
import java.util.zip.ZipEntry;
import java.util.zip.ZipInputStream;
import org.apache.commons.io.IOUtils;
import org.assertj.core.api.Assertions;
import org.gradle.api.provider.Property;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.mockito.Mockito;

public final class GenerateDockerFileTaskSuite {
    @Rule
    public TemporaryFolder tempFolder = new TemporaryFolder();

    private File srcDockerFile;
    private File destDockerFile;

    @Before
    public void before() throws IOException {
        File dockerFileDir = tempFolder.newFolder("docker");
        destDockerFile = new File(dockerFileDir, "Dockerfile");
        srcDockerFile = tempFolder.newFile("Dockerfile.original");
        File dockerResourcesFile = new File(System.getProperty("docker-resources-zip-path"));

        try (InputStream originalDockerBundleZipped = new FileInputStream(dockerResourcesFile);
                ZipInputStream unzipped = new ZipInputStream(originalDockerBundleZipped);
                FileOutputStream srcDockerFileStream = new FileOutputStream(srcDockerFile)) {
            ZipEntry currentEntry = unzipped.getNextEntry();
            boolean foundDockerFile = false;
            while (currentEntry != null && !foundDockerFile) {
                if (currentEntry.getName().equals("kubernetes/dockerfiles/spark/Dockerfile.original")) {
                    IOUtils.copy(unzipped, srcDockerFileStream);
                    foundDockerFile = true;
                } else {
                    currentEntry = unzipped.getNextEntry();
                }
            }
            if (!foundDockerFile) {
                throw new IllegalStateException("Dockerfile not found.");
            }
        }
    }

    @Test
    public void testGenerateDockerFile() throws IOException {
        GenerateDockerFileTask task = Mockito.mock(GenerateDockerFileTask.class);
        task.setDestDockerFile(destDockerFile);
        task.setSrcDockerFile(srcDockerFile);
        Property<String> baseImageProperty = Mockito.mock(Property.class);
        Mockito.when(baseImageProperty.isPresent()).thenReturn(true);
        Mockito.when(baseImageProperty.get()).thenReturn("fabric8/java-centos-openjdk8-jdk:latest");
        task.setBaseImage(baseImageProperty);
        task.generateDockerFile();
        Assertions.assertThat(destDockerFile).isFile();
        List<String> writtenLines = Files.readAllLines(
                destDockerFile.toPath(), StandardCharsets.UTF_8);
        try (InputStream expectedDockerFileInput = getClass().getResourceAsStream("/ExpectedDockerfile");
                InputStreamReader expectedDockerFileReader =
                        new InputStreamReader(expectedDockerFileInput, StandardCharsets.UTF_8);
                BufferedReader expectedDockerFileBuffered =
                        new BufferedReader(expectedDockerFileReader)) {
            List<String> expectedFileLines = expectedDockerFileBuffered
                    .lines()
                    .collect(Collectors.toList());
            Assertions.assertThat(writtenLines).isEqualTo(expectedFileLines);
        }
    }

}
